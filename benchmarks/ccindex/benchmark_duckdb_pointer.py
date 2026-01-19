#!/usr/bin/env python3
"""
Benchmark the DuckDB pointer index search performance.
Tests search speed, I/O efficiency, and scalability.
"""

import sys
import argparse
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
import time
import random
import statistics


class DuckDBPointerBenchmark:
    def __init__(self, db_path, parquet_dir):
        self.db_path = Path(db_path)
        self.parquet_dir = Path(parquet_dir)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Parquet directory not found: {self.parquet_dir}")
    
    def get_index_stats(self):
        """Get statistics about the pointer index"""
        conn = duckdb.connect(str(self.db_path), read_only=True)
        
        stats = {}
        
        # Total domains
        stats['total_domains'] = conn.execute(
            "SELECT COUNT(DISTINCT domain) FROM domain_pointers"
        ).fetchone()[0]
        
        # Total pointers
        stats['total_pointers'] = conn.execute(
            "SELECT COUNT(*) FROM domain_pointers"
        ).fetchone()[0]
        
        # Total URLs
        stats['total_urls'] = conn.execute(
            "SELECT SUM(row_count) FROM domain_pointers"
        ).fetchone()[0]
        
        # Parquet files
        stats['parquet_files'] = conn.execute(
            "SELECT COUNT(DISTINCT parquet_file) FROM domain_pointers"
        ).fetchone()[0]
        
        # Domain URL count distribution
        url_counts = conn.execute("""
            SELECT SUM(row_count) as url_count
            FROM domain_pointers
            GROUP BY domain
        """).fetchall()
        
        url_counts = [row[0] for row in url_counts]
        stats['avg_urls_per_domain'] = statistics.mean(url_counts)
        stats['median_urls_per_domain'] = statistics.median(url_counts)
        stats['min_urls_per_domain'] = min(url_counts)
        stats['max_urls_per_domain'] = max(url_counts)
        
        conn.close()
        
        return stats
    
    def get_sample_domains(self, count, strategy='random'):
        """
        Get sample domains for benchmarking.
        
        Strategies:
        - random: Random selection
        - small: Domains with few URLs
        - medium: Domains with medium URL count
        - large: Domains with many URLs
        """
        conn = duckdb.connect(str(self.db_path), read_only=True)
        
        if strategy == 'random':
            query = """
                SELECT domain, SUM(row_count) as url_count
                FROM domain_pointers
                GROUP BY domain
                ORDER BY RANDOM()
                LIMIT ?
            """
        elif strategy == 'small':
            query = """
                SELECT domain, SUM(row_count) as url_count
                FROM domain_pointers
                GROUP BY domain
                ORDER BY url_count ASC
                LIMIT ?
            """
        elif strategy == 'medium':
            query = """
                SELECT domain, SUM(row_count) as url_count
                FROM domain_pointers
                GROUP BY domain
                ORDER BY ABS(url_count - (SELECT AVG(SUM(row_count)) FROM domain_pointers GROUP BY domain))
                LIMIT ?
            """
        elif strategy == 'large':
            query = """
                SELECT domain, SUM(row_count) as url_count
                FROM domain_pointers
                GROUP BY domain
                ORDER BY url_count DESC
                LIMIT ?
            """
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        results = conn.execute(query, [count]).fetchall()
        conn.close()
        
        return [(domain, url_count) for domain, url_count in results]
    
    def benchmark_pointer_lookup(self, domains, verbose=False):
        """Benchmark pointer lookup speed (without data retrieval)"""
        conn = duckdb.connect(str(self.db_path), read_only=True)
        
        times = []
        
        for domain, expected_count in domains:
            start = time.time()
            
            result = conn.execute("""
                SELECT COUNT(*), SUM(row_count)
                FROM domain_pointers
                WHERE domain = ?
            """, [domain]).fetchone()
            
            elapsed = time.time() - start
            times.append(elapsed)
            
            file_count, url_count = result
            
            if verbose:
                print(f"  {domain}: {elapsed*1000:.2f}ms ({url_count} URLs in {file_count} files)")
        
        conn.close()
        
        return {
            'mean': statistics.mean(times) * 1000,  # ms
            'median': statistics.median(times) * 1000,
            'min': min(times) * 1000,
            'max': max(times) * 1000,
            'total': sum(times)
        }
    
    def benchmark_full_retrieval(self, domains, verbose=False):
        """Benchmark full URL retrieval using pointers"""
        conn = duckdb.connect(str(self.db_path), read_only=True)
        
        times = []
        throughputs = []
        
        for domain, expected_count in domains:
            start = time.time()
            
            # Get pointers
            pointers = conn.execute("""
                SELECT parquet_file, row_offset, row_count
                FROM domain_pointers
                WHERE domain = ?
            """, [domain]).fetchall()
            
            # Retrieve data
            total_urls = 0
            for parquet_file, offset, count in pointers:
                parquet_path = self.parquet_dir / parquet_file
                if parquet_path.exists():
                    table = pq.read_table(parquet_path)
                    subset = table.slice(offset, count)
                    total_urls += len(subset)
            
            elapsed = time.time() - start
            times.append(elapsed)
            
            if elapsed > 0:
                throughputs.append(total_urls / elapsed)
            
            if verbose:
                print(f"  {domain}: {elapsed*1000:.2f}ms ({total_urls} URLs, {total_urls/elapsed:.0f} URLs/sec)")
        
        conn.close()
        
        return {
            'mean_time': statistics.mean(times) * 1000,  # ms
            'median_time': statistics.median(times) * 1000,
            'min_time': min(times) * 1000,
            'max_time': max(times) * 1000,
            'mean_throughput': statistics.mean(throughputs) if throughputs else 0,
            'median_throughput': statistics.median(throughputs) if throughputs else 0
        }
    
    def benchmark_concurrent_lookups(self, num_lookups=100):
        """Benchmark many concurrent lookups"""
        domains = self.get_sample_domains(num_lookups, strategy='random')
        
        print(f"\nBenchmark: {num_lookups} concurrent lookups")
        
        start = time.time()
        stats = self.benchmark_pointer_lookup(domains, verbose=False)
        total_time = time.time() - start
        
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average per lookup: {stats['mean']:.2f}ms")
        print(f"  Median: {stats['median']:.2f}ms")
        print(f"  Min: {stats['min']:.2f}ms")
        print(f"  Max: {stats['max']:.2f}ms")
        print(f"  Throughput: {num_lookups/total_time:.0f} lookups/sec")
        
        return stats
    
    def run_full_benchmark(self, sample_size=20):
        """Run comprehensive benchmark suite"""
        print("="*70)
        print("DUCKDB POINTER INDEX BENCHMARK")
        print("="*70)
        
        # Get index stats
        print("\n1. Index Statistics")
        print("-"*70)
        stats = self.get_index_stats()
        print(f"Total domains: {stats['total_domains']:,}")
        print(f"Total pointers: {stats['total_pointers']:,}")
        print(f"Total URLs: {stats['total_urls']:,}")
        print(f"Parquet files: {stats['parquet_files']:,}")
        print(f"Average URLs per domain: {stats['avg_urls_per_domain']:.1f}")
        print(f"Median URLs per domain: {stats['median_urls_per_domain']:.1f}")
        print(f"URL range: {stats['min_urls_per_domain']:,} - {stats['max_urls_per_domain']:,}")
        
        # Benchmark pointer lookups
        print("\n2. Pointer Lookup Performance")
        print("-"*70)
        
        for strategy in ['small', 'medium', 'large']:
            domains = self.get_sample_domains(sample_size, strategy=strategy)
            print(f"\n  {strategy.upper()} domains ({sample_size} samples):")
            result = self.benchmark_pointer_lookup(domains, verbose=False)
            print(f"    Mean: {result['mean']:.2f}ms")
            print(f"    Median: {result['median']:.2f}ms")
            print(f"    Range: {result['min']:.2f}ms - {result['max']:.2f}ms")
        
        # Benchmark full retrieval
        print("\n3. Full URL Retrieval Performance")
        print("-"*70)
        
        for strategy in ['small', 'medium', 'large']:
            domains = self.get_sample_domains(min(sample_size, 10), strategy=strategy)
            print(f"\n  {strategy.upper()} domains ({len(domains)} samples):")
            result = self.benchmark_full_retrieval(domains, verbose=False)
            print(f"    Mean time: {result['mean_time']:.2f}ms")
            print(f"    Median time: {result['median_time']:.2f}ms")
            print(f"    Mean throughput: {result['mean_throughput']:.0f} URLs/sec")
            print(f"    Median throughput: {result['median_throughput']:.0f} URLs/sec")
        
        # Benchmark concurrent access
        print("\n4. Concurrent Access Performance")
        print("-"*70)
        self.benchmark_concurrent_lookups(100)
        
        print("\n" + "="*70)
        print("BENCHMARK COMPLETE")
        print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark DuckDB pointer index performance"
    )
    
    parser.add_argument('--db', default='/storage/ccindex_duckdb/domain_pointer.duckdb',
                       help='Path to DuckDB pointer database')
    parser.add_argument('--parquet-dir', default='/storage/ccindex_parquet',
                       help='Directory containing sorted parquet files')
    parser.add_argument('--sample-size', type=int, default=20,
                       help='Number of samples per test (default: 20)')
    parser.add_argument('--quick', action='store_true',
                       help='Quick benchmark with smaller sample size')
    
    args = parser.parse_args()
    
    if args.quick:
        args.sample_size = 5
    
    try:
        benchmark = DuckDBPointerBenchmark(args.db, args.parquet_dir)
        benchmark.run_full_benchmark(sample_size=args.sample_size)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
