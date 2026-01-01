#!/usr/bin/env python3
"""Benchmark DuckDB domain pointer index search performance.

This script tests the search performance of the DuckDB pointer index
by searching for various domains and measuring query times.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import List, Dict
import duckdb
import pyarrow.parquet as pq
import random

def benchmark_index_lookup(db_path: Path, domain: str) -> Dict[str, float]:
    """Benchmark just the index lookup time."""
    con = duckdb.connect(str(db_path), read_only=True)
    
    start = time.time()
    results = con.execute("""
        SELECT parquet_file, row_offset, row_count
        FROM domain_index
        WHERE domain = ?
    """, [domain]).fetchall()
    index_time = time.time() - start
    
    con.close()
    
    return {
        'index_lookup_time': index_time,
        'files_found': len(results),
        'total_rows': sum(r[2] for r in results) if results else 0
    }

def benchmark_full_retrieval(db_path: Path, domain: str) -> Dict[str, float]:
    """Benchmark full data retrieval including reading from parquet."""
    con = duckdb.connect(str(db_path), read_only=True)
    
    # Index lookup
    index_start = time.time()
    results = con.execute("""
        SELECT parquet_file, row_offset, row_count
        FROM domain_index
        WHERE domain = ?
    """, [domain]).fetchall()
    index_time = time.time() - index_start
    
    con.close()
    
    if len(results) == 0:
        return {
            'index_lookup_time': index_time,
            'parquet_read_time': 0,
            'total_time': index_time,
            'files_found': 0,
            'total_rows': 0,
            'rows_retrieved': 0
        }
    
    # Parquet reading
    parquet_start = time.time()
    total_rows_retrieved = 0
    
    for parquet_file, row_offset, row_count in results:
        try:
            table = pq.read_table(
                parquet_file,
                columns=['url', 'host', 'timestamp', 'status', 'mime', 'digest',
                         'warc_filename', 'warc_offset', 'warc_length']
            )
            records = table.slice(row_offset, row_count).to_pylist()
            total_rows_retrieved += len(records)
        except Exception as e:
            print(f"Error reading {parquet_file}: {e}", file=sys.stderr)
    
    parquet_time = time.time() - parquet_start
    total_time = index_time + parquet_time
    
    return {
        'index_lookup_time': index_time,
        'parquet_read_time': parquet_time,
        'total_time': total_time,
        'files_found': len(results),
        'total_rows': sum(r[2] for r in results),
        'rows_retrieved': total_rows_retrieved
    }

def get_sample_domains(db_path: Path, count: int = 10) -> List[str]:
    """Get sample domains from the index for testing."""
    con = duckdb.connect(str(db_path), read_only=True)
    
    # Get random domains with various frequencies
    domains = con.execute("""
        SELECT domain, COUNT(*) as file_count, SUM(row_count) as total_rows
        FROM domain_index
        GROUP BY domain
        ORDER BY RANDOM()
        LIMIT ?
    """, [count]).fetchall()
    
    con.close()
    
    return [(d[0], d[1], d[2]) for d in domains]

def print_stats(stats: Dict[str, float], domain: str, domain_info: tuple = None):
    """Print benchmark statistics."""
    print(f"\nDomain: {domain}")
    if domain_info:
        print(f"  Files containing domain: {domain_info[0]}")
        print(f"  Total rows for domain: {domain_info[1]}")
    
    print(f"\nTiming:")
    print(f"  Index lookup: {stats['index_lookup_time']*1000:.2f} ms")
    if 'parquet_read_time' in stats:
        print(f"  Parquet read: {stats['parquet_read_time']*1000:.2f} ms")
        print(f"  Total time:   {stats['total_time']*1000:.2f} ms")
    
    print(f"\nResults:")
    print(f"  Files found: {stats['files_found']}")
    print(f"  Total rows: {stats['total_rows']}")
    if 'rows_retrieved' in stats:
        print(f"  Rows retrieved: {stats['rows_retrieved']}")
    
    if stats['total_rows'] > 0 and 'total_time' in stats:
        throughput = stats['total_rows'] / stats['total_time']
        print(f"\nThroughput: {throughput:.0f} rows/second")

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark DuckDB domain pointer index search performance"
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="DuckDB pointer database path"
    )
    parser.add_argument(
        "--domain",
        type=str,
        help="Specific domain to benchmark (optional)"
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=10,
        help="Number of random domains to test (default: 10)"
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Only benchmark index lookup, not full retrieval"
    )
    
    args = parser.parse_args()
    
    if not args.db.exists():
        print(f"Error: Database does not exist: {args.db}", file=sys.stderr)
        sys.exit(1)
    
    print(f"=== DuckDB Domain Pointer Index Benchmark ===")
    print(f"Database: {args.db}\n")
    
    # Get database statistics
    con = duckdb.connect(str(args.db), read_only=True)
    db_stats = con.execute("""
        SELECT 
            COUNT(DISTINCT domain) as unique_domains,
            COUNT(*) as total_entries,
            COUNT(DISTINCT parquet_file) as files_indexed,
            SUM(row_count) as total_rows
        FROM domain_index
    """).fetchone()
    con.close()
    
    print(f"Database Statistics:")
    print(f"  Unique domains: {db_stats[0]:,}")
    print(f"  Domain-file entries: {db_stats[1]:,}")
    print(f"  Parquet files indexed: {db_stats[2]:,}")
    print(f"  Total rows indexed: {db_stats[3]:,}")
    print()
    
    # Benchmark specific domain or random samples
    if args.domain:
        domains = [(args.domain, None, None)]
    else:
        print(f"Selecting {args.sample_count} random domains for testing...")
        sample_data = get_sample_domains(args.db, args.sample_count)
        domains = [(d[0], d[1], d[2]) for d in sample_data]
        print(f"Selected {len(domains)} domains\n")
    
    # Run benchmarks
    all_times = []
    
    for domain, file_count, row_count in domains:
        if args.index_only:
            stats = benchmark_index_lookup(args.db, domain)
        else:
            stats = benchmark_full_retrieval(args.db, domain)
        
        print_stats(stats, domain, (file_count, row_count) if file_count else None)
        print("-" * 60)
        
        all_times.append(stats['total_time'] if 'total_time' in stats else stats['index_lookup_time'])
    
    # Summary statistics
    if len(all_times) > 1:
        avg_time = sum(all_times) / len(all_times)
        min_time = min(all_times)
        max_time = max(all_times)
        
        print(f"\n=== Summary ===")
        print(f"Queries tested: {len(all_times)}")
        print(f"Average time: {avg_time*1000:.2f} ms")
        print(f"Min time: {min_time*1000:.2f} ms")
        print(f"Max time: {max_time*1000:.2f} ms")

if __name__ == "__main__":
    main()
