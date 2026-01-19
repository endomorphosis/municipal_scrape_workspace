#!/usr/bin/env python3
"""
Benchmark DuckDB pointer index search performance.

Tests:
1. Cold start: Search after clearing OS cache
2. Warm cache: Repeat searches to test cache effectiveness
3. Multiple domains: Test various domain patterns
4. Scalability: Measure query time vs result size
"""
import argparse
import sys
import time
import subprocess
from pathlib import Path
from typing import List, Dict, Any
import duckdb
import statistics

def clear_os_cache():
    """Attempt to clear OS page cache (requires sudo)"""
    try:
        subprocess.run(['sudo', 'sync'], check=True, capture_output=True)
        subprocess.run(['sudo', 'sh', '-c', 'echo 3 > /proc/sys/vm/drop_caches'], 
                      check=True, capture_output=True)
        print("✓ OS cache cleared")
        return True
    except (subprocess.CalledProcessError, PermissionError, FileNotFoundError):
        print("⚠ Could not clear OS cache (requires sudo)")
        return False

def get_db_stats(db_path: Path) -> Dict[str, Any]:
    """Get database statistics"""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Get table info
        stats = {}
        stats['db_size_mb'] = db_path.stat().st_size / (1024 * 1024)
        
        tables = con.execute("SHOW TABLES").fetchall()
        stats['tables'] = [t[0] for t in tables]
        
        if 'cc_pointers' in stats['tables']:
            row_count = con.execute("SELECT COUNT(*) FROM cc_pointers").fetchone()[0]
            stats['total_rows'] = row_count
            
            # Get schema
            schema = con.execute("DESCRIBE cc_pointers").fetchdf()
            stats['columns'] = schema['column_name'].tolist()
            
            # Check for indexes
            try:
                indexes = con.execute("PRAGMA show_indexes('cc_pointers')").fetchall()
                stats['indexes'] = [idx[0] for idx in indexes] if indexes else []
            except:
                stats['indexes'] = []
        
        return stats
    finally:
        con.close()

def benchmark_query(con: duckdb.DuckDBPyConnection, domain: str, query_name: str) -> Dict[str, Any]:
    """Execute and time a single query"""
    
    # Detect query type
    schema = con.execute("DESCRIBE cc_pointers").fetchdf()
    columns = schema['column_name'].tolist()
    
    if 'domain' in columns:
        query = "SELECT * FROM cc_pointers WHERE domain = ?"
    elif 'host' in columns:
        query = "SELECT * FROM cc_pointers WHERE host = ?"
    else:
        query = "SELECT * FROM cc_pointers WHERE url LIKE ?"
        domain = f'%{domain}%'
    
    start = time.time()
    results = con.execute(query, [domain]).fetchall()
    elapsed = time.time() - start
    
    return {
        'query_name': query_name,
        'domain': domain,
        'elapsed_ms': elapsed * 1000,
        'result_count': len(results),
        'throughput_rows_per_sec': len(results) / elapsed if elapsed > 0 else 0,
    }

def run_benchmark_suite(db_path: Path, test_domains: List[str], 
                       clear_cache: bool = False) -> List[Dict[str, Any]]:
    """Run a comprehensive benchmark suite"""
    results = []
    
    con = duckdb.connect(str(db_path), read_only=True)
    
    try:
        print(f"\n{'='*60}")
        print("Benchmark Configuration")
        print(f"{'='*60}")
        print(f"Database: {db_path}")
        print(f"Test domains: {len(test_domains)}")
        print(f"Cache clearing: {'Enabled' if clear_cache else 'Disabled'}")
        
        # Get stats
        stats = get_db_stats(db_path)
        print(f"\nDatabase Stats:")
        print(f"  Size: {stats['db_size_mb']:.2f} MB")
        print(f"  Total rows: {stats.get('total_rows', 'N/A'):,}")
        print(f"  Columns: {', '.join(stats.get('columns', []))}")
        print(f"  Indexes: {', '.join(stats.get('indexes', [])) if stats.get('indexes') else 'None'}")
        
        # Cold start test
        if clear_cache:
            print(f"\n{'='*60}")
            print("Cold Start Test (cache cleared)")
            print(f"{'='*60}")
            clear_os_cache()
            time.sleep(2)
        
        for domain in test_domains:
            result = benchmark_query(con, domain, 'cold_start' if clear_cache else 'warm')
            results.append(result)
            print(f"{domain:30} | {result['elapsed_ms']:8.2f} ms | {result['result_count']:8,} rows")
        
        # Warm cache test - repeat queries
        print(f"\n{'='*60}")
        print("Warm Cache Test (3 iterations)")
        print(f"{'='*60}")
        
        for iteration in range(3):
            print(f"\nIteration {iteration + 1}:")
            for domain in test_domains:
                result = benchmark_query(con, domain, f'warm_{iteration+1}')
                results.append(result)
                print(f"{domain:30} | {result['elapsed_ms']:8.2f} ms | {result['result_count']:8,} rows")
        
        # Summary statistics
        print(f"\n{'='*60}")
        print("Summary Statistics")
        print(f"{'='*60}")
        
        elapsed_times = [r['elapsed_ms'] for r in results]
        print(f"Query times (ms):")
        print(f"  Min:    {min(elapsed_times):.2f}")
        print(f"  Max:    {max(elapsed_times):.2f}")
        print(f"  Mean:   {statistics.mean(elapsed_times):.2f}")
        print(f"  Median: {statistics.median(elapsed_times):.2f}")
        if len(elapsed_times) > 1:
            print(f"  StdDev: {statistics.stdev(elapsed_times):.2f}")
        
        result_counts = [r['result_count'] for r in results]
        if max(result_counts) > 0:
            throughputs = [r['throughput_rows_per_sec'] for r in results if r['result_count'] > 0]
            if throughputs:
                print(f"\nThroughput (rows/sec):")
                print(f"  Min:    {min(throughputs):,.0f}")
                print(f"  Max:    {max(throughputs):,.0f}")
                print(f"  Mean:   {statistics.mean(throughputs):,.0f}")
        
    finally:
        con.close()
    
    return results

def main():
    parser = argparse.ArgumentParser(description='Benchmark DuckDB pointer index')
    parser.add_argument('--db', type=Path,
                       default=Path('/storage/ccindex_duckdb/cc_pointers.duckdb'),
                       help='Path to DuckDB database')
    parser.add_argument('--domains', nargs='+',
                       default=['example.com', 'google.com', 'github.com'],
                       help='Domains to test')
    parser.add_argument('--clear-cache', action='store_true',
                       help='Clear OS cache before benchmark (requires sudo)')
    parser.add_argument('--output', type=Path,
                       help='Save results to JSON file')
    
    args = parser.parse_args()
    
    if not args.db.exists():
        print(f"Error: Database not found at {args.db}", file=sys.stderr)
        return 1
    
    results = run_benchmark_suite(args.db, args.domains, args.clear_cache)
    
    if args.output:
        import json
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Results saved to {args.output}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
