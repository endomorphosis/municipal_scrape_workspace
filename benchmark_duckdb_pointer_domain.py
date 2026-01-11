#!/usr/bin/env python3
"""
Benchmark DuckDB pointer index domain searches.
Tests query performance with various domain patterns and validates completeness.
"""
import argparse
import duckdb
import time
import statistics
from pathlib import Path
from typing import List, Tuple

def get_test_domains(db_path: str, count: int = 20) -> List[str]:
    """Get a sample of domains from the index for testing."""
    con = duckdb.connect(str(db_path), read_only=True)
    
    query = """
    SELECT DISTINCT domain
    FROM domain_pointers
    WHERE domain_count > 5
    ORDER BY domain_count DESC
    LIMIT ?
    """
    
    domains = [row[0] for row in con.execute(query, [count]).fetchall()]
    con.close()
    return domains

def benchmark_domain_search(db_path: str, domain: str) -> Tuple[int, float, float]:
    """
    Benchmark a single domain search.
    Returns (result_count, pointer_time, total_time).
    """
    con = duckdb.connect(str(db_path), read_only=True)
    
    # Time the pointer lookup
    start_time = time.time()
    pointer_query = """
    SELECT 
        parquet_file,
        row_group_id,
        start_row,
        end_row,
        domain_count
    FROM domain_pointers
    WHERE domain = ?
    """
    pointers = con.execute(pointer_query, [domain]).fetchall()
    pointer_time = time.time() - start_time
    
    if not pointers:
        con.close()
        return 0, pointer_time, pointer_time
    
    # Time the full data retrieval
    total_results = 0
    for parquet_file, row_group_id, start_row, end_row, domain_count in pointers:
        file_query = """
        SELECT COUNT(*)
        FROM read_parquet(?, hive_partitioning=false)
        WHERE url LIKE ? || '%'
        """
        pattern = f"http://{domain}/" if not domain.startswith('http') else domain
        count = con.execute(file_query, [parquet_file, pattern]).fetchone()[0]
        total_results += count
    
    total_time = time.time() - start_time
    con.close()
    
    return total_results, pointer_time, total_time

def validate_index_completeness(db_path: str, parquet_root: str) -> bool:
    """
    Validate that all parquet files are represented in the pointer index.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    
    # Get all parquet files from index
    indexed_files = set(row[0] for row in con.execute(
        "SELECT DISTINCT parquet_file FROM domain_pointers"
    ).fetchall())
    
    # Get all actual parquet files
    actual_files = set()
    parquet_path = Path(parquet_root)
    for pf in parquet_path.rglob("*.parquet"):
        if pf.name != "sample_2025_43_1file_1m.parquet":  # Skip test files
            actual_files.add(str(pf))
    
    con.close()
    
    missing = actual_files - indexed_files
    if missing:
        print(f"WARNING: {len(missing)} parquet files not in index:")
        for f in sorted(missing)[:10]:
            print(f"  - {f}")
        if len(missing) > 10:
            print(f"  ... and {len(missing) - 10} more")
        return False
    
    extra = indexed_files - actual_files
    if extra:
        print(f"WARNING: {len(extra)} indexed files don't exist:")
        for f in sorted(extra)[:10]:
            print(f"  - {f}")
        return False
    
    print(f"✓ Index is complete: {len(indexed_files)} parquet files")
    return True

def main():
    parser = argparse.ArgumentParser(
        description='Benchmark DuckDB pointer index domain searches'
    )
    parser.add_argument('--db', required=True,
                        help='Path to DuckDB pointer database')
    parser.add_argument('--parquet-root', 
                        default='/storage/ccindex_parquet',
                        help='Root directory of parquet files')
    parser.add_argument('--domains', type=int, default=20,
                        help='Number of domains to test (default: 20)')
    parser.add_argument('--validate', action='store_true',
                        help='Validate index completeness before benchmarking')
    
    args = parser.parse_args()
    
    print(f"DuckDB Pointer Index Benchmark")
    print(f"Database: {args.db}\n")
    
    if args.validate:
        print("Validating index completeness...")
        if not validate_index_completeness(args.db, args.parquet_root):
            print("\n⚠️  Index validation failed!")
            return 1
        print()
    
    # Get test domains
    print(f"Getting {args.domains} test domains...")
    test_domains = get_test_domains(args.db, args.domains)
    print(f"Testing with {len(test_domains)} domains\n")
    
    # Run benchmarks
    results = []
    for i, domain in enumerate(test_domains, 1):
        print(f"[{i}/{len(test_domains)}] Testing {domain}...", end=' ', flush=True)
        result_count, pointer_time, total_time = benchmark_domain_search(args.db, domain)
        results.append((domain, result_count, pointer_time, total_time))
        print(f"{result_count} URLs in {total_time:.3f}s (pointer: {pointer_time:.3f}s)")
    
    # Calculate statistics
    print("\n" + "="*70)
    print("BENCHMARK RESULTS")
    print("="*70)
    
    pointer_times = [r[2] for r in results]
    total_times = [r[3] for r in results]
    result_counts = [r[1] for r in results]
    
    print(f"\nPointer Lookup Times:")
    print(f"  Min:    {min(pointer_times):.3f}s")
    print(f"  Max:    {max(pointer_times):.3f}s")
    print(f"  Mean:   {statistics.mean(pointer_times):.3f}s")
    print(f"  Median: {statistics.median(pointer_times):.3f}s")
    
    print(f"\nTotal Query Times:")
    print(f"  Min:    {min(total_times):.3f}s")
    print(f"  Max:    {max(total_times):.3f}s")
    print(f"  Mean:   {statistics.mean(total_times):.3f}s")
    print(f"  Median: {statistics.median(total_times):.3f}s")
    
    print(f"\nResults per Query:")
    print(f"  Min:    {min(result_counts)} URLs")
    print(f"  Max:    {max(result_counts)} URLs")
    print(f"  Mean:   {int(statistics.mean(result_counts))} URLs")
    print(f"  Median: {int(statistics.median(result_counts))} URLs")
    
    print(f"\nThroughput:")
    total_urls = sum(result_counts)
    total_time = sum(total_times)
    print(f"  Total URLs retrieved: {total_urls:,}")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  URLs/second: {total_urls/total_time:,.0f}")
    
    return 0

if __name__ == '__main__':
    exit(main())
