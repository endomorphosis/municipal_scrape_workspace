#!/usr/bin/env python3
"""Benchmark the DuckDB domain pointer index search performance.

This script tests the search performance of the domain pointer index by:
1. Testing various domain sizes (small, medium, large)
2. Measuring query time vs data retrieval time
3. Comparing cold vs warm cache performance
4. Testing concurrent search performance

Usage:
    python benchmark_domain_pointer_index.py
    python benchmark_domain_pointer_index.py --domains example.com,test.com
    python benchmark_domain_pointer_index.py --cold-cache --iterations 10
"""

import argparse
import json
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import duckdb
import pyarrow.parquet as pq


def clear_os_cache():
    """Attempt to clear OS page cache (requires sudo)."""
    import subprocess
    try:
        subprocess.run(
            ["sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
            check=True,
            capture_output=True,
        )
        return True
    except:
        return False


def get_sample_domains(
    db_path: str,
    count: int = 10,
    min_urls: int = 10,
    max_urls: int = None,
) -> List[Tuple[str, int]]:
    """Get sample domains from the index with varying sizes.
    
    Returns list of (domain, estimated_url_count) tuples.
    """
    conn = duckdb.connect(db_path, read_only=True)
    
    query = """
        SELECT domain, SUM(row_count) as total_urls
        FROM domain_pointers
        GROUP BY domain
        HAVING total_urls >= ?
    """
    params = [min_urls]
    
    if max_urls:
        query += " AND total_urls <= ?"
        params.append(max_urls)
    
    query += f" ORDER BY RANDOM() LIMIT {count}"
    
    domains = conn.execute(query, params).fetchall()
    conn.close()
    
    return domains


def benchmark_single_search(
    domain: str,
    db_path: str,
    parquet_root: str,
    limit: int = None,
) -> dict:
    """Benchmark a single domain search and return timing breakdown."""
    
    start_time = time.time()
    
    # Phase 1: Query DuckDB index
    query_start = time.time()
    conn = duckdb.connect(db_path, read_only=True)
    
    query = """
        SELECT parquet_file, row_start, row_end, row_count
        FROM domain_pointers
        WHERE domain = ?
        ORDER BY parquet_file, row_start
    """
    
    ranges = conn.execute(query, [domain]).fetchall()
    conn.close()
    query_time = time.time() - query_start
    
    # Phase 2: Read from parquet files
    read_start = time.time()
    results = []
    files_read = 0
    rows_read = 0
    
    for parquet_file, row_start, row_end, row_count in ranges:
        parquet_path = Path(parquet_root) / parquet_file
        
        if not parquet_path.exists():
            continue
        
        try:
            table = pq.read_table(str(parquet_path))
            chunk = table.slice(row_start, row_count)
            
            for i in range(chunk.num_rows):
                row_dict = {
                    col: chunk.column(col)[i].as_py()
                    for col in chunk.column_names
                }
                results.append(row_dict)
                rows_read += 1
                
                if limit and len(results) >= limit:
                    break
            
            files_read += 1
            
        except Exception as e:
            continue
        
        if limit and len(results) >= limit:
            break
    
    read_time = time.time() - read_start
    total_time = time.time() - start_time
    
    return {
        "domain": domain,
        "query_time": query_time,
        "read_time": read_time,
        "total_time": total_time,
        "ranges_found": len(ranges),
        "files_read": files_read,
        "rows_returned": len(results),
        "rows_scanned": rows_read,
    }


def benchmark_concurrent_searches(
    domains: List[str],
    db_path: str,
    parquet_root: str,
    workers: int = 4,
) -> List[dict]:
    """Benchmark concurrent domain searches."""
    
    start_time = time.time()
    results = []
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                benchmark_single_search,
                domain,
                db_path,
                parquet_root,
                None,
            ): domain
            for domain in domains
        }
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                domain = futures[future]
                print(f"Error searching {domain}: {e}", file=sys.stderr)
    
    total_time = time.time() - start_time
    
    return results, total_time


def print_benchmark_results(results: List[dict], title: str = "Benchmark Results"):
    """Print formatted benchmark results."""
    
    print(f"\n{'='*80}")
    print(f"{title:^80}")
    print(f"{'='*80}")
    
    if not results:
        print("No results to display")
        return
    
    # Calculate statistics
    query_times = [r["query_time"] for r in results]
    read_times = [r["read_time"] for r in results]
    total_times = [r["total_time"] for r in results]
    rows_returned = [r["rows_returned"] for r in results]
    
    print(f"\nSearches performed: {len(results)}")
    print(f"Total rows returned: {sum(rows_returned):,}")
    
    print(f"\nQuery Time (DuckDB index lookup):")
    print(f"  Min:    {min(query_times)*1000:.2f} ms")
    print(f"  Max:    {max(query_times)*1000:.2f} ms")
    print(f"  Mean:   {statistics.mean(query_times)*1000:.2f} ms")
    print(f"  Median: {statistics.median(query_times)*1000:.2f} ms")
    
    print(f"\nRead Time (Parquet file access):")
    print(f"  Min:    {min(read_times):.4f} s")
    print(f"  Max:    {max(read_times):.4f} s")
    print(f"  Mean:   {statistics.mean(read_times):.4f} s")
    print(f"  Median: {statistics.median(read_times):.4f} s")
    
    print(f"\nTotal Time:")
    print(f"  Min:    {min(total_times):.4f} s")
    print(f"  Max:    {max(total_times):.4f} s")
    print(f"  Mean:   {statistics.mean(total_times):.4f} s")
    print(f"  Median: {statistics.median(total_times):.4f} s")
    
    print(f"\nDetailed Results:")
    print(f"{'Domain':<30} {'Query(ms)':<12} {'Read(s)':<10} {'Total(s)':<10} {'Rows':<10}")
    print("-" * 80)
    
    for r in sorted(results, key=lambda x: x["total_time"]):
        print(
            f"{r['domain']:<30} "
            f"{r['query_time']*1000:<12.2f} "
            f"{r['read_time']:<10.4f} "
            f"{r['total_time']:<10.4f} "
            f"{r['rows_returned']:<10,}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark domain pointer index search performance"
    )
    parser.add_argument(
        "--db",
        default="/storage/ccindex_duckdb/domain_pointer.duckdb",
        help="Path to DuckDB pointer index"
    )
    parser.add_argument(
        "--parquet-root",
        default="/storage/ccindex_parquet",
        help="Root directory containing parquet files"
    )
    parser.add_argument(
        "--domains",
        help="Comma-separated list of domains to test (default: random sample)"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of random domains to test (default: 10)"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of iterations per domain (default: 3)"
    )
    parser.add_argument(
        "--cold-cache",
        action="store_true",
        help="Clear OS cache between iterations (requires sudo)"
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        help="Test concurrent searches with N workers"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit results per search (for faster benchmarking)"
    )
    parser.add_argument(
        "--output-json",
        help="Save results to JSON file"
    )
    
    args = parser.parse_args()
    
    # Get domains to test
    if args.domains:
        test_domains = [d.strip() for d in args.domains.split(",")]
    else:
        print(f"Selecting {args.sample_size} random domains from index...")
        domain_tuples = get_sample_domains(
            args.db,
            count=args.sample_size,
            min_urls=10,
        )
        test_domains = [d[0] for d in domain_tuples]
        print(f"Selected domains: {', '.join(test_domains)}")
    
    all_results = []
    
    # Single-threaded benchmark
    print(f"\nRunning {args.iterations} iteration(s) per domain...")
    
    for iteration in range(args.iterations):
        if args.cold_cache and iteration > 0:
            print(f"\nClearing OS cache for iteration {iteration + 1}...")
            if not clear_os_cache():
                print("Warning: Could not clear OS cache (requires sudo)")
        
        iteration_results = []
        
        for domain in test_domains:
            result = benchmark_single_search(
                domain=domain,
                db_path=args.db,
                parquet_root=args.parquet_root,
                limit=args.limit,
            )
            iteration_results.append(result)
            all_results.append(result)
        
        print_benchmark_results(
            iteration_results,
            f"Iteration {iteration + 1}/{args.iterations}"
        )
    
    # Print overall statistics
    if args.iterations > 1:
        print_benchmark_results(all_results, "Overall Results (All Iterations)")
    
    # Concurrent benchmark
    if args.concurrent:
        print(f"\n\nRunning concurrent benchmark with {args.concurrent} workers...")
        concurrent_results, total_time = benchmark_concurrent_searches(
            domains=test_domains,
            db_path=args.db,
            parquet_root=args.parquet_root,
            workers=args.concurrent,
        )
        
        print_benchmark_results(concurrent_results, "Concurrent Search Results")
        print(f"\nTotal concurrent execution time: {total_time:.4f} s")
        print(f"Throughput: {len(test_domains)/total_time:.2f} searches/sec")
    
    # Save results to JSON
    if args.output_json:
        output_data = {
            "config": {
                "db": args.db,
                "parquet_root": args.parquet_root,
                "domains": test_domains,
                "iterations": args.iterations,
                "cold_cache": args.cold_cache,
                "concurrent_workers": args.concurrent,
                "limit": args.limit,
            },
            "results": all_results,
        }
        
        with open(args.output_json, "w") as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nResults saved to: {args.output_json}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
