#!/usr/bin/env python3
"""Benchmark the DuckDB pointer index search performance.

This script tests:
1. Index lookup speed (finding which parquet files contain a domain)
2. Parquet read speed (with and without row group targeting)
3. Overall end-to-end search latency
4. Comparison of range-indexed vs full-scan approaches

Usage:
    python benchmark_cc_pointer_search.py --db-dir /storage/ccindex_duckdb --parquet-root /storage/ccindex_parquet
"""

import argparse
import sys
import time
import statistics
from pathlib import Path
from typing import List, Dict, Any, Tuple
import random

import duckdb
import pyarrow.parquet as pq


def reverse_domain(domain: str) -> str:
    """Convert domain to reversed format for indexing."""
    parts = [p for p in domain.lower().strip().split(".") if p]
    if not parts:
        return ""
    return ",".join(reversed(parts))


def get_sample_domains(
    db_path: Path,
    count: int = 100
) -> List[str]:
    """Extract sample domains from a DuckDB pointer index."""
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        
        # Check which table exists
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if "cc_domain_shards" in table_names:
            query = f"""
                SELECT DISTINCT host
                FROM cc_domain_shards
                WHERE host IS NOT NULL
                LIMIT {count * 2}
            """
            rows = con.execute(query).fetchall()
            domains = [row[0] for row in rows if row[0]]
        else:
            print("Warning: No cc_domain_shards table found", file=sys.stderr)
            con.close()
            return []
        
        con.close()
        
        # Shuffle and limit
        random.shuffle(domains)
        return domains[:count]
    
    except Exception as e:
        print(f"Error getting sample domains: {e}", file=sys.stderr)
        return []


def benchmark_index_lookup(
    db_path: Path,
    domain: str,
    use_range_index: bool = True
) -> Tuple[float, int]:
    """Benchmark index lookup time for a single domain.
    
    Returns (time_seconds, result_count)
    """
    host_rev = reverse_domain(domain)
    if not host_rev:
        return 0.0, 0
    
    t0 = time.perf_counter()
    
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        
        if use_range_index:
            query = """
                SELECT COUNT(*)
                FROM cc_parquet_rowgroups
                WHERE ? BETWEEN host_rev_min AND host_rev_max
            """
        else:
            query = """
                SELECT COUNT(*)
                FROM cc_domain_shards
                WHERE host_rev = ?
            """
        
        result = con.execute(query, [host_rev]).fetchone()
        count = result[0] if result else 0
        
        con.close()
    
    except Exception as e:
        print(f"Error in index lookup: {e}", file=sys.stderr)
        return 0.0, 0
    
    t1 = time.perf_counter()
    return t1 - t0, count


def benchmark_parquet_read(
    parquet_path: Path,
    domain: str,
    row_groups: List[int] = None
) -> Tuple[float, int]:
    """Benchmark parquet file read for a domain.
    
    Returns (time_seconds, record_count)
    """
    host_rev = reverse_domain(domain)
    if not host_rev:
        return 0.0, 0
    
    if not parquet_path.exists():
        return 0.0, 0
    
    t0 = time.perf_counter()
    
    try:
        pf = pq.ParquetFile(parquet_path)
        
        count = 0
        if row_groups:
            # Targeted read
            for rg in row_groups:
                try:
                    table = pf.read_row_group(rg)
                    df = table.to_pandas()
                    matching = df[df['host_rev'] == host_rev]
                    count += len(matching)
                except Exception:
                    continue
        else:
            # Full scan
            table = pf.read()
            df = table.to_pandas()
            matching = df[df['host_rev'] == host_rev]
            count = len(matching)
    
    except Exception as e:
        print(f"Error reading parquet: {e}", file=sys.stderr)
        return 0.0, 0
    
    t1 = time.perf_counter()
    return t1 - t0, count


def run_benchmark_suite(
    db_dir: Path,
    parquet_root: Path,
    test_domains: List[str],
    *,
    verbose: bool = False
) -> Dict[str, Any]:
    """Run comprehensive benchmark suite."""
    
    results = {
        "index_lookup_with_range": [],
        "index_lookup_without_range": [],
        "parquet_read_targeted": [],
        "parquet_read_full": [],
        "end_to_end_with_range": [],
        "end_to_end_without_range": [],
    }
    
    # Find first available DB
    db_files = list(db_dir.glob("*.duckdb"))
    if not db_files:
        print("Error: No DuckDB files found", file=sys.stderr)
        return results
    
    db_path = db_files[0]
    
    print(f"Running benchmark with {len(test_domains)} test domains...")
    print(f"Using DB: {db_path.name}")
    print()
    
    for i, domain in enumerate(test_domains, 1):
        if verbose and i % 10 == 0:
            print(f"  Progress: {i}/{len(test_domains)}")
        
        # Test 1: Index lookup with range index
        try:
            t, count = benchmark_index_lookup(db_path, domain, use_range_index=True)
            results["index_lookup_with_range"].append(t)
        except Exception as e:
            if verbose:
                print(f"  Error (range index lookup): {e}")
        
        # Test 2: Index lookup without range index
        try:
            t, count = benchmark_index_lookup(db_path, domain, use_range_index=False)
            results["index_lookup_without_range"].append(t)
        except Exception as e:
            if verbose:
                print(f"  Error (domain shard lookup): {e}")
        
        # For parquet tests, we need actual file paths
        # We'll get them from a range-indexed query
        try:
            host_rev = reverse_domain(domain)
            con = duckdb.connect(str(db_path), read_only=True)
            
            query = """
                SELECT DISTINCT parquet_relpath, row_group
                FROM cc_parquet_rowgroups
                WHERE ? BETWEEN host_rev_min AND host_rev_max
                LIMIT 1
            """
            row = con.execute(query, [host_rev]).fetchone()
            
            if row and row[0]:
                parquet_relpath = row[0]
                row_group = row[1]
                parquet_path = parquet_root / parquet_relpath
                
                # Test 3: Targeted parquet read (single row group)
                t, count = benchmark_parquet_read(parquet_path, domain, row_groups=[row_group])
                results["parquet_read_targeted"].append(t)
                
                # Test 4: Full parquet scan
                t, count = benchmark_parquet_read(parquet_path, domain, row_groups=None)
                results["parquet_read_full"].append(t)
            
            con.close()
        
        except Exception as e:
            if verbose:
                print(f"  Error (parquet tests): {e}")
    
    return results


def print_statistics(name: str, times: List[float]) -> None:
    """Print statistical summary of timing results."""
    if not times:
        print(f"{name}: No data")
        return
    
    times_ms = [t * 1000 for t in times]
    
    print(f"{name}:")
    print(f"  Count:  {len(times_ms)}")
    print(f"  Mean:   {statistics.mean(times_ms):.2f} ms")
    print(f"  Median: {statistics.median(times_ms):.2f} ms")
    print(f"  Min:    {min(times_ms):.2f} ms")
    print(f"  Max:    {max(times_ms):.2f} ms")
    print(f"  StdDev: {statistics.stdev(times_ms):.2f} ms" if len(times_ms) > 1 else "  StdDev: N/A")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark DuckDB pointer index search performance"
    )
    ap.add_argument(
        "--db-dir",
        required=True,
        type=str,
        help="Directory containing DuckDB pointer index files"
    )
    ap.add_argument(
        "--parquet-root",
        required=True,
        type=str,
        help="Root directory of parquet files"
    )
    ap.add_argument(
        "--test-domains",
        type=str,
        default=None,
        help="File containing test domains (one per line). If not provided, samples from DB"
    )
    ap.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of test domains to use"
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible results"
    )
    ap.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Verbose output"
    )
    
    args = ap.parse_args()
    
    db_dir = Path(args.db_dir).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    
    if not db_dir.exists() or not db_dir.is_dir():
        print(f"Error: DB directory not found: {db_dir}", file=sys.stderr)
        return 1
    
    if not parquet_root.exists() or not parquet_root.is_dir():
        print(f"Error: Parquet root not found: {parquet_root}", file=sys.stderr)
        return 1
    
    random.seed(args.seed)
    
    # Get test domains
    test_domains = []
    
    if args.test_domains:
        test_domains_path = Path(args.test_domains).expanduser().resolve()
        if test_domains_path.exists():
            with open(test_domains_path, "r") as f:
                test_domains = [line.strip() for line in f if line.strip()]
        else:
            print(f"Error: Test domains file not found: {test_domains_path}", file=sys.stderr)
            return 1
    else:
        # Sample from DB
        db_files = list(db_dir.glob("*.duckdb"))
        if not db_files:
            print("Error: No DuckDB files found", file=sys.stderr)
            return 1
        
        print(f"Sampling {args.count} domains from {db_files[0].name}...")
        test_domains = get_sample_domains(db_files[0], count=args.count)
    
    if not test_domains:
        print("Error: No test domains available", file=sys.stderr)
        return 1
    
    test_domains = test_domains[:args.count]
    
    print(f"Benchmark Configuration:")
    print(f"  DB Directory:   {db_dir}")
    print(f"  Parquet Root:   {parquet_root}")
    print(f"  Test Domains:   {len(test_domains)}")
    print(f"  Random Seed:    {args.seed}")
    print()
    
    # Run benchmarks
    results = run_benchmark_suite(
        db_dir,
        parquet_root,
        test_domains,
        verbose=args.verbose
    )
    
    # Print results
    print("=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    print()
    
    print_statistics("Index Lookup (with row-group range)", results["index_lookup_with_range"])
    print_statistics("Index Lookup (domain shards only)", results["index_lookup_without_range"])
    print_statistics("Parquet Read (targeted row group)", results["parquet_read_targeted"])
    print_statistics("Parquet Read (full scan)", results["parquet_read_full"])
    
    # Calculate speedup
    if results["parquet_read_targeted"] and results["parquet_read_full"]:
        targeted_mean = statistics.mean(results["parquet_read_targeted"])
        full_mean = statistics.mean(results["parquet_read_full"])
        speedup = full_mean / targeted_mean if targeted_mean > 0 else 0
        print(f"Row-group targeting speedup: {speedup:.1f}x faster")
        print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
