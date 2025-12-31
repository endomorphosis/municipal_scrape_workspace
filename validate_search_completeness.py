#!/usr/bin/env python3
"""
Validate that domain searches return complete results from all parquet files.

This checks that the index correctly references ALL parquet files containing
a domain and that searches return all WARC locations.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import duckdb
import pyarrow.parquet as pq


def find_domain_in_parquet_files(parquet_root: Path, domain: str) -> Dict[str, int]:
    """
    Scan ALL parquet files to find which ones contain the domain.
    
    Returns: {parquet_path: url_count}
    """
    host_rev = _host_to_rev(domain)
    like_pattern = host_rev + ",%"
    
    results: Dict[str, int] = {}
    
    con = duckdb.connect(":memory:")
    
    for pq_file in sorted(parquet_root.rglob("*.parquet")):
        try:
            count = con.execute(
                """
                SELECT count(*)
                FROM read_parquet(?)
                WHERE host_rev = ? OR host_rev LIKE ?
                """,
                [str(pq_file), host_rev, like_pattern],
            ).fetchone()[0]
            
            if count > 0:
                results[str(pq_file)] = count
        except Exception as e:
            print(f"Error scanning {pq_file}: {e}", file=sys.stderr)
    
    con.close()
    return results


def get_parquet_files_from_index(duckdb_dir: Path, domain: str) -> Set[str]:
    """
    Query the DuckDB index to see which parquet files it says contain the domain.
    
    Returns: Set of parquet paths
    """
    host_rev = _host_to_rev(domain)
    like_pattern = host_rev + ",%"
    
    parquet_files: Set[str] = set()
    
    for db_file in duckdb_dir.glob("*.duckdb"):
        try:
            con = duckdb.connect(str(db_file), read_only=True)
            
            # Check if domain index table exists
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            
            if ('cc_domain_shards',) not in tables:
                con.close()
                continue
            
            rows = con.execute(
                """
                SELECT DISTINCT parquet_relpath
                FROM cc_domain_shards
                WHERE host_rev = ? OR host_rev LIKE ?
                """,
                [host_rev, like_pattern],
            ).fetchall()
            
            for (relpath,) in rows:
                if relpath:
                    parquet_files.add(relpath)
            
            con.close()
        except Exception as e:
            print(f"Error querying {db_file}: {e}", file=sys.stderr)
    
    return parquet_files


def count_urls_from_index_search(duckdb_dir: Path, parquet_root: Path, domain: str) -> int:
    """
    Use the index to find parquet files, then count URLs.
    
    This simulates what a real search would return.
    """
    parquet_files = get_parquet_files_from_index(duckdb_dir, domain)
    
    if not parquet_files:
        return 0
    
    host_rev = _host_to_rev(domain)
    like_pattern = host_rev + ",%"
    
    total = 0
    con = duckdb.connect(":memory:")
    
    for relpath in parquet_files:
        full_path = parquet_root / relpath
        if not full_path.exists():
            print(f"WARNING: Parquet file not found: {full_path}", file=sys.stderr)
            continue
        
        try:
            count = con.execute(
                """
                SELECT count(*)
                FROM read_parquet(?)
                WHERE host_rev = ? OR host_rev LIKE ?
                """,
                [str(full_path), host_rev, like_pattern],
            ).fetchone()[0]
            
            total += count
        except Exception as e:
            print(f"Error reading {full_path}: {e}", file=sys.stderr)
    
    con.close()
    return total


def _host_to_rev(host: str) -> str:
    """Convert host to reverse domain notation."""
    parts = [p for p in (host or "").lower().split(".") if p]
    return ",".join(reversed(parts))


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate domain search completeness")
    ap.add_argument("--duckdb-dir", required=True, type=str, help="DuckDB index directory")
    ap.add_argument("--parquet-root", required=True, type=str, help="Parquet files root directory")
    ap.add_argument("--domain", required=True, type=str, help="Domain to test")
    ap.add_argument("--exhaustive", action="store_true", help="Do full parquet scan to verify")
    
    args = ap.parse_args()
    
    duckdb_dir = Path(args.duckdb_dir).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    domain = args.domain.strip().lower()
    
    if domain.startswith("www."):
        domain = domain[4:]
    
    print("=" * 80)
    print("DOMAIN SEARCH VALIDATION")
    print("=" * 80)
    print(f"Domain: {domain}")
    print(f"Index:  {duckdb_dir}")
    print(f"Data:   {parquet_root}")
    print()
    
    # Step 1: Query the index
    print("[1/3] Querying DuckDB index...")
    indexed_files = get_parquet_files_from_index(duckdb_dir, domain)
    print(f"  Index says domain is in {len(indexed_files)} parquet files")
    
    if indexed_files:
        print("  Files:")
        for f in sorted(indexed_files)[:10]:
            print(f"    {f}")
        if len(indexed_files) > 10:
            print(f"    ... and {len(indexed_files) - 10} more")
    print()
    
    # Step 2: Count URLs using the index
    print("[2/3] Counting URLs via index search...")
    indexed_count = count_urls_from_index_search(duckdb_dir, parquet_root, domain)
    print(f"  Found {indexed_count:,} URLs via index")
    print()
    
    # Step 3: Exhaustive scan if requested
    if args.exhaustive:
        print("[3/3] Exhaustive parquet scan (this may take a while)...")
        actual_files = find_domain_in_parquet_files(parquet_root, domain)
        actual_count = sum(actual_files.values())
        
        print(f"  Found {len(actual_files)} parquet files with domain")
        print(f"  Total URLs: {actual_count:,}")
        print()
        
        # Compare
        print("=" * 80)
        print("VALIDATION RESULTS")
        print("=" * 80)
        print()
        
        missing_from_index = set(actual_files.keys()) - {str(parquet_root / f) for f in indexed_files}
        extra_in_index = {str(parquet_root / f) for f in indexed_files} - set(actual_files.keys())
        
        if not missing_from_index and not extra_in_index and indexed_count == actual_count:
            print("✅ PASS: Index is complete and accurate")
            print(f"   - All {len(actual_files)} parquet files are indexed")
            print(f"   - All {actual_count:,} URLs are reachable")
            return 0
        else:
            print("❌ FAIL: Index is incomplete or incorrect")
            print()
            
            if missing_from_index:
                print(f"  Missing from index ({len(missing_from_index)} files):")
                for f in sorted(missing_from_index)[:5]:
                    rel = Path(f).relative_to(parquet_root)
                    print(f"    {rel} ({actual_files[f]:,} URLs)")
                if len(missing_from_index) > 5:
                    print(f"    ... and {len(missing_from_index) - 5} more")
                print()
            
            if extra_in_index:
                print(f"  Extra in index ({len(extra_in_index)} files):")
                for f in sorted(extra_in_index)[:5]:
                    try:
                        rel = Path(f).relative_to(parquet_root)
                        print(f"    {rel}")
                    except:
                        print(f"    {f}")
                if len(extra_in_index) > 5:
                    print(f"    ... and {len(extra_in_index) - 5} more")
                print()
            
            if indexed_count != actual_count:
                print(f"  URL count mismatch:")
                print(f"    Index:  {indexed_count:,}")
                print(f"    Actual: {actual_count:,}")
                print(f"    Diff:   {actual_count - indexed_count:+,}")
            
            return 1
    else:
        print("[3/3] Skipped (use --exhaustive for full validation)")
        print()
        print("ℹ️  Index returned results, but not validated against actual data")
        print("   Run with --exhaustive to verify completeness")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
