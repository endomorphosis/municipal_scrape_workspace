#!/usr/bin/env python3
"""
Sort unsorted parquet files in parallel.
"""

import argparse
import multiprocessing as mp
import sys
from pathlib import Path
from typing import List, Tuple

import duckdb


def sort_parquet_file(args: Tuple[Path, Path, int]) -> Tuple[Path, bool, str]:
    """
    Sort a single parquet file with memory limits and isolated temp dir.
    Returns (file, success, message)
    """
    parquet_file, temp_dir, worker_id = args
    
    try:
        # Each worker gets its own temp directory to avoid conflicts
        worker_temp = temp_dir / f"worker_{worker_id}"
        worker_temp.mkdir(parents=True, exist_ok=True)
        
        sorted_tmp = worker_temp / f"{parquet_file.name}.sorted.tmp"
        
        # Sort using DuckDB with memory limit and isolated temp directory
        con = duckdb.connect(":memory:")
        
        # Set memory limit and temp directory PER WORKER
        con.execute("SET memory_limit='2GB'")
        con.execute(f"SET temp_directory='{worker_temp}'")
        con.execute("SET preserve_insertion_order=false")  # Reduce memory usage
        
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{parquet_file}')
                ORDER BY host_rev, url, ts
            )
            TO '{sorted_tmp}' (FORMAT 'parquet', COMPRESSION 'zstd')
        """)
        con.close()
        
        # Verify it's sorted
        con = duckdb.connect(":memory:")
        result = con.execute(f"""
            WITH check_sorted AS (
                SELECT 
                    host_rev,
                    LAG(host_rev) OVER (ORDER BY ROWID) as prev_host_rev
                FROM (SELECT host_rev, ROWID FROM read_parquet('{sorted_tmp}') LIMIT 1000)
            )
            SELECT COUNT(*) as unsorted_count
            FROM check_sorted
            WHERE prev_host_rev > host_rev
        """).fetchone()
        con.close()
        
        unsorted_count = result[0] if result else 0
        
        if unsorted_count > 0:
            sorted_tmp.unlink()
            return (parquet_file, False, f"Verification failed: still {unsorted_count} unsorted")
        
        # Replace original
        parquet_file.unlink()
        sorted_tmp.rename(parquet_file)
        
        return (parquet_file, True, "Sorted and verified")
        
    except Exception as e:
        return (parquet_file, False, f"Error: {e}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Sort unsorted parquet files in parallel")
    ap.add_argument("--unsorted-list", required=True, help="File containing list of unsorted files")
    ap.add_argument("--parquet-root", required=True, help="Root directory")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4 for memory safety)")
    ap.add_argument("--temp-dir", default="/tmp/sort_temp", help="Temp directory for sorting")
    
    args = ap.parse_args()
    
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Parquet root: {parquet_root}")
    print(f"Workers:      {args.workers}")
    print(f"Temp dir:     {temp_dir}")
    print()
    
    # Read unsorted file list from the validation log
    unsorted_files = []
    try:
        with open(args.unsorted_list, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('❌') or line.startswith('cc_pointers'):
                    # Extract filename from validation output
                    if ':' in line:
                        filepath = line.split(':')[0].replace('❌', '').strip()
                    else:
                        filepath = line.strip()
                    
                    full_path = parquet_root / filepath
                    if full_path.exists():
                        unsorted_files.append(full_path)
    except Exception as e:
        print(f"Error reading unsorted list: {e}")
        return 1
    
    if not unsorted_files:
        print("No unsorted files to process")
        return 0
    
    print(f"Found {len(unsorted_files)} unsorted files to sort")
    print()
    
    # Sort in parallel
    sorted_count = 0
    failed_count = 0
    
    # Assign worker IDs to avoid temp file conflicts
    work_items = [(unsorted_files[i], temp_dir, i % args.workers) for i in range(len(unsorted_files))]
    
    with mp.Pool(processes=args.workers) as pool:
        results = pool.imap_unordered(sort_parquet_file, work_items, chunksize=1)
        
        for i, (pq_file, success, message) in enumerate(results, 1):
            rel_path = pq_file.relative_to(parquet_root)
            
            if success:
                sorted_count += 1
                print(f"✅ [{i}/{len(unsorted_files)}] {rel_path}")
            else:
                failed_count += 1
                print(f"❌ [{i}/{len(unsorted_files)}] {rel_path}: {message}")
            
            if i % 10 == 0:
                print(f"Progress: {i}/{len(unsorted_files)} - Success: {sorted_count}, Failed: {failed_count}", flush=True)
    
    print()
    print("=" * 80)
    print("SORTING COMPLETE")
    print("=" * 80)
    print(f"Total:   {len(unsorted_files)}")
    print(f"Sorted:  {sorted_count}")
    print(f"Failed:  {failed_count}")
    
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
