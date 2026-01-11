#!/usr/bin/env python3
"""
Validate parquet files are sorted and mark them with .sorted extension.

This script:
1. Checks if files are already marked as .sorted (skips them)
2. Validates if unmarked files are actually sorted
3. Adds .sorted extension to files that are already sorted
4. Optionally sorts unsorted files and marks them
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

import pyarrow.parquet as pq
import duckdb


def is_sorted_by_content(parquet_file: Path, sample_size: int = 1000) -> Tuple[bool, str]:
    """
    Check if a parquet file is sorted by host_rev.
    
    Returns: (is_sorted, reason)
    """
    try:
        pf = pq.ParquetFile(parquet_file)
        
        if pf.metadata.num_row_groups == 0:
            return False, "No row groups"
        
        # Check within first row group
        table = pf.read_row_group(0, columns=['host_rev'])
        vals = table['host_rev'].to_pylist()
        
        if len(vals) < 2:
            return True, "Too few rows to check"
        
        # Sample check within row group
        step = max(1, len(vals) // sample_size)
        sample = vals[::step]
        
        for i in range(len(sample) - 1):
            if sample[i] > sample[i + 1]:
                return False, f"Unsorted within row group: {sample[i]} > {sample[i+1]}"
        
        # Check across row groups if multiple exist
        if pf.metadata.num_row_groups > 1:
            last_val = vals[-1]
            
            for rg_idx in range(1, min(pf.metadata.num_row_groups, 10)):  # Check first 10 row groups
                table = pf.read_row_group(rg_idx, columns=['host_rev'])
                vals = table['host_rev'].to_pylist()
                
                if len(vals) > 0:
                    first_val = vals[0]
                    if last_val > first_val:
                        return False, f"Unsorted between row groups: {last_val} > {first_val}"
                    
                    last_val = vals[-1]
        
        return True, "Verified sorted"
        
    except Exception as e:
        return False, f"Error: {e}"


def sort_parquet_file(input_file: Path, output_file: Path, memory_limit_gb: float = 4.0) -> bool:
    """Sort a parquet file by host_rev, url, ts using DuckDB."""
    try:
        con = duckdb.connect(":memory:")
        con.execute(f"SET memory_limit='{memory_limit_gb}GB'")
        con.execute("""
            COPY (
                SELECT * FROM read_parquet(?)
                ORDER BY host_rev, url, ts
            )
            TO ? (FORMAT 'parquet', COMPRESSION 'zstd')
        """, [str(input_file), str(output_file)])
        con.close()
        return True
    except Exception as e:
        print(f"❌ Error sorting {input_file.name}: {e}", file=sys.stderr)
        return False


def check_single_file(pq_file: Path, parquet_root: Path, verify_only: bool) -> Tuple[str, Path, bool, str]:
    """
    Check a single parquet file and optionally mark it as sorted.
    Returns: (status, file_path, is_sorted, reason)
    status: 'already_marked', 'sorted_unmarked', 'unsorted', 'error'
    """
    rel_path = pq_file.relative_to(parquet_root)
    
    # Skip already marked files
    if '.sorted.' in pq_file.name or pq_file.name.endswith('.sorted.parquet'):
        return ('already_marked', pq_file, True, 'Already marked')
    
    try:
        # Check if file is sorted by content
        is_sorted, reason = is_sorted_by_content(pq_file)
        
        if is_sorted:
            # Mark as sorted if not in verify-only mode
            if not verify_only:
                # Rename to add .sorted before .parquet
                if pq_file.name.endswith('.gz.parquet'):
                    new_name = pq_file.name.replace('.gz.parquet', '.gz.sorted.parquet')
                else:
                    new_name = pq_file.name.replace('.parquet', '.sorted.parquet')
                
                new_path = pq_file.parent / new_name
                pq_file.rename(new_path)
                return ('sorted_unmarked', new_path, True, f'Marked as {new_name}')
            else:
                return ('sorted_unmarked', pq_file, True, 'Sorted but not marked (verify-only)')
        else:
            return ('unsorted', pq_file, False, reason)
    except Exception as e:
        return ('error', pq_file, False, str(e))


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate and mark sorted parquet files")
    ap.add_argument("--parquet-root", required=True, type=str, help="Root directory of parquet files")
    ap.add_argument("--sort-unsorted", action="store_true", help="Sort any unsorted files found")
    ap.add_argument("--verify-only", action="store_true", help="Only verify, don't mark or sort")
    ap.add_argument("--memory-per-sort", type=float, default=4.0, help="GB memory per sort operation")
    ap.add_argument("--workers", type=int, default=None, help="Number of parallel workers (default: CPU count)")
    
    args = ap.parse_args()
    
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    
    if not parquet_root.exists():
        print(f"❌ ERROR: Parquet root not found: {parquet_root}")
        return 1
    
    print("=" * 80)
    print("PARQUET FILE VALIDATION AND MARKING")
    print("=" * 80)
    print(f"Root: {parquet_root}")
    print()
    
    # Find all parquet files
    all_files = sorted(parquet_root.rglob("*.parquet"))
    print(f"Found {len(all_files)} parquet files")
    print()
    
    already_marked: List[Path] = []
    sorted_unmarked: List[Path] = []
    unsorted_files: List[Path] = []
    error_files: List[Tuple[Path, str]] = []
    
    # Check each file in parallel
    print("Checking files...")
    print("-" * 80)
    
    num_workers = args.workers or multiprocessing.cpu_count()
    print(f"Using {num_workers} parallel workers")
    print()
    
    completed = 0
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(check_single_file, pq_file, parquet_root, args.verify_only): pq_file
            for pq_file in all_files
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            completed += 1
            pq_file = futures[future]
            
            try:
                status, result_path, is_sorted, reason = future.result()
                rel_path = result_path.relative_to(parquet_root)
                
                if status == 'already_marked':
                    already_marked.append(result_path)
                    if completed % 100 == 0:
                        print(f"[{completed}/{len(all_files)}] ⏭️  {rel_path} (already marked)")
                elif status == 'sorted_unmarked':
                    sorted_unmarked.append(result_path)
                    print(f"[{completed}/{len(all_files)}] ✅ {rel_path} - {reason}")
                elif status == 'unsorted':
                    unsorted_files.append(result_path)
                    print(f"[{completed}/{len(all_files)}] ❌ {rel_path} - UNSORTED: {reason}")
                elif status == 'error':
                    error_files.append((result_path, reason))
                    print(f"[{completed}/{len(all_files)}] ⚠️  {rel_path} - ERROR: {reason}")
                
                if completed % 50 == 0:
                    print(f"Progress: {completed}/{len(all_files)} - Marked: {len(already_marked)}, Sorted: {len(sorted_unmarked)}, Unsorted: {len(unsorted_files)}", flush=True)
                    
            except Exception as e:
                print(f"[{completed}/{len(all_files)}] ⚠️  {pq_file.name} - Exception: {e}")
    
    print("-" * 80)
    print()
    print("Summary:")
    print(f"  Total files:           {len(all_files)}")
    print(f"  ✅ Already marked:     {len(already_marked)}")
    print(f"  ✅ Sorted (unmarked):  {len(sorted_unmarked)}")
    print(f"  ❌ Unsorted:           {len(unsorted_files)}")
    print(f"  ⚠️  Errors:            {len(error_files)}")
    print(f"  Total sorted:          {len(already_marked) + len(sorted_unmarked)}")
    print(f"  Percentage sorted:     {(len(already_marked) + len(sorted_unmarked)) / len(all_files) * 100:.1f}%")
    print()
    
    # Sort unsorted files if requested
    if unsorted_files and args.sort_unsorted and not args.verify_only:
        print("=" * 80)
        print("SORTING UNSORTED FILES")
        print("=" * 80)
        print()
        
        sorted_count = 0
        failed_count = 0
        
        for i, unsorted_file in enumerate(unsorted_files, 1):
            print(f"[{i}/{len(unsorted_files)}] Sorting: {unsorted_file.name}")
            
            # Create temporary sorted file
            sorted_tmp = unsorted_file.with_suffix(".parquet.tmp")
            
            if sort_parquet_file(unsorted_file, sorted_tmp, args.memory_per_sort):
                # Verify it's now sorted
                is_sorted, reason = is_sorted_by_content(sorted_tmp)
                
                if is_sorted:
                    # Create final sorted filename
                    if unsorted_file.name.endswith('.gz.parquet'):
                        new_name = unsorted_file.name.replace('.gz.parquet', '.gz.sorted.parquet')
                    else:
                        new_name = unsorted_file.name.replace('.parquet', '.sorted.parquet')
                    
                    sorted_final = unsorted_file.parent / new_name
                    
                    # Remove original and rename temp
                    unsorted_file.unlink()
                    sorted_tmp.rename(sorted_final)
                    
                    sorted_count += 1
                    print(f"  ✅ Sorted, verified, and marked: {new_name}")
                else:
                    print(f"  ❌ Sort verification failed: {reason}")
                    sorted_tmp.unlink()
                    failed_count += 1
            else:
                failed_count += 1
                print(f"  ❌ Sort operation failed")
        
        print()
        print(f"Sorting complete:")
        print(f"  Succeeded: {sorted_count}")
        print(f"  Failed:    {failed_count}")
        print(f"  Total sorted files: {len(already_marked) + len(sorted_unmarked) + sorted_count}/{len(all_files)}")
    
    # Exit status
    if unsorted_files and not args.sort_unsorted:
        print()
        print("⚠️  WARNING: Some files are not sorted!")
        print("   Run with --sort-unsorted to fix")
        return 1
    
    print()
    print("✅ All files verified and marked as sorted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
