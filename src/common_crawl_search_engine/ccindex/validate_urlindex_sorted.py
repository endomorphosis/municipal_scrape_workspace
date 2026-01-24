#!/usr/bin/env python3
"""Validate that URL index parquet files are sorted by urlkey and (optionally) mark them.

Canonical implementation for:
  validate_urlindex_sorted.py (repo-root wrapper)

This script specifically handles Common Crawl URL index parquet files, not pointer databases.
"""

from __future__ import annotations

import argparse
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import pyarrow.parquet as pq


def is_url_index_sorted(parquet_file: Path, sample_size: int = 1000) -> Tuple[bool, str]:
    """Check if a URL index parquet file is sorted by urlkey.

    Returns: (is_sorted, reason)
    """

    try:
        pf = pq.ParquetFile(parquet_file)

        if "urlkey" not in pf.schema.names:
            return False, "Not a URL index file (no 'urlkey' field)"

        if pf.metadata.num_row_groups == 0:
            return False, "No row groups"

        table = pf.read_row_group(0, columns=["urlkey"])
        vals = table["urlkey"].to_pylist()

        if len(vals) < 2:
            return True, "Too few rows to check"

        step = max(1, len(vals) // sample_size)
        sample = vals[::step]

        for i in range(len(sample) - 1):
            if sample[i] > sample[i + 1]:
                return False, f"Unsorted within row group: {sample[i][:50]}... > {sample[i+1][:50]}..."

        if pf.metadata.num_row_groups > 1:
            last_val = vals[-1]

            for rg_idx in range(1, min(pf.metadata.num_row_groups, 10)):
                table = pf.read_row_group(rg_idx, columns=["urlkey"])
                vals = table["urlkey"].to_pylist()

                if len(vals) > 0:
                    first_val = vals[0]
                    if last_val > first_val:
                        return False, "Unsorted between row groups"

                    last_val = vals[-1]

        return True, "Verified sorted by urlkey"

    except Exception as e:
        return False, f"Error: {e}"


def check_single_file(pq_file: Path, parquet_root: Path, verify_only: bool) -> Tuple[str, Path, bool, str]:
    """Check a single parquet file and optionally mark it as sorted.

    Returns: (status, file_path, is_sorted, reason)
    status: 'already_marked', 'sorted_unmarked', 'not_urlindex', 'unsorted', 'error'
    """

    if ".sorted." in pq_file.name or pq_file.name.endswith(".sorted.parquet"):
        return ("already_marked", pq_file, True, "Already marked")

    rel_path = str(pq_file.relative_to(parquet_root))
    if any(marker in rel_path for marker in ["cc_pointers", "by_year_test", "by_collection", "ccindex_duckdb", "sample_"]):
        return ("not_urlindex", pq_file, False, "Pointer/test file")

    try:
        is_sorted, reason = is_url_index_sorted(pq_file)

        if "Not a URL index file" in reason:
            return ("not_urlindex", pq_file, False, reason)

        if is_sorted:
            if not verify_only:
                if pq_file.name.endswith(".gz.parquet"):
                    new_name = pq_file.name.replace(".gz.parquet", ".gz.sorted.parquet")
                else:
                    new_name = pq_file.name.replace(".parquet", ".sorted.parquet")

                new_path = pq_file.parent / new_name
                pq_file.rename(new_path)
                return ("sorted_unmarked", new_path, True, "Marked as sorted")

            return ("sorted_unmarked", pq_file, True, "Sorted but not marked (verify-only)")

        return ("unsorted", pq_file, False, reason)

    except Exception as e:
        return ("error", pq_file, False, str(e))


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate and mark sorted URL index parquet files")
    ap.add_argument("--parquet-root", required=True, type=str, help="Root directory of parquet files")
    ap.add_argument("--verify-only", action="store_true", help="Only verify, don't mark")
    ap.add_argument("--workers", type=int, default=None, help="Number of parallel workers (default: CPU count)")

    args = ap.parse_args(argv)

    parquet_root = Path(args.parquet_root).expanduser().resolve()

    if not parquet_root.exists():
        print(f"❌ ERROR: Parquet root not found: {parquet_root}")
        return 1

    print("=" * 80)
    print("URL INDEX PARQUET FILE VALIDATION AND MARKING")
    print("=" * 80)
    print(f"Root: {parquet_root}")
    print()

    all_files = sorted(parquet_root.rglob("*.parquet"))
    print(f"Found {len(all_files)} total parquet files")
    print()

    already_marked: List[Path] = []
    sorted_unmarked: List[Path] = []
    unsorted_files: List[Path] = []
    not_urlindex: List[Path] = []
    error_files: List[Tuple[Path, str]] = []

    print("Checking files...")
    print("-" * 80)

    num_workers = args.workers or multiprocessing.cpu_count()
    print(f"Using {num_workers} parallel workers")
    print()

    completed = 0
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(check_single_file, pq_file, parquet_root, args.verify_only): pq_file
            for pq_file in all_files
        }

        for future in as_completed(futures):
            completed += 1
            pq_file = futures[future]

            try:
                status, result_path, _is_sorted, reason = future.result()
                rel_path = result_path.relative_to(parquet_root)

                if status == "already_marked":
                    already_marked.append(result_path)
                    if completed % 100 == 0:
                        print(f"[{completed}/{len(all_files)}] ⏭️  {rel_path} (already marked)")

                elif status == "sorted_unmarked":
                    sorted_unmarked.append(result_path)
                    if not args.verify_only:
                        print(f"[{completed}/{len(all_files)}] ✅ {rel_path} - MARKED")

                elif status == "not_urlindex":
                    not_urlindex.append(result_path)
                    if completed % 100 == 0:
                        print(f"[{completed}/{len(all_files)}] ⏭️  {rel_path} (not URL index)")

                elif status == "unsorted":
                    unsorted_files.append(result_path)
                    print(f"[{completed}/{len(all_files)}] ❌ {rel_path} - UNSORTED: {reason}")

                elif status == "error":
                    error_files.append((result_path, reason))
                    print(f"[{completed}/{len(all_files)}] ⚠️  {rel_path} - ERROR: {reason}")

                if completed % 100 == 0:
                    print(
                        f"Progress: {completed}/{len(all_files)} - Marked: {len(already_marked)}, Newly marked: {len(sorted_unmarked)}, Unsorted: {len(unsorted_files)}",
                        flush=True,
                    )

            except Exception as e:
                print(f"[{completed}/{len(all_files)}] ⚠️  {pq_file.name} - Exception: {e}")

    print("-" * 80)
    print()
    print("Summary:")
    print(f"  Total files:                {len(all_files)}")
    print(f"  ✅ Already marked as sorted: {len(already_marked)}")
    print(f"  ✅ Newly marked as sorted:   {len(sorted_unmarked)}")
    print(f"  ⏭️  Not URL index files:     {len(not_urlindex)}")
    print(f"  ❌ Unsorted URL indexes:     {len(unsorted_files)}")
    print(f"  ⚠️  Errors:                  {len(error_files)}")
    print()
    print(f"  Total URL index files:       {len(already_marked) + len(sorted_unmarked) + len(unsorted_files)}")
    print(f"  Total sorted URL indexes:    {len(already_marked) + len(sorted_unmarked)}")
    if len(already_marked) + len(sorted_unmarked) + len(unsorted_files) > 0:
        pct = (
            (len(already_marked) + len(sorted_unmarked))
            / (len(already_marked) + len(sorted_unmarked) + len(unsorted_files))
            * 100
        )
        print(f"  Percentage sorted:           {pct:.1f}%")
    print()

    if unsorted_files:
        print("⚠️  WARNING: Some URL index files are not sorted!")
        print("   These files need to be regenerated or sorted.")
        return 1

    if not args.verify_only and sorted_unmarked:
        print(f"✅ Marked {len(sorted_unmarked)} files as sorted")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
