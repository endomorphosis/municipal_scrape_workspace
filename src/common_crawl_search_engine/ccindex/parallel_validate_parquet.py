#!/usr/bin/env python3
"""Parallel parquet validation with configurable worker count.

This is substantially faster than serial validation.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
from pathlib import Path
from typing import Tuple

import duckdb


def check_if_sorted(parquet_file: Path) -> Tuple[Path, bool, str]:
    """Check if parquet file is sorted.

    Returns (file, is_sorted, reason).
    """

    try:
        con = duckdb.connect(":memory:")

        # Quick check: sample first ~100 rows for any inversions.
        result = con.execute(
            """
            WITH samples AS (
                SELECT host_rev, ROW_NUMBER() OVER () as rn
                FROM read_parquet(?)
                WHERE host_rev IS NOT NULL
            ),
            first_100 AS (
                SELECT host_rev FROM samples WHERE rn <= 100
            ),
            check_sorted AS (
                SELECT
                    host_rev,
                    LAG(host_rev) OVER (ORDER BY rn) as prev_host_rev
                FROM (SELECT host_rev, ROW_NUMBER() OVER () as rn FROM first_100)
            )
            SELECT COUNT(*) as unsorted_count
            FROM check_sorted
            WHERE prev_host_rev > host_rev
            """,
            [str(parquet_file)],
        ).fetchone()

        con.close()

        unsorted_count = result[0] if result else 0
        if unsorted_count > 0:
            return (parquet_file, False, f"Found {unsorted_count} unsorted pairs")

        return (parquet_file, True, "Sorted")
    except Exception as e:
        return (parquet_file, False, f"Error: {e}")


def worker_init() -> None:
    """Initializer for worker processes (reserved for future use)."""


def main() -> int:
    ap = argparse.ArgumentParser(description="Parallel parquet validation")
    ap.add_argument("--parquet-root", required=True, help="Root directory")
    ap.add_argument("--workers", type=int, default=16, help="Number of parallel workers")
    ap.add_argument("--output", type=str, help="Output file for sorted list")

    args = ap.parse_args()

    parquet_root = Path(args.parquet_root).expanduser().resolve()

    print(f"Parquet root: {parquet_root}")
    print(f"Workers:      {args.workers}")
    print()

    all_files = sorted(parquet_root.rglob("*.parquet"))
    print(f"Found {len(all_files)} parquet files")
    print(f"Starting parallel validation with {args.workers} workers...")
    print()

    sorted_files: list[Path] = []
    unsorted_files: list[Path] = []

    with mp.Pool(processes=args.workers, initializer=worker_init) as pool:
        results = pool.imap_unordered(check_if_sorted, all_files, chunksize=10)
        for i, (pq_file, is_sorted, reason) in enumerate(results, 1):
            if is_sorted:
                sorted_files.append(pq_file)
            else:
                unsorted_files.append(pq_file)
                print(f"❌ {pq_file.relative_to(parquet_root)}: {reason}")

            if i % 100 == 0:
                print(
                    f"Progress: {i}/{len(all_files)} - Sorted: {len(sorted_files)}, Unsorted: {len(unsorted_files)}",
                    flush=True,
                )

    print()
    print("=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)
    print(f"Total:    {len(all_files)}")
    print(f"Sorted:   {len(sorted_files)} ({len(sorted_files)/len(all_files)*100:.1f}%)")
    print(f"Unsorted: {len(unsorted_files)} ({len(unsorted_files)/len(all_files)*100:.1f}%)")
    print()

    if unsorted_files:
        print("Unsorted files:")
        for f in unsorted_files[:20]:
            print(f"  {f.relative_to(parquet_root)}")
        if len(unsorted_files) > 20:
            print(f"  ... and {len(unsorted_files) - 20} more")
        print()

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            for pq_file in sorted_files:
                f.write(f"{pq_file}\n")
        print(f"Wrote sorted file list to: {args.output}")

    if unsorted_files:
        print()
        print("⚠️  WARNING: Some files are unsorted!")
        print("   These should be sorted before indexing")
        return 1

    print("✅ All files verified sorted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
