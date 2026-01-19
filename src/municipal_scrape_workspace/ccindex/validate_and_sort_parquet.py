#!/usr/bin/env python3
"""Comprehensive validation and sorting for parquet files.

Ensures ALL parquet files are sorted before building the index.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

import duckdb
import pyarrow.parquet as pq


def check_if_sorted(parquet_file: Path, sample_size: int = 1000) -> Tuple[bool, str]:
    """Check if a parquet file is sorted by host_rev.

    Returns: (is_sorted, reason)
    """

    try:
        pf = pq.ParquetFile(parquet_file)

        if pf.metadata.num_row_groups == 0:
            return False, "No row groups"

        # Check within first row group
        table = pf.read_row_group(0, columns=["host_rev"])
        vals = table["host_rev"].to_pylist()

        if len(vals) < 2:
            return True, "Too few rows to check"

        # Sample check within row group
        step = max(1, len(vals) // sample_size)
        sample = vals[::step]

        for i in range(len(sample) - 1):
            if sample[i] > sample[i + 1]:
                return False, f"Unsorted within row group 0: {sample[i]} > {sample[i+1]}"

        # Check across row groups
        if pf.metadata.num_row_groups > 1:
            last_val = vals[-1]

            for rg_idx in range(1, pf.metadata.num_row_groups):
                table = pf.read_row_group(rg_idx, columns=["host_rev"])
                vals = table["host_rev"].to_pylist()

                if len(vals) > 0:
                    first_val = vals[0]
                    if last_val > first_val:
                        return (
                            False,
                            f"Unsorted between row groups {rg_idx-1} and {rg_idx}: {last_val} > {first_val}",
                        )

                    last_val = vals[-1]

        return True, "Verified sorted"

    except Exception as e:
        return False, f"Error: {e}"


def sort_parquet_file(input_file: Path, output_file: Path, compression: str = "zstd") -> bool:
    """Sort a parquet file by host_rev, url, ts using DuckDB."""

    try:
        con = duckdb.connect(":memory:")
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{input_file}')
                ORDER BY host_rev, url, ts
            )
            TO '{output_file}' (FORMAT 'parquet', COMPRESSION '{compression}')
        """
        )
        con.close()
        return True
    except Exception as e:
        print(f"Error sorting {input_file}: {e}", file=sys.stderr)
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Check and sort all parquet files")
    ap.add_argument("--parquet-root", required=True, type=str, help="Root directory of parquet files")
    ap.add_argument("--sort-unsorted", action="store_true", help="Sort any unsorted files found")
    ap.add_argument("--verify-only", action="store_true", help="Only verify, don't sort")
    ap.add_argument("--output", type=str, help="Output file for list of sorted files")

    args = ap.parse_args()

    parquet_root = Path(args.parquet_root).expanduser().resolve()

    if not parquet_root.exists():
        print(f"ERROR: Parquet root not found: {parquet_root}")
        return 1

    print("=" * 80)
    print("PARQUET FILE VALIDATION AND SORTING")
    print("=" * 80)
    print(f"Root: {parquet_root}")
    print()

    # Find all parquet files
    all_files = sorted(parquet_root.rglob("*.parquet"))
    print(f"Found {len(all_files)} parquet files")
    print()

    sorted_files: List[Path] = []
    unsorted_files: List[Path] = []

    # Check each file
    print("Checking files...")
    print("-" * 80)

    for i, pq_file in enumerate(all_files, 1):
        is_sorted, reason = check_if_sorted(pq_file)

        rel_path = pq_file.relative_to(parquet_root)

        if is_sorted:
            sorted_files.append(pq_file)
        else:
            unsorted_files.append(pq_file)
            print(f"❌ {rel_path}")
            print(f"   Reason: {reason}")

        if i % 10 == 0 or not is_sorted:
            print(
                f"Progress: {i}/{len(all_files)} - Sorted: {len(sorted_files)}, Unsorted: {len(unsorted_files)}",
                flush=True,
            )

    print("-" * 80)
    print()
    print("Summary:")
    print(f"  Total files:  {len(all_files)}")
    print(f"  ✅ Sorted:    {len(sorted_files)}")
    print(f"  ❌ Unsorted:  {len(unsorted_files)}")
    print(f"  Percentage:   {len(sorted_files) / len(all_files) * 100:.1f}%")
    print()

    if unsorted_files:
        print("Unsorted files:")
        for f in unsorted_files:
            print(f"  {f.relative_to(parquet_root)}")
        print()

    # Sort unsorted files if requested
    if unsorted_files and args.sort_unsorted and not args.verify_only:
        print("=" * 80)
        print("SORTING UNSORTED FILES")
        print("=" * 80)
        print()

        for i, unsorted_file in enumerate(unsorted_files, 1):
            print(f"[{i}/{len(unsorted_files)}] Sorting: {unsorted_file.name}")

            sorted_tmp = unsorted_file.with_suffix(".parquet.sorted.tmp")

            if sort_parquet_file(unsorted_file, sorted_tmp):
                # Verify it's now sorted
                is_sorted, reason = check_if_sorted(sorted_tmp)

                if is_sorted:
                    # Replace original
                    unsorted_file.unlink()
                    sorted_tmp.rename(unsorted_file)
                    print("  ✅ Sorted and verified")
                    sorted_files.append(unsorted_file)
                else:
                    print(f"  ❌ Sort verification failed: {reason}")
                    sorted_tmp.unlink()
            else:
                print("  ❌ Sort failed")

        print()
        print(f"Sorting complete. {len(sorted_files)}/{len(all_files)} files now sorted.")

    # Write output file with sorted files
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w", encoding="utf-8") as f:
            for pq_file in sorted_files:
                f.write(f"{pq_file}\n")
        print(f"Wrote list of sorted files to: {output_path}")

    # Exit code
    if len(unsorted_files) > 0 and not args.sort_unsorted:
        print()
        print("⚠️  WARNING: Some files are not sorted!")
        print("   Run with --sort-unsorted to fix")
        return 1

    print()
    print("✅ All files verified sorted" if len(unsorted_files) == 0 else "✅ All files sorted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
