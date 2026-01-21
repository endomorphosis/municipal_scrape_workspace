#!/usr/bin/env python3
"""Validate parquet files are sorted and mark them with .sorted extension.

Compatibility tool for older pipeline runs.

This script:
1) Skips files already marked as *.sorted.parquet
2) Validates unmarked parquet files are sorted by host_rev
3) Marks sorted files by renaming to *.sorted.parquet
4) Optionally sorts unsorted files (DuckDB external sort) and marks them

Notes:
- Sorting uses `ORDER BY host_rev, url, ts`.
- This script intentionally ignores parquet files under hidden/temp directories
  (e.g. `.duckdb_sort_tmp`, `.cc_sort_work_*`) so partial/resume runs don't
  accidentally treat scratch artifacts as inputs.
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, as_completed, wait
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb
import pyarrow.parquet as pq


def _is_hidden_path(parquet_root: Path, p: Path) -> bool:
    """Return True if the file is under a hidden directory relative to parquet_root."""

    try:
        rel = p.relative_to(parquet_root)
    except Exception:
        return False
    # Ignore hidden directories (not the file name itself).
    return any(part.startswith(".") for part in rel.parts[:-1])


def _iter_candidate_parquet_files(parquet_root: Path) -> List[Path]:
    """Find parquet files, skipping hidden/temp artifacts."""

    files: List[Path] = []
    for p in parquet_root.rglob("*.parquet"):
        try:
            if not p.is_file():
                continue
            if _is_hidden_path(parquet_root, p):
                continue
            # Skip obvious temp outputs.
            if p.name.endswith(".tmp.parquet") or p.name.endswith(".sorted.tmp"):
                continue
            files.append(p)
        except Exception:
            continue
    return sorted(files)


def is_sorted_by_content(parquet_file: Path, sample_size: int = 1000) -> Tuple[bool, str]:
    """Check if a parquet file is sorted by host_rev.

    Returns: (is_sorted, reason)
    """

    try:
        pf = pq.ParquetFile(parquet_file)

        if pf.metadata is None or pf.metadata.num_row_groups == 0:
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
                return False, f"Unsorted within row group: {sample[i]} > {sample[i+1]}"

        # Check across row groups if multiple exist
        if pf.metadata.num_row_groups > 1:
            last_val = vals[-1]

            for rg_idx in range(1, min(pf.metadata.num_row_groups, 10)):
                table = pf.read_row_group(rg_idx, columns=["host_rev"])
                vals = table["host_rev"].to_pylist()

                if len(vals) > 0:
                    first_val = vals[0]
                    if last_val > first_val:
                        return False, f"Unsorted between row groups: {last_val} > {first_val}"

                    last_val = vals[-1]

        return True, "Verified sorted"

    except Exception as e:
        return False, f"Error: {e}"


def sort_parquet_file(
    input_file: Path,
    output_file: Path,
    memory_limit_gb: float = 4.0,
    temp_directory: Optional[Path] = None,
) -> bool:
    """Sort a parquet file by host_rev, url, ts using DuckDB."""

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"SET memory_limit='{memory_limit_gb}GB'")
        # Isolate DuckDB temp usage per-sort to avoid contention.
        td = temp_directory if temp_directory else output_file.parent
        con.execute(f"SET temp_directory='{td}'")
        con.execute("PRAGMA threads=1")

        # DuckDB parameter binding inside COPY/TO can be surprising; use escaped literals.
        in_path = str(input_file).replace("'", "''")
        out_path = str(output_file).replace("'", "''")
        con.execute(
            """
            COPY (
                SELECT * FROM read_parquet('{in_path}')
                ORDER BY host_rev, url, ts
            )
            TO '{out_path}' (FORMAT 'parquet', COMPRESSION 'zstd')
            """.format(in_path=in_path, out_path=out_path)
        )
        con.close()
        return True
    except Exception as e:
        print(f"❌ Error sorting {input_file.name}: {e}", file=sys.stderr)
        return False


def check_single_file(pq_file: Path, parquet_root: Path, verify_only: bool) -> Tuple[str, Path, bool, str]:
    """Check a single parquet file and optionally mark it as sorted.

    Returns: (status, file_path, is_sorted, reason)
    status: 'already_marked', 'sorted_unmarked', 'unsorted', 'error'
    """

    # Skip already marked files
    if ".sorted." in pq_file.name or pq_file.name.endswith(".sorted.parquet"):
        return ("already_marked", pq_file, True, "Already marked")

    try:
        is_sorted, reason = is_sorted_by_content(pq_file)

        if is_sorted:
            if verify_only:
                return ("sorted_unmarked", pq_file, True, "Sorted but not marked (verify-only)")

            if pq_file.name.endswith(".gz.parquet"):
                new_name = pq_file.name.replace(".gz.parquet", ".gz.sorted.parquet")
            else:
                new_name = pq_file.name.replace(".parquet", ".sorted.parquet")

            new_path = pq_file.parent / new_name

            # If a marked file already exists, treat the unmarked one as a duplicate.
            if new_path.exists():
                try:
                    pq_file.unlink()
                except Exception:
                    pass
                return ("sorted_unmarked", new_path, True, f"Marked already existed; removed duplicate")

            pq_file.rename(new_path)
            return ("sorted_unmarked", new_path, True, f"Marked as {new_name}")

        return ("unsorted", pq_file, False, reason)

    except Exception as e:
        return ("error", pq_file, False, str(e))


def sort_and_mark_one(args: Tuple[str, float, str]) -> Tuple[str, bool, str, str]:
    """Sort one parquet file and mark it as *.sorted.parquet.

    Args tuple: (unsorted_file_path, memory_per_sort_gb, temp_root)
    Returns: (source_path, success, message, output_path)
    """

    src_path, memory_per_sort_gb, temp_root = args
    src = Path(src_path)
    tmp_root = Path(temp_root)
    work_dir: Optional[Path] = None
    duckdb_temp_dir: Optional[Path] = None

    try:
        # Write tmp output in destination directory so the final rename is atomic.
        safe = src.name.replace(os.sep, "_")
        work_dir = src.parent / f".cc_sort_work_{safe}"
        work_dir.mkdir(parents=True, exist_ok=True)

        # DuckDB spill temp directory MUST be unique per sort when running in parallel.
        duckdb_temp_dir = tmp_root / f"duckdb_sort_{safe}"
        duckdb_temp_dir.mkdir(parents=True, exist_ok=True)

        sorted_tmp = work_dir / f"{src.name}.tmp.parquet"
        if not sort_parquet_file(src, sorted_tmp, memory_per_sort_gb, temp_directory=duckdb_temp_dir):
            return str(src), False, "sort failed", ""

        ok, reason = is_sorted_by_content(sorted_tmp)
        if not ok:
            try:
                sorted_tmp.unlink()
            except Exception:
                pass
            return str(src), False, f"verification failed: {reason}", ""

        if src.name.endswith(".gz.parquet"):
            new_name = src.name.replace(".gz.parquet", ".gz.sorted.parquet")
        else:
            new_name = src.name.replace(".parquet", ".sorted.parquet")
        out = src.parent / new_name

        # If a sorted output already exists, treat as success and remove duplicate unsorted.
        if out.exists():
            try:
                src.unlink()
            except Exception:
                pass
            if work_dir is not None:
                shutil.rmtree(work_dir, ignore_errors=True)
            return str(src), True, "sorted output already existed; removed duplicate", str(out)

        try:
            sorted_tmp.replace(out)
        except OSError:
            shutil.move(str(sorted_tmp), str(out))

        try:
            src.unlink()
        except Exception:
            pass

        if work_dir is not None:
            shutil.rmtree(work_dir, ignore_errors=True)
        if duckdb_temp_dir is not None:
            shutil.rmtree(duckdb_temp_dir, ignore_errors=True)

        return str(src), True, "sorted + marked", str(out)

    except Exception as e:
        try:
            if work_dir is not None:
                shutil.rmtree(work_dir, ignore_errors=True)
            if duckdb_temp_dir is not None:
                shutil.rmtree(duckdb_temp_dir, ignore_errors=True)
        except Exception:
            pass
        return str(src), False, f"exception: {e}", ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate and mark sorted parquet files")
    ap.add_argument("--parquet-root", required=True, type=str, help="Root directory of parquet files")
    ap.add_argument("--sort-unsorted", action="store_true", help="Sort any unsorted files found")
    ap.add_argument("--verify-only", action="store_true", help="Only verify, don't mark or sort")
    ap.add_argument("--memory-per-sort", type=float, default=4.0, help="GB memory per sort operation")
    ap.add_argument("--workers", type=int, default=None, help="Number of parallel workers (default: CPU count)")
    ap.add_argument(
        "--sort-workers",
        type=int,
        default=1,
        help="Parallel workers for sorting unsorted files (default: 1; keep low for memory safety)",
    )
    ap.add_argument(
        "--temp-dir",
        type=str,
        default=None,
        help="Temp directory for DuckDB external sort spill (default: system temp)",
    )
    ap.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=30,
        help="Print a periodic heartbeat every N seconds during long phases (default: 30)",
    )

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

    all_files = _iter_candidate_parquet_files(parquet_root)
    print(f"Found {len(all_files)} parquet files")
    print()

    already_marked: List[Path] = []
    sorted_unmarked: List[Path] = []
    unsorted_files: List[Path] = []
    error_files: List[Tuple[Path, str]] = []

    print("Checking files...")
    print("-" * 80)

    num_workers = args.workers or multiprocessing.cpu_count()
    print(f"Using {num_workers} parallel workers")
    print()

    completed = 0
    heartbeat_seconds = max(1, int(args.heartbeat_seconds))
    start_check = time.monotonic()
    last_hb = start_check

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
                    print(f"[{completed}/{len(all_files)}] ✅ {rel_path} - {reason}")

                elif status == "unsorted":
                    unsorted_files.append(result_path)
                    print(f"[{completed}/{len(all_files)}] ❌ {rel_path} - UNSORTED: {reason}")

                elif status == "error":
                    error_files.append((result_path, reason))
                    print(f"[{completed}/{len(all_files)}] ⚠️  {rel_path} - ERROR: {reason}")

                if completed % 50 == 0:
                    print(
                        f"Progress: {completed}/{len(all_files)} - Marked: {len(already_marked)}, "
                        f"Sorted: {len(sorted_unmarked)}, Unsorted: {len(unsorted_files)}",
                        flush=True,
                    )

                now = time.monotonic()
                if now - last_hb >= heartbeat_seconds:
                    elapsed = now - start_check
                    print(
                        f"Heartbeat(check): {completed}/{len(all_files)} done in {elapsed/60:.1f} min "
                        f"(marked={len(already_marked)}, sorted={len(sorted_unmarked)}, "
                        f"unsorted={len(unsorted_files)}, errors={len(error_files)})",
                        flush=True,
                    )
                    last_hb = now

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
    total_sorted = len(already_marked) + len(sorted_unmarked)
    print(f"  Total sorted:          {total_sorted}")
    if all_files:
        print(f"  Percentage sorted:     {total_sorted / len(all_files) * 100:.1f}%")
    print()

    failed_count = 0
    if unsorted_files and args.sort_unsorted and not args.verify_only:
        print("=" * 80)
        print("SORTING UNSORTED FILES")
        print("=" * 80)
        print()

        sorted_count = 0

        sort_workers = max(1, int(args.sort_workers))
        temp_root = Path(args.temp_dir).expanduser().resolve() if args.temp_dir else Path(tempfile.gettempdir())
        temp_root.mkdir(parents=True, exist_ok=True)

        print(f"Sorting {len(unsorted_files)} file(s) with {sort_workers} worker(s)")
        with ProcessPoolExecutor(max_workers=sort_workers) as executor:
            work_items = [(str(p), float(args.memory_per_sort), str(temp_root)) for p in unsorted_files]
            futures = {executor.submit(sort_and_mark_one, item): item[0] for item in work_items}

            done = 0
            start_sort = time.monotonic()
            last_sort_hb = start_sort
            pending = set(futures.keys())

            while pending:
                finished, pending = wait(pending, timeout=heartbeat_seconds, return_when=FIRST_COMPLETED)

                if not finished:
                    now = time.monotonic()
                    if now - last_sort_hb >= heartbeat_seconds:
                        elapsed = now - start_sort
                        print(
                            f"Heartbeat(sort): {done}/{len(unsorted_files)} done in {elapsed/60:.1f} min "
                            f"(ok={sorted_count}, fail={failed_count}, pending={len(pending)})",
                            flush=True,
                        )
                        last_sort_hb = now
                    continue

                for fut in finished:
                    done += 1
                    src = futures[fut]
                    try:
                        _src_path, ok, msg, out_path = fut.result()
                        if ok and out_path:
                            sorted_count += 1
                            print(f"✅ [{done}/{len(unsorted_files)}] {Path(src).name} -> {Path(out_path).name}")
                        else:
                            failed_count += 1
                            print(f"❌ [{done}/{len(unsorted_files)}] {Path(src).name}: {msg}")
                    except Exception as e:
                        failed_count += 1
                        print(f"❌ [{done}/{len(unsorted_files)}] {Path(src).name}: exception {e}")

        print()
        print("Sorting complete:")
        print(f"  Succeeded: {sorted_count}")
        print(f"  Failed:    {failed_count}")
        print(f"  Total sorted files: {len(already_marked) + len(sorted_unmarked) + sorted_count}/{len(all_files)}")

    if unsorted_files and not args.sort_unsorted:
        print()
        print("⚠️  WARNING: Some files are not sorted!")
        print("   Run with --sort-unsorted to fix")
        return 1

    if args.sort_unsorted and failed_count:
        print()
        print(f"❌ Sorting failed for {failed_count} file(s)")
        return 2

    print()
    print("✅ All files verified and marked as sorted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
