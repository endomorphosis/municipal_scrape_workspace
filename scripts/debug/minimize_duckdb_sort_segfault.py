#!/usr/bin/env python3
"""Minimize DuckDB sort segfaults by bisecting a Parquet shard.

Goal: help identify whether a specific URL/value slice triggers DuckDB native crashes
(e.g., SIGSEGV during `ORDER BY host_rev, url, ts`).

Strategy:
- Split the input Parquet by *row-group ranges* (fast to reason about; stable boundaries).
- Materialize a candidate subset Parquet.
- Run the DuckDB sort in a separate subprocess.
- If it segfaults, bisect further to find a minimal row-group window that still crashes.

This is intentionally a debug/ops tool; it does not mutate the source shard.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


SIGSEGV_RETURNS = {-11, 139}  # subprocess returns -SIGNAL when exec'd directly; shells may use 128+sig


@dataclass(frozen=True)
class RowGroupRange:
    start: int  # inclusive
    end: int  # exclusive

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < 0 or self.end < self.start:
            raise ValueError(f"Invalid range: {self.start}:{self.end}")

    def size(self) -> int:
        return self.end - self.start

    def mid(self) -> int:
        return self.start + self.size() // 2

    def left(self) -> "RowGroupRange":
        m = self.mid()
        return RowGroupRange(self.start, m)

    def right(self) -> "RowGroupRange":
        m = self.mid()
        return RowGroupRange(m, self.end)

    def __str__(self) -> str:
        return f"rg[{self.start}:{self.end})"


def _ensure_empty_dir(dir_path: Path) -> None:
    if dir_path.exists():
        shutil.rmtree(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)


def _iter_row_groups(range_: RowGroupRange) -> list[int]:
    return list(range(range_.start, range_.end))


def write_subset_parquet(
    pf: pq.ParquetFile,
    range_: RowGroupRange,
    out_path: Path,
    *,
    preserve_dictionary: bool = True,
) -> int:
    # Stream row-groups to keep memory bounded.
    # NOTE: output row-group boundaries won't match input, but that is fine for minimization.
    writer: pq.ParquetWriter | None = None
    total_rows = 0
    try:
        for rg_index in _iter_row_groups(range_):
            rg_table = pf.read_row_group(rg_index)
            if writer is None:
                writer = pq.ParquetWriter(
                    where=str(out_path),
                    schema=rg_table.schema,
                    compression="zstd",
                    use_dictionary=preserve_dictionary,
                )
            writer.write_table(rg_table)
            total_rows += rg_table.num_rows
    finally:
        if writer is not None:
            writer.close()
    return total_rows


def duckdb_sort_subprocess(
    in_path: Path,
    out_path: Path,
    *,
    order_by: str,
    memory_gb: float,
    temp_dir: Path,
    read_via_arrow: bool,
    row_group_size: int | None,
) -> tuple[int, str]:
    # Run in a *fresh process* so a segfault doesn't take down the minimizer.
    # We inline the code to avoid needing to import repo modules.
    template = textwrap.dedent(
        """
        import os
        import duckdb

        in_path = __IN_PATH__
        out_path = __OUT_PATH__
        order_by = __ORDER_BY__
        memory_gb = __MEMORY_GB__
        temp_dir = __TEMP_DIR__
        read_via_arrow = __READ_VIA_ARROW__
        row_group_size = __ROW_GROUP_SIZE__

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)

        con = duckdb.connect(database=":memory:")
        con.execute("SET memory_limit='" + str(memory_gb) + "GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET temp_directory='" + temp_dir.replace("'", "''") + "'")
        con.execute("PRAGMA threads=1")

        if read_via_arrow:
            import pyarrow.parquet as pq
            t = pq.read_table(in_path)
            con.register("_src", t)
            src_sql = "_src"
        else:
            in_path_sql = in_path.replace("'", "''")
            src_sql = "read_parquet('" + in_path_sql + "')"

        out_path_sql = out_path.replace("'", "''")

        copy_opts = ["FORMAT 'parquet'", "COMPRESSION 'zstd'"]
        if row_group_size is not None and int(row_group_size) > 0:
            copy_opts.append("ROW_GROUP_SIZE " + str(int(row_group_size)))
        opt_sql = ", ".join(copy_opts)

        q = "COPY (SELECT * FROM " + src_sql + " ORDER BY " + order_by + ") TO '" + out_path_sql + "' (" + opt_sql + ")"
        con.execute(q)
        print("OK")
        """
    ).strip()

    code = (
        template.replace("__IN_PATH__", repr(str(in_path)))
        .replace("__OUT_PATH__", repr(str(out_path)))
        .replace("__ORDER_BY__", repr(str(order_by)))
        .replace("__MEMORY_GB__", repr(float(memory_gb)))
        .replace("__TEMP_DIR__", repr(str(temp_dir)))
        .replace("__READ_VIA_ARROW__", repr(bool(read_via_arrow)))
        .replace("__ROW_GROUP_SIZE__", repr(int(row_group_size) if row_group_size is not None else None))
    )

    env = dict(os.environ)
    env.setdefault("PYTHONFAULTHANDLER", "1")

    p = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    return p.returncode, p.stdout[-8000:]


def is_segfault_returncode(rc: int) -> bool:
    return rc in SIGSEGV_RETURNS


def summarize_url_column(table: pa.Table, url_col: str = "url") -> str:
    if url_col not in table.column_names:
        return f"No '{url_col}' column in subset (cols={table.column_names})"

    col = table[url_col]
    chunked = col.combine_chunks()

    # Best-effort length stats depending on type.
    try:
        arr = chunked
        if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
            lens = pc.utf8_length(arr)
        elif pa.types.is_binary(arr.type) or pa.types.is_large_binary(arr.type):
            lens = pc.binary_length(arr)
        else:
            return f"'{url_col}' type={arr.type} (not string/binary); skipping length stats"

        nulls = arr.null_count
        max_len = pc.max(lens).as_py() if lens.null_count < lens.length else None
        p99 = pc.quantile(lens, q=[0.99], interpolation="linear").to_pylist()[0] if lens.length else None

        # Grab a few examples of very long URLs.
        if max_len is not None and max_len > 0:
            # Filter to top ~20 longest; avoid huge memory by doing a partial approach.
            # Sort indices by length descending.
            take_n = 10
            lengths_py = lens.to_pylist()
            idxs = sorted(range(len(lengths_py)), key=lambda i: (lengths_py[i] or -1), reverse=True)[:take_n]
            examples = []
            for i in idxs:
                v = arr[i].as_py()
                if v is None:
                    continue
                v = str(v)
                examples.append(v[:200])
        else:
            examples = []

        return (
            f"url.type={arr.type}, rows={arr.length}, nulls={nulls}, "
            f"p99_len≈{p99}, max_len={max_len}, longest_examples(sampled, truncated)=\n"
            + "\n".join(f"  - {e}" for e in examples)
        )
    except Exception as e:
        return f"Failed URL stats: {e}"


def bisect_to_min_crash(
    pf: pq.ParquetFile,
    in_path: Path,
    *,
    work_dir: Path,
    order_by: str,
    memory_gb: float,
    read_via_arrow: bool,
    row_group_size: int | None,
    attempts_per_test: int,
    min_row_groups: int,
    max_steps: int,
) -> tuple[RowGroupRange | None, list[str]]:
    logs: list[str] = []
    total_rgs = pf.metadata.num_row_groups
    current = RowGroupRange(0, total_rgs)

    attempts = max(1, int(attempts_per_test))

    def test_materialized(range_: RowGroupRange) -> bool:
        _ensure_empty_dir(work_dir)
        subset_path = work_dir / "subset.parquet"
        out_path = work_dir / "sorted.parquet"
        temp_dir = work_dir / "duckdb_tmp"
        nrows = write_subset_parquet(pf, range_, subset_path)
        segfault_any = False
        last_rc = 0
        last_out = ""
        for attempt in range(1, attempts + 1):
            rc, out = duckdb_sort_subprocess(
                subset_path,
                out_path,
                order_by=order_by,
                memory_gb=memory_gb,
                temp_dir=temp_dir,
                read_via_arrow=read_via_arrow,
                row_group_size=row_group_size,
            )
            last_rc, last_out = rc, out
            if is_segfault_returncode(rc):
                segfault_any = True
                break
        logs.append(
            f"TEST {range_} rows={nrows} rc={last_rc} segfault={segfault_any} attempts={attempts}"
        )
        if last_rc != 0 and not segfault_any:
            logs.append("OUTPUT(tail):\n" + last_out)
        return segfault_any

    def test_original() -> bool:
        _ensure_empty_dir(work_dir)
        out_path = work_dir / "sorted_from_original.parquet"
        temp_dir = work_dir / "duckdb_tmp"
        segfault_any = False
        last_rc = 0
        last_out = ""
        for attempt in range(1, attempts + 1):
            rc, out = duckdb_sort_subprocess(
                in_path,
                out_path,
                order_by=order_by,
                memory_gb=memory_gb,
                temp_dir=temp_dir,
                read_via_arrow=read_via_arrow,
                row_group_size=row_group_size,
            )
            last_rc, last_out = rc, out
            if is_segfault_returncode(rc):
                segfault_any = True
                break
        logs.append(f"TEST original rows=? rc={last_rc} segfault={segfault_any} attempts={attempts}")
        if last_rc != 0 and not segfault_any:
            logs.append("OUTPUT(tail):\n" + last_out)
        return segfault_any

    # First confirm the full file reproduces, without materializing.
    logs.append(f"Input: {in_path} row_groups={total_rgs}")
    if not test_original():
        logs.append("Full shard did NOT segfault under this harness. Try: larger memory_gb, read_via_arrow flip, or different order_by.")
        return None, logs

    steps = 0
    while current.size() > max(min_row_groups, 1) and steps < max_steps:
        steps += 1
        left = current.left()
        right = current.right()

        # Prefer the smallest crashing half.
        if left.size() >= 1 and test_materialized(left):
            current = left
            continue
        if right.size() >= 1 and test_materialized(right):
            current = right
            continue

        logs.append(f"Neither half crashed at step={steps}. Crash may be size-sensitive; stopping at {current}.")
        break

    logs.append(f"MIN_RANGE {current}")
    return current, logs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-parquet", required=True, type=str)
    ap.add_argument("--work-dir", default="/tmp/duckdb_sort_segfault_min", type=str)
    ap.add_argument("--order-by", default="host_rev, url, ts", type=str)
    ap.add_argument("--memory-gb", default=4.0, type=float)
    ap.add_argument("--row-group-size", default=71680, type=int)
    ap.add_argument("--read-via-arrow", action="store_true", default=False)
    ap.add_argument(
        "--attempts-per-test",
        default=1,
        type=int,
        help=(
            "Run each sort test up to N times and treat it as crashing if any attempt segfaults. "
            "Useful for intermittent native crashes (default: 1)."
        ),
    )
    ap.add_argument("--min-row-groups", default=1, type=int)
    ap.add_argument("--max-steps", default=25, type=int)
    args = ap.parse_args()

    in_path = Path(args.input_parquet).expanduser().resolve()
    work_dir = Path(args.work_dir).expanduser().resolve()

    if not in_path.exists():
        print(f"ERROR: input not found: {in_path}")
        return 2

    pf = pq.ParquetFile(in_path)

    min_range, logs = bisect_to_min_crash(
        pf,
        in_path,
        work_dir=work_dir,
        order_by=args.order_by,
        memory_gb=float(args.memory_gb),
        read_via_arrow=bool(args.read_via_arrow),
        row_group_size=(int(args.row_group_size) if int(args.row_group_size) > 0 else None),
        attempts_per_test=int(args.attempts_per_test),
        min_row_groups=int(args.min_row_groups),
        max_steps=int(args.max_steps),
    )

    print("\n".join(logs))

    if min_range is None:
        return 1

    # Materialize the minimal subset and print URL stats to help eyeballing "weird" rows.
    _ensure_empty_dir(work_dir)
    subset_path = work_dir / "subset_min.parquet"
    write_subset_parquet(pf, min_range, subset_path)

    try:
        t = pq.read_table(subset_path)
        print("\n--- URL STATS (subset_min) ---")
        print(summarize_url_column(t, "url"))
    except Exception as e:
        print(f"Failed to read subset_min for stats: {e}")

    print(f"\nWrote minimal crashing subset to: {subset_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
