#!/usr/bin/env python3
"""Repeat a DuckDB Parquet sort in a subprocess until a segfault occurs.

Why this exists:
- Some DuckDB native segfaults during wide-key sorts are intermittent.
- We want a harness that keeps retrying until it catches one, then immediately
  launches the row-group bisection minimizer.

Exit codes:
- 0: segfault observed (and minimizer launched)
- 1: no segfault observed within max attempts
- 2: invalid arguments / missing input
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path


SIGSEGV_RETURNS = {-11, 139}  # -SIGNAL (direct) or 128+signal (shells)


def is_segfault_rc(rc: int) -> bool:
    return rc in SIGSEGV_RETURNS


def run_duckdb_sort_subprocess(
    *,
    in_path: Path,
    out_path: Path,
    temp_dir: Path,
    order_by: str,
    memory_gb: float,
    read_via_arrow: bool,
    row_group_size: int | None,
    threads: int,
) -> tuple[int, str]:
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
        threads = __THREADS__

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)

        con = duckdb.connect(database=":memory:")
        con.execute("SET memory_limit='" + str(memory_gb) + "GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET temp_directory='" + temp_dir.replace("'", "''") + "'")
        con.execute("PRAGMA threads=" + str(int(threads)))

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
        .replace("__THREADS__", repr(int(threads)))
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Repeat DuckDB sort until segfault, then bisect")
    ap.add_argument("--input-parquet", required=True, type=str)
    ap.add_argument("--work-dir", default="/tmp/duckdb_sort_repeat", type=str)
    ap.add_argument("--order-by", default="host_rev, url, ts", type=str)
    ap.add_argument("--memory-gb", default=4.0, type=float)
    ap.add_argument("--row-group-size", default=71680, type=int)
    ap.add_argument("--threads", default=1, type=int)
    ap.add_argument("--read-via-arrow", action="store_true", default=False)
    ap.add_argument("--max-attempts", default=0, type=int, help="0 means infinite")
    ap.add_argument("--sleep-seconds", default=0.0, type=float)

    ap.add_argument(
        "--keep-ok-output",
        action="store_true",
        default=False,
        help="Keep the large sorted.parquet for successful attempts (default: delete to avoid filling disk).",
    )
    ap.add_argument(
        "--keep-ok-temp",
        action="store_true",
        default=False,
        help="Keep DuckDB temp directories for successful attempts (default: delete).",
    )

    ap.add_argument(
        "--bisect",
        action="store_true",
        default=True,
        help="Launch the row-group bisection minimizer immediately after a segfault (default: on).",
    )
    ap.add_argument(
        "--no-bisect",
        action="store_false",
        dest="bisect",
        help="Do not run minimizer; just stop after catching a segfault.",
    )

    ap.add_argument("--bisect-max-steps", default=25, type=int)
    ap.add_argument("--bisect-attempts-per-test", default=3, type=int)

    args = ap.parse_args()

    in_path = Path(args.input_parquet).expanduser().resolve()
    if not in_path.exists():
        print(f"ERROR: input not found: {in_path}")
        return 2

    work_dir = Path(args.work_dir).expanduser().resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    row_group_size = int(args.row_group_size)
    rgs = row_group_size if row_group_size > 0 else None

    max_attempts = int(args.max_attempts)
    attempt = 0

    print(f"Input: {in_path}")
    print(
        "Config: "
        f"order_by={args.order_by!r} memory_gb={float(args.memory_gb)} "
        f"row_group_size={rgs} threads={int(args.threads)} read_via_arrow={bool(args.read_via_arrow)}"
    )

    while True:
        attempt += 1
        attempt_dir = work_dir / f"attempt_{attempt:06d}"
        attempt_dir.mkdir(parents=True, exist_ok=True)

        out_path = attempt_dir / "sorted.parquet"
        temp_dir = attempt_dir / "duckdb_tmp"
        log_path = attempt_dir / "duckdb_sort_tail.log"

        rc, out_tail = run_duckdb_sort_subprocess(
            in_path=in_path,
            out_path=out_path,
            temp_dir=temp_dir,
            order_by=str(args.order_by),
            memory_gb=float(args.memory_gb),
            read_via_arrow=bool(args.read_via_arrow),
            row_group_size=rgs,
            threads=int(args.threads),
        )
        log_path.write_text(out_tail)

        status = "SEGV" if is_segfault_rc(rc) else ("OK" if rc == 0 else f"ERR(rc={rc})")
        print(f"Attempt {attempt}: {status}  (log: {log_path})", flush=True)

        # Avoid filling disk during long runs.
        if rc == 0:
            if not bool(args.keep_ok_output):
                try:
                    out_path.unlink(missing_ok=True)
                except Exception:
                    pass
            if not bool(args.keep_ok_temp):
                try:
                    if temp_dir.exists():
                        for child in temp_dir.glob("**/*"):
                            try:
                                if child.is_file() or child.is_symlink():
                                    child.unlink(missing_ok=True)
                            except Exception:
                                pass
                        # Try to remove directories bottom-up.
                        for child_dir in sorted([p for p in temp_dir.glob("**/*") if p.is_dir()], reverse=True):
                            try:
                                child_dir.rmdir()
                            except Exception:
                                pass
                        try:
                            temp_dir.rmdir()
                        except Exception:
                            pass
                except Exception:
                    pass

        if is_segfault_rc(rc):
            print("\nCaught a segfault return code.")
            if not args.bisect:
                return 0

            # Launch minimizer in a *new process* so it can keep running even if this
            # process is interrupted.
            bisect_dir = work_dir / f"bisect_from_attempt_{attempt:06d}"
            bisect_dir.mkdir(parents=True, exist_ok=True)

            cmd = [
                sys.executable,
                str(Path(__file__).parent / "minimize_duckdb_sort_segfault.py"),
                "--input-parquet",
                str(in_path),
                "--work-dir",
                str(bisect_dir),
                "--order-by",
                str(args.order_by),
                "--memory-gb",
                str(float(args.memory_gb)),
                "--row-group-size",
                str(int(args.row_group_size)),
                "--max-steps",
                str(int(args.bisect_max_steps)),
                "--attempts-per-test",
                str(int(args.bisect_attempts_per_test)),
            ]
            if bool(args.read_via_arrow):
                cmd.append("--read-via-arrow")

            print("Launching minimizer:")
            print(" ", " ".join(cmd))
            p = subprocess.run(cmd)
            return 0 if p.returncode == 0 else 0

        if max_attempts > 0 and attempt >= max_attempts:
            print("No segfault observed within max attempts.")
            return 1

        if float(args.sleep_seconds) > 0:
            time.sleep(float(args.sleep_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
