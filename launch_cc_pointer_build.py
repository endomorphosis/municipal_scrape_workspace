#!/usr/bin/env python3
"""Launch multiple Common Crawl pointer-index builders in parallel.

This is a small orchestrator around build_cc_pointer_duckdb.py to increase
throughput by sharding work *within* a year (typically per collection).

It:
- enumerates collections under --input-root (optionally filtered by regex)
- splits them across --workers
- spawns one build_cc_pointer_duckdb.py process per worker using:
    --shard-by-collection + repeated --collections
- writes per-worker pid/log files

Example
  /home/barberb/municipal_scrape_workspace/.venv/bin/python launch_cc_pointer_build.py \
    --input-root /storage/ccindex \
        --db-dir /storage/ccindex_duckdb/cc_domain_by_collection \
    --parquet-out /storage/ccindex_parquet/cc_pointers_by_collection \
    --collections-regex 'CC-MAIN-2024-.*' \
    --workers 8 \
    --threads-per-worker 2 \
        --duckdb-index-mode domain \
        --progress-dir /storage/ccindex_duckdb/cc_domain_by_collection

Notes
- Safe resume is handled by build_cc_pointer_duckdb.py via cc_ingested_files.
- Output DBs will be named cc_pointers_<collection>.duckdb in --db-dir.
"""

from __future__ import annotations

import argparse
import os
import json
import re
import shlex
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen
from typing import Any, Dict, List, Optional, Sequence


@dataclass
class WorkerPlan:
    worker_index: int
    collections: List[str]
    pid_file: Path
    log_file: Path
    cdx_shard_mod: Optional[int] = None
    cdx_shard_rem: Optional[int] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "worker_index": int(self.worker_index),
            "collections": list(self.collections),
            "pid_file": str(self.pid_file),
            "log_file": str(self.log_file),
            "cdx_shard_mod": int(self.cdx_shard_mod) if self.cdx_shard_mod is not None else None,
            "cdx_shard_rem": int(self.cdx_shard_rem) if self.cdx_shard_rem is not None else None,
        }


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_pid(pid_file: Path) -> Optional[int]:
    try:
        return int(pid_file.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _list_collections(input_root: Path, collections_regex: Optional[str]) -> List[str]:
    rx = re.compile(collections_regex) if collections_regex else None
    cols: List[str] = []
    for entry in sorted(input_root.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if rx and not rx.search(name):
            continue
        cols.append(name)
    return cols


def _chunk_round_robin(items: Sequence[str], k: int) -> List[List[str]]:
    groups: List[List[str]] = [[] for _ in range(max(1, k))]
    for i, it in enumerate(items):
        groups[i % len(groups)].append(it)
    return groups


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _abspath_preserve_symlinks(path: Path) -> Path:
    # Avoid Path.resolve() because it dereferences symlinks. For virtualenv
    # Python, the symlink path matters.
    return Path(os.path.abspath(str(path.expanduser())))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--python",
        type=str,
        default=None,
        help="Python executable to run workers with (default: current interpreter)",
    )

    ap.add_argument(
        "--plan-file",
        type=str,
        default=None,
        help="Where to write a JSON plan for watchdog/restarts (default: <db-dir>/build_plan.json)",
    )
    ap.add_argument("--input-root", required=True, type=str, help="Root folder containing CC shards (e.g. /storage/ccindex)")
    ap.add_argument("--db-dir", required=True, type=str, help="Directory to place cc_pointers_<collection>.duckdb outputs")
    ap.add_argument("--parquet-out", type=str, default=None, help="Optional Parquet output root")
    ap.add_argument(
        "--parquet-action",
        type=str,
        default="skip-if-exists",
        choices=["write", "skip-if-exists", "skip"],
        help="Whether to write Parquet shards. Default 'skip-if-exists' avoids rewriting existing shards.",
    )
    ap.add_argument(
        "--parquet-validate",
        type=str,
        default="quick",
        choices=["quick", "none"],
        help="Validate existing Parquet shards before skipping them (passed through)",
    )

    ap.add_argument(
        "--duckdb-index-mode",
        type=str,
        default="url",
        choices=["url", "domain"],
        help="What to store in DuckDB: 'url' stores per-URL pointers; 'domain' stores only domain->shard/parquet mapping",
    )
    ap.add_argument(
        "--domain-index-action",
        type=str,
        default="append",
        choices=["append", "rebuild"],
        help="Only used with --duckdb-index-mode domain. 'append' keeps existing; 'rebuild' clears and rebuilds cc_domain_shards",
    )
    ap.add_argument(
        "--resume-require-parquet",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="When --parquet-out is set, only skip an already-ingested shard if its Parquet file exists (default: true)",
    )

    ap.add_argument("--collections-regex", type=str, default=None, help="Regex to select collections from --input-root")
    ap.add_argument(
        "--collections",
        action="append",
        default=None,
        help="Explicit collections list (repeatable). If provided, skips filesystem enumeration.",
    )

    ap.add_argument("--workers", type=int, default=4, help="Number of worker processes to spawn")
    ap.add_argument("--threads-per-worker", type=int, default=2, help="DuckDB threads per worker process")

    ap.add_argument(
        "--cdx-shard-mod",
        type=int,
        default=None,
        help=(
            "Optional: split each collection into mod disjoint shard-file partitions (cdx-XXXXX modulo mod == rem). "
            "Every remainder is processed; DBs will be cc_pointers_<collection>__m<mod>r<rem>.duckdb"
        ),
    )

    ap.add_argument("--progress-dir", type=str, default=None, help="Progress snapshot dir (passed through)")
    ap.add_argument("--progress-interval-seconds", type=int, default=30, help="Snapshot interval (passed through)")

    ap.add_argument("--batch-rows", type=int, default=None, help="Batch rows (passed through)")
    ap.add_argument("--max-files", type=int, default=None, help="Max files per worker (passed through)")

    ap.add_argument("--pid-dir", type=str, default=None, help="Where to write worker PID files (default: --db-dir)")
    ap.add_argument("--log-dir", type=str, default=None, help="Where to write worker log files (default: --db-dir)")

    ap.add_argument("--dry-run", action="store_true", default=False, help="Print commands and exit")
    ap.add_argument("--force", action="store_true", default=False, help="Overwrite PID files even if they exist")

    args = ap.parse_args()

    if str(args.duckdb_index_mode) != "domain" and str(args.domain_index_action) != "append":
        raise SystemExit("--domain-index-action is only valid with --duckdb-index-mode domain")

    input_root = Path(args.input_root).expanduser().resolve()
    db_dir = Path(args.db_dir).expanduser().resolve()

    pid_dir = Path(args.pid_dir).expanduser().resolve() if args.pid_dir else db_dir
    log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else db_dir

    db_dir.mkdir(parents=True, exist_ok=True)
    pid_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.collections:
        collections = [c for c in args.collections if c]
    else:
        collections = _list_collections(input_root, args.collections_regex)

    if not collections:
        raise SystemExit("No collections selected")

    workers = max(1, int(args.workers))
    cdx_mod = int(args.cdx_shard_mod) if args.cdx_shard_mod is not None else None
    if cdx_mod is not None and cdx_mod <= 0:
        raise SystemExit("--cdx-shard-mod must be > 0")

    plans: List[WorkerPlan] = []

    if cdx_mod is None:
        groups = _chunk_round_robin(sorted(collections), workers)

        # Build worker plans, skipping empties.
        for i, cols in enumerate(groups):
            if not cols:
                continue
            pid_file = pid_dir / f"build_worker_{i}.pid"
            log_file = log_dir / f"build_worker_{i}.log"
            plans.append(WorkerPlan(worker_index=i, collections=cols, pid_file=pid_file, log_file=log_file))
    else:
        # When partitioning within a collection by (cdx_number % mod == rem), we must ensure
        # that *every* remainder sees *every* collection exactly once.
        # We do this by creating worker buckets per remainder and distributing collections
        # among the workers in that remainder bucket.

        rem_to_worker_idxs: List[List[int]] = [[] for _ in range(cdx_mod)]
        for i in range(workers):
            rem_to_worker_idxs[i % cdx_mod].append(i)

        # If some remainder has no worker, we'd miss shards. Fail loudly.
        for rem, idxs in enumerate(rem_to_worker_idxs):
            if not idxs:
                raise SystemExit(
                    f"--workers ({workers}) must be >= --cdx-shard-mod ({cdx_mod}) so every remainder 0..{cdx_mod-1} is covered"
                )

        # Seed empty plans for all workers.
        idx_to_plan: Dict[int, WorkerPlan] = {}
        for i in range(workers):
            pid_file = pid_dir / f"build_worker_{i}.pid"
            log_file = log_dir / f"build_worker_{i}.log"
            idx_to_plan[i] = WorkerPlan(
                worker_index=i,
                collections=[],
                pid_file=pid_file,
                log_file=log_file,
                cdx_shard_mod=cdx_mod,
                cdx_shard_rem=(i % cdx_mod),
            )

        # Distribute collections within each remainder bucket.
        cols_sorted = sorted(collections)
        for rem, worker_idxs in enumerate(rem_to_worker_idxs):
            k = len(worker_idxs)
            for j, col in enumerate(cols_sorted):
                wi = worker_idxs[j % k]
                idx_to_plan[wi].collections.append(col)

        plans = [idx_to_plan[i] for i in range(workers) if idx_to_plan[i].collections]

    build_script = (Path(__file__).parent / "build_cc_pointer_duckdb.py").resolve()
    if not build_script.exists():
        raise SystemExit(f"Missing build script: {build_script}")

    plan_path = Path(args.plan_file).expanduser().resolve() if args.plan_file else (db_dir / "build_plan.json")

    # Print plan
    print(f"Selected collections: {len(collections)}")
    print(f"Workers: {len(plans)}")
    python_exe = _abspath_preserve_symlinks(Path(args.python)) if args.python else _abspath_preserve_symlinks(Path(sys.executable))

    plan_payload: Dict[str, Any] = {
        "created_at_epoch": time.time(),
        "input_root": str(input_root),
        "db_dir": str(db_dir),
        "parquet_out": str(Path(args.parquet_out).expanduser().resolve()) if args.parquet_out else None,
        "collections_regex": args.collections_regex,
        "workers": int(workers),
        "threads_per_worker": int(args.threads_per_worker),
        "cdx_shard_mod": int(cdx_mod) if cdx_mod is not None else None,
        "progress_dir": str(Path(args.progress_dir).expanduser().resolve()) if args.progress_dir else None,
        "progress_interval_seconds": int(args.progress_interval_seconds),
        "batch_rows": int(args.batch_rows) if args.batch_rows is not None else None,
        "max_files": int(args.max_files) if args.max_files is not None else None,
        "python": str(python_exe),
        "build_script": str(build_script),
        "worker_plans": [p.to_json() for p in plans],
    }

    for p in plans:
        extra = ""
        if p.cdx_shard_mod is not None:
            extra = f"\tcdx_mod={p.cdx_shard_mod}\tcdx_rem={p.cdx_shard_rem}"
        print(f"  worker={p.worker_index}\tcollections={len(p.collections)}\tpid={p.pid_file}\tlog={p.log_file}{extra}")

    procs: List[Popen] = []

    for p in plans:
        # PID file safety
        if p.pid_file.exists() and not args.force:
            old_pid = _read_pid(p.pid_file)
            if old_pid is not None and _pid_alive(old_pid):
                raise SystemExit(f"PID file exists and process alive: {p.pid_file} (pid={old_pid})")

        cmd: List[str] = [
            str(python_exe),
            str(build_script),
            "--input-root",
            str(input_root),
            "--db",
            str(db_dir),
            "--shard-by-collection",
            "--duckdb-index-mode",
            str(args.duckdb_index_mode),
            "--domain-index-action",
            str(args.domain_index_action),
            "--threads",
            str(int(args.threads_per_worker)),
            "--progress-interval-seconds",
            str(int(args.progress_interval_seconds)),
        ]

        if p.cdx_shard_mod is not None:
            cmd += ["--cdx-shard-mod", str(int(p.cdx_shard_mod)), "--cdx-shard-rem", str(int(p.cdx_shard_rem or 0))]

        if args.parquet_out:
            cmd += ["--parquet-out", str(Path(args.parquet_out).expanduser().resolve())]

            cmd += ["--parquet-validate", str(args.parquet_validate)]
            cmd += ["--parquet-action", str(args.parquet_action)]

            if args.resume_require_parquet is not None:
                cmd += ["--resume-require-parquet" if bool(args.resume_require_parquet) else "--no-resume-require-parquet"]

        if args.progress_dir:
            cmd += ["--progress-dir", str(Path(args.progress_dir).expanduser().resolve())]

        if args.batch_rows is not None:
            cmd += ["--batch-rows", str(int(args.batch_rows))]

        if args.max_files is not None:
            cmd += ["--max-files", str(int(args.max_files))]

        for c in p.collections:
            cmd += ["--collections", c]

        cmd_str = " ".join(shlex.quote(x) for x in cmd)
        print(f"\n[worker {p.worker_index}] {cmd_str}")

        if args.dry_run:
            continue

        _ensure_parent(p.log_file)
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"

        with open(p.log_file, "ab", buffering=0) as log_f:
            log_f.write(f"\n# started_at={time.time()}\n".encode("utf-8"))
            log_f.write((cmd_str + "\n").encode("utf-8"))
            log_f.flush()
            proc = Popen(cmd, stdout=log_f, stderr=log_f, close_fds=True, env=env)

        _ensure_parent(p.pid_file)
        p.pid_file.write_text(str(proc.pid), encoding="utf-8")
        procs.append(proc)

    # Persist plan for watchdog/restarts (even in dry-run, to allow review).
    try:
        _ensure_parent(plan_path)
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(plan_payload, f, indent=2, sort_keys=True)
        print(f"\nWrote plan: {plan_path}")
    except Exception as e:
        print(f"WARNING: failed to write plan file {plan_path}: {type(e).__name__}: {e}")

    if args.dry_run:
        print("\n(dry-run) no processes started")
        return 0

    print("\nStarted processes:")
    for p, proc in zip(plans, procs):
        extra = ""
        if p.cdx_shard_mod is not None:
            extra = f"\tcdx_mod={p.cdx_shard_mod}\tcdx_rem={p.cdx_shard_rem}"
        print(f"  worker={p.worker_index}\tpid={proc.pid}\tlog={p.log_file}{extra}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
