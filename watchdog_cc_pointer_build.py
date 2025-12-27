#!/usr/bin/env python3
"""Watchdog for Common Crawl pointer-index builders.

Purpose
- Prevent OOM by dynamically throttling worker processes when RAM gets low.
- Restart stopped/crashed workers with backoff when the system recovers.

How it works
- Reads a plan file written by launch_cc_pointer_build.py (default: <db-dir>/build_plan.json)
- Tracks worker PIDs via per-worker pid files
- On low-memory, stops workers (SIGINT -> SIGKILL fallback) until memory recovers
- On recovery, restarts workers up to --target-running with exponential backoff per worker

This script is stdlib-only.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen
from typing import Any, Dict, List, Optional, Tuple


def _now() -> float:
    return time.time()


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


def _write_pid(pid_file: Path, pid: int) -> None:
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(int(pid)), encoding="utf-8")


def _parse_meminfo_kb() -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split(":", 1)
                if len(parts) != 2:
                    continue
                key = parts[0].strip()
                rest = parts[1].strip().split()
                if not rest:
                    continue
                try:
                    out[key] = int(rest[0])
                except Exception:
                    continue
    except Exception:
        pass
    return out


def _mem_available_gib() -> float:
    mi = _parse_meminfo_kb()
    kb = float(mi.get("MemAvailable", 0))
    return kb / 1024.0 / 1024.0


def _swap_free_gib() -> float:
    mi = _parse_meminfo_kb()
    kb = float(mi.get("SwapFree", 0))
    return kb / 1024.0 / 1024.0


def _send_sigint_then_kill(pid: int, grace_seconds: int) -> None:
    try:
        os.kill(pid, signal.SIGINT)
    except Exception:
        return

    deadline = _now() + float(max(0, grace_seconds))
    while _now() < deadline:
        if not _pid_alive(pid):
            return
        time.sleep(0.5)

    try:
        os.kill(pid, signal.SIGKILL)
    except Exception:
        return


@dataclass
class Worker:
    worker_index: int
    collections: List[str]
    pid_file: Path
    log_file: Path

    # runtime state
    last_start_epoch: float = 0.0
    last_stop_epoch: float = 0.0
    consecutive_failures: int = 0
    backoff_until_epoch: float = 0.0


def _load_plan(plan_path: Path) -> Dict[str, Any]:
    with open(plan_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit(f"Invalid plan file (not an object): {plan_path}")
    return data


def _workers_from_plan(plan: Dict[str, Any]) -> Tuple[List[Worker], List[str]]:
    errors: List[str] = []

    wp = plan.get("worker_plans")
    if not isinstance(wp, list):
        raise SystemExit("Plan missing worker_plans")

    workers: List[Worker] = []
    for item in wp:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("worker_index"))
            cols = item.get("collections")
            if not isinstance(cols, list):
                cols = []
            pid_file = Path(str(item.get("pid_file"))).expanduser().resolve()
            log_file = Path(str(item.get("log_file"))).expanduser().resolve()
            workers.append(Worker(worker_index=idx, collections=[str(c) for c in cols], pid_file=pid_file, log_file=log_file))
        except Exception as e:
            errors.append(f"bad worker plan: {type(e).__name__}: {e}")

    workers.sort(key=lambda w: w.worker_index)
    return workers, errors


def _build_cmd(plan: Dict[str, Any], w: Worker) -> List[str]:
    python_exe = str(plan.get("python") or sys.executable)
    build_script = str(plan.get("build_script") or (Path(__file__).parent / "build_cc_pointer_duckdb.py"))

    cmd: List[str] = [
        python_exe,
        build_script,
        "--input-root",
        str(plan["input_root"]),
        "--db",
        str(plan["db_dir"]),
        "--shard-by-collection",
        "--threads",
        str(int(plan.get("threads_per_worker", 2))),
        "--progress-interval-seconds",
        str(int(plan.get("progress_interval_seconds", 30))),
    ]

    parquet_out = plan.get("parquet_out")
    if parquet_out:
        cmd += ["--parquet-out", str(parquet_out)]

    progress_dir = plan.get("progress_dir")
    if progress_dir:
        cmd += ["--progress-dir", str(progress_dir)]

    batch_rows = plan.get("batch_rows")
    if batch_rows is not None:
        cmd += ["--batch-rows", str(int(batch_rows))]

    max_files = plan.get("max_files")
    if max_files is not None:
        cmd += ["--max-files", str(int(max_files))]

    for c in w.collections:
        cmd += ["--collections", c]

    return cmd


def _start_worker(plan: Dict[str, Any], w: Worker) -> int:
    cmd = _build_cmd(plan, w)

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    w.log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(w.log_file, "ab", buffering=0) as log_f:
        log_f.write(f"\n# watchdog_start_at={_now()}\n".encode("utf-8"))
        log_f.write((" ".join(cmd) + "\n").encode("utf-8"))
        log_f.flush()
        proc = Popen(cmd, stdout=log_f, stderr=log_f, close_fds=True, env=env)

    _write_pid(w.pid_file, proc.pid)
    w.last_start_epoch = _now()
    return int(proc.pid)


def _current_running(workers: List[Worker]) -> List[Tuple[Worker, int]]:
    out: List[Tuple[Worker, int]] = []
    for w in workers:
        pid = _read_pid(w.pid_file)
        if pid is None:
            continue
        if _pid_alive(pid):
            out.append((w, pid))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-dir", required=True, type=str, help="Directory containing build_plan.json + build_worker_*.pid/log")
    ap.add_argument("--plan-file", type=str, default=None, help="Path to plan JSON (default: <db-dir>/build_plan.json)")

    ap.add_argument("--interval", type=int, default=15, help="Seconds between checks")

    ap.add_argument("--target-running", type=int, default=8, help="Desired number of running workers when healthy")
    ap.add_argument("--min-running", type=int, default=1, help="Never reduce below this many workers")

    ap.add_argument("--low-mem-gib", type=float, default=20.0, help="If MemAvailable < this, start stopping workers")
    ap.add_argument("--high-mem-gib", type=float, default=40.0, help="If MemAvailable > this, allow restarts")

    ap.add_argument("--critical-mem-gib", type=float, default=8.0, help="If MemAvailable < this, stop down to --min-running immediately")
    ap.add_argument("--stop-grace-seconds", type=int, default=20, help="Seconds to wait after SIGINT before SIGKILL")

    ap.add_argument("--initial-backoff-seconds", type=int, default=30, help="Base backoff before restarting a failed worker")
    ap.add_argument("--max-backoff-seconds", type=int, default=15 * 60, help="Max backoff between restarts per worker")

    ap.add_argument("--once", action="store_true", default=False, help="Print a single decision and exit")
    args = ap.parse_args()

    db_dir = Path(args.db_dir).expanduser().resolve()
    plan_path = Path(args.plan_file).expanduser().resolve() if args.plan_file else (db_dir / "build_plan.json")

    if not plan_path.exists():
        raise SystemExit(f"Plan file not found: {plan_path}")

    plan = _load_plan(plan_path)
    workers, plan_errors = _workers_from_plan(plan)
    if plan_errors:
        for e in plan_errors:
            print(f"plan_warning\t{e}")

    target_running = max(1, int(args.target_running))
    min_running = max(0, int(args.min_running))

    def _print_status(prefix: str) -> None:
        mem_av = _mem_available_gib()
        swap_free = _swap_free_gib()
        running = _current_running(workers)
        print(
            f"{prefix}\tmem_avail_gib={mem_av:.1f}\tswap_free_gib={swap_free:.1f}\trunning={len(running)}\ttarget={target_running}"
        )

    while True:
        mem_av = _mem_available_gib()
        swap_free = _swap_free_gib()

        running = _current_running(workers)
        running_count = len(running)

        # Detect crashed/exited workers and apply backoff.
        for w in workers:
            pid = _read_pid(w.pid_file)
            if pid is None:
                continue
            if not _pid_alive(pid):
                # pid file points at a dead process
                w.consecutive_failures = min(1000000, w.consecutive_failures + 1)
                backoff = min(
                    int(args.max_backoff_seconds),
                    int(args.initial_backoff_seconds) * (2 ** max(0, w.consecutive_failures - 1)),
                )
                w.backoff_until_epoch = max(w.backoff_until_epoch, _now() + float(backoff))

        # Decide desired concurrency based on memory.
        desired = target_running
        if mem_av < float(args.low_mem_gib):
            desired = max(min_running, min(desired, max(min_running, running_count - 1)))
        if mem_av < float(args.critical_mem_gib):
            desired = min_running

        # Apply: stop if too many running.
        if running_count > desired:
            # Stop one worker at a time (highest worker_index first for determinism).
            running_sorted = sorted(running, key=lambda t: t[0].worker_index, reverse=True)
            w, pid = running_sorted[0]
            print(f"action\tstop\tworker={w.worker_index}\tpid={pid}\tmem_avail_gib={mem_av:.1f}")
            _send_sigint_then_kill(pid, grace_seconds=int(args.stop_grace_seconds))
            w.last_stop_epoch = _now()

        # Apply: start if too few running AND memory is healthy (hysteresis).
        elif running_count < desired and mem_av > float(args.high_mem_gib):
            # Start the lowest-index worker that is not running and not in backoff.
            running_pids = {pid for _, pid in running}
            startable: List[Worker] = []
            for w in workers:
                pid = _read_pid(w.pid_file)
                if pid is not None and pid in running_pids:
                    continue
                if pid is not None and _pid_alive(pid):
                    continue
                if _now() < w.backoff_until_epoch:
                    continue
                startable.append(w)
            startable.sort(key=lambda w: w.worker_index)
            if startable:
                w = startable[0]
                new_pid = _start_worker(plan, w)
                print(
                    f"action\tstart\tworker={w.worker_index}\tpid={new_pid}\tmem_avail_gib={mem_av:.1f}\tbackoff_failures={w.consecutive_failures}"
                )
            else:
                _print_status("status")
        else:
            _print_status("status")

        if args.once:
            return 0

        time.sleep(max(1, int(args.interval)))


if __name__ == "__main__":
    raise SystemExit(main())
