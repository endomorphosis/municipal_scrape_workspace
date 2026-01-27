#!/usr/bin/env python3
"""Wait for a PID to exit, then start the CC pipeline orchestrator for ALL.

This is a small ops helper to safely chain long-running jobs without relying on
interactive terminals (avoids accidental Ctrl-C/KeyboardInterrupt issues).

Typical usage:
  PYTHONPATH=src CC_SORT_ROW_GROUP_MIN_MB=4 \
    ./scripts/ops/resume_all_existing_parquet_after_pid.py \
      --wait-pid-file logs/orchestrator_rerun_CC-MAIN-2021-39_rgmin4_<ts>.pid

It will start:
  python -m common_crawl_search_engine.ccindex.cc_pipeline_orchestrator \
    --filter all --existing-parquet-only --force-reindex --rewrite-sorted-parquet

and write fresh log + pid files into ./logs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Unlikely here, but if it happens assume it's alive.
        return True
    else:
        return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Wait for a PID to exit, then run ALL existing-parquet resort+reindex")
    ap.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    ap.add_argument("--config", type=Path, default=None)
    ap.add_argument("--wait-pid-file", type=Path, required=True, help="PID file to wait for")
    ap.add_argument("--poll-seconds", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--sort-workers", type=int, default=8)
    ap.add_argument("--sort-memory-per-worker-gb", type=float, default=4.0)
    ap.add_argument("--heartbeat-seconds", type=int, default=30)
    args = ap.parse_args(argv)

    repo_root: Path = args.repo_root.resolve()
    config: Path = (args.config or (repo_root / "pipeline_config.json")).resolve()
    logs_dir = (repo_root / "logs").resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)

    if not args.wait_pid_file.exists():
        print(f"ERROR: wait pid file not found: {args.wait_pid_file}", file=sys.stderr)
        return 2

    pid_str = args.wait_pid_file.read_text(encoding="utf-8").strip()
    if not pid_str.isdigit():
        print(f"ERROR: invalid PID in {args.wait_pid_file}: {pid_str!r}", file=sys.stderr)
        return 2

    wait_pid = int(pid_str)
    print(f"Waiting for PID {wait_pid} from {args.wait_pid_file}...", flush=True)
    while _pid_alive(wait_pid):
        time.sleep(float(args.poll_seconds))

    ts = time.strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"orchestrator_resort_reindex_ALL_existing_parquet_rgmin4_{ts}.log"
    pid_path = logs_dir / f"orchestrator_resort_reindex_ALL_existing_parquet_rgmin4_{ts}.pid"

    cmd = [
        sys.executable,
        "-m",
        "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator",
        "--config",
        str(config),
        "--filter",
        "all",
        "--existing-parquet-only",
        "--force-reindex",
        "--rewrite-sorted-parquet",
        "--heartbeat-seconds",
        str(int(args.heartbeat_seconds)),
        "--workers",
        str(int(args.workers)),
        "--sort-workers",
        str(int(args.sort_workers)),
        "--sort-memory-per-worker-gb",
        str(float(args.sort_memory_per_worker_gb)),
        "--yes",
    ]

    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str((repo_root / "src").resolve()))

    print(f"Starting ALL run (existing-parquet-only).", flush=True)
    print(f"log: {log_path}", flush=True)
    print("cmd: " + " ".join(cmd), flush=True)

    with open(log_path, "w", encoding="utf-8") as out:
        proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, cwd=str(repo_root), env=env)

    pid_path.write_text(str(proc.pid) + "\n", encoding="utf-8")
    print(f"PID: {proc.pid} (written to {pid_path})", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
