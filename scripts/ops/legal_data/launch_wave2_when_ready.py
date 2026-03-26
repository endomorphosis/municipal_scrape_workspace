#!/usr/bin/env python3
"""Auto-launch Wave 2 state batches when Wave 1 daemons finish.

Polls Wave 1 PIDs. When all have exited, launches Wave 2 daemons + watchers.
Usage: python launch_wave2_when_ready.py [--dry-run]
"""
from __future__ import annotations
import argparse, os, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[3]
PYTHON = str(WORKSPACE / ".venv" / "bin" / "python")

WAVE1_PIDS = [3029816, 3030346, 1654284, 1654543, 1654813]

WAVE2_BATCHES = [
    ("batch_az_nv_nm_ut_ks_20260326_wave2",   "AZ,NV,NM,UT,KS"),
    ("batch_ky_la_sc_al_ar_20260326_wave2",   "KY,LA,SC,AL,AR"),
    ("batch_ia_ne_ok_ms_ct_20260326_wave2",   "IA,NE,OK,MS,CT"),
    ("batch_ks_wv_ri_vt_nh_me_de_20260326_wave2", "WV,RI,VT,NH,ME,DE,ND,SD,MT,WY,ID,AK,HI,DC"),
]

COMMON_FLAGS = [
    "--max-cycles", "4",
    "--explore-probability", "0.05",
    "--per-state-timeout-seconds", "5400",
    "--scrape-timeout-seconds", "18000",
    "--archive-warmup-urls", "0",
    "--admin-agentic-max-candidates-per-state", "2000",
    "--admin-agentic-max-fetch-per-state", "2000",
    "--admin-agentic-max-results-per-domain", "1500",
    "--admin-agentic-max-hops", "5",
    "--admin-agentic-max-pages", "800",
    "--no-admin-parallel-assist-enabled",
    "--target-score", "0.80",
    "--post-cycle-release",
    "--post-cycle-release-timeout-seconds", "7200",
    "--post-cycle-release-workspace-root", str(WORKSPACE),
    "--post-cycle-release-python-bin", PYTHON,
]


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _launch_wave2(dry: bool) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] Wave 1 complete — launching Wave 2", flush=True)
    for dir_name, states in WAVE2_BATCHES:
        out_dir = WORKSPACE / "artifacts" / "state_admin_rules" / dir_name
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            PYTHON, "-m",
            "ipfs_datasets_py.processors.legal_scrapers.state_laws_agentic_daemon",
            "--corpus", "state_admin_rules",
            "--states", states,
            "--output-dir", str(out_dir),
        ] + COMMON_FLAGS
        log = out_dir / "daemon.log"
        print(f"  Launching {dir_name} ({states})", flush=True)
        if not dry:
            proc = subprocess.Popen(
                cmd, cwd=str(WORKSPACE),
                stdout=open(log, "w"), stderr=subprocess.STDOUT,
            )
            daemon_pid = proc.pid
            print(f"    Daemon PID: {daemon_pid}", flush=True)
            # Launch watcher
            watcher_cmd = [
                PYTHON, str(WORKSPACE / "scripts/ops/legal_data/watch_and_release.py"),
                "--output-dir", str(out_dir),
                "--corpus", "state_admin_rules",
                "--states", states,
                "--daemon-pid", str(daemon_pid),
                "--min-statutes", "200",
                "--min-score", "0.70",
                "--poll-interval", "120",
                "--max-wait-minutes", "900",
            ]
            watcher_log = out_dir / "watcher.log"
            watcher = subprocess.Popen(
                watcher_cmd, cwd=str(WORKSPACE),
                stdout=open(watcher_log, "w"), stderr=subprocess.STDOUT,
            )
            print(f"    Watcher PID: {watcher.pid}", flush=True)
        else:
            print(f"    [DRY-RUN] would run: {' '.join(cmd[:6])} ...", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=120)
    args = parser.parse_args()

    print(f"Watching Wave 1 PIDs: {WAVE1_PIDS}", flush=True)
    while True:
        alive = [p for p in WAVE1_PIDS if _pid_alive(p)]
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}] Wave1 alive: {alive}", flush=True)
        if not alive:
            _launch_wave2(args.dry_run)
            return 0
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
