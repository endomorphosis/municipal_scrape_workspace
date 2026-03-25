#!/usr/bin/env python3
"""Watch a running daemon output directory and trigger the HF release pipeline
when a cycle completes successfully.

Usage
-----
python watch_and_release.py \\
    --output-dir artifacts/state_admin_rules/ca_uncapped_20260325_212912 \\
    --corpus state_admin_rules \\
    --states CA \\
    --min-statutes 1000 \\
    [--dry-run] [--hf-token <token>]

The watcher polls the `cycles/` directory.  When a `cycle_NNNN.json` appears
(no `.in_progress` extension) it reads the critic score, checks the pass flag,
and if criteria are met runs the full release pipeline via
``post_cycle_release_with_gate.py release``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence, Set


_SCRIPT_DIR = Path(__file__).resolve().parent
_WORKSPACE = _SCRIPT_DIR.parents[2]


def _python() -> str:
    venv = _WORKSPACE / ".venv" / "bin" / "python"
    return str(venv) if venv.exists() else sys.executable


def _gate_script() -> Path:
    return _SCRIPT_DIR / "post_cycle_release_with_gate.py"


def _find_completed_cycles(cycles_dir: Path, already_released: Set[str]) -> list[Path]:
    """Return cycle JSON files that are finished and not yet released."""
    completed = []
    if not cycles_dir.exists():
        return completed
    for path in sorted(cycles_dir.iterdir()):
        name = path.name
        # Finished cycles look like cycle_0001.json; in-progress like cycle_0001.in_progress.json
        if not name.endswith(".json") or ".in_progress" in name:
            continue
        if name in already_released:
            continue
        completed.append(path)
    return completed


def _read_cycle(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _cycle_passed(cycle: dict, min_statutes: int, min_score: float) -> tuple[bool, str]:
    """Return (passed, reason)."""
    passed = bool(cycle.get("passed") or cycle.get("is_success"))
    score = float(cycle.get("score") or cycle.get("critic_score") or 0.0)
    total_statutes = int(
        (cycle.get("diagnostics") or {}).get("etl_readiness", {}).get("total_statutes", 0) or 0
    )

    if not passed:
        return False, f"cycle.passed=False"
    if total_statutes < min_statutes:
        return False, f"total_statutes {total_statutes} < min_statutes {min_statutes}"
    if score < min_score:
        return False, f"score {score:.3f} < min_score {min_score:.3f}"
    return True, f"passed=True score={score:.3f} statutes={total_statutes}"


def _run_release(
    *,
    output_dir: Path,
    corpus: str,
    states: str,
    min_statutes: int,
    hf_token: Optional[str],
    dry_run: bool,
) -> int:
    cmd = [
        _python(),
        str(_gate_script()),
        "release",
        "--corpus", corpus,
        "--states", states,
        "--daemon-output-dir", str(output_dir),
        "--min-statutes", str(min_statutes),
    ]
    if hf_token:
        cmd += ["--hf-token", hf_token]
    if dry_run:
        cmd += ["--dry-run"]

    print(f"\n[RELEASE] Running: {' '.join(cmd)}", flush=True)
    ret = subprocess.run(cmd, text=True)
    return ret.returncode


def watch(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    cycles_dir = output_dir / "cycles"
    released: Set[str] = set()
    poll_interval = int(args.poll_interval)
    min_statutes = int(args.min_statutes)
    min_score = float(args.min_score)
    max_wait_minutes = int(args.max_wait_minutes)
    start = time.monotonic()

    print(f"[WATCH] Monitoring {cycles_dir}", flush=True)
    print(f"[WATCH] Poll every {poll_interval}s, max wait {max_wait_minutes}m", flush=True)
    print(f"[WATCH] Release criteria: passed=True, statutes>={min_statutes}, score>={min_score}", flush=True)

    last_status_print = 0.0

    while True:
        elapsed = time.monotonic() - start
        if elapsed > max_wait_minutes * 60:
            print(f"\n[WATCH] Timeout after {max_wait_minutes}m — stopping watcher.", flush=True)
            return 1

        completed = _find_completed_cycles(cycles_dir, released)
        for path in completed:
            cycle = _read_cycle(path)
            passed, reason = _cycle_passed(cycle, min_statutes, min_score)
            print(f"\n[WATCH] Cycle {path.name}: {reason}", flush=True)
            if passed:
                rc = _run_release(
                    output_dir=output_dir,
                    corpus=args.corpus,
                    states=args.states,
                    min_statutes=min_statutes,
                    hf_token=args.hf_token,
                    dry_run=bool(args.dry_run),
                )
                print(f"[WATCH] Release for {path.name} exited {rc}", flush=True)
            released.add(path.name)

        # Check if daemon is still alive (PID watch)
        if args.daemon_pid:
            pid = int(args.daemon_pid)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                print(f"\n[WATCH] Daemon PID {pid} exited — final check.", flush=True)
                # One final sweep
                completed = _find_completed_cycles(cycles_dir, released)
                for path in completed:
                    cycle = _read_cycle(path)
                    passed, reason = _cycle_passed(cycle, min_statutes, min_score)
                    print(f"[WATCH] Final sweep {path.name}: {reason}", flush=True)
                    if passed:
                        _run_release(
                            output_dir=output_dir,
                            corpus=args.corpus,
                            states=args.states,
                            min_statutes=min_statutes,
                            hf_token=args.hf_token,
                            dry_run=bool(args.dry_run),
                        )
                    released.add(path.name)
                print("[WATCH] Done.", flush=True)
                return 0

        # Periodic status print
        if time.monotonic() - last_status_print > 300:
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            in_progress = list(cycles_dir.glob("*.in_progress.json")) if cycles_dir.exists() else []
            print(
                f"[WATCH {ts}] Elapsed {elapsed/60:.0f}m, "
                f"in_progress={len(in_progress)}, released={len(released)}",
                flush=True,
            )
            last_status_print = time.monotonic()

        time.sleep(poll_interval)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Watch daemon output and auto-release on cycle completion.")
    parser.add_argument("--output-dir", required=True, help="Daemon output directory to watch.")
    parser.add_argument("--corpus", required=True, choices=["state_laws", "state_admin_rules", "state_court_rules"])
    parser.add_argument("--states", required=True, help="Comma-separated state codes.")
    parser.add_argument("--daemon-pid", default=None, help="PID of the running daemon (stops watcher when it exits).")
    parser.add_argument("--min-statutes", type=int, default=1000, help="Min statutes for a passing cycle.")
    parser.add_argument("--min-score", type=float, default=0.85, help="Min critic score for a passing cycle.")
    parser.add_argument("--poll-interval", type=int, default=120, help="Seconds between polls. Default: 120.")
    parser.add_argument("--max-wait-minutes", type=int, default=480, help="Max wait in minutes before giving up.")
    parser.add_argument("--hf-token", default=None, help="HuggingFace API token.")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually run release pipeline.")
    args = parser.parse_args(argv)
    return watch(args)


if __name__ == "__main__":
    raise SystemExit(main())
