#!/usr/bin/env python3
"""Run the agentic legal daemon across all canonical state corpora."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


CORPORA = ["state_laws", "state_admin_rules", "state_court_rules"]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_output_root() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return _repo_root() / "artifacts" / "agentic_legal_full_run" / stamp


def _run_one(*, repo_root: Path, corpus: str, args: argparse.Namespace, output_root: Path, cache_dir: Path) -> Dict[str, object]:
    output_dir = output_root / corpus
    output_dir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["LEGAL_DAEMON_CORPUS"] = corpus
    env["LEGAL_DAEMON_STATES"] = args.states
    env["LEGAL_DAEMON_MAX_CYCLES"] = str(args.max_cycles)
    env["LEGAL_DAEMON_OUTPUT_DIR"] = str(output_dir)
    env["LEGAL_DAEMON_STOP_ON_TARGET_SCORE"] = "1" if args.stop_on_target_score else "0"
    env["LEGAL_DAEMON_TARGET_SCORE"] = str(args.target_score)
    env["IPFS_DATASETS_LEGAL_FETCH_CACHE_ENABLED"] = "1"
    env["IPFS_DATASETS_LEGAL_FETCH_CACHE_DIR"] = str(cache_dir)
    env["IPFS_DATASETS_LEGAL_FETCH_CACHE_IPFS_MIRROR"] = "1" if args.ipfs_mirror_cache else "0"

    optional_env = {
        "LEGAL_DAEMON_ARCHIVE_WARMUP_URLS": args.archive_warmup_urls,
        "LEGAL_DAEMON_PER_STATE_TIMEOUT_SECONDS": args.per_state_timeout_seconds,
        "LEGAL_DAEMON_SCRAPE_TIMEOUT_SECONDS": args.scrape_timeout_seconds,
        "LEGAL_DAEMON_ROUTER_LLM_TIMEOUT_SECONDS": args.router_llm_timeout_seconds,
        "LEGAL_DAEMON_ROUTER_EMBEDDINGS_TIMEOUT_SECONDS": args.router_embeddings_timeout_seconds,
        "LEGAL_DAEMON_ROUTER_IPFS_TIMEOUT_SECONDS": args.router_ipfs_timeout_seconds,
        "LEGAL_DAEMON_ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE": args.admin_agentic_max_candidates_per_state,
        "LEGAL_DAEMON_ADMIN_AGENTIC_MAX_FETCH_PER_STATE": args.admin_agentic_max_fetch_per_state,
        "LEGAL_DAEMON_ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN": args.admin_agentic_max_results_per_domain,
        "LEGAL_DAEMON_ADMIN_AGENTIC_MAX_HOPS": args.admin_agentic_max_hops,
        "LEGAL_DAEMON_ADMIN_AGENTIC_MAX_PAGES": args.admin_agentic_max_pages,
        "LEGAL_DAEMON_ADMIN_AGENTIC_FETCH_CONCURRENCY": args.admin_agentic_fetch_concurrency,
    }
    for key, value in optional_env.items():
        if value is not None:
            env[key] = str(value)

    completed = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    payload: Dict[str, object]
    try:
        payload = json.loads(completed.stdout) if completed.stdout.strip() else {}
    except Exception:
        payload = {
            "status": "error",
            "error": "invalid_json_output",
            "stdout": completed.stdout[-4000:],
        }

    payload["returncode"] = completed.returncode
    payload["stderr"] = completed.stderr[-4000:] if completed.stderr else ""
    payload["corpus"] = corpus
    payload["output_dir"] = str(output_dir)
    payload["cache_dir"] = str(cache_dir)
    return payload


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the agentic legal daemon for state laws, admin rules, and court rules.")
    parser.add_argument("--states", default="all", help="Comma-separated state codes, or 'all'.")
    parser.add_argument("--max-cycles", type=int, default=1, help="Cycles per corpus.")
    parser.add_argument("--target-score", type=float, default=0.92, help="Daemon target critic score.")
    parser.add_argument("--stop-on-target-score", action="store_true", help="Stop each corpus once the daemon passes.")
    parser.add_argument("--output-root", default=None, help="Root directory for per-corpus daemon outputs.")
    parser.add_argument("--cache-dir", default=None, help="Shared fetch cache directory.")
    parser.add_argument("--ipfs-mirror-cache", action="store_true", help="Mirror cached fetch payloads to the IPFS router when available.")
    parser.add_argument("--archive-warmup-urls", type=int, default=None)
    parser.add_argument("--per-state-timeout-seconds", type=float, default=None)
    parser.add_argument("--scrape-timeout-seconds", type=float, default=None)
    parser.add_argument("--router-llm-timeout-seconds", type=float, default=None)
    parser.add_argument("--router-embeddings-timeout-seconds", type=float, default=None)
    parser.add_argument("--router-ipfs-timeout-seconds", type=float, default=None)
    parser.add_argument("--admin-agentic-max-candidates-per-state", type=int, default=None)
    parser.add_argument("--admin-agentic-max-fetch-per-state", type=int, default=None)
    parser.add_argument("--admin-agentic-max-results-per-domain", type=int, default=None)
    parser.add_argument("--admin-agentic-max-hops", type=int, default=None)
    parser.add_argument("--admin-agentic-max-pages", type=int, default=None)
    parser.add_argument("--admin-agentic-fetch-concurrency", type=int, default=None)
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    output_root = Path(args.output_root).expanduser().resolve() if args.output_root else _default_output_root()
    cache_dir = Path(args.cache_dir).expanduser().resolve() if args.cache_dir else (output_root / "shared_fetch_cache")
    output_root.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    runs = []
    for corpus in CORPORA:
        runs.append(_run_one(repo_root=repo_root, corpus=corpus, args=args, output_root=output_root, cache_dir=cache_dir))

    report = {
        "status": "success" if all(int(run.get("returncode", 1) or 1) == 0 for run in runs) else "partial_success",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "states": args.states,
        "output_root": str(output_root),
        "cache_dir": str(cache_dir),
        "cache_ipfs_mirror_enabled": bool(args.ipfs_mirror_cache),
        "runs": runs,
    }
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
