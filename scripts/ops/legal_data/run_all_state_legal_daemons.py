#!/usr/bin/env python3
"""Run the canonical state legal daemons across all three corpora.

This wrapper keeps the existing single-corpus daemon as the implementation
surface while making it easy to launch state laws, state court rules, and
state administrative rules with the same agentic, router, and archive settings.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List


DEFAULT_CORPORA = [
    "state_laws",
    "state_court_rules",
    "state_admin_rules",
]


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_python_bin(root: Path) -> str:
    candidates = [
        root / ".venv" / "bin" / "python",
        root / "ipfs_datasets_py" / ".venv" / "bin" / "python",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def _default_output_dir(root: Path) -> Path:
    return root / "data" / "agentic_legal_daemon_runs"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the existing agentic legal daemon for multiple state-law corpora.")
    parser.add_argument(
        "--corpora",
        default=",".join(DEFAULT_CORPORA),
        help="Comma-separated corpus list. Defaults to state_laws,state_court_rules,state_admin_rules.",
    )
    parser.add_argument("--states", default=os.environ.get("LEGAL_DAEMON_STATES", "all"))
    parser.add_argument("--max-cycles", type=int, default=int(os.environ.get("LEGAL_DAEMON_MAX_CYCLES", "1")))
    parser.add_argument("--max-statutes", type=int, default=int(os.environ.get("LEGAL_DAEMON_MAX_STATUTES", "0")))
    parser.add_argument("--archive-warmup-urls", type=int, default=int(os.environ.get("LEGAL_DAEMON_ARCHIVE_WARMUP_URLS", "25")))
    parser.add_argument("--per-state-timeout-seconds", type=float, default=float(os.environ.get("LEGAL_DAEMON_PER_STATE_TIMEOUT_SECONDS", "86400")))
    parser.add_argument("--scrape-timeout-seconds", type=float, default=float(os.environ.get("LEGAL_DAEMON_SCRAPE_TIMEOUT_SECONDS", "0")))
    parser.add_argument("--router-llm-timeout-seconds", type=float, default=float(os.environ.get("LEGAL_DAEMON_ROUTER_LLM_TIMEOUT_SECONDS", "20")))
    parser.add_argument("--router-embeddings-timeout-seconds", type=float, default=float(os.environ.get("LEGAL_DAEMON_ROUTER_EMBEDDINGS_TIMEOUT_SECONDS", "10")))
    parser.add_argument("--router-ipfs-timeout-seconds", type=float, default=float(os.environ.get("LEGAL_DAEMON_ROUTER_IPFS_TIMEOUT_SECONDS", "10")))
    parser.add_argument("--target-score", type=float, default=float(os.environ.get("LEGAL_DAEMON_TARGET_SCORE", "0.92")))
    parser.add_argument("--python-bin", default=os.environ.get("LEGAL_DAEMON_PYTHON_BIN"))
    parser.add_argument("--output-root", default=os.environ.get("LEGAL_DAEMON_OUTPUT_ROOT"))
    parser.add_argument("--continue-on-error", action="store_true", help="Keep running later corpora if one corpus fails.")
    parser.add_argument("--post-cycle-release", action="store_true")
    parser.add_argument("--post-cycle-release-dry-run", action="store_true")
    return parser


def _parse_corpora(value: str) -> List[str]:
    items = [item.strip() for item in str(value or "").split(",") if item.strip()]
    return items or list(DEFAULT_CORPORA)


def main(argv: List[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    root = _workspace_root()
    python_bin = args.python_bin or _default_python_bin(root)
    output_root = Path(args.output_root).expanduser().resolve() if args.output_root else _default_output_dir(root)
    output_root.mkdir(parents=True, exist_ok=True)

    failures: List[str] = []
    for corpus in _parse_corpora(args.corpora):
        corpus_output_dir = output_root / corpus
        corpus_output_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            python_bin,
            "-m",
            "ipfs_datasets_py.processors.legal_scrapers.state_laws_agentic_daemon",
            "--corpus",
            corpus,
            "--states",
            args.states,
            "--max-cycles",
            str(args.max_cycles),
            "--max-statutes",
            str(args.max_statutes),
            "--archive-warmup-urls",
            str(args.archive_warmup_urls),
            "--per-state-timeout-seconds",
            str(args.per_state_timeout_seconds),
            "--scrape-timeout-seconds",
            str(args.scrape_timeout_seconds),
            "--router-llm-timeout-seconds",
            str(args.router_llm_timeout_seconds),
            "--router-embeddings-timeout-seconds",
            str(args.router_embeddings_timeout_seconds),
            "--router-ipfs-timeout-seconds",
            str(args.router_ipfs_timeout_seconds),
            "--target-score",
            str(args.target_score),
            "--output-dir",
            str(corpus_output_dir),
        ]
        if args.post_cycle_release:
            cmd.append("--post-cycle-release")
        if args.post_cycle_release_dry_run:
            cmd.append("--post-cycle-release-dry-run")

        print(f"[run_all_state_legal_daemons] launching corpus={corpus} output_dir={corpus_output_dir}", file=sys.stderr)
        completed = subprocess.run(cmd, cwd=str(root))
        if completed.returncode != 0:
            failures.append(corpus)
            print(
                f"[run_all_state_legal_daemons] corpus={corpus} failed with exit code {completed.returncode}",
                file=sys.stderr,
            )
            if not args.continue_on_error:
                return completed.returncode

    if failures:
        print(
            f"[run_all_state_legal_daemons] completed with failures: {', '.join(failures)}",
            file=sys.stderr,
        )
        return 1

    print("[run_all_state_legal_daemons] completed successfully", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
