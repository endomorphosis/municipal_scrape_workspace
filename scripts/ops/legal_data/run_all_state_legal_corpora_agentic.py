#!/usr/bin/env python3
"""Run the agentic legal daemon across state laws, court rules, and admin rules.

This thin wrapper keeps the existing per-corpus daemon as the source of truth,
but makes it easier to run the full state-legal sweep with consistent defaults
for router-assisted recovery and IPFS-backed page reuse.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


DEFAULT_CORPORA = [
    "state_laws",
    "state_court_rules",
    "state_admin_rules",
]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the existing state legal agentic daemon sequentially for multiple corpora.",
    )
    parser.add_argument(
        "--corpora",
        default=",".join(DEFAULT_CORPORA),
        help="Comma-separated corpora to run. Defaults to all three state corpora.",
    )
    parser.add_argument("--states", default="all", help="Comma-separated states or 'all'.")
    parser.add_argument("--output-dir", default=None, help="Output root for multi-corpus artifacts.")
    parser.add_argument("--python-bin", default=sys.executable, help="Python interpreter for the daemon module.")
    parser.add_argument("--max-cycles", type=int, default=1, help="Max cycles for each corpus daemon.")
    parser.add_argument("--max-statutes", type=int, default=0, help="Optional debug cap passed to each corpus daemon.")
    parser.add_argument("--archive-warmup-urls", type=int, default=25, help="Weak-state URL warmup budget per corpus.")
    parser.add_argument("--per-state-timeout-seconds", type=float, default=86400.0, help="Per-state scrape timeout.")
    parser.add_argument("--scrape-timeout-seconds", type=float, default=0.0, help="Optional full scrape timeout per corpus.")
    parser.add_argument("--target-score", type=float, default=0.92, help="Daemon convergence target score.")
    parser.add_argument("--router-llm-timeout-seconds", type=float, default=20.0, help="Router LLM timeout.")
    parser.add_argument("--router-embeddings-timeout-seconds", type=float, default=10.0, help="Router embeddings timeout.")
    parser.add_argument("--router-ipfs-timeout-seconds", type=float, default=10.0, help="Router IPFS persistence timeout.")
    parser.add_argument(
        "--admin-parallel-assist-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable the admin-rules supplemental parallel assist stage when the daemon supports it.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running later corpora if one corpus daemon fails.",
    )
    return parser


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _daemon_command(args: argparse.Namespace, *, corpus: str, output_root: Path) -> List[str]:
    command = [
        str(args.python_bin),
        "-m",
        "ipfs_datasets_py.processors.legal_scrapers.state_laws_agentic_daemon",
        "--corpus",
        corpus,
        "--states",
        str(args.states),
        "--max-cycles",
        str(int(args.max_cycles)),
        "--max-statutes",
        str(int(args.max_statutes)),
        "--archive-warmup-urls",
        str(int(args.archive_warmup_urls)),
        "--per-state-timeout-seconds",
        str(float(args.per_state_timeout_seconds)),
        "--target-score",
        str(float(args.target_score)),
        "--router-llm-timeout-seconds",
        str(float(args.router_llm_timeout_seconds)),
        "--router-embeddings-timeout-seconds",
        str(float(args.router_embeddings_timeout_seconds)),
        "--router-ipfs-timeout-seconds",
        str(float(args.router_ipfs_timeout_seconds)),
        "--output-dir",
        str(output_root / corpus),
    ]
    if float(args.scrape_timeout_seconds) > 0.0:
        command.extend(["--scrape-timeout-seconds", str(float(args.scrape_timeout_seconds))])
    if args.admin_parallel_assist_enabled:
        command.append("--admin-parallel-assist-enabled")
    else:
        command.append("--no-admin-parallel-assist-enabled")
    return command


def main() -> int:
    args = _build_parser().parse_args()
    corpora = _split_csv(args.corpora) or list(DEFAULT_CORPORA)
    output_root = Path(args.output_dir or (Path.cwd() / "tmp" / "all_state_legal_corpora_agentic")).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    # Default to page-level IPFS dedupe unless the caller has set a preference already.
    os.environ.setdefault("LEGAL_SCRAPER_IPFS_PAGE_CACHE_ENABLED", "1")

    summary: Dict[str, Dict[str, object]] = {
        "requested_corpora": corpora,
        "states": str(args.states),
        "output_root": str(output_root),
        "runs": {},
    }

    for corpus in corpora:
        command = _daemon_command(args, corpus=corpus, output_root=output_root)
        completed = subprocess.run(command, check=False, text=True, capture_output=True)
        summary["runs"][corpus] = {
            "returncode": int(completed.returncode),
            "command": command,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        if completed.returncode != 0 and not args.continue_on_error:
            print(json.dumps(summary, indent=2))
            return int(completed.returncode)

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
