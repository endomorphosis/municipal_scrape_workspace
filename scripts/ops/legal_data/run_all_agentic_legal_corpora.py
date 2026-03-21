"""Run the agentic legal daemon across laws, admin rules, and court rules.

This wrapper keeps the three canonical corpora on the same caching and router
settings so repeated page fetches can be reused across runs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence


Runner = Callable[..., Any]


def _parse_states(raw: str) -> List[str]:
    value = str(raw or "all").strip()
    if not value or value.lower() == "all":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _default_output_root() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return (Path("artifacts") / "agentic_legal_full_run" / stamp).resolve()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run all canonical state legal corpora through the agentic daemon.")
    parser.add_argument("--states", default="all", help="Comma-separated state codes or 'all'.")
    parser.add_argument("--output-root", default=None, help="Root directory for per-corpus daemon outputs.")
    parser.add_argument("--max-cycles", type=int, default=1, help="Maximum cycles per corpus.")
    parser.add_argument("--max-statutes", type=int, default=0, help="Optional per-run item cap for debug runs.")
    parser.add_argument("--cycle-interval-seconds", type=float, default=900.0, help="Sleep interval between cycles within each corpus daemon.")
    parser.add_argument("--archive-warmup-urls", type=int, default=25, help="Archive warmup budget passed through to each daemon.")
    parser.add_argument("--per-state-timeout-seconds", type=float, default=86400.0, help="Per-state timeout budget.")
    parser.add_argument("--router-llm-timeout-seconds", type=float, default=20.0, help="Timeout budget for llm_router review.")
    parser.add_argument("--router-embeddings-timeout-seconds", type=float, default=10.0, help="Timeout budget for embeddings router ranking.")
    parser.add_argument("--router-ipfs-timeout-seconds", type=float, default=10.0, help="Timeout budget for IPFS router persistence.")
    parser.add_argument("--target-score", type=float, default=0.92, help="Stop target for each daemon.")
    parser.add_argument("--stop-on-target-score", action="store_true", help="Stop an individual corpus once the target score is reached.")
    parser.add_argument("--cache-dir", default=None, help="Optional shared fetch-cache directory.")
    parser.add_argument("--disable-fetch-cache", action="store_true", help="Disable the shared legal fetch cache.")
    parser.add_argument("--disable-ipfs-cache-mirror", action="store_true", help="Disable mirroring cache payloads through the IPFS router.")
    parser.add_argument("--post-cycle-release", action="store_true", help="Enable post-cycle merge/parquet/embed automation for each corpus.")
    parser.add_argument("--random-seed", type=int, default=None, help="Optional deterministic seed for tactic selection.")
    return parser


def _apply_cache_env(args: argparse.Namespace) -> None:
    if args.disable_fetch_cache:
        os.environ["IPFS_DATASETS_LEGAL_FETCH_CACHE_ENABLED"] = "0"
        os.environ["LEGAL_SCRAPER_FETCH_CACHE_ENABLED"] = "0"
    else:
        os.environ.setdefault("IPFS_DATASETS_LEGAL_FETCH_CACHE_ENABLED", "1")
        os.environ.setdefault("LEGAL_SCRAPER_FETCH_CACHE_ENABLED", "1")

    if args.disable_ipfs_cache_mirror:
        os.environ["IPFS_DATASETS_LEGAL_FETCH_CACHE_IPFS_MIRROR"] = "0"
        os.environ["LEGAL_SCRAPER_FETCH_CACHE_IPFS_MIRROR"] = "0"
    else:
        os.environ.setdefault("IPFS_DATASETS_LEGAL_FETCH_CACHE_IPFS_MIRROR", "1")
        os.environ.setdefault("LEGAL_SCRAPER_FETCH_CACHE_IPFS_MIRROR", "1")

    if args.cache_dir:
        cache_dir = str(Path(args.cache_dir).expanduser().resolve())
        os.environ["IPFS_DATASETS_LEGAL_FETCH_CACHE_DIR"] = cache_dir
        os.environ["LEGAL_SCRAPER_FETCH_CACHE_DIR"] = cache_dir


async def _run_all(args: argparse.Namespace) -> Dict[str, Any]:
    from ipfs_datasets_py.processors.legal_scrapers.state_laws_agentic_daemon import (
        run_state_admin_rules_agentic_daemon,
        run_state_court_rules_agentic_daemon,
        run_state_laws_agentic_daemon,
    )

    _apply_cache_env(args)

    output_root = Path(args.output_root).expanduser().resolve() if args.output_root else _default_output_root()
    output_root.mkdir(parents=True, exist_ok=True)
    states = _parse_states(args.states)

    shared_kwargs = {
        "states": states or None,
        "cycle_interval_seconds": float(args.cycle_interval_seconds),
        "max_cycles": int(args.max_cycles),
        "max_statutes": int(args.max_statutes),
        "archive_warmup_urls": int(args.archive_warmup_urls),
        "per_state_timeout_seconds": float(args.per_state_timeout_seconds),
        "router_llm_timeout_seconds": float(args.router_llm_timeout_seconds),
        "router_embeddings_timeout_seconds": float(args.router_embeddings_timeout_seconds),
        "router_ipfs_timeout_seconds": float(args.router_ipfs_timeout_seconds),
        "target_score": float(args.target_score),
        "stop_on_target_score": bool(args.stop_on_target_score),
        "random_seed": args.random_seed,
        "post_cycle_release_enabled": bool(args.post_cycle_release),
    }

    runners: Sequence[tuple[str, Runner, Dict[str, Any]]] = [
        ("state_laws", run_state_laws_agentic_daemon, {}),
        (
            "state_admin_rules",
            run_state_admin_rules_agentic_daemon,
            {
                "admin_parallel_assist_enabled": True,
            },
        ),
        ("state_court_rules", run_state_court_rules_agentic_daemon, {}),
    ]

    summary: Dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "output_root": str(output_root),
        "states": states or "all",
        "cache_env": {
            "enabled": os.environ.get("IPFS_DATASETS_LEGAL_FETCH_CACHE_ENABLED"),
            "ipfs_mirror": os.environ.get("IPFS_DATASETS_LEGAL_FETCH_CACHE_IPFS_MIRROR"),
            "cache_dir": os.environ.get("IPFS_DATASETS_LEGAL_FETCH_CACHE_DIR")
            or os.environ.get("LEGAL_SCRAPER_FETCH_CACHE_DIR"),
        },
        "results": {},
    }

    for corpus_key, runner, extra_kwargs in runners:
        corpus_output_dir = output_root / corpus_key
        result = await runner(
            output_dir=str(corpus_output_dir),
            **shared_kwargs,
            **extra_kwargs,
        )
        summary["results"][corpus_key] = result

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    summary_path = output_root / "run_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    summary["summary_path"] = str(summary_path)
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    summary = asyncio.run(_run_all(args))
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
