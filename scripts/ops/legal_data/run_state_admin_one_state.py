#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import anyio

from ipfs_datasets_py.processors.legal_scrapers.state_admin_rules_scraper import (
    scrape_state_admin_rules,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _reexec_in_repo_venv() -> None:
    if os.environ.get("MUNICIPAL_SCRAPE_IN_VENV", "").lower() == "true":
        return

    venv_python = _repo_root() / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return

    try:
        in_venv = Path(sys.prefix).resolve() == venv_python.parent.parent.resolve()
    except Exception:
        in_venv = False

    if in_venv:
        return

    os.environ["MUNICIPAL_SCRAPE_IN_VENV"] = "true"
    os.execv(str(venv_python), [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run state admin-rules scrape for one state")
    p.add_argument("--state", required=True)
    p.add_argument("--output-json", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--per-state-timeout-seconds", type=float, default=90.0)
    p.add_argument("--require-substantive-rule-text", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--retry-zero-rule-states", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--agentic-max-candidates-per-state", type=int, default=40)
    p.add_argument("--agentic-max-fetch-per-state", type=int, default=16)
    p.add_argument("--agentic-max-results-per-domain", type=int, default=35)
    p.add_argument("--agentic-max-hops", type=int, default=2)
    p.add_argument("--agentic-max-pages", type=int, default=18)
    p.add_argument("--agentic-fetch-concurrency", type=int, default=6)
    p.add_argument("--parallel-workers", type=int, default=6)
    return p.parse_args()


async def _run(args: argparse.Namespace) -> dict:
    state = str(args.state or "").strip().upper()
    result = await scrape_state_admin_rules(
        states=[state],
        output_format="json",
        include_metadata=True,
        rate_limit_delay=0.2,
        max_rules=None,
        output_dir=args.output_dir,
        write_jsonld=True,
        strict_full_text=False,
        min_full_text_chars=200,
        hydrate_rule_text=True,
        parallel_workers=int(args.parallel_workers),
        per_state_retry_attempts=1,
        retry_zero_rule_states=bool(args.retry_zero_rule_states),
        max_base_statutes=None,
        per_state_timeout_seconds=float(args.per_state_timeout_seconds),
        include_dc=False,
        agentic_fallback_enabled=True,
        agentic_max_candidates_per_state=int(args.agentic_max_candidates_per_state),
        agentic_max_fetch_per_state=int(args.agentic_max_fetch_per_state),
        agentic_max_results_per_domain=int(args.agentic_max_results_per_domain),
        agentic_max_hops=int(args.agentic_max_hops),
        agentic_max_pages=int(args.agentic_max_pages),
        agentic_fetch_concurrency=int(args.agentic_fetch_concurrency),
        write_agentic_kg_corpus=True,
        require_substantive_rule_text=bool(args.require_substantive_rule_text),
    )
    meta = result.get("metadata") or {}
    return {
        "state": state,
        "status": result.get("status"),
        "rules_count": int(meta.get("rules_count") or 0),
        "states_with_rules": list(meta.get("states_with_rules") or []),
        "missing_rule_states": list(meta.get("missing_rule_states") or []),
    }


def main() -> int:
    _reexec_in_repo_venv()
    args = parse_args()
    payload = anyio.run(_run, args)
    with open(args.output_json, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, indent=2))
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
