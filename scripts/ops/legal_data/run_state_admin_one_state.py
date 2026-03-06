#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

import anyio

from ipfs_datasets_py.processors.legal_scrapers.state_admin_rules_scraper import (
    scrape_state_admin_rules,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run state admin-rules scrape for one state")
    p.add_argument("--state", required=True)
    p.add_argument("--output-json", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--per-state-timeout-seconds", type=float, default=90.0)
    p.add_argument("--require-substantive-rule-text", action=argparse.BooleanOptionalAction, default=False)
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
        parallel_workers=4,
        per_state_retry_attempts=1,
        retry_zero_rule_states=True,
        max_base_statutes=None,
        per_state_timeout_seconds=float(args.per_state_timeout_seconds),
        include_dc=False,
        agentic_fallback_enabled=True,
        agentic_max_candidates_per_state=20,
        agentic_max_fetch_per_state=8,
        agentic_max_results_per_domain=25,
        agentic_max_hops=1,
        agentic_max_pages=10,
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
    args = parse_args()
    payload = anyio.run(_run, args)
    with open(args.output_json, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, indent=2))
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
