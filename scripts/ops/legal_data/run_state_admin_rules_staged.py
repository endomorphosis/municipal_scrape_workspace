#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anyio

from ipfs_datasets_py.processors.legal_scrapers.state_admin_rules_scraper import (
    US_50_STATE_CODES,
    scrape_state_admin_rules,
)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[idx: idx + size] for idx in range(0, len(items), size)]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def run_staged(args: argparse.Namespace) -> dict[str, Any]:
    states = list(US_50_STATE_CODES)
    batches = _chunks(states, max(1, int(args.batch_size)))

    start_batch = max(1, int(args.start_batch))
    end_batch = int(args.end_batch) if args.end_batch else len(batches)
    end_batch = min(end_batch, len(batches))

    report_path = Path(args.report_path).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    run_summary: dict[str, Any] = {
        "started_at": _now(),
        "batch_size": int(args.batch_size),
        "start_batch": start_batch,
        "end_batch": end_batch,
        "total_batches": len(batches),
        "states_targeted": states,
        "batches": [],
        "states_with_rules_union": [],
        "missing_states_union": [],
    }

    states_with_rules_union: set[str] = set()
    missing_states_union: set[str] = set()

    for batch_idx in range(start_batch, end_batch + 1):
        batch_states = batches[batch_idx - 1]
        print(f"batch_start index={batch_idx} states={batch_states}")

        result = await scrape_state_admin_rules(
            states=batch_states,
            output_format=args.output_format,
            include_metadata=True,
            rate_limit_delay=float(args.rate_limit_delay),
            max_rules=(int(args.max_rules) if args.max_rules and int(args.max_rules) > 0 else None),
            output_dir=args.output_dir,
            write_jsonld=bool(args.write_jsonld),
            strict_full_text=bool(args.strict_full_text),
            min_full_text_chars=int(args.min_full_text_chars),
            hydrate_rule_text=bool(args.hydrate_rule_text),
            parallel_workers=int(args.parallel_workers),
            per_state_retry_attempts=int(args.per_state_retry_attempts),
            retry_zero_rule_states=bool(args.retry_zero_rule_states),
            max_base_statutes=(
                int(args.max_base_statutes)
                if args.max_base_statutes and int(args.max_base_statutes) > 0
                else None
            ),
            per_state_timeout_seconds=float(args.per_state_timeout_seconds),
            include_dc=False,
        )

        metadata = result.get("metadata") or {}
        states_with_rules = list(metadata.get("states_with_rules") or [])
        missing_states = list(metadata.get("missing_rule_states") or [])

        states_with_rules_union.update(states_with_rules)
        missing_states_union.update(missing_states)

        batch_summary = {
            "batch_index": batch_idx,
            "states": batch_states,
            "status": result.get("status"),
            "rules_count": int(metadata.get("rules_count") or 0),
            "states_with_rules_count": int(metadata.get("states_with_rules_count") or 0),
            "states_with_rules": states_with_rules,
            "missing_rule_states": missing_states,
            "coverage_ratio": float(metadata.get("coverage_ratio") or 0.0),
            "scraped_at": metadata.get("scraped_at"),
            "elapsed_time_seconds": metadata.get("elapsed_time_seconds"),
        }
        run_summary["batches"].append(batch_summary)

        run_summary["states_with_rules_union"] = sorted(states_with_rules_union)
        run_summary["missing_states_union"] = sorted(missing_states_union)
        run_summary["updated_at"] = _now()

        report_path.write_text(json.dumps(run_summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            "batch_done "
            + json.dumps(
                {
                    "batch_index": batch_idx,
                    "status": batch_summary["status"],
                    "rules_count": batch_summary["rules_count"],
                    "states_with_rules_count": batch_summary["states_with_rules_count"],
                    "coverage_ratio": batch_summary["coverage_ratio"],
                },
                ensure_ascii=False,
            )
        )

    run_summary["completed_at"] = _now()
    run_summary["states_with_rules_union"] = sorted(states_with_rules_union)
    run_summary["missing_states_union"] = sorted(missing_states_union)
    run_summary["states_with_rules_union_count"] = len(states_with_rules_union)
    run_summary["missing_states_union_count"] = len(missing_states_union)
    run_summary["overall_coverage_ratio"] = (
        len(states_with_rules_union) / float(len(states)) if states else 0.0
    )

    report_path.write_text(json.dumps(run_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return run_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run staged 50-state admin-rule scraping with coverage reporting")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--start-batch", type=int, default=1)
    parser.add_argument("--end-batch", type=int, default=0, help="0 means all remaining batches")
    parser.add_argument(
        "--report-path",
        default="artifacts/state_admin_rules/staged_50_state_coverage_report.json",
    )
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--output-format", default="json")
    parser.add_argument("--write-jsonld", action="store_true")
    parser.add_argument("--strict-full-text", action="store_true")
    parser.add_argument("--hydrate-rule-text", action="store_true")
    parser.add_argument("--retry-zero-rule-states", action="store_true")
    parser.add_argument("--rate-limit-delay", type=float, default=1.5)
    parser.add_argument("--parallel-workers", type=int, default=6)
    parser.add_argument("--per-state-retry-attempts", type=int, default=1)
    parser.add_argument("--per-state-timeout-seconds", type=float, default=480.0)
    parser.add_argument("--min-full-text-chars", type=int, default=300)
    parser.add_argument("--max-rules", type=int, default=0)
    parser.add_argument("--max-base-statutes", type=int, default=0)
    args = parser.parse_args()
    if args.end_batch == 0:
        args.end_batch = None
    return args


def main() -> int:
    args = parse_args()
    summary = anyio.run(run_staged, args)
    print(
        "run_done "
        + json.dumps(
            {
                "states_with_rules_union_count": summary.get("states_with_rules_union_count"),
                "missing_states_union_count": summary.get("missing_states_union_count"),
                "overall_coverage_ratio": summary.get("overall_coverage_ratio"),
                "report_path": str(Path(args.report_path).expanduser().resolve()),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
