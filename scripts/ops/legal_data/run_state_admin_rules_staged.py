#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import anyio

from ipfs_datasets_py.processors.legal_scrapers.state_admin_rules_scraper import (
    US_50_STATE_CODES,
    scrape_state_admin_rules,
)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[idx: idx + size] for idx in range(0, len(items), size)]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_kg_rows(batch_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for state_block in batch_data:
        if not isinstance(state_block, dict):
            continue
        state_code = str(state_block.get("state_code") or "").strip().upper()
        state_name = str(state_block.get("state_name") or "").strip()
        statutes = list(state_block.get("statutes") or [])
        for statute in statutes:
            if not isinstance(statute, dict):
                continue
            url = str(statute.get("source_url") or "").strip()
            text = str(statute.get("full_text") or "").strip()
            if not state_code or not url:
                continue
            rows.append(
                {
                    "state_code": state_code,
                    "state_name": state_name,
                    "url": url,
                    "domain": urlparse(url).netloc,
                    "title": str(statute.get("section_name") or statute.get("short_title") or "").strip(),
                    "text": text,
                    "query": f"{state_name or state_code} administrative code regulations agency rules",
                    "source": "state_admin_rules_pipeline",
                    "official_cite": str(statute.get("official_cite") or "").strip(),
                    "section_number": str(statute.get("section_number") or "").strip(),
                    "fetched_at": _now(),
                }
            )
    return rows


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            if not isinstance(row, dict):
                continue
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            written += 1
    return written


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    tmp_path.replace(path)


def _load_existing_summary(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


async def run_staged(args: argparse.Namespace) -> dict[str, Any]:
    states = list(US_50_STATE_CODES)
    batches = _chunks(states, max(1, int(args.batch_size)))

    start_batch = max(1, int(args.start_batch))
    end_batch = int(args.end_batch) if args.end_batch else len(batches)
    end_batch = min(end_batch, len(batches))

    report_path = Path(args.report_path).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_root = Path(args.output_dir).expanduser().resolve() if args.output_dir else report_path.parent / "output"
    output_root.mkdir(parents=True, exist_ok=True)
    kg_corpus_path = output_root / "aggregates" / "state_admin_rule_kg_corpus_all_states.jsonl"
    existing_summary = _load_existing_summary(report_path)
    if kg_corpus_path.exists() and args.start_batch <= 1:
        kg_corpus_path.unlink(missing_ok=True)

    prior_batches: list[dict[str, Any]] = []
    prior_states_with_rules_union: set[str] = set()
    prior_missing_states_union: set[str] = set()
    if args.start_batch > 1 and isinstance(existing_summary, dict):
        for batch in list(existing_summary.get("batches") or []):
            if not isinstance(batch, dict):
                continue
            batch_index = int(batch.get("batch_index") or 0)
            if 0 < batch_index < start_batch:
                prior_batches.append(batch)
                prior_states_with_rules_union.update(str(state or "").upper() for state in list(batch.get("states_with_rules") or []))
                prior_missing_states_union.update(str(state or "").upper() for state in list(batch.get("missing_rule_states") or []))

    run_summary: dict[str, Any] = {
        "started_at": _now(),
        "batch_size": int(args.batch_size),
        "start_batch": start_batch,
        "end_batch": end_batch,
        "total_batches": len(batches),
        "states_targeted": states,
        "batches": prior_batches,
        "states_with_rules_union": sorted(prior_states_with_rules_union),
        "missing_states_union": sorted(prior_missing_states_union),
        "status": "running",
    }
    if args.start_batch > 1 and prior_batches:
        run_summary["resumed_from_batch"] = start_batch
        run_summary["resumed_at"] = _now()
    if isinstance(existing_summary, dict) and existing_summary.get("kg_corpus_jsonl"):
        run_summary["kg_corpus_jsonl"] = str(existing_summary.get("kg_corpus_jsonl"))

    _write_json_atomic(report_path, run_summary)

    states_with_rules_union: set[str] = set(prior_states_with_rules_union)
    missing_states_union: set[str] = set(prior_missing_states_union)

    for batch_idx in range(start_batch, end_batch + 1):
        batch_states = batches[batch_idx - 1]
        run_summary["current_batch_index"] = batch_idx
        run_summary["current_batch_states"] = batch_states
        run_summary["updated_at"] = _now()
        _write_json_atomic(report_path, run_summary)
        print(f"batch_start index={batch_idx} states={batch_states}", flush=True)

        result = await scrape_state_admin_rules(
            states=batch_states,
            output_format=args.output_format,
            include_metadata=True,
            rate_limit_delay=float(args.rate_limit_delay),
            max_rules=(int(args.max_rules) if args.max_rules and int(args.max_rules) > 0 else None),
            output_dir=str(output_root),
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
            agentic_fallback_enabled=bool(args.agentic_fallback_enabled),
            agentic_max_candidates_per_state=int(args.agentic_max_candidates_per_state),
            agentic_max_fetch_per_state=int(args.agentic_max_fetch_per_state),
            agentic_max_results_per_domain=int(args.agentic_max_results_per_domain),
            agentic_max_hops=int(args.agentic_max_hops),
            agentic_max_pages=int(args.agentic_max_pages),
            agentic_fetch_concurrency=int(args.agentic_fetch_concurrency),
            write_agentic_kg_corpus=bool(args.write_agentic_kg_corpus),
            require_substantive_rule_text=bool(args.require_substantive_rule_text),
        )

        metadata = result.get("metadata") or {}
        batch_data = list(result.get("data") or [])
        states_with_rules = list(metadata.get("states_with_rules") or [])
        missing_states = list(metadata.get("missing_rule_states") or [])

        batch_kg_rows = _extract_kg_rows(batch_data)
        batch_kg_rows_written = _append_jsonl(kg_corpus_path, batch_kg_rows)

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
            "agentic_recovered_states": list(metadata.get("agentic_recovered_states") or []),
            "agentic_attempted_states": list(metadata.get("agentic_attempted_states") or []),
            "kg_rows_written": int(batch_kg_rows_written),
            "scraped_at": metadata.get("scraped_at"),
            "elapsed_time_seconds": metadata.get("elapsed_time_seconds"),
        }
        run_summary["batches"].append(batch_summary)

        run_summary["states_with_rules_union"] = sorted(states_with_rules_union)
        run_summary["missing_states_union"] = sorted(missing_states_union)
        run_summary["kg_corpus_jsonl"] = str(kg_corpus_path)
        run_summary["updated_at"] = _now()

        _write_json_atomic(report_path, run_summary)
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
            ),
            flush=True,
        )

    run_summary["completed_at"] = _now()
    run_summary["status"] = "completed"
    run_summary["states_with_rules_union"] = sorted(states_with_rules_union)
    run_summary["missing_states_union"] = sorted(missing_states_union)
    run_summary["states_with_rules_union_count"] = len(states_with_rules_union)
    run_summary["missing_states_union_count"] = len(missing_states_union)
    run_summary["kg_corpus_jsonl"] = str(kg_corpus_path)
    run_summary["overall_coverage_ratio"] = (
        len(states_with_rules_union) / float(len(states)) if states else 0.0
    )
    run_summary.pop("current_batch_index", None)
    run_summary.pop("current_batch_states", None)

    _write_json_atomic(report_path, run_summary)
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
    parser.add_argument(
        "--per-state-timeout-seconds",
        type=float,
        default=86400.0,
        help="Per-state timeout in seconds. Use 0 or a negative value to disable the state time limit.",
    )
    parser.add_argument("--min-full-text-chars", type=int, default=300)
    parser.add_argument("--max-rules", type=int, default=0)
    parser.add_argument("--max-base-statutes", type=int, default=0)
    parser.add_argument(
        "--agentic-fallback-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable agentic web-archiving fallback for zero-rule states",
    )
    parser.add_argument("--agentic-max-candidates-per-state", type=int, default=1000)
    parser.add_argument("--agentic-max-fetch-per-state", type=int, default=1000)
    parser.add_argument("--agentic-max-results-per-domain", type=int, default=1000)
    parser.add_argument("--agentic-max-hops", type=int, default=4)
    parser.add_argument("--agentic-max-pages", type=int, default=1000)
    parser.add_argument("--agentic-fetch-concurrency", type=int, default=6)
    parser.add_argument(
        "--write-agentic-kg-corpus",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write agentic discovery ETL corpus JSONL for KG downstream",
    )
    parser.add_argument(
        "--require-substantive-rule-text",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require substantive administrative rule text (filters portal/reference placeholders)",
    )
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
