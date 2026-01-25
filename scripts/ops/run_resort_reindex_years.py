#!/usr/bin/env python3
"""Run the CC pipeline orchestrator sequentially across years.

Purpose
- Resort/rewrite already-sorted Parquet shards with optimized row-group sizing
- Rebuild per-collection DuckDB pointer indexes
- Rebuild per-collection domain->rowgroup slice indexes (cc_domain_rowgroups)

This script is intentionally thin: it just loops years and shells out to the
existing orchestrator so the behavior stays consistent with production.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def _parse_years(value: str) -> list[int]:
    years: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        years.append(int(part))
    if not years:
        raise ValueError("No years provided")
    return years


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Sequentially run resort+reindex for selected years")
    ap.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Path to repo root (default: inferred from this script)",
    )
    ap.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to pipeline_config.json (default: <repo-root>/pipeline_config.json)",
    )
    ap.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years, e.g. '2021,2022,2023' (default: 2021-2025)",
    )
    ap.add_argument("--start-year", type=int, default=None, help="Start year (inclusive)")
    ap.add_argument("--end-year", type=int, default=None, help="End year (inclusive)")
    ap.add_argument("--workers", type=int, default=8, help="Max workers for orchestrator")
    ap.add_argument("--heartbeat-seconds", type=int, default=30, help="Heartbeat interval")
    ap.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue to next year even if a year fails",
    )

    ap.add_argument(
        "--cc-sort-row-group-strategy",
        type=str,
        default=os.getenv("CC_SORT_ROW_GROUP_STRATEGY", "domain_pct"),
    )
    ap.add_argument(
        "--cc-sort-row-group-domain-pct",
        type=str,
        default=os.getenv("CC_SORT_ROW_GROUP_DOMAIN_PCT", "90"),
    )
    ap.add_argument(
        "--cc-sort-row-group-min-mb",
        type=str,
        default=os.getenv("CC_SORT_ROW_GROUP_MIN_MB", "8"),
    )

    args = ap.parse_args(argv)

    repo_root: Path = args.repo_root.resolve()
    config: Path = (args.config or (repo_root / "pipeline_config.json")).resolve()
    if not config.exists():
        print(f"ERROR: config not found: {config}", file=sys.stderr)
        return 2

    if args.years:
        years = _parse_years(args.years)
    else:
        start = int(args.start_year) if args.start_year is not None else 2021
        end = int(args.end_year) if args.end_year is not None else 2025
        if end < start:
            raise ValueError("end-year must be >= start-year")
        years = list(range(start, end + 1))

    logs_dir = (repo_root / "logs").resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = str((repo_root / "src").resolve())
    env["CC_SORT_ROW_GROUP_STRATEGY"] = str(args.cc_sort_row_group_strategy)
    env["CC_SORT_ROW_GROUP_DOMAIN_PCT"] = str(args.cc_sort_row_group_domain_pct)
    env["CC_SORT_ROW_GROUP_MIN_MB"] = str(args.cc_sort_row_group_min_mb)

    overall_ok = True

    for year in years:
        ts = time.strftime("%Y%m%d_%H%M%S")
        log_path = logs_dir / f"orchestrator_resort_reindex_{year}_{ts}.log"
        pid_path = logs_dir / f"orchestrator_resort_reindex_{year}_{ts}.pid"

        cmd = [
            sys.executable,
            "-m",
            "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator",
            "--config",
            str(config),
            "--filter",
            str(year),
            "--force-reindex",
            "--rewrite-sorted-parquet",
            "--heartbeat-seconds",
            str(int(args.heartbeat_seconds)),
            "--workers",
            str(int(args.workers)),
            "--yes",
        ]

        print(f"\n=== Year {year} ===", flush=True)
        print(f"log: {log_path}", flush=True)
        print("cmd: " + " ".join(cmd), flush=True)

        with open(log_path, "w", encoding="utf-8") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, env=env, cwd=str(repo_root))
            pid_path.write_text(str(proc.pid) + "\n", encoding="utf-8")
            rc = proc.wait()

        if rc != 0:
            overall_ok = False
            print(f"Year {year} failed (exit {rc}); see {log_path}", file=sys.stderr, flush=True)
            if not args.continue_on_error:
                return rc

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
