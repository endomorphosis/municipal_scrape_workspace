#!/usr/bin/env python3
"""Run the CC pipeline orchestrator sequentially across multiple years.

This is a thin wrapper around the orchestrator so behavior stays consistent.
Defaults are tuned for the "continue processing years" workflow:
- resume enabled (orchestrator default)
- rewrite sorted parquet enabled (row-group sizing)
- build per-collection domain->rowgroup slice indexes enabled
- update per-year global domain+rowgroup index enabled (Stage 6)

Logs and per-year PID files are written under <repo-root>/logs.
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
	ap = argparse.ArgumentParser(description="Sequentially run the pipeline for selected years")
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
		help="Comma-separated years, e.g. '2022,2023,2024,2025' (default: 2022-2025)",
	)
	ap.add_argument("--start-year", type=int, default=None, help="Start year (inclusive)")
	ap.add_argument("--end-year", type=int, default=None, help="End year (inclusive)")

	ap.add_argument("--workers", type=int, default=8, help="Max workers for orchestrator")
	ap.add_argument("--heartbeat-seconds", type=int, default=30, help="Heartbeat interval")

	ap.add_argument("--sort-workers", type=int, default=1, help="Parallelism for parquet sort/rewrite")
	ap.add_argument("--sort-memory-per-worker-gb", type=float, default=2.0)
	ap.add_argument(
		"--sort-temp-dir-base",
		type=Path,
		default=Path("/storage/ccindex_parquet/tmp/duckdb_sort"),
		help=(
			"Base temp directory for DuckDB sort spill; per-year dirs will be created under this path. "
			"Default: /storage/ccindex_parquet/tmp/duckdb_sort (keeps temp files on the same ZFS dataset as parquet outputs)"
		),
	)

	ap.add_argument(
		"--existing-parquet-only",
		action="store_true",
		help="Only process collections with existing parquet",
	)

	ap.add_argument(
		"--rewrite-sorted-parquet",
		action="store_true",
		default=True,
		help="Rewrite sorted parquet shards to apply row-group sizing (default: on)",
	)
	ap.add_argument(
		"--no-rewrite-sorted-parquet",
		dest="rewrite_sorted_parquet",
		action="store_false",
		help="Disable rewriting sorted parquet",
	)

	ap.add_argument(
		"--update-domain-year-index",
		action="store_true",
		default=True,
		help="Update per-year global domain index at end (Stage 6) (default: on)",
	)
	ap.add_argument(
		"--no-update-domain-year-index",
		dest="update_domain_year_index",
		action="store_false",
		help="Disable per-year global domain index update",
	)

	ap.add_argument(
		"--continue-on-error",
		action="store_true",
		help="Continue to next year even if a year fails",
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
		start = int(args.start_year) if args.start_year is not None else 2022
		end = int(args.end_year) if args.end_year is not None else 2025
		if end < start:
			raise ValueError("end-year must be >= start-year")
		years = list(range(start, end + 1))

	logs_dir = (repo_root / "logs").resolve()
	logs_dir.mkdir(parents=True, exist_ok=True)

	env = os.environ.copy()
	env["PYTHONPATH"] = str((repo_root / "src").resolve())

	overall_ok = True

	for year in years:
		ts = time.strftime("%Y%m%d_%H%M%S")
		label = f"orchestrator_pipeline_{year}_{ts}"
		log_path = logs_dir / f"{label}.log"
		pid_path = logs_dir / f"{label}.pid"

		sort_tmp = (repo_root / args.sort_temp_dir_base / str(year)).resolve()
		sort_tmp.mkdir(parents=True, exist_ok=True)

		cmd: list[str] = [
			sys.executable,
			"-m",
			"common_crawl_search_engine.ccindex.cc_pipeline_orchestrator",
			"--config",
			str(config),
			"--filter",
			str(year),
			"--workers",
			str(int(args.workers)),
			"--heartbeat-seconds",
			str(int(args.heartbeat_seconds)),
			"--sort-workers",
			str(int(args.sort_workers)),
			"--sort-memory-per-worker-gb",
			str(float(args.sort_memory_per_worker_gb)),
			"--sort-temp-dir",
			str(sort_tmp),
			"--yes",
		]

		if args.existing_parquet_only:
			cmd.append("--existing-parquet-only")

		if args.rewrite_sorted_parquet:
			cmd.append("--rewrite-sorted-parquet")

		if args.update_domain_year_index:
			cmd.append("--update-domain-year-index")

		cmd.append("--build-domain-rowgroup-index")

		print(f"\n=== Year {year} ===", flush=True)
		print(f"log: {log_path}", flush=True)
		print("cmd: " + " ".join(cmd), flush=True)

		with open(log_path, "w", encoding="utf-8") as out:
			out.write(f"# started {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
			out.write("# cmd: " + " ".join(cmd) + "\n")
			out.flush()

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