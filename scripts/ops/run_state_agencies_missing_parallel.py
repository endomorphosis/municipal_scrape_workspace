#!/usr/bin/env python3
"""Run state agency scraping for a list of jurisdictions in parallel.

This avoids shell quoting issues (e.g., "United States Virgin Islands") and
writes one JSONL + stderr log per jurisdiction plus a run summary JSON.

Intended as an ops helper; output is deterministic and easy to resume.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Item:
    abbr: str
    name: str


def _load_items(path: Path) -> list[Item]:
    raw = json.loads(path.read_text("utf-8"))
    items: list[Item] = []
    for it in raw:
        abbr = str(it.get("abbr", "")).strip()
        name = str(it.get("name", "")).strip()
        if not abbr or not name:
            raise ValueError(f"Invalid item in {path}: {it!r}")
        items.append(Item(abbr=abbr, name=name))
    return items


def _count_lines(path: Path) -> int:
    try:
        with path.open("rb") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return -1


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--missing-list",
        default="artifacts/missing_jurisdictions/missing_list.json",
        help="JSON list of {abbr,name} items",
    )
    p.add_argument(
        "--outdir",
        default="artifacts/missing_jurisdictions",
        help="Directory for per-jurisdiction outputs/logs",
    )
    p.add_argument("--max-workers", type=int, default=6)
    p.add_argument("--per-state-timeout", type=int, default=1200)
    p.add_argument("--agency-max-pages", type=int, default=250)
    p.add_argument("--agency-max-depth", type=int, default=3)
    p.add_argument("--sleep", type=float, default=0.2)
    p.add_argument("--seed-timeout", type=int, default=30)
    p.add_argument(
        "--state-domains-script",
        default="data/state_domains/state_domains.py",
        help="Path to state_domains.py",
    )
    p.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing *.jsonl and *.stderr.log before running",
    )
    args = p.parse_args(argv)

    missing_list = Path(args.missing_list)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    items = _load_items(missing_list)

    if args.clean:
        for pat in ("*.jsonl", "*.stderr.log", "run_summary.json"):
            for f in outdir.glob(pat):
                try:
                    f.unlink()
                except FileNotFoundError:
                    pass

    state_script = Path(args.state_domains_script)

    def run_one(item: Item) -> dict[str, Any]:
        ab = item.abbr
        name = item.name
        out_path = outdir / f"{ab}.jsonl"
        err_path = outdir / f"{ab}.stderr.log"

        cmd = [
            sys.executable,
            str(state_script),
            "--mode",
            "agencies",
            "--jurisdiction",
            name,
            "--agency-max-pages",
            str(args.agency_max_pages),
            "--agency-max-depth",
            str(args.agency_max_depth),
            "--sleep",
            str(args.sleep),
            "--seed-timeout",
            str(args.seed_timeout),
            "--out",
            str(out_path),
        ]

        started = time.time()
        rc: int
        timed_out = False

        with err_path.open("w", encoding="utf-8") as err:
            try:
                r = subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=err,
                    timeout=args.per_state_timeout,
                )
                rc = r.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                rc = 124
            except KeyboardInterrupt:
                # Preserve partial results but exit quickly.
                raise

        elapsed_s = time.time() - started
        lines = _count_lines(out_path)
        stderr_bytes = err_path.stat().st_size if err_path.exists() else -1

        return {
            "abbr": ab,
            "name": name,
            "rc": rc,
            "timed_out": timed_out,
            "lines": lines,
            "stderr_bytes": stderr_bytes,
            "elapsed_s": round(elapsed_s, 3),
            "out": str(out_path),
            "stderr": str(err_path),
        }

    results: list[dict[str, Any]] = []

    # Run in parallel, printing status as each completes.
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = [ex.submit(run_one, it) for it in items]
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            print(
                f"{res['abbr']}: rc={res['rc']} lines={res['lines']} "
                f"timeout={res['timed_out']} elapsed_s={res['elapsed_s']} stderr_bytes={res['stderr_bytes']}",
                flush=True,
            )

    results = sorted(results, key=lambda r: r["abbr"])
    (outdir / "run_summary.json").write_text(json.dumps(results, indent=2), "utf-8")

    bad = [r for r in results if r["rc"] != 0 or r["lines"] <= 0]
    print(f"wrote {outdir / 'run_summary.json'}")
    print(f"failed_or_empty {len(bad)}")
    for r in bad:
        print(
            f" - {r['abbr']}: rc={r['rc']} lines={r['lines']} timeout={r['timed_out']} "
            f"stderr_bytes={r['stderr_bytes']}",
            flush=True,
        )

    return 0 if not bad else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
