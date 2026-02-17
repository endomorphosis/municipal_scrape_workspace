#!/usr/bin/env python3
"""Meta-orchestrator for Common Crawl collections.

Runs the existing CC pipeline orchestrator one collection at a time. After each run:
- validates completeness via validate_collection_completeness.py
- if incomplete, reruns with progressively higher workers + sort memory
- once complete, uploads:
  1) Parquet pointers for that collection to: endomorphosis/common_crawl_pointers_by_collection
  2) DuckDB artifacts (pointer index + optional rowgroup index) to: endomorphosis/common_crawl_meta_indexes

This script intentionally shells out to the existing orchestrator/validator/uploader
so operational behavior remains consistent with manual runs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
ORCH = REPO_ROOT / "src/common_crawl_search_engine/ccindex/cc_pipeline_orchestrator.py"
VALIDATE = REPO_ROOT / "src/common_crawl_search_engine/ccindex/validate_collection_completeness.py"
HF_UPLOAD = REPO_ROOT / "scripts/ops/hf_upload_cc_pointers_by_collection.py"


@dataclass(frozen=True)
class AttemptPlan:
    workers: int
    sort_workers: int
    sort_mem_gb: float


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> int:
    p = subprocess.Popen(cmd, env=env)
    return int(p.wait())


def _run_stream_and_scan(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    scan_patterns: list[tuple[str, re.Pattern[str]]] | None = None,
) -> tuple[int, dict[str, bool]]:
    """Run a subprocess while streaming output, and scan for failure signals.

    Returns: (returncode, flags)
    flags contains keys from scan_patterns entries.
    """

    flags: dict[str, bool] = {}
    pats = scan_patterns or []
    for k, _p in pats:
        flags[k] = False

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None

    for line in proc.stdout:
        # Keep parent logs identical to a normal run.
        sys.stdout.write(line)
        sys.stdout.flush()
        for k, p in pats:
            if not flags.get(k) and p.search(line):
                flags[k] = True

    rc = int(proc.wait())
    return rc, flags


def _bump_sort_memory_gb(curr: float, *, cap_gb: float) -> float:
    bumped = max(float(curr) + 4.0, float(curr) * 1.5)
    return float(min(float(cap_gb), bumped))


def _run_capture_json(cmd: list[str], *, env: dict[str, str] | None = None) -> dict:
    out = subprocess.check_output(cmd, env=env)
    return json.loads(out.decode("utf-8"))


def _collection_to_filter(collection: str) -> str:
    # CC-MAIN-2025-05 -> 2025-05
    parts = collection.split("-")
    if len(parts) < 4 or not parts[2].isdigit():
        raise ValueError(f"Unrecognized collection format: {collection}")
    year = parts[2]
    tail = parts[3]
    return f"{year}-{tail}"


def _collection_year(collection: str) -> str:
    parts = collection.split("-")
    if len(parts) < 3:
        raise ValueError(f"Unrecognized collection format: {collection}")
    return parts[2]


def _iter_collections_from_filter(filter_str: str) -> list[str]:
    """Resolve a list of collections using the validator's collinfo.json.

    Supports:
      - '2025' (year)
      - '2025-05' (single collection suffix)
      - 'CC-MAIN-2025-05' (explicit)
      - 'all'
    """

    # Import validator in-process for collinfo parsing.
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from common_crawl_search_engine.ccindex.validate_collection_completeness import CollectionValidator  # type: ignore

    # Paths here are only used to discover collections; actual validation is done via subprocess.
    v = CollectionValidator(Path("/"), Path("/"), Path("/"))
    all_colls = sorted(v.get_all_collections())

    f = (filter_str or "").strip()
    if not f:
        raise ValueError("filter is required")

    if f == "all":
        return all_colls

    if f.startswith("CC-MAIN-"):
        return [f]

    # 'YYYY'
    if len(f) == 4 and f.isdigit():
        return [c for c in all_colls if c.startswith(f"CC-MAIN-{f}-")]

    # 'YYYY-XX'
    if len(f) == 7 and f[:4].isdigit() and f[4] == "-":
        year = f[:4]
        tail = f[5:]
        want = f"CC-MAIN-{year}-{tail}"
        return [want]

    raise ValueError(f"Unsupported filter format: {filter_str}")


def _default_attempt_plans() -> list[AttemptPlan]:
    return [
        AttemptPlan(workers=8, sort_workers=1, sort_mem_gb=20.0),
        AttemptPlan(workers=12, sort_workers=1, sort_mem_gb=28.0),
        AttemptPlan(workers=16, sort_workers=2, sort_mem_gb=32.0),
        AttemptPlan(workers=24, sort_workers=2, sort_mem_gb=48.0),
    ]


def _make_meta_upload_view(
    *,
    root: Path,
    collection: str,
    pointer_db: Path,
    domain_rowgroup_db: Path | None,
) -> Path:
    """Create a per-collection upload view with symlinks.

    Layout:
      <root>/<year>/<collection>/...

    Returns the root view directory to pass to hf_upload_cc_pointers_by_collection.py.
    """

    year = _collection_year(collection)
    base = root / year / collection
    base.mkdir(parents=True, exist_ok=True)

    def _link(src: Path, dst: Path) -> None:
        if dst.exists() or dst.is_symlink():
            dst.unlink()
        dst.symlink_to(src)

    _link(pointer_db, base / pointer_db.name)

    marker = pointer_db.with_suffix(pointer_db.suffix + ".sorted")
    if marker.exists():
        _link(marker, base / marker.name)

    if domain_rowgroup_db is not None and domain_rowgroup_db.exists():
        _link(domain_rowgroup_db, base / domain_rowgroup_db.name)

    return root


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Orchestrator-of-orchestrators for CC collections")

    ap.add_argument("--config", required=True, help="Path to cc_pipeline_orchestrator JSON config")
    ap.add_argument(
        "--filter",
        required=True,
        help="Collections filter: '2025', '2025-05', 'CC-MAIN-2025-05', or 'all'",
    )
    ap.add_argument(
        "--cc-orchestrator-repo-id",
        default="endomorphosis/common_crawl_pointers_by_collection",
        help="HF dataset repo for parquet pointers",
    )
    ap.add_argument(
        "--meta-indexes-repo-id",
        default="endomorphosis/common_crawl_meta_indexes",
        help="HF dataset repo for DuckDB meta indexes",
    )

    ap.add_argument("--hf-token", default=None, help="HF token (optional); otherwise uses cached login")
    ap.add_argument("--require-xet", action="store_true", default=True, help="Require Xet-enabled destination repos")

    ap.add_argument(
        "--attempt-plans-json",
        default=None,
        help=(
            "Optional JSON list of attempt plans. Example: "
            "[{\"workers\":8,\"sort_workers\":1,\"sort_mem_gb\":20.0}, ...]"
        ),
    )

    ap.add_argument(
        "--domain-rowgroup-index-root",
        default="/home/barberb/ccindex_storage/duckdb/cc_domain_rowgroups_by_collection",
        help="Where to write per-collection domain_rowgroups DuckDBs (avoid /storage default)",
    )

    ap.add_argument(
        "--no-build-domain-rowgroup-index",
        action="store_true",
        help="Pass through to cc_pipeline_orchestrator.py",
    )

    ap.add_argument("--heartbeat-seconds", type=int, default=60)
    ap.add_argument("--yes", action="store_true", help="Pass --yes to orchestrator cleanup prompts")

    ap.add_argument(
        "--max-attempts-per-collection",
        type=int,
        default=12,
        help="Hard cap on orchestrator retries per collection (safety valve).",
    )
    ap.add_argument(
        "--sort-mem-max-gb",
        type=float,
        default=float(os.environ.get("CC_META_SORT_MEM_MAX_GB", "96") or 96),
        help="Maximum sort memory-per-worker (GB) to allow when auto-bumping on failure.",
    )

    args = ap.parse_args(list(argv) if argv is not None else None)

    cfg_path = Path(args.config).expanduser().resolve()
    if not cfg_path.exists():
        raise SystemExit(f"Config not found: {cfg_path}")

    cfg = json.loads(cfg_path.read_text())
    ccindex_root = Path(cfg["ccindex_root"]).expanduser().resolve()
    parquet_root = Path(cfg["parquet_root"]).expanduser().resolve()
    duckdb_collection_root = Path(cfg["duckdb_collection_root"]).expanduser().resolve()
    duckdb_parent = duckdb_collection_root
    # validator expects pointer_dir to be a root that may contain cc_pointers_by_collection
    if duckdb_collection_root.name == "cc_pointers_by_collection":
        duckdb_parent = duckdb_collection_root.parent

    collections = _iter_collections_from_filter(args.filter)
    if not collections:
        raise SystemExit(f"No collections match filter: {args.filter}")

    if args.attempt_plans_json:
        raw = json.loads(args.attempt_plans_json)
        plans = [AttemptPlan(**p) for p in raw]
    else:
        plans = _default_attempt_plans()

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")

    for collection in collections:
        print(f"\n=== Collection {collection} ===", flush=True)
        filter_str = _collection_to_filter(collection)

        # Fast-path: skip collections that are already complete.
        # This matters when resuming a long year run after fixing a single broken collection.
        validate_cmd = [
            sys.executable,
            "-u",
            str(VALIDATE),
            "--collection",
            collection,
            "--ccindex-dir",
            str(ccindex_root),
            "--parquet-dir",
            str(parquet_root),
            "--pointer-dir",
            str(duckdb_parent),
            "--json",
        ]
        pre_status = _run_capture_json(validate_cmd, env=env)
        if bool(pre_status.get("complete")):
            print(f"[skip] already complete: {collection}", flush=True)
            continue

        complete = False
        last_status: dict | None = None

        attempt_idx = 0
        curr_plan_idx = 0
        curr_workers = int(plans[0].workers)
        curr_sort_workers = int(plans[0].sort_workers)
        curr_sort_mem_gb = float(plans[0].sort_mem_gb)

        # Scan patterns for detecting OOM/sort instability.
        scan_patterns: list[tuple[str, re.Pattern[str]]] = [
            (
                "oom",
                re.compile(
                    r"out of memory|MemoryError|std::bad_alloc|cannot allocate memory|oom[- ]kill|killed|SIGKILL",
                    re.I,
                ),
            ),
            (
                "pool_crash",
                re.compile(
                    r"BrokenProcessPool|process-pool crash|process pool.*terminated|terminated abruptly|segmentation fault|segfault",
                    re.I,
                ),
            ),
            (
                "sort_failed",
                re.compile(
                    r"sorting failed|sort failed|duckdb sort crashed|segmentation fault|segfault",
                    re.I,
                ),
            ),
        ]

        while attempt_idx < int(args.max_attempts_per_collection):
            attempt_idx += 1

            # If we have predefined plans left, honor them first (they encode good defaults).
            if curr_plan_idx < len(plans):
                p = plans[curr_plan_idx]
                curr_plan_idx += 1
                curr_workers = int(p.workers)
                curr_sort_workers = int(p.sort_workers)
                curr_sort_mem_gb = float(p.sort_mem_gb)

            print(
                f"[attempt {attempt_idx}] running orchestrator filter={filter_str} workers={curr_workers} sort_workers={curr_sort_workers} sort_mem_gb={curr_sort_mem_gb}",
                flush=True,
            )

            orch_cmd = [
                sys.executable,
                "-u",
                str(ORCH),
                "--config",
                str(cfg_path),
                "--filter",
                filter_str,
                "--resume",
                "--workers",
                str(curr_workers),
                "--sort-workers",
                str(curr_sort_workers),
                "--sort-memory-per-worker-gb",
                str(curr_sort_mem_gb),
                "--heartbeat-seconds",
                str(int(args.heartbeat_seconds)),
                "--cleanup-extraneous",
                "--cleanup-source-archives",
                "--domain-rowgroup-index-root",
                str(Path(args.domain_rowgroup_index_root).expanduser().resolve()),
            ]
            if args.yes:
                orch_cmd.append("--yes")
            if args.no_build_domain_rowgroup_index:
                orch_cmd.append("--no-build-domain-rowgroup-index")

            rc, flags = _run_stream_and_scan(orch_cmd, env=env, scan_patterns=scan_patterns)
            print(f"[attempt {attempt_idx}] orchestrator rc={rc} flags={flags}", flush=True)

            validate_cmd = [
                sys.executable,
                "-u",
                str(VALIDATE),
                "--collection",
                collection,
                "--ccindex-dir",
                str(ccindex_root),
                "--parquet-dir",
                str(parquet_root),
                "--pointer-dir",
                str(duckdb_parent),
                "--json",
            ]
            status = _run_capture_json(validate_cmd, env=env)
            last_status = status
            complete = bool(status.get("complete"))

            print(
                f"[attempt {attempt_idx}] validator complete={complete} parquet={status.get('parquet_converted')} pointer_index={status.get('pointer_index_exists')}",
                flush=True,
            )

            if complete:
                break

            # If we saw OOM-like signals, prioritize memory bump + reduced concurrency.
            if bool(flags.get("oom")):
                next_mem = _bump_sort_memory_gb(curr_sort_mem_gb, cap_gb=float(args.sort_mem_max_gb))
                next_workers = max(1, int(curr_sort_workers // 2))
                if next_mem > curr_sort_mem_gb or next_workers < curr_sort_workers:
                    print(
                        f"[attempt {attempt_idx}] OOM-like failure detected; adjusting sort retry: "
                        f"sort_workers {curr_sort_workers}->{next_workers}, sort_mem_gb {curr_sort_mem_gb}->{next_mem}",
                        flush=True,
                    )
                curr_sort_workers = int(next_workers)
                curr_sort_mem_gb = float(next_mem)
                # Small backoff to reduce thrash.
                time.sleep(10.0)
                continue

            # Other sort instability signals: cautiously bump memory only.
            if bool(flags.get("pool_crash")) or bool(flags.get("sort_failed")) or rc != 0:
                next_mem = _bump_sort_memory_gb(curr_sort_mem_gb, cap_gb=float(args.sort_mem_max_gb))
                if next_mem > curr_sort_mem_gb:
                    print(
                        f"[attempt {attempt_idx}] sort instability detected; bumping sort_mem_gb {curr_sort_mem_gb}->{next_mem} and retrying",
                        flush=True,
                    )
                    curr_sort_mem_gb = float(next_mem)
                    time.sleep(10.0)
                    continue

            # Otherwise: fall through and try the next predefined plan (if any) or retry as-is.

        if not complete:
            raise SystemExit(
                f"Collection did not complete after {attempt_idx} attempts: {collection}. Last status: {json.dumps(last_status or {}, indent=2)}"
            )

        # Upload parquet pointers for this collection.
        year = _collection_year(collection)
        parquet_src = parquet_root / "cc_pointers_by_collection"
        if not parquet_src.exists():
            raise SystemExit(f"Parquet source root missing: {parquet_src}")

        print(f"[upload pointers] {collection} -> {args.cc_orchestrator_repo_id}", flush=True)
        pointers_cmd = [
            sys.executable,
            "-u",
            str(HF_UPLOAD),
            "--repo-id",
            args.cc_orchestrator_repo_id,
            "--src",
            str(parquet_src),
            "--years",
            year,
            "--collections",
            collection,
            "--chunk-by",
            "collection",
            "--num-workers",
            "1",
            "--max-get-upload-mode-workers",
            "1",
            "--max-preupload-workers",
            "1",
            "--sha256-cache",
            "rw",
        ]
        if args.hf_token:
            pointers_cmd += ["--token", args.hf_token]
        if args.require_xet:
            pointers_cmd += ["--require-xet"]

        rc = _run(pointers_cmd, env=env)
        if rc != 0:
            raise SystemExit(f"Pointer upload failed for {collection} rc={rc}")

        # Upload DuckDB meta indexes for this collection.
        pointer_db = duckdb_collection_root / f"{collection}.duckdb"
        if not pointer_db.exists():
            raise SystemExit(f"Missing pointer DuckDB for {collection}: {pointer_db}")

        domain_rowgroup_db = Path(args.domain_rowgroup_index_root).expanduser().resolve() / f"{collection}.domain_rowgroups.duckdb"
        if not domain_rowgroup_db.exists():
            domain_rowgroup_db = None

        with tempfile.TemporaryDirectory(prefix=f"cc_meta_upload_{collection}_") as td:
            view_root = Path(td) / "meta_indexes"
            view_root.mkdir(parents=True, exist_ok=True)
            _make_meta_upload_view(
                root=view_root,
                collection=collection,
                pointer_db=pointer_db,
                domain_rowgroup_db=domain_rowgroup_db,
            )

            print(f"[upload meta indexes] {collection} -> {args.meta_indexes_repo_id}", flush=True)
            meta_cmd = [
                sys.executable,
                "-u",
                str(HF_UPLOAD),
                "--repo-id",
                args.meta_indexes_repo_id,
                "--src",
                str(view_root),
                "--years",
                year,
                "--collections",
                collection,
                "--chunk-by",
                "collection",
                "--num-workers",
                "1",
                "--max-get-upload-mode-workers",
                "1",
                "--max-preupload-workers",
                "1",
                "--sha256-cache",
                "rw",
                "--include-suffix",
                ".duckdb",
                "--include-suffix",
                ".duckdb.sorted",
                "--include-suffix",
                ".domain_rowgroups.duckdb",
            ]
            if args.hf_token:
                meta_cmd += ["--token", args.hf_token]
            if args.require_xet:
                meta_cmd += ["--require-xet"]

            rc = _run(meta_cmd, env=env)
            if rc != 0:
                raise SystemExit(f"Meta index upload failed for {collection} rc={rc}")

        print(f"[done] {collection}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
