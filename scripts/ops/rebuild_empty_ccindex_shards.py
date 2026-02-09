#!/usr/bin/env python3
"""Rebuild shards that were previously marked as empty.

This targets Parquet shards with a sidecar marker:
  cdx-XXXXX.gz.parquet.empty

Those markers are created by the converter when the source cdx-XXXXX.gz contains
no parsable rows.

Workflow (per shard)
--------------------
1) (Optional) Download the corresponding cdx-XXXXX.gz into /storage/ccindex/<collection>/
2) Delete existing Parquet outputs for that shard (unsorted, sorted, marker) + sort work dir
3) Re-run conversion for only that shard
4) Re-run sort/mark for only that shard

Notes
-----
- This script does NOT rebuild DuckDB indexes. If any shard becomes non-empty after
  rebuild, rerun the orchestrator for that year/collection so downstream stages
  incorporate the new data.
- Do not run this while an orchestrator run is active unless you know what you're
  doing; it can race on the same files. Use --force to override the safety check.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from collections import defaultdict
from typing import Iterable, Optional


DEFAULT_PARQUET_ROOT = Path("/storage/ccindex_parquet/cc_pointers_by_collection")
DEFAULT_GZ_ROOT = Path("/storage/ccindex")
DEFAULT_DUCKDB_SORT_TEMP = Path("/storage/ccindex_parquet/tmp/duckdb_sort/rebuild_empty")


@dataclass(frozen=True)
class EmptyShard:
    year: int
    collection: str
    gz_name: str
    marker_path: str


def _iter_empty_markers(parquet_root: Path) -> Iterable[Path]:
    # rglob is simplest; keep it streaming and filter in Python.
    yield from parquet_root.rglob("*.parquet.empty")


def _parse_marker(parquet_root: Path, marker: Path) -> Optional[EmptyShard]:
    try:
        rel = marker.resolve().relative_to(parquet_root.resolve())
    except Exception:
        return None

    # Expected layout: <year>/<collection>/<cdx-xxxxx.gz.parquet.empty>
    if len(rel.parts) < 3:
        return None

    year_s = rel.parts[0]
    collection = rel.parts[1]
    name = rel.parts[-1]

    if not name.endswith(".parquet.empty"):
        return None

    try:
        year = int(year_s)
    except Exception:
        return None

    gz_name = name[: -len(".parquet.empty")]
    if not gz_name.endswith(".gz"):
        # Very defensive; in our pipeline this is always .gz
        return None

    return EmptyShard(year=year, collection=collection, gz_name=gz_name, marker_path=str(marker))


def _sorted_parquet_path(parquet_path: Path) -> Path:
    name = parquet_path.name
    if name.endswith(".gz.parquet"):
        return parquet_path.with_name(name.replace(".gz.parquet", ".gz.sorted.parquet"))
    return parquet_path.with_name(name.replace(".parquet", ".sorted.parquet"))


def _cc_sort_workdir_for(parquet_path: Path) -> Path:
    # Must match validate_and_mark_sorted.py: work_dir = src.parent / f".cc_sort_work_{safe}"
    safe = parquet_path.name.replace(os.sep, "_")
    return parquet_path.parent / f".cc_sort_work_{safe}"


def _cleanup_duckdb_sort_temp(temp_root: Path, parquet_path: Path) -> None:
    # validate_and_mark_sorted.py creates temp dirs like:
    #   <temp_root>/duckdb_sort_<safe>_<pid>_<rand>
    safe = parquet_path.name.replace(os.sep, "_")
    prefix = f"duckdb_sort_{safe}_"
    try:
        if not temp_root.exists():
            return
        for child in temp_root.iterdir():
            if child.is_dir() and child.name.startswith(prefix):
                shutil.rmtree(child, ignore_errors=True)
    except Exception:
        return


def _download_gz(collection: str, gz_name: str, dest_path: Path) -> None:
    url = f"https://data.commoncrawl.org/cc-index/collections/{collection}/indexes/{gz_name}"
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Prefer wget for resumable downloads.
    wget = shutil.which("wget")
    if wget:
        tmp = dest_path.with_suffix(dest_path.suffix + ".download")
        try:
            _run([wget, "-c", "-O", str(tmp), url])
            tmp.replace(dest_path)
            return
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "municipal-scrape-workspace/empty-shard-rebuild",
        },
    )

    tmp = dest_path.with_suffix(dest_path.suffix + ".download")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp, tmp.open("wb") as f:
            shutil.copyfileobj(resp, f, length=1024 * 1024)
        tmp.replace(dest_path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def _run(cmd: list[str], *, cwd: Optional[Path] = None) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd) if cwd else None)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")


def _looks_like_orchestrator_is_running(workspace_root: Path) -> bool:
    # Best-effort safety check based on pid files under logs/.
    logs_dir = workspace_root / "logs"
    if not logs_dir.exists():
        return False

    pid_files = list(logs_dir.glob("*.pid"))
    if not pid_files:
        return False

    for pid_file in pid_files:
        try:
            text = pid_file.read_text(encoding="utf-8").strip()
            if not text:
                continue
            pid = int(text)
        except Exception:
            continue

        if pid <= 1:
            continue

        proc_dir = Path("/proc") / str(pid)
        if not proc_dir.exists():
            continue

        try:
            cmdline = (proc_dir / "cmdline").read_bytes().decode("utf-8", errors="ignore")
        except Exception:
            cmdline = ""

        # Heuristics: match orchestrator or year runner wrapper.
        if "cc_pipeline_orchestrator" in cmdline or "run_pipeline_years" in cmdline:
            return True

        # Conservative: a live pid file named like orchestrator_* is also suspicious.
        if "orchestrator" in pid_file.name:
            return True

    return False


def _select(items: list[EmptyShard], years: Optional[set[int]], collections: Optional[set[str]]) -> list[EmptyShard]:
    out: list[EmptyShard] = []
    for it in items:
        if years is not None and it.year not in years:
            continue
        if collections is not None and it.collection not in collections:
            continue
        out.append(it)
    return out


def _group_by_collection(items: list[EmptyShard]) -> dict[tuple[int, str], list[EmptyShard]]:
    grouped: dict[tuple[int, str], list[EmptyShard]] = defaultdict(list)
    for it in items:
        grouped[(it.year, it.collection)].append(it)
    # Stable ordering within each group.
    for k in list(grouped.keys()):
        grouped[k].sort(key=lambda x: x.gz_name)
    return dict(sorted(grouped.items(), key=lambda kv: (kv[0][0], kv[0][1])))


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Rebuild *.parquet.empty shards (download + reconvert + resort)")
    ap.add_argument(
        "--parquet-root",
        type=Path,
        default=DEFAULT_PARQUET_ROOT,
        help=f"Root of per-collection Parquet tree (default: {DEFAULT_PARQUET_ROOT})",
    )
    ap.add_argument(
        "--gz-root",
        type=Path,
        default=DEFAULT_GZ_ROOT,
        help=f"Root of per-collection .gz tree (default: {DEFAULT_GZ_ROOT})",
    )
    ap.add_argument(
        "--duckdb-sort-temp",
        type=Path,
        default=DEFAULT_DUCKDB_SORT_TEMP,
        help=f"Temp root for DuckDB sort spill (default: {DEFAULT_DUCKDB_SORT_TEMP})",
    )
    ap.add_argument("--year", action="append", default=None, help="Restrict to a year (repeatable)")
    ap.add_argument("--collection", action="append", default=None, help="Restrict to a collection (repeatable)")
    ap.add_argument("--limit", type=int, default=None, help="Process at most N markers")
    ap.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Convenience: sets both --convert-workers and validate worker count. "
            "If provided, overrides --convert-workers and --validate-workers."
        ),
    )
    ap.add_argument("--download", action="store_true", help="Download each corresponding cdx-*.gz before rebuild")
    ap.add_argument(
        "--download-only",
        action="store_true",
        help=(
            "Only download the corresponding cdx-*.gz files (no parquet deletion/conversion/sort). "
            "Safe to run while the orchestrator is active."
        ),
    )
    ap.add_argument(
        "--redownload",
        action="store_true",
        help="When downloading, overwrite existing .gz files (default: skip if present)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print what would be done without changes")
    ap.add_argument("--force", action="store_true", help="Bypass orchestrator-running safety check")
    ap.add_argument("--convert-workers", type=int, default=1, help="Workers for conversion (default: 1)")
    ap.add_argument(
        "--validate-workers",
        type=int,
        default=None,
        help="Workers for validate_and_mark_sorted file checking (default: match --convert-workers)",
    )
    ap.add_argument("--sort-workers", type=int, default=1, help="Parallel sorts for validate_and_mark_sorted (default: 1)")
    ap.add_argument("--memory-per-sort", type=float, default=4.0, help="GB memory per sort (default: 4.0)")

    args = ap.parse_args(argv)

    if args.workers is not None:
        args.convert_workers = int(args.workers)
        args.validate_workers = int(args.workers)
    if args.validate_workers is None:
        args.validate_workers = int(args.convert_workers)

    workspace_root = Path(__file__).resolve().parents[2]

    if not args.download_only:
        if not args.force and _looks_like_orchestrator_is_running(workspace_root):
            print("❌ Refusing to run: an orchestrator-like process appears active (pid file under logs/).")
            print("   Stop it first, or rerun with --force if you're sure.")
            return 2

    parquet_root = args.parquet_root.expanduser().resolve()
    gz_root = args.gz_root.expanduser().resolve()
    temp_root = args.duckdb_sort_temp.expanduser().resolve()

    if not parquet_root.exists():
        print(f"❌ Parquet root not found: {parquet_root}")
        return 1

    years: Optional[set[int]] = None
    if args.year:
        years = set()
        for y in args.year:
            try:
                years.add(int(str(y).strip()))
            except Exception:
                print(f"❌ Invalid --year: {y}")
                return 1

    collections: Optional[set[str]] = None
    if args.collection:
        collections = {str(c).strip() for c in args.collection if str(c).strip()}

    markers: list[EmptyShard] = []
    for marker in _iter_empty_markers(parquet_root):
        it = _parse_marker(parquet_root, marker)
        if it is None:
            continue
        markers.append(it)

    markers = _select(markers, years, collections)
    markers.sort(key=lambda x: (x.year, x.collection, x.gz_name))

    if args.limit is not None:
        markers = markers[: max(0, int(args.limit))]

    print(f"Found {len(markers)} empty markers to rebuild")
    if not markers:
        return 0

    if args.download_only:
        if args.dry_run:
            for it in markers:
                gz_path = (gz_root / it.collection / it.gz_name)
                print(f"DRY RUN: would download {it.collection}/{it.gz_name} -> {gz_path}")
            return 0

        downloaded = 0
        skipped = 0
        failed = 0
        for i, it in enumerate(markers, start=1):
            gz_path = (gz_root / it.collection / it.gz_name)
            print("-" * 80)
            print(f"[{i}/{len(markers)}] download {it.collection}/{it.gz_name}")

            try:
                if gz_path.exists() and not args.redownload:
                    skipped += 1
                    continue
                _download_gz(it.collection, it.gz_name, gz_path)
                downloaded += 1
            except Exception as e:
                failed += 1
                print(f"⚠️  download failed: {e}")

        print("=" * 80)
        print(f"Downloads complete: downloaded={downloaded}, skipped={skipped}, failed={failed}")
        return 0 if failed == 0 else 1

    convert_script = workspace_root / "src" / "common_crawl_search_engine" / "ccindex" / "bulk_convert_gz_to_parquet.py"
    sort_script = workspace_root / "src" / "common_crawl_search_engine" / "ccindex" / "validate_and_mark_sorted.py"

    if not convert_script.exists() or not sort_script.exists():
        print("❌ Could not find helper scripts under src/common_crawl_search_engine/ccindex")
        print(f"   Missing: {convert_script if not convert_script.exists() else ''}")
        print(f"   Missing: {sort_script if not sort_script.exists() else ''}")
        return 1

    temp_root.mkdir(parents=True, exist_ok=True)

    started = time.strftime("%Y%m%d_%H%M%S")
    report_path = workspace_root / "artifacts" / f"rebuild_empty_shards_report_{started}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    rebuilt = 0
    non_empty_after: list[dict] = []
    failures: list[dict] = []

    grouped = _group_by_collection(markers)
    group_items = [(k, v) for k, v in grouped.items()]

    group_idx = 0
    for (year, collection), items in group_items:
        group_idx += 1
        parquet_dir = parquet_root / str(year) / collection
        gz_dir = gz_root / collection
        only_args: list[str] = []
        for it in items:
            only_args.extend(["--only", it.gz_name])

        print("-" * 80)
        print(f"[{group_idx}/{len(group_items)}] {year}/{collection}: {len(items)} shard(s)")

        if args.dry_run:
            for it in items:
                print(f"DRY RUN: would rebuild {year}/{collection}/{it.gz_name}")
            continue

        try:
            # Ensure gz present (and optionally download).
            for it in items:
                gz_path = gz_dir / it.gz_name
                if args.download:
                    if (not gz_path.exists()) or args.redownload:
                        print(f"Downloading {it.gz_name} -> {gz_path}")
                        _download_gz(collection, it.gz_name, gz_path)
                if not gz_path.exists():
                    raise FileNotFoundError(f"Missing source gz: {gz_path}")

            # Clean outputs for all shards in this group.
            for it in items:
                marker_path = Path(it.marker_path)
                parquet_path = marker_path.with_suffix("")
                sorted_path = _sorted_parquet_path(parquet_path)
                work_dir = _cc_sort_workdir_for(parquet_path)

                for p in (
                    marker_path,
                    parquet_path,
                    sorted_path,
                    parquet_path.with_suffix(parquet_path.suffix + ".tmp"),
                ):
                    try:
                        if p.exists():
                            p.unlink()
                    except Exception:
                        pass

                try:
                    if work_dir.exists():
                        shutil.rmtree(work_dir, ignore_errors=True)
                except Exception:
                    pass

                _cleanup_duckdb_sort_temp(temp_root, parquet_path)

            # Convert all shards for this collection in one call (parallel workers matter here).
            _run(
                [
                    sys.executable,
                    "-u",
                    str(convert_script),
                    "--input-dir",
                    str(gz_dir),
                    "--output-dir",
                    str(parquet_dir),
                    "--workers",
                    str(int(args.convert_workers)),
                    "--overwrite",
                    *only_args,
                ]
            )

            # Sort/mark all shards for this collection in one call.
            _run(
                [
                    sys.executable,
                    "-u",
                    str(sort_script),
                    "--parquet-root",
                    str(parquet_dir),
                    "--sort-unsorted",
                    "--workers",
                    str(int(args.validate_workers)),
                    "--sort-workers",
                    str(int(args.sort_workers)),
                    "--memory-per-sort",
                    str(float(args.memory_per_sort)),
                    "--temp-dir",
                    str(temp_root),
                    *only_args,
                ]
            )

            # Summarize per-shard outcome.
            for it in items:
                marker_path = Path(it.marker_path)
                parquet_path = marker_path.with_suffix("")
                sorted_path = _sorted_parquet_path(parquet_path)

                rebuilt += 1
                if not marker_path.exists():
                    non_empty_after.append(
                        {
                            "year": it.year,
                            "collection": it.collection,
                            "gz_name": it.gz_name,
                            "parquet_dir": str(parquet_dir),
                            "sorted_parquet": str(sorted_path) if sorted_path.exists() else None,
                        }
                    )

        except Exception as e:
            print(f"⚠️  FAILED group {year}/{collection}: {e}")
            for it in items:
                failures.append(
                    {
                        "year": it.year,
                        "collection": it.collection,
                        "gz_name": it.gz_name,
                        "marker": it.marker_path,
                        "error": str(e),
                    }
                )

    report = {
        "parquet_root": str(parquet_root),
        "gz_root": str(gz_root),
        "duckdb_sort_temp": str(temp_root),
        "requested_years": sorted(list(years)) if years is not None else None,
        "requested_collections": sorted(list(collections)) if collections is not None else None,
        "download": bool(args.download),
        "dry_run": bool(args.dry_run),
        "workers": int(args.workers) if args.workers is not None else None,
        "convert_workers": int(args.convert_workers),
        "validate_workers": int(args.validate_workers),
        "sort_workers": int(args.sort_workers),
        "memory_per_sort": float(args.memory_per_sort),
        "rebuilt": int(rebuilt),
        "total_targets": int(len(markers)),
        "non_empty_after": non_empty_after,
        "failures": failures,
        "targets": [asdict(x) for x in markers],
    }

    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("=" * 80)
    print(f"Done. Rebuilt {rebuilt}/{len(markers)}")
    print(f"Report: {report_path}")
    if non_empty_after:
        print(f"⚠️  {len(non_empty_after)} shard(s) became non-empty; rerun orchestrator for affected collections.")
    if failures:
        print(f"⚠️  {len(failures)} shard(s) failed; see report for details.")

    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
