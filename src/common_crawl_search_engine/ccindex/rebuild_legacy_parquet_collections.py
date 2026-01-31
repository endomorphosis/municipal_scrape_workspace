#!/usr/bin/env python3
"""Detect and optionally rebuild legacy parquet collections missing required columns."""

from __future__ import annotations

import argparse
import urllib.request
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pyarrow.parquet as pq

from common_crawl_search_engine.ccindex.api import collection_year


REQUIRED_COLS = {"collection", "shard_file"}


def _collection_dirs(parquet_root: Path) -> List[Path]:
    out: List[Path] = []
    if not parquet_root.exists():
        return out
    # Expected layout: <root>/cc_pointers_by_collection/<year>/<collection>
    for year_dir in sorted((parquet_root / "cc_pointers_by_collection").glob("[0-9][0-9][0-9][0-9]")):
        if not year_dir.is_dir():
            continue
        for coll_dir in sorted(year_dir.iterdir()):
            if coll_dir.is_dir():
                out.append(coll_dir)
    return out


def _has_required_cols(pq_path: Path) -> bool:
    try:
        pf = pq.ParquetFile(str(pq_path))
        names = set(pf.schema.names)
        return REQUIRED_COLS.issubset(names)
    except Exception:
        return False


def _legacy_collections(parquet_root: Path) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    for coll_dir in _collection_dirs(parquet_root):
        files = sorted(coll_dir.glob("cdx-*.parquet"))
        if not files:
            continue
        if not _has_required_cols(files[0]):
            out.append((coll_dir.name, coll_dir))
    return out


def _download_missing_shards(
    *,
    collection_dir: Path,
    base_url: str,
    max_files: int,
) -> int:
    index_list_path = collection_dir / "index_files.txt"
    if not index_list_path.exists():
        return 0

    try:
        lines = [
            line.strip()
            for line in index_list_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            if line.strip().endswith(".gz")
        ]
    except Exception:
        return 0

    if not lines:
        return 0

    base = str(base_url).rstrip("/") + "/"
    downloaded = 0
    for rel in lines:
        if max_files and downloaded >= int(max_files):
            break
        fname = Path(rel).name
        dest = collection_dir / fname
        if dest.exists():
            continue
        url = base + rel.lstrip("/")
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            urllib.request.urlretrieve(url, tmp)
            tmp.rename(dest)
            downloaded += 1
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            continue
    return downloaded


def _run_rebuild(
    input_ccindex_root: Path,
    collection: str,
    output_dir: Path,
    workers: int,
    *,
    download_missing: bool,
    download_base: str,
    download_max_files: int,
) -> bool:
    script = Path(__file__).resolve().parent / "bulk_convert_gz_to_parquet.py"
    if not script.exists():
        raise SystemExit(f"Missing helper: {script}")

    ccindex_dir = input_ccindex_root / collection
    if not ccindex_dir.exists():
        print(f"Missing ccindex dir for collection: {ccindex_dir}")
        return False

    if download_missing:
        fetched = _download_missing_shards(
            collection_dir=ccindex_dir,
            base_url=download_base,
            max_files=int(download_max_files),
        )
        if fetched:
            print(f"Downloaded {fetched} shard(s) for {collection}")

    cmd = [
        sys.executable,
        str(script),
        "--input-dir",
        str(ccindex_dir),
        "--output-dir",
        str(output_dir),
        "--workers",
        str(int(workers)),
    ]
    print(" ".join(cmd))
    subprocess.check_call(cmd)
    return True


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Detect/rebuild legacy parquet collections")
    ap.add_argument(
        "--parquet-root",
        default="/storage/ccindex_parquet",
        help="Root containing cc_pointers_by_collection",
    )
    ap.add_argument(
        "--ccindex-root",
        default="/storage/ccindex",
        help="Root containing raw ccindex <collection>/cdx-*.gz",
    )
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--rebuild", action="store_true", help="Run rebuild for detected collections")
    ap.add_argument("--collections", default="", help="Comma-separated list to rebuild")
    ap.add_argument("--download-missing", action="store_true", help="Download shards listed in index_files.txt")
    ap.add_argument(
        "--download-base",
        default="https://data.commoncrawl.org/",
        help="Base URL for index_files.txt paths",
    )
    ap.add_argument(
        "--download-max-files",
        type=int,
        default=0,
        help="Limit downloads per collection (0 = no limit)",
    )

    args = ap.parse_args(list(argv) if argv is not None else None)

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    ccindex_root = Path(args.ccindex_root).expanduser().resolve()

    if args.collections:
        selected = {c.strip() for c in str(args.collections).split(",") if c.strip()}
        legacy = []
        for c in selected:
            y = collection_year(c)
            if not y:
                raise SystemExit(f"Invalid collection name: {c}")
            legacy.append((c, parquet_root / "cc_pointers_by_collection" / y / c))
    else:
        legacy = _legacy_collections(parquet_root)

    if not legacy:
        print("No legacy collections detected.")
        return 0

    print("Legacy collections (missing columns):")
    for c, p in legacy:
        print(f"  {c} -> {p}")

    if not args.rebuild:
        print("Dry-run only. Use --rebuild to execute conversions.")
        return 0

    for c, _p in legacy:
        y = collection_year(c)
        if not y:
            raise SystemExit(f"Invalid collection name: {c}")
        output_dir = parquet_root / "cc_pointers_by_collection" / y / c
        output_dir.mkdir(parents=True, exist_ok=True)
        _run_rebuild(
            ccindex_root,
            c,
            output_dir,
            workers=int(args.workers),
            download_missing=bool(args.download_missing),
            download_base=str(args.download_base),
            download_max_files=int(args.download_max_files),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
