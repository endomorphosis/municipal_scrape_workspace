#!/usr/bin/env python3
"""Repair legacy parquet shards missing collection/shard_file columns."""

from __future__ import annotations

import argparse
import time
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from common_crawl_search_engine.ccindex.api import collection_year

REQUIRED_COLS = ("collection", "shard_file")


def _collection_dirs(parquet_root: Path) -> List[Path]:
    out: List[Path] = []
    base = parquet_root / "cc_pointers_by_collection"
    if not base.exists():
        return out
    for year_dir in sorted(base.glob("[0-9][0-9][0-9][0-9]")):
        if not year_dir.is_dir():
            continue
        for coll_dir in sorted(year_dir.iterdir()):
            if coll_dir.is_dir():
                out.append(coll_dir)
    return out


def _parquet_missing_cols(pq_path: Path) -> bool:
    try:
        pf = pq.ParquetFile(str(pq_path))
        names = set(pf.schema.names)
        return not set(REQUIRED_COLS).issubset(names)
    except Exception:
        return False


def _repair_file(pq_path: Path, collection: str, overwrite: bool) -> bool:
    try:
        table = pq.read_table(str(pq_path))
    except Exception:
        return False

    cols = {name: table.column(name) for name in table.schema.names}
    n = table.num_rows

    if "collection" not in cols:
        cols["collection"] = pa.array([collection] * n)
    if "shard_file" not in cols:
        cols["shard_file"] = pa.array([pq_path.name] * n)

    repaired = pa.Table.from_pydict(cols)

    if overwrite:
        tmp = pq_path.with_suffix(pq_path.suffix + ".repair")
        pq.write_table(repaired, tmp)
        tmp.replace(pq_path)
    return True


def _iter_targets(parquet_root: Path, collections: Optional[List[str]]) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    if collections:
        for c in collections:
            y = collection_year(c)
            if not y:
                raise SystemExit(f"Invalid collection name: {c}")
            out.append((c, parquet_root / "cc_pointers_by_collection" / y / c))
        return out

    for coll_dir in _collection_dirs(parquet_root):
        out.append((coll_dir.name, coll_dir))
    return out


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Repair parquet shards missing collection/shard_file")
    ap.add_argument(
        "--parquet-root",
        default="/storage/ccindex_parquet",
        help="Root containing cc_pointers_by_collection",
    )
    ap.add_argument("--collections", default="", help="Comma-separated list of collections")
    ap.add_argument("--dry-run", action="store_true", help="Report files needing repair")
    ap.add_argument("--overwrite", action="store_true", help="Rewrite parquet files in place")
    args = ap.parse_args(list(argv) if argv is not None else None)

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    collections = [c.strip() for c in str(args.collections).split(",") if c.strip()]

    targets = _iter_targets(parquet_root, collections or None)
    if not targets:
        print("No parquet collections found.", flush=True)
        return 0

    repaired = 0
    scanned = 0
    t0 = time.time()
    last_log = t0
    print(
        f"starting repair parquet_root={parquet_root} collections={len(targets)} dry_run={args.dry_run}",
        flush=True,
    )
    for collection, coll_dir in targets:
        if not coll_dir.exists():
            continue
        print(f"collection_start {collection}", flush=True)
        for pq_path in sorted(coll_dir.glob("cdx-*.parquet")):
            scanned += 1
            if not _parquet_missing_cols(pq_path):
                pass
            else:
                if args.dry_run:
                    print(f"missing_cols: {pq_path}", flush=True)
                else:
                    ok = _repair_file(pq_path, collection, overwrite=bool(args.overwrite))
                    if ok:
                        repaired += 1

            now = time.time()
            if scanned % 200 == 0 or (now - last_log) >= 30:
                rate = scanned / max(1.0, (now - t0))
                print(
                    f"progress scanned={scanned} repaired={repaired} "
                    f"elapsed_s={int(now - t0)} rate={rate:.1f}/s",
                    flush=True,
                )
                last_log = now
        print(f"collection_done {collection}", flush=True)

    elapsed = time.time() - t0
    print(f"done scanned={scanned} repaired={repaired} elapsed_s={int(elapsed)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
