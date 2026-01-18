#!/usr/bin/env python3
"""Search Common Crawl pointer shards via master/year meta-indexes.

Goal: prove the full query chain works:
  master meta-index -> year meta-index -> collection DuckDB index -> pointer Parquet -> WARC candidates

This script is intentionally "glue": it does not assume a single monolithic DB.
Instead, it uses the meta-index registries to discover which per-collection
DuckDB DBs to query, then uses those DBs to discover the Parquet shards to scan.

Typical usage:
  # Search across all years registered in the master meta-index
  python search_cc_via_meta_indexes.py --domain 18f.gov \
    --master-db /storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb \
    --parquet-root /storage/ccindex_parquet \
    --max-matches 50

  # Search within a specific year (still using master -> collection_summary)
  python search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 50

  # Search using a year meta-index directly
  python search_cc_via_meta_indexes.py --domain 18f.gov \
    --year-db /storage/ccindex_duckdb/cc_pointers_by_year/cc_pointers_2024.duckdb \
    --parquet-root /storage/ccindex_parquet

  # Search a single collection DB directly
  python search_cc_via_meta_indexes.py --domain 18f.gov \
    --collection-db /storage/ccindex_duckdb/cc_pointers_by_collection/CC-MAIN-2024-26.duckdb \
    --parquet-root /storage/ccindex_parquet
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import duckdb


def _normalize_domain(domain: str) -> str:
    dom = (domain or "").strip().lower()
    dom = re.sub(r"^https?://", "", dom)
    dom = dom.split("/", 1)[0]
    if dom.startswith("www."):
        dom = dom[4:]
    return dom


def _host_to_rev(host: str) -> str:
    parts = [p for p in (host or "").lower().split(".") if p]
    return ",".join(reversed(parts))


def _collection_year(collection: str) -> Optional[str]:
    parts = (collection or "").split("-")
    if len(parts) >= 3 and parts[2].isdigit():
        return parts[2]
    return None


def _get_collection_parquet_dir(parquet_root: Path, collection: str) -> Path:
    """Mirror cc_pipeline_orchestrator._get_collection_parquet_dir()."""

    year = _collection_year(collection)
    if year:
        primary = parquet_root / "cc_pointers_by_collection" / year / collection
        if primary.exists():
            return primary
        secondary = parquet_root / year / collection
        if secondary.exists():
            return secondary
    return parquet_root / collection


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [str(table_name)],
    ).fetchone()
    return row is not None


@dataclass(frozen=True)
class CollectionRef:
    year: Optional[str]
    collection: str
    collection_db_path: Path


def _load_collections_from_master(master_db: Path, year: Optional[str]) -> List[CollectionRef]:
    con = duckdb.connect(str(master_db), read_only=True)
    try:
        if not _duckdb_has_table(con, "collection_summary"):
            raise RuntimeError(f"master DB missing collection_summary: {master_db}")

        if year:
            rows = con.execute(
                """
                SELECT year, collection, collection_db_path
                FROM collection_summary
                WHERE year = ?
                ORDER BY collection
                """,
                [str(year)],
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT year, collection, collection_db_path
                FROM collection_summary
                ORDER BY year, collection
                """
            ).fetchall()

        out: List[CollectionRef] = []
        for y, coll, dbp in rows:
            out.append(CollectionRef(year=str(y) if y is not None else None, collection=str(coll), collection_db_path=Path(str(dbp))))
        return out
    finally:
        con.close()


def _load_collections_from_year_db(year_db: Path) -> List[CollectionRef]:
    con = duckdb.connect(str(year_db), read_only=True)
    try:
        if not _duckdb_has_table(con, "collection_registry"):
            raise RuntimeError(f"year DB missing collection_registry: {year_db}")
        rows = con.execute(
            """
            SELECT collection, db_path
            FROM collection_registry
            ORDER BY collection
            """
        ).fetchall()
        out: List[CollectionRef] = []
        for coll, dbp in rows:
            out.append(CollectionRef(year=_collection_year(str(coll)), collection=str(coll), collection_db_path=Path(str(dbp))))
        return out
    finally:
        con.close()


def _parquet_relpaths_for_domain(collection_db: Path, host_rev_prefix: str) -> List[str]:
    like_pat = host_rev_prefix + ",%"

    con = duckdb.connect(str(collection_db), read_only=True)
    try:
        if not _duckdb_has_table(con, "cc_domain_shards"):
            return []
        rows = con.execute(
            """
            SELECT DISTINCT parquet_relpath
            FROM cc_domain_shards
            WHERE host_rev = ? OR host_rev LIKE ?
            ORDER BY parquet_relpath
            """,
            [host_rev_prefix, like_pat],
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    finally:
        con.close()


def _iter_warc_candidates_from_parquet(
    parquet_path: Path,
    host_rev_prefix: str,
    *,
    limit: int,
) -> Iterator[Dict[str, object]]:
    """Return candidate WARC pointer records from a single Parquet shard."""

    like_pat = host_rev_prefix + ",%"

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        rows = con.execute(
            """
            SELECT
                collection,
                shard_file,
                url,
                ts,
                status,
                mime,
                digest,
                warc_filename,
                warc_offset,
                warc_length
            FROM read_parquet(?)
            WHERE host_rev = ? OR host_rev LIKE ?
            LIMIT ?
            """,
            [str(parquet_path), host_rev_prefix, like_pat, int(limit)],
        ).fetchall()

        for (
            collection,
            shard_file,
            url,
            ts,
            status,
            mime,
            digest,
            warc_filename,
            warc_offset,
            warc_length,
        ) in rows:
            yield {
                "collection": collection,
                "shard_file": shard_file,
                "url": url,
                "timestamp": ts,
                "status": int(status) if status is not None else None,
                "mime": mime,
                "digest": digest,
                "warc_filename": warc_filename,
                "warc_offset": int(warc_offset) if warc_offset is not None else None,
                "warc_length": int(warc_length) if warc_length is not None else None,
                "parquet_path": str(parquet_path),
            }
    finally:
        con.close()


def _print_jsonl(records: Iterable[Dict[str, object]]) -> None:
    for rec in records:
        sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _eprint(msg: str) -> None:
    sys.stderr.write(str(msg) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Query master/year/collection meta-indexes to get candidate WARC pointers")

    src = ap.add_mutually_exclusive_group()
    src.add_argument(
        "--master-db",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
        help="Master meta-index DuckDB (default: /storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb)",
    )
    src.add_argument("--year-db", type=Path, help="Year meta-index DuckDB (cc_pointers_<year>.duckdb)")
    src.add_argument("--collection-db", type=Path, help="Single collection DuckDB (CC-MAIN-....duckdb)")

    ap.add_argument("--year", type=str, default=None, help="Restrict to a year (only used with --master-db)")

    ap.add_argument(
        "--parquet-root",
        type=Path,
        default=Path("/storage/ccindex_parquet"),
        help="Parquet root (default: /storage/ccindex_parquet)",
    )

    ap.add_argument("--domain", required=True, type=str, help="Domain or URL (e.g. 18f.gov or https://18f.gov)")

    ap.add_argument("--max-parquet-files", type=int, default=200, help="Cap number of Parquet shards scanned per collection")
    ap.add_argument("--max-matches", type=int, default=200, help="Stop after emitting this many candidate WARC pointers")
    ap.add_argument("--per-parquet-limit", type=int, default=2000, help="Cap matches read per Parquet shard")

    ap.add_argument("--verbose", action="store_true", default=False)

    args = ap.parse_args()

    domain = _normalize_domain(args.domain)
    if not domain:
        print("Empty domain", file=sys.stderr)
        return 2

    host_rev_prefix = _host_to_rev(domain)
    if not host_rev_prefix:
        print("Could not compute host_rev", file=sys.stderr)
        return 2

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    if not parquet_root.exists():
        print(f"Parquet root does not exist: {parquet_root}", file=sys.stderr)
        return 2

    t0 = time.perf_counter()

    # 1) Discover collections via meta-index layer.
    if args.collection_db:
        coll_db = Path(args.collection_db).expanduser().resolve()
        # Collection name is derived from DB name to locate Parquet layout.
        collection_name = coll_db.stem.replace("cc_pointers_", "")
        collections = [CollectionRef(year=_collection_year(collection_name), collection=collection_name, collection_db_path=coll_db)]
        meta_source = f"collection-db:{coll_db}"
    elif args.year_db:
        year_db = Path(args.year_db).expanduser().resolve()
        collections = _load_collections_from_year_db(year_db)
        meta_source = f"year-db:{year_db}"
    else:
        master_db = Path(args.master_db).expanduser().resolve()
        collections = _load_collections_from_master(master_db, args.year)
        meta_source = f"master-db:{master_db}"

    if not collections:
        _eprint(f"No collections found via {meta_source}")
        return 1

    if args.verbose:
        yr = args.year if args.year else "(all years)"
        _eprint(f"meta_source={meta_source}")
        _eprint(f"domain={domain} host_rev_prefix={host_rev_prefix} year_filter={yr}")
        _eprint(f"collections={len(collections)}")

    emitted = 0

    # 2) For each collection DB: find parquet shards, then expand to WARC pointers.
    for cref in collections:
        if emitted >= int(args.max_matches):
            break

        collection_db = cref.collection_db_path
        if not collection_db.exists():
            if args.verbose:
                _eprint(f"skip_missing_collection_db={collection_db}")
            continue

        parquet_relpaths = _parquet_relpaths_for_domain(collection_db, host_rev_prefix)
        if not parquet_relpaths:
            continue

        parquet_relpaths = parquet_relpaths[: max(0, int(args.max_parquet_files))]

        parquet_dir = _get_collection_parquet_dir(parquet_root, cref.collection)
        if not parquet_dir.exists():
            if args.verbose:
                _eprint(f"skip_missing_parquet_dir={parquet_dir}")
            continue

        if args.verbose:
            _eprint(f"collection={cref.collection} db={collection_db} parquet_shards={len(parquet_relpaths)}")

        for rel in parquet_relpaths:
            if emitted >= int(args.max_matches):
                break

            parquet_path = (parquet_dir / rel).resolve()
            if not parquet_path.exists():
                if args.verbose:
                    _eprint(f"missing_parquet={parquet_path}")
                continue

            remaining = int(args.max_matches) - emitted
            per_file_limit = min(int(args.per_parquet_limit), remaining)
            for rec in _iter_warc_candidates_from_parquet(parquet_path, host_rev_prefix, limit=per_file_limit):
                _print_jsonl([rec])
                emitted += 1
                if emitted >= int(args.max_matches):
                    break

    dt = time.perf_counter() - t0
    if args.verbose:
        _eprint(f"emitted={emitted} elapsed_s={dt:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
