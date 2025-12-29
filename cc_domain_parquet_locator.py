#!/usr/bin/env python3
"""Locate which Common Crawl pointer Parquet shards contain a domain.

This is a verification/debug helper for the "domain-only" DuckDB index mode.

Given:
- a DuckDB file or directory of per-collection DBs containing `cc_domain_shards`
- a Parquet root directory containing pointer shards

It prints the distinct Parquet shard paths that must be scanned to expand a
whole domain (including subdomains).

Example
  /home/barberb/municipal_scrape_workspace/.venv/bin/python cc_domain_parquet_locator.py \
    --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_collection \
    --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \
    --domain example.gov \
    --count-urls
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

import duckdb


def _host_to_rev(host: str) -> str:
    parts = [p for p in (host or "").lower().split(".") if p]
    return ",".join(reversed(parts))


def _iter_duckdb_files(path_or_dir: Path) -> List[Path]:
    if path_or_dir.is_file():
        return [path_or_dir]
    if path_or_dir.is_dir():
        return sorted(p for p in path_or_dir.glob("*.duckdb") if p.is_file())
    return []


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
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
    except Exception:
        return False


def _normalize_domain(domain: str) -> str:
    dom = (domain or "").strip().lower()
    dom = re.sub(r"^https?://", "", dom)
    dom = dom.split("/", 1)[0]
    if dom.startswith("www."):
        dom = dom[4:]
    return dom


def _distinct_parquet_relpaths_for_domain(
    db_files: Iterable[Path],
    *,
    host_rev_prefix: str,
) -> Tuple[Set[str], int, int]:
    """Return (relpaths, db_count_scanned, db_with_table)."""
    relpaths: Set[str] = set()
    scanned = 0
    with_table = 0

    like_pat = host_rev_prefix + ",%"

    for db in db_files:
        scanned += 1
        con = duckdb.connect(str(db), read_only=True)
        try:
            if not _duckdb_has_table(con, "cc_domain_shards"):
                con.close()
                continue
            with_table += 1
            rows = con.execute(
                """
                SELECT DISTINCT parquet_relpath
                FROM cc_domain_shards
                WHERE host_rev = ? OR host_rev LIKE ?
                """,
                [host_rev_prefix, like_pat],
            ).fetchall()
            for (rel,) in rows:
                if not rel:
                    continue
                relpaths.add(str(rel))
        finally:
            con.close()

    return relpaths, scanned, with_table


def _count_urls_in_parquet_shards(
    parquet_paths: List[Path],
    *,
    host_rev_prefix: str,
    limit_total: Optional[int],
) -> int:
    like_pat = host_rev_prefix + ",%"
    remaining = None if limit_total is None else max(0, int(limit_total))
    total = 0

    con = duckdb.connect(database=":memory:")
    try:
        for p in parquet_paths:
            if remaining is not None and remaining <= 0:
                break
            if not p.exists():
                continue
            lim = 1_000_000_000 if remaining is None else int(remaining)
            row = con.execute(
                """
                SELECT count(*)
                FROM read_parquet(?)
                WHERE host_rev = ? OR host_rev LIKE ?
                LIMIT 1
                """,
                [str(p), host_rev_prefix, like_pat],
            ).fetchone()
            n = int(row[0] if row and row[0] is not None else 0)
            total += n
            if remaining is not None:
                remaining -= n
    finally:
        con.close()

    return int(total)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb-dir", required=True, type=str, help="DuckDB file or directory containing cc_domain_shards")
    ap.add_argument("--parquet-root", required=True, type=str, help="Root folder containing pointer Parquet shards")
    ap.add_argument("--domain", required=True, type=str, help="Domain or URL (e.g. example.gov or https://example.gov)")
    ap.add_argument("--limit-shards", type=int, default=None, help="Only print first N shard paths")
    ap.add_argument("--count-urls", action="store_true", default=False, help="Also count matching URLs across those shards")
    ap.add_argument(
        "--count-limit",
        type=int,
        default=None,
        help="Optional cap when counting URLs (stop after reaching this many matches)",
    )

    args = ap.parse_args()

    duckdb_dir = Path(args.duckdb_dir).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve()

    dom = _normalize_domain(str(args.domain))
    if not dom:
        raise SystemExit("Empty domain")

    prefix = _host_to_rev(dom)
    if not prefix:
        raise SystemExit("Could not compute host_rev for domain")

    db_files = _iter_duckdb_files(duckdb_dir)
    if not db_files:
        raise SystemExit(f"No DuckDB files found under: {duckdb_dir}")

    relpaths, scanned, with_table = _distinct_parquet_relpaths_for_domain(db_files, host_rev_prefix=prefix)

    # Map to absolute Parquet paths.
    parquet_paths = sorted({(parquet_root / r).resolve() for r in relpaths})

    print(f"domain={dom}")
    print(f"host_rev_prefix={prefix}")
    print(f"duckdb_files_scanned={scanned}  dbs_with_cc_domain_shards={with_table}")
    print(f"parquet_shards={len(parquet_paths)}")

    limit = args.limit_shards
    shown = parquet_paths if limit is None else parquet_paths[: max(0, int(limit))]
    for p in shown:
        exists = "yes" if p.exists() else "NO"
        size = p.stat().st_size if p.exists() else 0
        print(f"{p}  exists={exists}  bytes={size}")

    if args.count_urls:
        total = _count_urls_in_parquet_shards(parquet_paths, host_rev_prefix=prefix, limit_total=args.count_limit)
        print(f"matching_urls={total}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
