#!/usr/bin/env python3
"""Build a per-year rowgroup slice index DB from per-collection DBs.

This avoids opening many per-collection DBs at query time by aggregating
cc_domain_rowgroups into one DB per year.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Iterable, List

import duckdb


def _iter_collection_dbs(collection_dir: Path, year: str) -> List[Path]:
    out: List[Path] = []
    if not collection_dir.exists():
        return out
    for p in sorted(collection_dir.glob(f"CC-MAIN-{year}-*.duckdb")):
        if p.is_file():
            out.append(p)
    return out


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cc_domain_rowgroups (
            collection TEXT,
            source_path TEXT,
            parquet_relpath TEXT,
            row_group INTEGER,
            dom_rg_row_start BIGINT,
            dom_rg_row_end BIGINT,
            host_rev TEXT
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_ccdr_host_rev ON cc_domain_rowgroups(host_rev)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_ccdr_collection ON cc_domain_rowgroups(collection)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_ccdr_coll_host ON cc_domain_rowgroups(collection, host_rev)")


def _copy_collection(con: duckdb.DuckDBPyConnection, db_path: Path, collection: str) -> int:
    con.execute(f"ATTACH '{db_path}' AS src")
    try:
        con.execute(
            """
            INSERT INTO cc_domain_rowgroups
            SELECT ?, source_path, parquet_relpath, row_group, dom_rg_row_start, dom_rg_row_end, host_rev
            FROM src.cc_domain_rowgroups
            """,
            [collection],
        )
        rows = con.execute("SELECT changes()").fetchone()[0]
    except Exception:
        # Fallback: count rows from source
        rows = con.execute("SELECT COUNT(*) FROM src.cc_domain_rowgroups").fetchone()[0]
        con.execute(
            """
            INSERT INTO cc_domain_rowgroups
            SELECT ?, source_path, parquet_relpath, row_group, dom_rg_row_start, dom_rg_row_end, host_rev
            FROM src.cc_domain_rowgroups
            """,
            [collection],
        )
    finally:
        con.execute("DETACH src")
    return int(rows or 0)


def build_year_index(collection_dir: Path, year: str, output_db: Path, *, overwrite: bool) -> None:
    if output_db.exists() and overwrite:
        output_db.unlink()
    output_db.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(output_db))
    try:
        con.execute("PRAGMA threads=4")
        _ensure_schema(con)

        total = 0
        files = _iter_collection_dbs(collection_dir, year)
        if not files:
            raise SystemExit(f"No collection DBs found for year {year} in {collection_dir}")

        for idx, db_path in enumerate(files, 1):
            collection = db_path.stem.replace(".domain_rowgroups", "")
            t0 = time.time()
            rows = _copy_collection(con, db_path, collection)
            total += rows
            dt = time.time() - t0
            print(f"[{idx}/{len(files)}] {collection}: {rows} rows in {dt:.2f}s")

        con.execute("ANALYZE")
        print(f"Wrote {total} rows to {output_db}")
    finally:
        con.close()


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build a per-year rowgroup index DB")
    ap.add_argument("--year", required=True, help="Year, e.g. 2025")
    ap.add_argument(
        "--collection-dir",
        default="/storage/ccindex_duckdb/cc_domain_rowgroups_by_collection",
        help="Directory with per-collection rowgroup DBs",
    )
    ap.add_argument(
        "--output-db",
        default=None,
        help="Output DB path (default: /storage/ccindex_duckdb/cc_domain_rowgroups_by_year/cc_domain_rowgroups_<year>.duckdb)",
    )
    ap.add_argument("--overwrite", action="store_true", help="Overwrite output DB if it exists")
    args = ap.parse_args(list(argv) if argv is not None else None)

    year = str(args.year).strip()
    if not year.isdigit():
        raise SystemExit("Year must be numeric")

    collection_dir = Path(args.collection_dir).expanduser().resolve()
    if args.output_db:
        output_db = Path(args.output_db).expanduser().resolve()
    else:
        output_db = Path(
            f"/storage/ccindex_duckdb/cc_domain_rowgroups_by_year/cc_domain_rowgroups_{year}.duckdb"
        ).expanduser().resolve()

    build_year_index(collection_dir, year, output_db, overwrite=bool(args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
