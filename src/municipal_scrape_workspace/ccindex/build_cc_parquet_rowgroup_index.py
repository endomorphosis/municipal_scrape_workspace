#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterator, Optional, Sequence, Tuple

import duckdb
import pyarrow.parquet as pq


def _iter_parquet_files(
    parquet_root: Path, collections: Optional[Sequence[str]], collections_regex: Optional[str]
) -> Iterator[Tuple[str, Path]]:
    rx = re.compile(collections_regex) if collections_regex else None

    if collections:
        for col in collections:
            parts = col.split("-")
            if len(parts) < 3:
                raise SystemExit(f"Invalid collection name: {col}")
            year = parts[2]
            col_dir = parquet_root / year / col
            if not col_dir.is_dir():
                continue
            for p in sorted(col_dir.glob("cdx-*.gz.parquet")):
                yield col, p
        return

    for year_dir in sorted(parquet_root.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for col_dir in sorted(year_dir.iterdir()):
            if not col_dir.is_dir():
                continue
            col = col_dir.name
            if rx and not rx.search(col):
                continue
            for p in sorted(col_dir.glob("cdx-*.gz.parquet")):
                yield col, p


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS parquet_files (
          parquet_path TEXT PRIMARY KEY,
          collection TEXT,
          year INTEGER,
          size_bytes BIGINT,
          mtime_ns BIGINT,
          row_groups INTEGER,
          total_rows BIGINT,
          indexed_at TIMESTAMP
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS parquet_rowgroups (
          parquet_path TEXT,
          row_group INTEGER,
          row_start BIGINT,
          row_count BIGINT,
          host_rev_min TEXT,
          host_rev_max TEXT
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_path ON parquet_rowgroups(parquet_path)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_min ON parquet_rowgroups(host_rev_min)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_max ON parquet_rowgroups(host_rev_max)")


def _get_host_rev_col_index(meta: pq.FileMetaData) -> int:
    schema = meta.schema
    for i in range(schema.num_columns):
        if schema.column(i).name == "host_rev":
            return i
    raise KeyError("host_rev column not found in Parquet schema")


def _to_text(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def index_one(con: duckdb.DuckDBPyConnection, collection: str, parquet_path: Path, dry_run: bool) -> Tuple[bool, str]:
    st = parquet_path.stat()
    size_bytes = int(st.st_size)
    mtime_ns = int(st.st_mtime_ns)

    existing = con.execute(
        "SELECT size_bytes, mtime_ns FROM parquet_files WHERE parquet_path = ?",
        [str(parquet_path)],
    ).fetchone()

    if existing and int(existing[0]) == size_bytes and int(existing[1]) == mtime_ns:
        return False, "skip(unchanged)"

    if dry_run:
        return True, "dry_run(would_index)"

    con.execute("DELETE FROM parquet_rowgroups WHERE parquet_path = ?", [str(parquet_path)])
    con.execute("DELETE FROM parquet_files WHERE parquet_path = ?", [str(parquet_path)])

    pf = pq.ParquetFile(str(parquet_path))
    meta = pf.metadata
    host_idx = _get_host_rev_col_index(meta)

    row_start = 0
    rows = []
    for rg in range(meta.num_row_groups):
        rg_meta = meta.row_group(rg)
        col = rg_meta.column(host_idx)
        stats = col.statistics
        host_min = _to_text(stats.min) if stats is not None else None
        host_max = _to_text(stats.max) if stats is not None else None
        row_count = int(rg_meta.num_rows)
        rows.append((str(parquet_path), int(rg), int(row_start), int(row_count), host_min, host_max))
        row_start += row_count

    con.executemany(
        "INSERT INTO parquet_rowgroups (parquet_path, row_group, row_start, row_count, host_rev_min, host_rev_max) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )

    parts = collection.split("-")
    year = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None

    con.execute(
        "INSERT INTO parquet_files (parquet_path, collection, year, size_bytes, mtime_ns, row_groups, total_rows, indexed_at) VALUES (?, ?, ?, ?, ?, ?, ?, now())",
        [str(parquet_path), str(collection), year, size_bytes, mtime_ns, int(meta.num_row_groups), int(meta.num_rows)],
    )

    return True, f"indexed(row_groups={meta.num_row_groups} rows={meta.num_rows})"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build a DuckDB index of Parquet row-group host_rev ranges + row offsets.")
    ap.add_argument("--parquet-root", required=True, type=str, help="Root like /storage/ccindex_parquet/cc_pointers_by_collection")
    ap.add_argument("--db", required=True, type=str, help="Output DuckDB path (e.g. /storage/ccindex_duckdb/cc_parquet_rowgroups.duckdb)")
    ap.add_argument("--collections", action="append", default=None, help="Repeatable: only index these collections")
    ap.add_argument("--collections-regex", type=str, default=None, help="Regex to select collections (when enumerating)")
    ap.add_argument("--max-files", type=int, default=None, help="Stop after indexing this many files")
    ap.add_argument("--dry-run", action="store_true", default=False)

    args = ap.parse_args(argv)

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    _ensure_schema(con)

    seen = 0
    did = 0
    skipped = 0
    for collection, p in _iter_parquet_files(parquet_root, args.collections, args.collections_regex):
        seen += 1
        ok, status = index_one(con, collection, p, dry_run=bool(args.dry_run))
        if ok:
            did += 1
        else:
            skipped += 1

        if seen % 200 == 0:
            print(f"seen={seen} did={did} skipped={skipped} last={p.name} status={status}")

        if args.max_files and did >= int(args.max_files):
            break

    print(f"done seen={seen} did={did} skipped={skipped} db={db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
