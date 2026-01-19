#!/usr/bin/env python3
"""Benchmark querying CC index pointers: Parquet scan vs DuckDB-native table.

What it does
- Uses a Parquet file produced by sample_ccindex_to_parquet.py
- Creates a DuckDB DB file and loads the same data as a native table
- Benchmarks a representative lookup workload: join N URLs against the index
  and return WARC pointers.

This provides a realistic comparison for "I have ~7000 URLs, find WARC locations".

Example
  /home/barberb/municipal_scrape_workspace/.venv/bin/python benchmark_ccindex_parquet_vs_duckdb.py \
    --parquet /home/barberb/municipal_scrape_workspace/out_ccindex_parquet_sample/sample_2025_43_1file.parquet \
    --db /storage/ccindex_duckdb_sample/bench.duckdb \
    --n-urls 7000
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Tuple

import duckdb


def _now() -> float:
    return time.perf_counter()


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def _fmt_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.2f} {u}"
        f /= 1024.0
    return f"{f:.2f} TiB"


def _time_query(con: duckdb.DuckDBPyConnection, sql: str) -> Tuple[float, int]:
    t0 = _now()
    res = con.execute(sql).fetchall()
    dt = _now() - t0
    return dt, len(res)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True, type=str, help="Input Parquet file")
    ap.add_argument("--db", required=True, type=str, help="DuckDB database file to create/use")
    ap.add_argument("--n-urls", type=int, default=7000, help="Number of URLs to lookup")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 4, help="DuckDB threads")
    args = ap.parse_args()

    parquet_path = Path(args.parquet)
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Parquet: {parquet_path} ({_fmt_bytes(_file_size(parquet_path))})")
    print(f"DuckDB:  {db_path}")
    print(f"Threads: {int(args.threads)}")

    # We keep two connections: one for Parquet queries and one for native table.
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(args.threads)}")

    # Views for Parquet access.
    con.execute("DROP VIEW IF EXISTS cc_idx_parquet")
    con.execute(f"CREATE VIEW cc_idx_parquet AS SELECT * FROM read_parquet('{parquet_path.as_posix()}')")

    # Build a URL list to lookup. We sample from the parquet itself to ensure hits.
    con.execute("DROP TABLE IF EXISTS lookup_urls")
    con.execute(
        f"""
        CREATE TABLE lookup_urls AS
        SELECT DISTINCT url
        FROM cc_idx_parquet
        WHERE url IS NOT NULL
        LIMIT {int(args.n_urls)}
        """
    )
    n_lookup = con.execute("SELECT count(*) FROM lookup_urls").fetchone()[0]
    print(f"Lookup URLs prepared: {n_lookup}")

    # Query 1: join directly against Parquet view.
    sql_join_parquet = """
    SELECT u.url, i.warc_filename, i.warc_offset, i.warc_length
    FROM lookup_urls u
    JOIN cc_idx_parquet i
      ON i.url = u.url
    WHERE i.warc_filename IS NOT NULL AND i.warc_offset IS NOT NULL AND i.warc_length IS NOT NULL
    """
    dt1, rows1 = _time_query(con, sql_join_parquet)
    print(f"Parquet join: {dt1:.3f}s, rows={rows1}")

    # Load into native table once.
    con.execute("DROP TABLE IF EXISTS cc_idx_native")
    t0 = _now()
    con.execute("CREATE TABLE cc_idx_native AS SELECT * FROM cc_idx_parquet")
    load_dt = _now() - t0
    native_bytes = _file_size(db_path)
    print(f"Native load: {load_dt:.3f}s, db size now {_fmt_bytes(native_bytes)}")

    # Query 2: join against native.
    sql_join_native = """
    SELECT u.url, i.warc_filename, i.warc_offset, i.warc_length
    FROM lookup_urls u
    JOIN cc_idx_native i
      ON i.url = u.url
    WHERE i.warc_filename IS NOT NULL AND i.warc_offset IS NOT NULL AND i.warc_length IS NOT NULL
    """
    dt2, rows2 = _time_query(con, sql_join_native)
    print(f"Native join: {dt2:.3f}s, rows={rows2}")

    # Repeat once more to show warm-cache behavior.
    dt1b, _ = _time_query(con, sql_join_parquet)
    dt2b, _ = _time_query(con, sql_join_native)
    print(f"Parquet join (2nd run): {dt1b:.3f}s")
    print(f"Native join (2nd run): {dt2b:.3f}s")

    con.close()

    print("")
    print("Notes:")
    print("- This benchmark uses URLs sampled from the same Parquet, so it measures lookup/join overhead, not miss rate.")
    print("- For real workloads, partitioning by collection/host often matters more than Parquet vs native.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
