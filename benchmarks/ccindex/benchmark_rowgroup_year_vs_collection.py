#!/usr/bin/env python3
"""Benchmark per-year vs per-collection rowgroup index lookups.

This measures the time to fetch rowgroup segments for host_rev values:
- Per-year DB: single query against cc_domain_rowgroups_<year>.duckdb
- Per-collection DBs: parallel queries across CC-MAIN-<year>-*.domain_rowgroups.duckdb

Usage example:
  ./.venv/bin/python benchmarks/ccindex/benchmark_rowgroup_year_vs_collection.py \
    --year 2024 --samples 100 --repeats 3 --workers 8
"""

from __future__ import annotations

import argparse
import random
import statistics
import time
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed


def _iter_collection_dbs(collection_dir: Path, year: str) -> List[Path]:
    out: List[Path] = []
    if not collection_dir.exists():
        return out
    for p in sorted(collection_dir.glob(f"CC-MAIN-{year}-*.duckdb")):
        if p.is_file():
            out.append(p)
    return out


def _open_ro(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def _sample_host_revs(con: duckdb.DuckDBPyConnection, n: int, seed: int) -> List[str]:
    rows = con.execute(
        """
        SELECT DISTINCT host_rev
        FROM cc_domain_rowgroups
        WHERE host_rev IS NOT NULL
        ORDER BY random()
        LIMIT ?
        """,
        [int(n)],
    ).fetchall()
    host_revs = [str(r[0]) for r in rows if r and r[0] is not None]
    if seed is not None:
        random.Random(int(seed)).shuffle(host_revs)
    return host_revs


def _query_year_segments(
    con: duckdb.DuckDBPyConnection,
    collections: Sequence[str],
    host_rev: str,
) -> int:
    coll_placeholders = ",".join(["?"] * len(collections))
    sql = (
        "SELECT collection, parquet_relpath, row_group, dom_rg_row_start, dom_rg_row_end "
        "FROM cc_domain_rowgroups "
        f"WHERE collection IN ({coll_placeholders}) AND host_rev = ?"
    )
    rows = con.execute(sql, list(collections) + [str(host_rev)]).fetchall()
    return int(len(rows))


def _query_collection_db(db_path: Path, host_rev: str) -> int:
    con = _open_ro(db_path)
    try:
        rows = con.execute(
            "SELECT parquet_relpath, row_group, dom_rg_row_start, dom_rg_row_end "
            "FROM cc_domain_rowgroups WHERE host_rev = ?",
            [str(host_rev)],
        ).fetchall()
        return int(len(rows))
    finally:
        con.close()


def _stats(xs: Sequence[float]) -> Tuple[float, float, float, float]:
    return (statistics.mean(xs), statistics.median(xs), min(xs), max(xs))


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark per-year vs per-collection rowgroup lookups")
    ap.add_argument("--year", required=True, help="Year, e.g. 2024")
    ap.add_argument(
        "--collection-dir",
        default="/storage/ccindex_duckdb/cc_domain_rowgroups_by_collection",
        help="Directory with per-collection rowgroup DBs",
    )
    ap.add_argument(
        "--year-dir",
        default="/storage/ccindex_duckdb/cc_domain_rowgroups_by_year",
        help="Directory with per-year rowgroup DBs",
    )
    ap.add_argument("--samples", type=int, default=100, help="Number of host_rev values to test")
    ap.add_argument("--repeats", type=int, default=3, help="Repeat each query N times")
    ap.add_argument("--workers", type=int, default=8, help="Parallel workers for per-collection lookups")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")
    args = ap.parse_args()

    year = str(args.year).strip()
    if not year.isdigit():
        raise SystemExit("Year must be numeric")

    collection_dir = Path(args.collection_dir).expanduser().resolve()
    year_dir = Path(args.year_dir).expanduser().resolve()
    year_db = year_dir / f"cc_domain_rowgroups_{year}.duckdb"

    if not year_db.exists():
        raise SystemExit(f"Year DB not found: {year_db}")

    collection_dbs = _iter_collection_dbs(collection_dir, year)
    if not collection_dbs:
        raise SystemExit(f"No per-collection DBs found for year {year} in {collection_dir}")

    collections = [p.stem.replace(".domain_rowgroups", "") for p in collection_dbs]

    con_year = _open_ro(year_db)
    try:
        host_revs = _sample_host_revs(con_year, int(args.samples), int(args.seed))
        if not host_revs:
            raise SystemExit("No host_rev values found in year DB")

        year_times: List[float] = []
        coll_times: List[float] = []
        year_rows = 0
        coll_rows = 0

        for hr in host_revs:
            for _rep in range(max(1, int(args.repeats))):
                t0 = time.perf_counter()
                year_rows += _query_year_segments(con_year, collections, hr)
                year_times.append((time.perf_counter() - t0) * 1000.0)

                t1 = time.perf_counter()
                with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as ex:
                    futs = {ex.submit(_query_collection_db, dbp, hr): dbp for dbp in collection_dbs}
                    for fut in as_completed(futs):
                        try:
                            coll_rows += int(fut.result())
                        except Exception:
                            pass
                coll_times.append((time.perf_counter() - t1) * 1000.0)

        y_mean, y_med, y_min, y_max = _stats(year_times)
        c_mean, c_med, c_min, c_max = _stats(coll_times)

        print("=" * 80)
        print("Rowgroup segment lookup benchmark")
        print("=" * 80)
        print(f"year:         {year}")
        print(f"collections:  {len(collections)}")
        print(f"samples:      {len(host_revs)}")
        print(f"repeats:      {int(args.repeats)}")
        print(f"workers:      {int(args.workers)}")
        print()
        print("Per-year DB lookup")
        print(f"  n={len(year_times)}  mean={y_mean:.3f}ms  median={y_med:.3f}ms  min={y_min:.3f}ms  max={y_max:.3f}ms")
        print("Per-collection DB lookup (parallel)")
        print(f"  n={len(coll_times)}  mean={c_mean:.3f}ms  median={c_med:.3f}ms  min={c_min:.3f}ms  max={c_max:.3f}ms")
        print()
        print(f"Rows fetched: year={year_rows} collection={coll_rows}")

    finally:
        con_year.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
