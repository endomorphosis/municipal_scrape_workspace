#!/usr/bin/env python3
"""Benchmark: domain lookup via Parquet scan vs row-group-index + slicing.

This benchmarks the execution strategy you described:
- Look up domain -> (parquet file, row_group, offsets)
- Read only those row groups via PyArrow
- Slice to the exact row-range for that domain within each row group

Baseline:
- DuckDB `read_parquet(file) WHERE host_rev = ?`

This is intended to be a *prototype* benchmark. It primarily measures Python-level
wall time for repeated lookups on a single shard/index.

Usage example:
  ./.venv/bin/python benchmarks/ccindex/benchmark_rowgroup_slice.py \
    --index-db /storage/ccindex_duckdb/proto_domain_rowgroups/CC-MAIN-2024-10.domain_rowgroups.duckdb \
    --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection/2024/CC-MAIN-2024-10 \
    --samples 100 --repeats 3 --threads 8 --verify
"""

from __future__ import annotations

import argparse
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


@dataclass(frozen=True)
class Segment:
    parquet_relpath: str
    row_group: int
    dom_rg_row_start: int
    dom_rg_row_end: int


def _open_ro_con(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def _pick_host_revs(con: duckdb.DuckDBPyConnection, n: int, seed: int) -> List[str]:
    # Sample from the index DB itself to ensure coverage.
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


def _segments_for_host_rev(
    con: duckdb.DuckDBPyConnection, host_rev: str
) -> List[Segment]:
    rows = con.execute(
        """
        SELECT parquet_relpath, row_group, dom_rg_row_start, dom_rg_row_end
        FROM cc_domain_rowgroups
        WHERE host_rev = ?
        ORDER BY parquet_relpath, row_group
        """,
        [str(host_rev)],
    ).fetchall()
    out: List[Segment] = []
    for rel, rg, s, e in rows:
        if rel is None or rg is None or s is None or e is None:
            continue
        out.append(
            Segment(
                parquet_relpath=str(rel),
                row_group=int(rg),
                dom_rg_row_start=int(s),
                dom_rg_row_end=int(e),
            )
        )
    return out


def _duckdb_count_for_host_rev(
    con: duckdb.DuckDBPyConnection, parquet_path: Path, host_rev: str
) -> int:
    row = con.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE host_rev = ?",
        [str(parquet_path), str(host_rev)],
    ).fetchone()
    return int(row[0] if row else 0)


def _rowgroup_slice_count_for_host_rev(
    pf: pq.ParquetFile,
    segments: Sequence[Segment],
    host_rev: str,
    *,
    columns: Sequence[str],
    verify: bool,
) -> int:
    total = 0
    for seg in segments:
        # Read full row group but only requested columns.
        tbl = pf.read_row_group(int(seg.row_group), columns=list(columns))
        start = max(0, int(seg.dom_rg_row_start))
        end = min(int(tbl.num_rows), int(seg.dom_rg_row_end))
        if end <= start:
            continue
        sliced = tbl.slice(offset=start, length=end - start)

        if verify:
            if "host_rev" in sliced.column_names:
                bad = pc.any(pc.not_equal(sliced["host_rev"], pa.scalar(str(host_rev)))).as_py()
                if bool(bad):
                    # Fall back to explicit filter (should not happen for correctly built index)
                    sliced = sliced.filter(pc.equal(sliced["host_rev"], pa.scalar(str(host_rev))))

        total += int(sliced.num_rows)
    return total


def _pct(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark row-group slicing vs full parquet scan")
    ap.add_argument("--index-db", required=True, type=Path, help="DuckDB with cc_domain_rowgroups")
    ap.add_argument("--parquet-root", required=True, type=Path, help="Root containing parquet files")
    ap.add_argument("--samples", type=int, default=50, help="Number of host_rev values to test")
    ap.add_argument("--repeats", type=int, default=3, help="Repeat each query N times")
    ap.add_argument("--threads", type=int, default=8, help="DuckDB threads for baseline")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")
    ap.add_argument(
        "--columns",
        type=str,
        default="host_rev,url,warc_filename,warc_offset,warc_length",
        help="Comma-separated columns to read for row-group method",
    )
    ap.add_argument(
        "--verify",
        action="store_true",
        help="Verify sliced rows all match host_rev (adds overhead)",
    )

    args = ap.parse_args()

    index_db = Path(args.index_db).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve()

    if not index_db.exists():
        raise SystemExit(f"index db not found: {index_db}")
    if not parquet_root.exists():
        raise SystemExit(f"parquet root not found: {parquet_root}")

    columns = [c.strip() for c in str(args.columns).split(",") if c.strip()]
    if "host_rev" not in columns:
        columns = ["host_rev"] + columns

    con_idx = _open_ro_con(index_db)
    try:
        host_revs = _pick_host_revs(con_idx, int(args.samples), int(args.seed))
        if not host_revs:
            raise SystemExit("no host_rev values found in cc_domain_rowgroups")

        # Preload all segments and ParquetFile handles for fairer timing.
        # (We still measure per-query times including row_group reads.)
        host_to_segments: Dict[str, List[Segment]] = {}
        parquet_cache: Dict[str, pq.ParquetFile] = {}

        for hr in host_revs:
            segs = _segments_for_host_rev(con_idx, hr)
            if not segs:
                continue
            host_to_segments[hr] = segs
            for seg in segs:
                if seg.parquet_relpath not in parquet_cache:
                    p = (parquet_root / seg.parquet_relpath).resolve()
                    parquet_cache[seg.parquet_relpath] = pq.ParquetFile(p)

        host_revs = [hr for hr in host_revs if hr in host_to_segments]
        if not host_revs:
            raise SystemExit("no host_rev values with segments")

    finally:
        con_idx.close()

    # Baseline DuckDB connection (in-memory)
    con_base = duckdb.connect(database=":memory:")
    con_base.execute(f"PRAGMA threads={max(1,int(args.threads))}")

    try:
        base_times_ms: List[float] = []
        rg_times_ms: List[float] = []
        mismatches = 0

        # Warm up (one baseline + one rowgroup)
        w = host_revs[0]
        segs_w = host_to_segments[w]
        pf_w = parquet_cache[segs_w[0].parquet_relpath]
        p_w = (parquet_root / segs_w[0].parquet_relpath).resolve()
        _ = _duckdb_count_for_host_rev(con_base, p_w, w)
        _ = _rowgroup_slice_count_for_host_rev(pf_w, segs_w, w, columns=columns, verify=False)

        for hr in host_revs:
            segs = host_to_segments[hr]
            # All segments for a host_rev may (in theory) be across multiple parquet files;
            # in practice for a single shard index it should be just one.
            rels = sorted({s.parquet_relpath for s in segs})
            if not rels:
                continue

            # Baseline: for fairness, benchmark only against the first parquet file.
            rel0 = rels[0]
            parquet_path = (parquet_root / rel0).resolve()

            for _rep in range(max(1, int(args.repeats))):
                t0 = time.perf_counter()
                base_cnt = _duckdb_count_for_host_rev(con_base, parquet_path, hr)
                base_times_ms.append((time.perf_counter() - t0) * 1000.0)

                # Row-group slicing method: sum counts across all segments (potentially multiple rowgroups)
                t1 = time.perf_counter()
                rg_cnt = 0
                for rel in rels:
                    pf = parquet_cache[rel]
                    segs_rel = [s for s in segs if s.parquet_relpath == rel]
                    rg_cnt += _rowgroup_slice_count_for_host_rev(
                        pf,
                        segs_rel,
                        hr,
                        columns=columns,
                        verify=bool(args.verify),
                    )
                rg_times_ms.append((time.perf_counter() - t1) * 1000.0)

                if base_cnt != rg_cnt:
                    mismatches += 1

        def _stats(xs: Sequence[float]) -> tuple[float, float, float, float]:
            return (
                statistics.mean(xs),
                statistics.median(xs),
                min(xs),
                max(xs),
            )

        b_mean, b_med, b_min, b_max = _stats(base_times_ms)
        r_mean, r_med, r_min, r_max = _stats(rg_times_ms)

        print("=" * 80)
        print("Rowgroup+slicing benchmark")
        print("=" * 80)
        print(f"index_db:     {index_db}")
        print(f"parquet_root: {parquet_root}")
        print(f"hosts:        {len(host_revs)}")
        print(f"repeats:      {int(args.repeats)}")
        print(f"columns(rg):  {','.join(columns)}")
        print(f"verify:       {bool(args.verify)}")
        print(f"mismatches:   {mismatches}")
        print()

        print("Baseline (DuckDB read_parquet WHERE host_rev=?)")
        print(f"  n={len(base_times_ms)}  mean={b_mean:.3f}ms  median={b_med:.3f}ms  min={b_min:.3f}ms  max={b_max:.3f}ms")
        print("Rowgroup index + PyArrow read_row_group + slice")
        print(f"  n={len(rg_times_ms)}  mean={r_mean:.3f}ms  median={r_med:.3f}ms  min={r_min:.3f}ms  max={r_max:.3f}ms")
        print()
        print(f"Delta (median): {_pct(b_med, r_med):+.1f}%")
        print(f"Delta (mean):   {_pct(b_mean, r_mean):+.1f}%")

    finally:
        con_base.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
