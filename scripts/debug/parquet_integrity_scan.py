#!/usr/bin/env python3
"""Quick Parquet integrity scan for CC pointer shards.

Checks:
- Parquet metadata readable
- Schema/column presence
- Row-group iteration works
- Basic string sanity: counts U+FFFD replacement chars in selected columns

This is meant to help validate the "conversion produced malformed parquet" hypothesis.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _count_replacement_chars(arr: pa.ChunkedArray) -> int:
    if arr.num_chunks == 0:
        return 0
    combined = arr.combine_chunks()
    if not (pa.types.is_string(combined.type) or pa.types.is_large_string(combined.type)):
        return 0
    # Count occurrences of the Unicode replacement character used by errors="replace".
    return int(pc.sum(pc.count_substring(combined, "\ufffd")).as_py() or 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument(
        "--columns",
        action="append",
        default=["url", "surt", "host_rev"],
        help="Column(s) to scan for replacement chars (can repeat). Default: url,surt,host_rev",
    )
    ap.add_argument("--max-row-groups", type=int, default=0, help="0 = scan all row groups")
    args = ap.parse_args()

    p = Path(args.parquet).expanduser().resolve()
    if not p.exists():
        print(f"ERROR: not found: {p}")
        return 2

    pf = pq.ParquetFile(p)
    md = pf.metadata
    print(f"path={p}")
    print(f"row_groups={md.num_row_groups if md else None} rows={md.num_rows if md else None}")
    print(f"schema={pf.schema_arrow}")

    cols = list(dict.fromkeys([str(c) for c in (args.columns or [])]))
    present = set(pf.schema_arrow.names)
    wanted = [c for c in cols if c in present]
    missing = [c for c in cols if c not in present]
    if missing:
        print(f"missing_columns={missing}")

    max_rgs = int(args.max_row_groups)
    rg_total = int(md.num_row_groups) if md else 0
    rg_n = rg_total if max_rgs <= 0 else min(rg_total, max_rgs)

    repl_counts = {c: 0 for c in wanted}
    for i in range(rg_n):
        t = pf.read_row_group(i, columns=wanted if wanted else None)
        for c in wanted:
            repl_counts[c] += _count_replacement_chars(t[c])

    if wanted:
        print("replacement_char_counts=" + ", ".join(f"{k}={v}" for k, v in repl_counts.items()))

    # Smoke test: can we read the full file (may be slow/large, but catches truncation)?
    # We avoid materializing all columns; just a few key ones.
    sample_cols = [c for c in ["host_rev", "url", "ts"] if c in present]
    try:
        _ = pq.read_table(p, columns=sample_cols)
        print(f"read_table_ok columns={sample_cols}")
    except Exception as e:
        print(f"read_table_failed columns={sample_cols} err={e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
