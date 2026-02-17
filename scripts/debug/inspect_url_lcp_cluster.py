#!/usr/bin/env python3
"""Inspect extreme URL common-prefix clusters in a Parquet shard.

This is a lightweight debugging tool to understand when a shard contains many
very-long, near-duplicate URLs (e.g., long querystrings that differ only in a
trailing parameter). Those patterns can stress wide-key sorting paths in some
native engines.

It samples K rows from *each* row group (uniform without replacement), sorts the
sample either by URL-only or by (host_rev, url, ts), then reports the top-N
adjacent longest-common-prefix (LCP) pairs.

Example:
  python scripts/debug/inspect_url_lcp_cluster.py \
    --input-parquet /tmp/cc_full_repro/CC-MAIN-2025-30/parquet/cdx-00147.gz.parquet \
    --per-row-group 2000 --seed 1 --topk 5 --mode within-host-rev
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


def _lcp_len(a: str, b: str) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def _sample_rows(
    pf: pq.ParquetFile,
    *,
    columns: Sequence[str],
    per_row_group: int,
    seed: int,
) -> List[Tuple]:
    md = pf.metadata
    random.seed(seed)

    rows: List[Tuple] = []
    for rg in range(md.num_row_groups):
        rg_rows = md.row_group(rg).num_rows
        k = min(int(per_row_group), int(rg_rows))
        if k <= 0 or rg_rows <= 0:
            continue
        idxs = random.sample(range(rg_rows), k)
        t = pf.read_row_group(rg, columns=list(columns)).take(pa.array(idxs, type=pa.int32()))
        cols_py = [t[c].to_pylist() for c in columns]
        rows.extend(zip(*cols_py))
    return rows


def _print_pair(*, l: int, host_rev: str | None, url1: str, url2: str, ts1: str | None, ts2: str | None) -> None:
    prefix = url1[:l]
    if host_rev is not None:
        print(f"lcp={l} host_rev={host_rev} ts1={ts1} ts2={ts2} len1={len(url1)} len2={len(url2)}")
    else:
        print(f"lcp={l} len1={len(url1)} len2={len(url2)}")

    print("  prefix_head=", prefix[:200].replace("\n", "\\n"))
    if l > 210:
        print("  prefix_tail=", prefix[-200:].replace("\n", "\\n"))

    print("  url1_head  =", url1[:220].replace("\n", "\\n"))
    print("  url2_head  =", url2[:220].replace("\n", "\\n"))
    if len(url1) > 240 or len(url2) > 240:
        print("  url1_tail  =", url1[-220:].replace("\n", "\\n"))
        print("  url2_tail  =", url2[-220:].replace("\n", "\\n"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect extreme URL LCP clusters in a Parquet shard")
    ap.add_argument("--input-parquet", required=True, type=str)
    ap.add_argument("--per-row-group", type=int, default=2000, help="Rows sampled per row group (default: 2000)")
    ap.add_argument("--seed", type=int, default=1, help="Random seed (default: 1)")
    ap.add_argument("--topk", type=int, default=10, help="Number of top adjacent LCP pairs to print (default: 10)")
    ap.add_argument(
        "--mode",
        choices=["url-only", "within-host-rev"],
        default="within-host-rev",
        help="Sort mode: URL-only, or (host_rev,url,ts) with LCP only within same host_rev",
    )

    args = ap.parse_args()

    in_path = Path(args.input_parquet).expanduser().resolve()
    if not in_path.exists():
        print(f"ERROR: not found: {in_path}")
        return 2

    pf = pq.ParquetFile(in_path)
    md = pf.metadata

    if args.mode == "url-only":
        cols = ["url"]
        rows = _sample_rows(pf, columns=cols, per_row_group=args.per_row_group, seed=args.seed)
        urls = [r[0] for r in rows if r and r[0] is not None]
        urls.sort()

        pairs: List[Tuple[int, int]] = []
        for i in range(len(urls) - 1):
            l = _lcp_len(urls[i], urls[i + 1])
            if l > 0:
                pairs.append((l, i))
        pairs.sort(reverse=True, key=lambda x: x[0])

        print(f"file {in_path}")
        print(f"row_groups {md.num_row_groups} rows {md.num_rows}")
        print(f"sample_size {len(urls)} seed {args.seed} per_row_group {args.per_row_group}")
        print("\nTOP adjacent LCP pairs (URL-only sort)")
        for rank, (l, i) in enumerate(pairs[: int(args.topk)], 1):
            print(f"#{rank:02d}")
            _print_pair(l=l, host_rev=None, url1=urls[i], url2=urls[i + 1], ts1=None, ts2=None)
        return 0

    # within-host-rev
    cols = ["host_rev", "url", "ts"]
    rows2 = _sample_rows(pf, columns=cols, per_row_group=args.per_row_group, seed=args.seed)
    rows2 = [r for r in rows2 if r and r[0] is not None and r[1] is not None]
    rows2.sort(key=lambda r: (r[0], r[1], r[2]))

    pairs2: List[Tuple[int, int]] = []
    for i in range(len(rows2) - 1):
        hr1, u1, ts1 = rows2[i]
        hr2, u2, ts2 = rows2[i + 1]
        if hr1 != hr2:
            continue
        l = _lcp_len(u1, u2)
        if l > 0:
            pairs2.append((l, i))

    pairs2.sort(reverse=True, key=lambda x: x[0])

    print(f"file {in_path}")
    print(f"row_groups {md.num_row_groups} rows {md.num_rows}")
    print(f"sample_size {len(rows2)} seed {args.seed} per_row_group {args.per_row_group}")
    print("\nTOP adjacent LCP pairs (within host_rev; sort by host_rev,url,ts)")
    for rank, (l, i) in enumerate(pairs2[: int(args.topk)], 1):
        hr, u1, ts1 = rows2[i]
        _, u2, ts2 = rows2[i + 1]
        print(f"#{rank:02d}")
        _print_pair(l=l, host_rev=str(hr), url1=str(u1), url2=str(u2), ts1=str(ts1), ts2=str(ts2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
