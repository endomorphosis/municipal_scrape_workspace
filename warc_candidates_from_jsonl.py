#!/usr/bin/env python3
"""Summarize candidate WARC files from JSONL pointer results.

This is intended to be used downstream of:
  - search_cc_via_meta_indexes.py
  - search_cc_pointer_index.py
  - any tool that emits JSON lines with at least: warc_filename, warc_offset, warc_length

Examples:
  # Produce a unique list of WARC filenames
  python search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 2000 \
    | python warc_candidates_from_jsonl.py --format list

    # Produce a unique list of full download URLs
    python search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 2000 \
        | python warc_candidates_from_jsonl.py --format list --prefix https://data.commoncrawl.org/

  # Include counts + total bytes per WARC, emit JSON
  python search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 2000 \
    | python warc_candidates_from_jsonl.py --format json --max-warcs 50

  # Read from a saved JSONL file
  python warc_candidates_from_jsonl.py --input results.jsonl --format csv > warcs.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, TextIO, Tuple


@dataclass
class WarcAgg:
    warc_filename: str
    record_count: int = 0
    total_warc_bytes: int = 0
    min_offset: Optional[int] = None
    max_offset_end: Optional[int] = None

    def add(self, offset: Optional[int], length: Optional[int]) -> None:
        self.record_count += 1
        if offset is not None and length is not None:
            end = int(offset) + int(length)
            self.total_warc_bytes += int(length)
            if self.min_offset is None or int(offset) < self.min_offset:
                self.min_offset = int(offset)
            if self.max_offset_end is None or end > self.max_offset_end:
                self.max_offset_end = end


def _iter_jsonl(fp: TextIO) -> Iterator[Dict[str, object]]:
    for line in fp:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def _open_input(path: Optional[Path]) -> TextIO:
    if path is None:
        return sys.stdin
    return path.open("r", encoding="utf-8")


def _summarize(records: Iterable[Dict[str, object]]) -> Dict[str, WarcAgg]:
    by_warc: Dict[str, WarcAgg] = {}
    for rec in records:
        warc = rec.get("warc_filename")
        if not warc:
            continue
        warc_s = str(warc)
        agg = by_warc.get(warc_s)
        if agg is None:
            agg = WarcAgg(warc_filename=warc_s)
            by_warc[warc_s] = agg

        offset = rec.get("warc_offset")
        length = rec.get("warc_length")
        try:
            off_i = int(offset) if offset is not None else None
        except Exception:
            off_i = None
        try:
            len_i = int(length) if length is not None else None
        except Exception:
            len_i = None

        agg.add(off_i, len_i)

    return by_warc


def _sorted_aggs(by_warc: Dict[str, WarcAgg], sort_by: str) -> List[WarcAgg]:
    aggs = list(by_warc.values())
    if sort_by == "bytes":
        aggs.sort(key=lambda a: (a.total_warc_bytes, a.record_count, a.warc_filename), reverse=True)
    elif sort_by == "count":
        aggs.sort(key=lambda a: (a.record_count, a.total_warc_bytes, a.warc_filename), reverse=True)
    else:  # filename
        aggs.sort(key=lambda a: a.warc_filename)
    return aggs


def main() -> int:
    ap = argparse.ArgumentParser(description="Deduplicate/group JSONL pointer results into candidate WARC filenames")
    ap.add_argument("--input", type=Path, default=None, help="Input JSONL file (default: stdin)")
    ap.add_argument(
        "--format",
        choices=["list", "json", "csv"],
        default="list",
        help="Output format",
    )
    ap.add_argument(
        "--prefix",
        type=str,
        default=None,
        help=(
            "Optional prefix to turn warc_filename into a full download URL. "
            "Example: https://data.commoncrawl.org/ (a trailing '/' is added if missing)."
        ),
    )
    ap.add_argument(
        "--sort",
        choices=["bytes", "count", "filename"],
        default="bytes",
        help="Sort order (default: bytes desc)",
    )
    ap.add_argument("--max-warcs", type=int, default=None, help="Limit number of WARC files emitted")
    ap.add_argument("--min-count", type=int, default=1, help="Only emit WARCs with at least this many matches")

    args = ap.parse_args()

    fp = _open_input(args.input)
    try:
        by_warc = _summarize(_iter_jsonl(fp))
    finally:
        if fp is not sys.stdin:
            fp.close()

    aggs = _sorted_aggs(by_warc, args.sort)

    min_count = max(1, int(args.min_count))
    aggs = [a for a in aggs if a.record_count >= min_count]

    if args.max_warcs is not None:
        aggs = aggs[: max(0, int(args.max_warcs))]

    if args.format == "list":
        prefix = None
        if args.prefix:
            prefix = str(args.prefix)
            if prefix and not prefix.endswith("/"):
                prefix += "/"
        for a in aggs:
            if prefix:
                sys.stdout.write(prefix + a.warc_filename.lstrip("/") + "\n")
            else:
                sys.stdout.write(a.warc_filename + "\n")
        return 0

    if args.format == "json":
        prefix = None
        if args.prefix:
            prefix = str(args.prefix)
            if prefix and not prefix.endswith("/"):
                prefix += "/"
        out = [
            {
                "warc_filename": a.warc_filename,
                "download_url": (prefix + a.warc_filename.lstrip("/")) if prefix else None,
                "record_count": a.record_count,
                "total_warc_bytes": a.total_warc_bytes,
                "min_offset": a.min_offset,
                "max_offset_end": a.max_offset_end,
            }
            for a in aggs
        ]
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        return 0

    # csv
    prefix = None
    if args.prefix:
        prefix = str(args.prefix)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
    w = csv.writer(sys.stdout)
    w.writerow(["warc_filename", "download_url", "record_count", "total_warc_bytes", "min_offset", "max_offset_end"])
    for a in aggs:
        url = (prefix + a.warc_filename.lstrip("/")) if prefix else ""
        w.writerow([a.warc_filename, url, a.record_count, a.total_warc_bytes, a.min_offset, a.max_offset_end])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
