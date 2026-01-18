#!/usr/bin/env python3
"""Verify candidate Common Crawl WARC files are retrievable.

This script is designed to consume output from:
  - warc_candidates_from_jsonl.py --format list --prefix https://data.commoncrawl.org/
  - warc_candidates_from_jsonl.py --format json   (uses download_url)
  - search_cc_via_meta_indexes.py JSONL (uses warc_filename + optional prefix)

It performs:
  1) HTTP HEAD (or minimal GET fallback) to check the object exists and get size.
  2) Optional small Range GET to confirm ranged retrieval works.

Examples:
  python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 200 \
    | python3 warc_candidates_from_jsonl.py --format list --prefix https://data.commoncrawl.org/ --max-warcs 5 \
    | python3 verify_warc_retrieval.py --range 0:64

  python3 warc_candidates_from_jsonl.py --input warc_candidates_top50.json --format json \
    | python3 verify_warc_retrieval.py --max 3
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


@dataclass(frozen=True)
class Candidate:
    url: str
    source: str


def _eprint(msg: str) -> None:
    sys.stderr.write(str(msg) + "\n")


def _normalize_prefix(prefix: Optional[str]) -> Optional[str]:
    if not prefix:
        return None
    p = str(prefix)
    if p and not p.endswith("/"):
        p += "/"
    return p


def _iter_candidates_from_stdin(prefix: Optional[str]) -> Iterator[Candidate]:
    """Accepts plain URL lines, JSON arrays, or JSONL dicts."""

    pref = _normalize_prefix(prefix)

    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue

        # JSON array (single line)
        if raw.startswith("["):
            try:
                arr = json.loads(raw)
            except Exception:
                arr = None
            if isinstance(arr, list):
                for obj in arr:
                    if isinstance(obj, dict):
                        url = obj.get("download_url") or obj.get("url")
                        warc = obj.get("warc_filename")
                        if url:
                            yield Candidate(url=str(url), source="json-array")
                        elif warc and pref:
                            yield Candidate(url=pref + str(warc).lstrip("/"), source="json-array")
                continue

        # JSONL dict
        if raw.startswith("{") and raw.endswith("}"):
            try:
                obj = json.loads(raw)
            except Exception:
                obj = None
            if isinstance(obj, dict):
                url = obj.get("download_url") or obj.get("url")
                warc = obj.get("warc_filename")
                if url:
                    yield Candidate(url=str(url), source="jsonl")
                    continue
                if warc and pref:
                    yield Candidate(url=pref + str(warc).lstrip("/"), source="jsonl")
                    continue

        # Plain string: either a full URL or a crawl-data path
        if raw.startswith("http://") or raw.startswith("https://"):
            yield Candidate(url=raw, source="text")
        elif pref:
            yield Candidate(url=pref + raw.lstrip("/"), source="text")


def _http_head(url: str, timeout_s: float) -> Tuple[int, Dict[str, str]]:
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        status = getattr(resp, "status", 200)
        headers = {k.lower(): v for k, v in resp.headers.items()}
        return int(status), headers


def _http_range_get(url: str, start: int, end_inclusive: int, timeout_s: float) -> Tuple[int, Dict[str, str], bytes]:
    # RFC 7233: bytes=<first>-<last>
    req = urllib.request.Request(url, method="GET")
    req.add_header("Range", f"bytes={start}-{end_inclusive}")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        status = getattr(resp, "status", 200)
        headers = {k.lower(): v for k, v in resp.headers.items()}
        data = resp.read()
        return int(status), headers, data


def _format_size(n: Optional[int]) -> str:
    if n is None:
        return "?"
    size = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024.0
    return f"{size:.1f}PB"


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify Common Crawl WARC URLs are retrievable")
    ap.add_argument(
        "--prefix",
        type=str,
        default=None,
        help="If input lines are warc filenames (crawl-data/...), prefix them with this URL (e.g. https://data.commoncrawl.org/)",
    )
    ap.add_argument("--max", type=int, default=10, help="Max URLs to check (default: 10)")
    ap.add_argument("--timeout", type=float, default=20.0, help="Network timeout in seconds")
    ap.add_argument(
        "--range",
        type=str,
        default=None,
        help="Optional byte range to GET as START:END (inclusive), e.g. 0:63",
    )
    ap.add_argument("--show-bytes", action="store_true", default=False, help="Print first 64 bytes (hex) of the ranged GET")

    args = ap.parse_args()

    range_tuple: Optional[Tuple[int, int]] = None
    if args.range:
        try:
            start_s, end_s = str(args.range).split(":", 1)
            start = int(start_s)
            end = int(end_s)
            if start < 0 or end < start:
                raise ValueError
            range_tuple = (start, end)
        except Exception:
            _eprint("Invalid --range; expected START:END with START<=END")
            return 2

    checked = 0
    ok = 0

    t0 = time.perf_counter()

    for cand in _iter_candidates_from_stdin(args.prefix):
        if checked >= int(args.max):
            break
        checked += 1

        url = cand.url
        try:
            status, headers = _http_head(url, timeout_s=float(args.timeout))
            clen = headers.get("content-length")
            size = int(clen) if clen and clen.isdigit() else None
            accept_ranges = headers.get("accept-ranges", "")

            msg = f"OK HEAD {status} size={_format_size(size)} accept_ranges={accept_ranges!r} url={url}"
            print(msg)

            if range_tuple is not None:
                rs, re = range_tuple
                r_status, r_headers, data = _http_range_get(url, rs, re, timeout_s=float(args.timeout))
                cr = r_headers.get("content-range")
                print(f"  RANGE {r_status} bytes={len(data)} content_range={cr!r}")
                if args.show_bytes:
                    print("  DATA_HEX", data[:64].hex())

            ok += 1

        except urllib.error.HTTPError as e:
            _eprint(f"FAIL HTTP {e.code} url={url}")
        except urllib.error.URLError as e:
            _eprint(f"FAIL URL {e.reason} url={url}")
        except Exception as e:
            _eprint(f"FAIL {type(e).__name__}: {e} url={url}")

    dt = time.perf_counter() - t0
    _eprint(f"checked={checked} ok={ok} elapsed_s={dt:.2f}")

    return 0 if ok == checked else 1


if __name__ == "__main__":
    raise SystemExit(main())
