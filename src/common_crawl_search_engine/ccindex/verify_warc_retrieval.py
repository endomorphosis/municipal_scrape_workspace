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
from pathlib import Path
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


def _safe_filename_from_url(url: str) -> str:
    # Keep it simple: use the last path component.
    # Example: .../CC-MAIN-....warc.gz
    return url.rstrip("/").rsplit("/", 1)[-1] or "download.bin"


def _download_to_file(
    url: str,
    out_path: Path,
    *,
    timeout_s: float,
    range_tuple: Optional[Tuple[int, int]] = None,
    overwrite: bool = False,
    retries: int = 2,
    chunk_bytes: int = 8 * 1024 * 1024,
) -> Tuple[bool, str]:
    """Download URL (full or byte range) to out_path.

    Returns (ok, message).
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pre-flight: decide if we can skip.
    expected_size: Optional[int] = None
    if range_tuple is not None:
        expected_size = int(range_tuple[1]) - int(range_tuple[0]) + 1
    else:
        try:
            _, headers = _http_head(url, timeout_s=timeout_s)
            clen = headers.get("content-length")
            if clen and clen.isdigit():
                expected_size = int(clen)
        except Exception:
            expected_size = None

    if out_path.exists() and not overwrite and expected_size is not None:
        try:
            if out_path.stat().st_size == expected_size:
                return True, f"skip_existing size={_format_size(expected_size)} path={out_path}"
        except Exception:
            pass

    # Download with retries.
    last_err: Optional[str] = None
    for attempt in range(max(1, int(retries) + 1)):
        try:
            req = urllib.request.Request(url, method="GET")
            if range_tuple is not None:
                rs, re = range_tuple
                req.add_header("Range", f"bytes={int(rs)}-{int(re)}")

            tmp_path = out_path.with_suffix(out_path.suffix + ".part")
            if tmp_path.exists() and overwrite:
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = getattr(resp, "status", 200)
                if range_tuple is not None and int(status) != 206:
                    raise RuntimeError(f"expected 206 for range GET, got {status}")

                with tmp_path.open("wb") as f:
                    while True:
                        chunk = resp.read(int(chunk_bytes))
                        if not chunk:
                            break
                        f.write(chunk)

            # Validate size if known.
            if expected_size is not None:
                got = tmp_path.stat().st_size
                if got != expected_size:
                    raise RuntimeError(f"size mismatch expected={expected_size} got={got}")

            # Atomic-ish finalize.
            if out_path.exists() and overwrite:
                out_path.unlink()
            tmp_path.replace(out_path)
            return True, f"downloaded bytes={_format_size(expected_size)} path={out_path}" if expected_size else f"downloaded path={out_path}"

        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < max(1, int(retries) + 1):
                time.sleep(1.0)
            continue

    return False, f"download_failed url={url} err={last_err}"


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

    ap.add_argument(
        "--download-dir",
        type=Path,
        default=None,
        help="If set, download each verified URL into this directory (optional)",
    )
    ap.add_argument(
        "--download-mode",
        choices=["none", "full", "range"],
        default="none",
        help="Download mode when --download-dir is provided (default: none)",
    )
    ap.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing downloads")
    ap.add_argument("--retries", type=int, default=2, help="Download retries (default: 2)")

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

            # Optional download.
            if args.download_dir and args.download_mode != "none":
                dl_dir = Path(args.download_dir).expanduser().resolve()
                fname = _safe_filename_from_url(url)
                out_path = dl_dir / fname

                dl_range: Optional[Tuple[int, int]] = None
                if args.download_mode == "range":
                    if range_tuple is None:
                        raise RuntimeError("--download-mode range requires --range")
                    dl_range = range_tuple
                    # Make the filename explicit.
                    rs, re = dl_range
                    out_path = dl_dir / f"{fname}.range.{rs}-{re}.bin"

                ok_dl, msg_dl = _download_to_file(
                    url,
                    out_path,
                    timeout_s=float(args.timeout),
                    range_tuple=dl_range if args.download_mode == "range" else None,
                    overwrite=bool(args.overwrite),
                    retries=int(args.retries),
                )
                if ok_dl:
                    print(f"  DOWNLOAD ok {msg_dl}")
                else:
                    _eprint(f"  DOWNLOAD fail {msg_dl}")

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
