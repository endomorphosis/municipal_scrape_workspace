#!/usr/bin/env python3
"""Download exact WARC byte ranges for pointer records.

Input is JSONL records (typically from search_cc_via_meta_indexes.py) containing:
  - warc_filename (e.g. crawl-data/CC-MAIN-.../*.warc.gz)
  - warc_offset (int)
  - warc_length (int)
Optionally: url, timestamp, collection, digest, etc.

For each record, this script issues a HTTP Range GET for:
  bytes = [warc_offset, warc_offset + warc_length - 1]

It writes each downloaded payload to disk and emits a JSONL manifest to stdout
with the download result.

Examples:
  # Download up to 10 pointer records (range blobs) for a domain
  python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 50 \
    | python3 download_warc_records.py --out-dir /tmp/cc_warc_records --max-records 10

  # If input lacks full URLs, provide the Common Crawl prefix
  python3 download_warc_records.py --out-dir /tmp/cc_warc_records --prefix https://data.commoncrawl.org/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple


def _eprint(msg: str) -> None:
    sys.stderr.write(str(msg) + "\n")


def _normalize_prefix(prefix: Optional[str]) -> Optional[str]:
    if not prefix:
        return None
    p = str(prefix)
    if p and not p.endswith("/"):
        p += "/"
    return p


def _safe_filename(s: str) -> str:
    # Conservative filesystem-safe name.
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:200] if len(s) > 200 else s


def _url_for_record(rec: Dict[str, object], prefix: Optional[str]) -> Optional[str]:
    # Prefer explicit fields if present.
    url = rec.get("download_url")
    if url:
        return str(url)

    warc = rec.get("warc_filename")
    if not warc:
        return None

    warc_s = str(warc)
    if warc_s.startswith("http://") or warc_s.startswith("https://"):
        return warc_s

    pref = _normalize_prefix(prefix)
    if not pref:
        return None

    return pref + warc_s.lstrip("/")


def _parse_int(v: object) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _iter_jsonl_stdin() -> Iterator[Dict[str, object]]:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


@dataclass(frozen=True)
class DownloadResult:
    ok: bool
    status: Optional[int]
    url: str
    out_path: Optional[str]
    bytes_requested: int
    bytes_written: int
    sha256: Optional[str]
    error: Optional[str]


def _download_range(
    url: str,
    start: int,
    end_inclusive: int,
    out_path: Path,
    *,
    timeout_s: float,
    overwrite: bool,
    retries: int,
    chunk_bytes: int = 4 * 1024 * 1024,
) -> DownloadResult:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    bytes_requested = int(end_inclusive) - int(start) + 1

    if out_path.exists() and not overwrite:
        try:
            if out_path.stat().st_size == bytes_requested:
                # Still compute sha for reproducibility? Skip for speed.
                return DownloadResult(
                    ok=True,
                    status=200,
                    url=url,
                    out_path=str(out_path),
                    bytes_requested=bytes_requested,
                    bytes_written=bytes_requested,
                    sha256=None,
                    error=None,
                )
        except Exception:
            pass

    last_err: Optional[str] = None

    for attempt in range(max(1, int(retries) + 1)):
        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("Range", f"bytes={int(start)}-{int(end_inclusive)}")

            tmp_path = out_path.with_suffix(out_path.suffix + ".part")
            if tmp_path.exists() and overwrite:
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

            h = hashlib.sha256()
            written = 0

            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = getattr(resp, "status", 200)
                if int(status) != 206:
                    raise RuntimeError(f"expected 206 for range GET, got {status}")

                with tmp_path.open("wb") as f:
                    while True:
                        chunk = resp.read(int(chunk_bytes))
                        if not chunk:
                            break
                        f.write(chunk)
                        h.update(chunk)
                        written += len(chunk)

            if written != bytes_requested:
                raise RuntimeError(f"size mismatch expected={bytes_requested} got={written}")

            if out_path.exists() and overwrite:
                out_path.unlink()
            tmp_path.replace(out_path)

            return DownloadResult(
                ok=True,
                status=206,
                url=url,
                out_path=str(out_path),
                bytes_requested=bytes_requested,
                bytes_written=written,
                sha256=h.hexdigest(),
                error=None,
            )

        except urllib.error.HTTPError as e:
            last_err = f"HTTPError {e.code}"
        except urllib.error.URLError as e:
            last_err = f"URLError {e.reason}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        if attempt < max(1, int(retries) + 1):
            time.sleep(1.0)

    return DownloadResult(
        ok=False,
        status=None,
        url=url,
        out_path=None,
        bytes_requested=bytes_requested,
        bytes_written=0,
        sha256=None,
        error=last_err,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Download exact WARC byte ranges from pointer JSONL records")
    ap.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory to write downloaded record blobs",
    )
    ap.add_argument(
        "--prefix",
        type=str,
        default="https://data.commoncrawl.org/",
        help="Prefix for warc_filename paths when input does not contain full URLs",
    )
    ap.add_argument("--max-records", type=int, default=20, help="Max records to download (default: 20)")
    ap.add_argument(
        "--max-bytes",
        type=int,
        default=2_000_000,
        help="Skip records larger than this many bytes (default: 2,000,000)",
    )
    ap.add_argument("--timeout", type=float, default=30.0, help="Network timeout seconds")
    ap.add_argument("--retries", type=int, default=2, help="Retries per record")
    ap.add_argument("--overwrite", action="store_true", default=False)

    args = ap.parse_args()

    out_dir = Path(args.out_dir).expanduser().resolve()
    prefix = _normalize_prefix(args.prefix)

    downloaded = 0
    attempted = 0
    skipped = 0
    failed = 0

    t0 = time.perf_counter()

    for rec in _iter_jsonl_stdin():
        if attempted >= int(args.max_records):
            break

        warc_off = _parse_int(rec.get("warc_offset"))
        warc_len = _parse_int(rec.get("warc_length"))
        warc_fn = rec.get("warc_filename")

        if warc_off is None or warc_len is None or not warc_fn:
            skipped += 1
            continue

        if warc_len > int(args.max_bytes):
            skipped += 1
            continue

        url = _url_for_record(rec, prefix)
        if not url:
            skipped += 1
            continue

        start = int(warc_off)
        end_inclusive = int(warc_off) + int(warc_len) - 1

        attempted += 1

        base = _safe_filename(str(warc_fn).rsplit("/", 1)[-1])
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        out_name = f"{base}.{url_hash}.off{start}.len{warc_len}.bin"
        out_path = out_dir / out_name

        res = _download_range(
            url,
            start,
            end_inclusive,
            out_path,
            timeout_s=float(args.timeout),
            overwrite=bool(args.overwrite),
            retries=int(args.retries),
        )

        if res.ok:
            downloaded += 1
        else:
            failed += 1

        # Emit manifest line (JSONL) to stdout.
        manifest = {
            "ok": res.ok,
            "url": res.url,
            "warc_filename": str(warc_fn),
            "warc_offset": start,
            "warc_length": int(warc_len),
            "http_status": res.status,
            "out_path": res.out_path,
            "bytes_requested": res.bytes_requested,
            "bytes_written": res.bytes_written,
            "sha256": res.sha256,
            "error": res.error,
            "source": {
                "collection": rec.get("collection"),
                "timestamp": rec.get("timestamp"),
                "url": rec.get("url"),
                "digest": rec.get("digest"),
            },
        }
        sys.stdout.write(json.dumps(manifest, ensure_ascii=False) + "\n")

    dt = time.perf_counter() - t0
    _eprint(
        f"attempted={attempted} downloaded={downloaded} failed={failed} skipped={skipped} elapsed_s={dt:.2f} out_dir={out_dir}"
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
