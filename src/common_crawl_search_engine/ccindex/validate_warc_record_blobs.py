#!/usr/bin/env python3
"""Validate downloaded WARC record blobs.

This is meant to be used after download_warc_records.py, which downloads byte
ranges out of a *.warc.gz file (usually individual gzip members that contain one
WARC record).

Checks performed per blob:
- Looks for gzip header (0x1f 0x8b 0x08) at byte 0
- Attempts gzip decompression (best-effort)
- Confirms decompressed payload begins with "WARC/1." and parses headers

Outputs one JSON line per input file.

Examples:
  python3 validate_warc_record_blobs.py /tmp/cc_warc_records_smoke/*.bin | head

  # Validate everything in a directory
  python3 validate_warc_record_blobs.py --dir /tmp/cc_warc_records_smoke
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


def _iter_files(paths: List[Path], directory: Optional[Path]) -> Iterator[Path]:
    for p in paths:
        yield p
    if directory:
        d = directory
        if d.exists() and d.is_dir():
            for p in sorted(d.glob("*.bin")):
                yield p


def _is_gzip_header(b: bytes) -> bool:
    return len(b) >= 3 and b[0] == 0x1F and b[1] == 0x8B and b[2] == 0x08


def _parse_warc_headers(text: str) -> Dict[str, str]:
    # Parse until the first blank line.
    # WARC headers are CRLF separated; tolerate LF.
    hdr_block = text.split("\r\n\r\n", 1)[0]
    if hdr_block == text:
        hdr_block = text.split("\n\n", 1)[0]

    lines = [ln.strip("\r") for ln in hdr_block.splitlines() if ln.strip("\r")]
    out: Dict[str, str] = {}

    # First line is WARC version.
    if lines:
        out["_warc_version_line"] = lines[0]

    for ln in lines[1:]:
        if ":" not in ln:
            continue
        k, v = ln.split(":", 1)
        out[k.strip()] = v.strip()

    return out


def _validate_one(path: Path, max_decompressed_bytes: int) -> Dict[str, object]:
    res: Dict[str, object] = {
        "path": str(path),
        "ok": False,
        "size_bytes": None,
        "gzip_header": False,
        "decompressed_ok": False,
        "decompressed_bytes": None,
        "warc_header_ok": False,
        "warc": {},
        "error": None,
    }

    try:
        data = path.read_bytes()
        res["size_bytes"] = len(data)
        res["gzip_header"] = _is_gzip_header(data)

        # If it isn't gzip, still try to interpret as plain WARC text.
        if not res["gzip_header"]:
            try:
                text = data[: max_decompressed_bytes].decode("utf-8", errors="replace")
                if text.startswith("WARC/1."):
                    res["warc_header_ok"] = True
                    res["warc"] = _parse_warc_headers(text)
                    res["ok"] = True
                else:
                    res["error"] = "missing_gzip_header_and_no_plain_warc_header"
            except Exception as e:
                res["error"] = f"plain_decode_failed: {type(e).__name__}: {e}"
            return res

        # Gzip path.
        try:
            decompressed = gzip.decompress(data)
            res["decompressed_ok"] = True
            if len(decompressed) > max_decompressed_bytes:
                decompressed = decompressed[:max_decompressed_bytes]
            res["decompressed_bytes"] = len(decompressed)

            text = decompressed.decode("utf-8", errors="replace")
            if text.startswith("WARC/1."):
                res["warc_header_ok"] = True
                res["warc"] = _parse_warc_headers(text)
                res["ok"] = True
            else:
                res["error"] = "decompressed_but_missing_warc_header"
        except Exception as e:
            res["error"] = f"gzip_decompress_failed: {type(e).__name__}: {e}"

        return res

    except Exception as e:
        res["error"] = f"read_failed: {type(e).__name__}: {e}"
        return res


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate downloaded WARC record blobs (gzip member -> WARC header)")
    ap.add_argument("paths", nargs="*", type=Path, help="One or more .bin files")
    ap.add_argument("--dir", type=Path, default=None, help="Directory containing .bin blobs")
    ap.add_argument(
        "--max-decompressed-bytes",
        type=int,
        default=256 * 1024,
        help="Cap decompressed bytes examined per record (default: 256KB)",
    )

    args = ap.parse_args()

    files = list(_iter_files([p.expanduser().resolve() for p in args.paths], args.dir.expanduser().resolve() if args.dir else None))
    if not files:
        print("No input files", file=sys.stderr)
        return 2

    ok = 0
    n = 0
    for p in files:
        if not p.exists() or not p.is_file():
            continue
        n += 1
        rec = _validate_one(p, int(args.max_decompressed_bytes))
        if rec.get("ok"):
            ok += 1
        sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    sys.stderr.write(f"checked={n} ok={ok}\n")
    return 0 if ok == n else 1


if __name__ == "__main__":
    raise SystemExit(main())
