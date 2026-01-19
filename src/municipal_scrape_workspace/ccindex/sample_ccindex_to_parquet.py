#!/usr/bin/env python3
"""Sample Common Crawl local index shards -> compressed Parquet.

Purpose
- Estimate disk overhead of converting CC index .gz shards into Parquet.
- Extract only the fields needed to fetch WARC records (filename/offset/length)
  plus a few useful metadata columns.

This is intended for sampling (a handful of shard files), not a full 4+ TB build.

Example
  /home/barberb/municipal_scrape_workspace/.venv/bin/python sample_ccindex_to_parquet.py \
    --input-root /storage/ccindex \
    --max-files 4 \
    --out /storage/ccindex_parquet_sample/sample.parquet \
    --compression zstd
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class ParsedRow:
    collection: str
    shard_file: str
    surt: Optional[str]
    timestamp: Optional[str]
    url: Optional[str]
    status: Optional[int]
    mime: Optional[str]
    digest: Optional[str]
    warc_filename: Optional[str]
    warc_offset: Optional[int]
    warc_length: Optional[int]


def _iter_index_files(input_root: Path) -> Iterator[Path]:
    # Local layout is /storage/ccindex/<collection>/cdx-XXXXX.gz
    # but we recurse to be robust.
    yield from sorted(p for p in input_root.rglob("cdx-*.gz") if p.is_file())


def _guess_collection_from_path(p: Path) -> str:
    # Expect: .../<collection>/cdx-00000.gz
    try:
        return p.parent.name
    except Exception:
        return ""


def _parse_cdxj_line(line: str) -> Optional[ParsedRow]:
    # Typical CC CDXJ format:
    #   <surt> <timestamp> <url> <json>
    # Some variants omit url in the preamble; we handle best-effort.
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    json_pos = line.find("{")
    meta: Dict[str, Any] = {}
    pre = line
    if json_pos != -1:
        pre = line[:json_pos].strip()
        json_str = line[json_pos:].strip()
        try:
            meta = json.loads(json_str)
        except Exception:
            meta = {}

    parts = pre.split()
    surt = parts[0] if len(parts) >= 1 else None
    ts = parts[1] if len(parts) >= 2 else None
    url = parts[2] if len(parts) >= 3 else meta.get("url")

    def _to_int(x: Any) -> Optional[int]:
        if x is None:
            return None
        try:
            return int(x)
        except Exception:
            return None

    return ParsedRow(
        collection="",  # filled by caller
        shard_file="",  # filled by caller
        surt=surt,
        timestamp=ts,
        url=url,
        status=_to_int(meta.get("status")),
        mime=meta.get("mime"),
        digest=meta.get("digest"),
        warc_filename=meta.get("filename"),
        warc_offset=_to_int(meta.get("offset")),
        warc_length=_to_int(meta.get("length")),
    )


def _write_parquet(
    rows: List[ParsedRow],
    out_path: Path,
    compression: str,
    compression_level: Optional[int],
    writer: Optional[pq.ParquetWriter],
) -> pq.ParquetWriter:
    # Convert batch to Arrow and append.
    batch = {
        "collection": [r.collection for r in rows],
        "shard_file": [r.shard_file for r in rows],
        "surt": [r.surt for r in rows],
        "timestamp": [r.timestamp for r in rows],
        "url": [r.url for r in rows],
        "status": [r.status for r in rows],
        "mime": [r.mime for r in rows],
        "digest": [r.digest for r in rows],
        "warc_filename": [r.warc_filename for r in rows],
        "warc_offset": [r.warc_offset for r in rows],
        "warc_length": [r.warc_length for r in rows],
    }

    table = pa.Table.from_pydict(
        batch,
        schema=pa.schema(
            [
                ("collection", pa.string()),
                ("shard_file", pa.string()),
                ("surt", pa.string()),
                ("timestamp", pa.string()),
                ("url", pa.string()),
                ("status", pa.int32()),
                ("mime", pa.string()),
                ("digest", pa.string()),
                ("warc_filename", pa.string()),
                ("warc_offset", pa.int64()),
                ("warc_length", pa.int64()),
            ]
        ),
    )

    if writer is None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(
            out_path,
            table.schema,
            compression=compression,
            compression_level=compression_level,
            use_dictionary=True,
        )

    writer.write_table(table)
    return writer


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-root", required=True, type=str, help="Root folder of local CC index shards (e.g. /storage/ccindex)")
    ap.add_argument("--max-files", type=int, default=4, help="Max number of shard files to sample")
    ap.add_argument("--max-lines-per-file", type=int, default=250000, help="Cap lines read per shard file")
    ap.add_argument("--out", required=True, type=str, help="Output Parquet file path")
    ap.add_argument("--compression", type=str, default="zstd", choices=["zstd", "snappy", "gzip"], help="Parquet compression")
    ap.add_argument("--compression-level", type=int, default=None, help="Compression level (codec-dependent, optional)")
    ap.add_argument("--batch-rows", type=int, default=200000, help="Rows per Parquet write batch")
    args = ap.parse_args()

    input_root = Path(args.input_root)
    out_path = Path(args.out)

    files = list(_iter_index_files(input_root))
    if not files:
        raise SystemExit(f"No cdx-*.gz files found under {input_root}")

    selected = files[: max(1, int(args.max_files))]

    input_bytes = sum(p.stat().st_size for p in selected)
    print(f"Sampling {len(selected)} shard files")
    print(f"Input bytes (gz): {input_bytes:,}")
    print("Files:")
    for p in selected:
        print(f"  - {p}")

    rows: List[ParsedRow] = []
    total_rows = 0
    written_rows = 0
    writer: Optional[pq.ParquetWriter] = None

    for shard_path in selected:
        collection = _guess_collection_from_path(shard_path)
        shard_file = shard_path.name

        with gzip.open(shard_path, "rt", encoding="utf-8", errors="ignore") as f:
            for line_no, line in enumerate(f, 1):
                if line_no > int(args.max_lines_per_file):
                    break

                parsed = _parse_cdxj_line(line)
                if not parsed:
                    continue

                parsed.collection = collection
                parsed.shard_file = shard_file
                rows.append(parsed)
                total_rows += 1

                if len(rows) >= int(args.batch_rows):
                    writer = _write_parquet(
                        rows,
                        out_path=out_path,
                        compression=str(args.compression),
                        compression_level=args.compression_level,
                        writer=writer,
                    )
                    written_rows += len(rows)
                    rows.clear()

    if rows:
        writer = _write_parquet(
            rows,
            out_path=out_path,
            compression=str(args.compression),
            compression_level=args.compression_level,
            writer=writer,
        )
        written_rows += len(rows)
        rows.clear()

    if writer is not None:
        writer.close()

    out_bytes = out_path.stat().st_size if out_path.exists() else 0
    ratio = (out_bytes / input_bytes) if input_bytes else 0.0

    print("")
    print(f"Parsed rows: {total_rows:,}")
    print(f"Written rows: {written_rows:,}")
    print(f"Output bytes (parquet): {out_bytes:,}")
    print(f"Output/Input ratio: {ratio:.3f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
