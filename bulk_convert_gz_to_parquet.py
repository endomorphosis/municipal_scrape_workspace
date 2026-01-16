#!/usr/bin/env python3
"""
Bulk convert Common Crawl .gz indexes to .gz.parquet format

Converts all .gz files in a collection directory to parquet format in parallel.
"""

import argparse
import gzip
import json
import logging
import multiprocessing
import re
import sys
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


_REQUIRE_COLUMNS_IF_PRESENT = {"host_rev", "url", "ts"}


def _extract_host(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = str(url).strip()
    i = u.find("://")
    if i == -1:
        return None
    start = i + 3
    end = u.find("/", start)
    host = u[start:] if end == -1 else u[start:end]
    host = host.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _host_to_rev(host: Optional[str]) -> Optional[str]:
    if not host:
        return None
    parts = [p for p in str(host).lower().split(".") if p]
    if not parts:
        return None
    return ",".join(reversed(parts))


def _parse_cdxj_line(line: str) -> Optional[tuple[str, Optional[str], Optional[str], dict]]:
    """Parse CC CDXJ: <surt> <ts> <json> OR <surt> <ts> <url> <json>."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    json_pos = line.find("{")
    meta: dict = {}
    pre = line
    if json_pos != -1:
        pre = line[:json_pos].strip()
        json_str = line[json_pos:].strip()
        try:
            meta = json.loads(json_str)
        except Exception:
            meta = {}

    parts = pre.split()
    if len(parts) < 2:
        return None

    surt = parts[0]
    ts = parts[1]

    url: Optional[str] = None
    if len(parts) >= 3:
        # If third token is not JSON, it's a URL.
        if not parts[2].startswith("{"):
            url = parts[2]
    if not url:
        url = meta.get("url") if isinstance(meta, dict) else None

    return surt, ts, url, meta


def _parquet_has_required_columns(parquet_path: Path) -> bool:
    """Best-effort schema check to avoid skipping old/incompatible parquet files."""
    try:
        pf = pq.ParquetFile(parquet_path)
        names = set(pf.schema_arrow.names)
        return _REQUIRE_COLUMNS_IF_PRESENT.issubset(names)
    except Exception:
        return False


def _coerce_int(value: object) -> Optional[int]:
    """Best-effort conversion of CC index numeric fields.

    Common Crawl CDX JSON often represents numeric fields as strings (e.g. "200").
    We normalize those to Python ints so Arrow can write typed parquet reliably.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        # Avoid treating booleans as ints for these fields.
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # Some parsers may yield floats; only accept integral floats.
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text == "-":
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def convert_gz_to_parquet(gz_path: Path, output_path: Path, chunk_size: int = 100000) -> bool:
    """Convert a single .gz file to parquet.

    Important: these shards can be 10M+ rows. We stream in chunks to avoid
    huge RAM spikes (which can look like "stalled" workers / low CPU due to
    swapping and allocator pressure).
    """
    schema = pa.schema(
        [
            ("surt", pa.string()),
            ("ts", pa.string()),
            ("url", pa.string()),
            ("host", pa.string()),
            ("host_rev", pa.string()),
            ("status", pa.int32()),
            ("mime", pa.string()),
            ("digest", pa.string()),
            ("warc_filename", pa.string()),
            ("warc_offset", pa.int64()),
            ("warc_length", pa.int64()),
        ]
    )

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")

    def _flush(writer: pq.ParquetWriter, buf: dict) -> int:
        if not buf["surt"]:
            return 0
        table = pa.table(buf, schema=schema)
        writer.write_table(table)
        n = len(buf["surt"])
        for k in buf.keys():
            buf[k].clear()
        return n

    try:
        logger.info(f"Converting {gz_path.name}...")

        # Build columns as python lists; Arrow will convert efficiently.
        buf = {
            "surt": [],
            "ts": [],
            "url": [],
            "host": [],
            "host_rev": [],
            "status": [],
            "mime": [],
            "digest": [],
            "warc_filename": [],
            "warc_offset": [],
            "warc_length": [],
        }

        total_rows = 0
        writer: Optional[pq.ParquetWriter] = None

        try:
            with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
                for line in f:
                    parsed = _parse_cdxj_line(line)
                    if not parsed:
                        continue

                    surt, ts, url, meta = parsed
                    host = _extract_host(url)
                    host_rev = _host_to_rev(host)

                    buf["surt"].append(surt)
                    buf["ts"].append(ts)
                    buf["url"].append(url)
                    buf["host"].append(host)
                    buf["host_rev"].append(host_rev)
                    buf["status"].append(_coerce_int(meta.get("status")))
                    buf["mime"].append(meta.get("mime"))
                    buf["digest"].append(meta.get("digest"))
                    buf["warc_filename"].append(meta.get("filename"))
                    buf["warc_offset"].append(_coerce_int(meta.get("offset")))
                    buf["warc_length"].append(_coerce_int(meta.get("length")))

                    if len(buf["surt"]) >= chunk_size:
                        if writer is None:
                            writer = pq.ParquetWriter(
                                tmp_path,
                                schema,
                                compression="zstd",
                                compression_level=3,
                            )
                        total_rows += _flush(writer, buf)

            # Final flush
            if writer is None:
                # No data at all
                if not buf["surt"]:
                    logger.warning(f"No valid rows in {gz_path.name}")
                    return False
                writer = pq.ParquetWriter(
                    tmp_path,
                    schema,
                    compression="zstd",
                    compression_level=3,
                )
            total_rows += _flush(writer, buf)
        finally:
            if writer is not None:
                writer.close()

        # Atomic-ish replace
        tmp_path.replace(output_path)
        logger.info(f"✓ Converted {gz_path.name} ({total_rows} rows)")
        return True
    except Exception as e:
        logger.error(f"Failed to convert {gz_path.name}: {e}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return False


def convert_collection(
    input_dir: Path,
    output_dir: Path,
    workers: int = 4,
    skip_existing: bool = True
) -> tuple[int, int]:
    """Convert all .gz files in a collection directory"""
    
    # Find all .gz files
    gz_files = sorted(input_dir.glob("cdx-*.gz"))
    if not gz_files:
        logger.warning(f"No .gz files found in {input_dir}")
        return 0, 0
    
    logger.info(f"Found {len(gz_files)} .gz files in {input_dir}")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare work list
    work = []
    skipped = 0
    for gz_file in gz_files:
        output_file = output_dir / f"{gz_file.name}.parquet"
        sorted_output_file = output_dir / f"{gz_file.name}.sorted.parquet"

        # Treat already-sorted outputs as "already converted" to avoid duplicating work.
        if skip_existing and sorted_output_file.exists():
            logger.info(f"Skipping existing sorted {sorted_output_file.name}")
            skipped += 1
            continue

        if skip_existing and output_file.exists():
            # Resume mode: if the existing parquet uses an older/incompatible schema,
            # schedule a rebuild so downstream sort/index steps don't fail.
            if _parquet_has_required_columns(output_file):
                logger.info(f"Skipping existing {output_file.name}")
                skipped += 1
                continue
            logger.warning(f"Rebuilding {output_file.name} (missing required columns: {sorted(_REQUIRE_COLUMNS_IF_PRESENT)})")

        work.append((gz_file, output_file))
    
    if not work:
        logger.info("All files already converted")
        return len(gz_files), len(gz_files)
    
    logger.info(f"Converting {len(work)} files with {workers} workers...")
    
    # Convert in parallel
    with multiprocessing.Pool(workers) as pool:
        results = pool.starmap(
            convert_gz_to_parquet,
            [(gz, out, 100000) for gz, out in work]
        )
    
    success_count = sum(1 for r in results if r)
    logger.info(f"Converted {success_count}/{len(work)} files successfully")

    # Count skipped files as success for resume mode.
    return len(gz_files), skipped + success_count


def main():
    parser = argparse.ArgumentParser(description="Bulk convert CC .gz to parquet")
    parser.add_argument("--input-dir", type=Path, required=True, help="Input directory with .gz files")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for parquet files")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing parquet files")
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        logger.error(f"Input directory does not exist: {args.input_dir}")
        return 1
    
    total, success = convert_collection(
        args.input_dir,
        args.output_dir,
        workers=args.workers,
        skip_existing=not args.overwrite
    )
    
    logger.info(f"Final: {success}/{total} files converted")
    return 0 if success == total else 1


if __name__ == "__main__":
    sys.exit(main())
