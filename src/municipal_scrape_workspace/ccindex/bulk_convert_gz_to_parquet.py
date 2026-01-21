#!/usr/bin/env python3
"""Bulk convert Common Crawl .gz indexes to Parquet.

Canonical implementation for:
  bulk_convert_gz_to_parquet.py (repo-root wrapper)

Converts all .gz files in a collection directory to parquet format in parallel.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import multiprocessing
import sys
import time
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


_REQUIRE_COLUMNS_IF_PRESENT = {"host_rev", "url", "ts"}


def _convert_one_worker(item: tuple[str, str, int]) -> tuple[bool, str]:
    gz_s, out_s, chunk_size = item
    ok = convert_gz_to_parquet(Path(gz_s), Path(out_s), chunk_size)
    return ok, Path(gz_s).name


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


def _parse_cdxj_line(line: str) -> Optional[tuple[str, str, Optional[str], dict]]:
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
        if not parts[2].startswith("{"):
            url = parts[2]
    if not url:
        url = meta.get("url") if isinstance(meta, dict) else None

    return surt, ts, url, meta


def _parquet_has_required_columns(parquet_path: Path) -> bool:
    try:
        pf = pq.ParquetFile(parquet_path)
        names = set(pf.schema_arrow.names)
        return _REQUIRE_COLUMNS_IF_PRESENT.issubset(names)
    except Exception:
        return False


def _empty_marker_path(parquet_path: Path) -> Path:
    """Sidecar marker indicating a shard was converted and confirmed empty."""

    return parquet_path.with_suffix(parquet_path.suffix + ".empty")


def _parquet_num_rows(parquet_path: Path) -> Optional[int]:
    try:
        pf = pq.ParquetFile(parquet_path)
        if pf.metadata is None:
            return None
        return int(pf.metadata.num_rows)
    except Exception:
        return None


def _parquet_is_effectively_empty(parquet_path: Path) -> bool:
    """Return True if parquet exists but contains no data rows.

    Treat unreadable/missing-metadata as NOT empty here (handled elsewhere).
    """

    n = _parquet_num_rows(parquet_path)
    return n == 0


def _coerce_int(value: object) -> Optional[int]:
    """Best-effort conversion of CC index numeric fields."""

    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
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
    """Convert a single .gz file to parquet."""

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
    empty_marker = _empty_marker_path(output_path)

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
        logger.info("Converting %s...", gz_path.name)

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

            if writer is None:
                if not buf["surt"]:
                    # Some CC index shards can be empty / contain no parsable lines.
                    # For resume + completeness, we still emit an empty Parquet file
                    # with the expected schema so downstream stages can proceed.
                    logger.warning("No valid rows in %s; writing empty parquet", gz_path.name)
                    writer = pq.ParquetWriter(
                        tmp_path,
                        schema,
                        compression="zstd",
                        compression_level=3,
                    )
                else:
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

        tmp_path.replace(output_path)
        # Track truly-empty shards so resume runs can treat them as complete.
        try:
            if total_rows == 0:
                empty_marker.write_text(f"empty source: {gz_path.name}\n", encoding="utf-8")
            else:
                if empty_marker.exists():
                    empty_marker.unlink()
        except Exception:
            pass
        logger.info("✓ Converted %s (%d rows)", gz_path.name, total_rows)
        return True
    except Exception as e:
        logger.error("Failed to convert %s: %s", gz_path.name, e)
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
    skip_existing: bool = True,
    heartbeat_seconds: int = 30,
) -> tuple[int, int]:
    """Convert all .gz files in a collection directory."""

    gz_files = sorted(input_dir.glob("cdx-*.gz"))
    if not gz_files:
        logger.warning("No .gz files found in %s", input_dir)
        return 0, 0

    logger.info("Found %d .gz files in %s", len(gz_files), input_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    work: list[tuple[Path, Path]] = []
    skipped = 0
    for gz_file in gz_files:
        output_file = output_dir / f"{gz_file.name}.parquet"
        sorted_output_file = output_dir / f"{gz_file.name}.sorted.parquet"
        empty_marker = _empty_marker_path(output_file)

        if skip_existing and sorted_output_file.exists():
            logger.info("Skipping existing sorted %s", sorted_output_file.name)
            skipped += 1
            continue

        if skip_existing and output_file.exists():
            # If the parquet is empty (0 rows), treat it as incomplete unless it has
            # an explicit empty marker from a prior confirmed conversion.
            try:
                if _parquet_is_effectively_empty(output_file):
                    if empty_marker.exists():
                        logger.info("Skipping confirmed-empty %s", output_file.name)
                        skipped += 1
                        continue
                    logger.warning(
                        "Reconverting %s (empty parquet without marker)",
                        output_file.name,
                    )
                elif _parquet_has_required_columns(output_file):
                    logger.info("Skipping existing %s", output_file.name)
                    skipped += 1
                    continue
                else:
                    logger.warning(
                        "Rebuilding %s (missing required columns: %s)",
                        output_file.name,
                        sorted(_REQUIRE_COLUMNS_IF_PRESENT),
                    )
            except Exception:
                # If we can't inspect the parquet, force reconvert.
                logger.warning("Rebuilding %s (unable to read parquet metadata)", output_file.name)

        work.append((gz_file, output_file))

    if not work:
        logger.info("All files already converted")
        return len(gz_files), len(gz_files)

    logger.info("Converting %d files with %d workers...", len(work), workers)

    success_count = 0
    fail_count = 0
    done = 0
    total_work = len(work)
    start = time.monotonic()
    last_heartbeat = start

    iterable = [(str(gz), str(out), 100000) for gz, out in work]
    with multiprocessing.Pool(workers) as pool:
        for ok, gz_name in pool.imap_unordered(_convert_one_worker, iterable, chunksize=1):
            done += 1
            if ok:
                success_count += 1
            else:
                fail_count += 1

            now = time.monotonic()
            if now - last_heartbeat >= max(1, int(heartbeat_seconds)):
                elapsed = now - start
                rate = done / elapsed if elapsed > 0 else 0.0
                remaining = total_work - done
                eta_s = (remaining / rate) if rate > 0 else 0.0
                logger.info(
                    "Heartbeat: %d/%d done (ok=%d, fail=%d), rate=%.2f files/s, eta=%.1f min (last=%s)",
                    done,
                    total_work,
                    success_count,
                    fail_count,
                    rate,
                    eta_s / 60.0,
                    gz_name,
                )
                last_heartbeat = now

    logger.info("Converted %d/%d files successfully", success_count, len(work))

    return len(gz_files), skipped + success_count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bulk convert CC .gz to parquet")
    parser.add_argument("--input-dir", type=Path, required=True, help="Input directory with .gz files")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for parquet files")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing parquet files")
    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=30,
        help="Print a periodic progress heartbeat every N seconds (default: 30)",
    )

    args = parser.parse_args(argv)

    if not args.input_dir.exists():
        logger.error("Input directory does not exist: %s", args.input_dir)
        return 1

    total, success = convert_collection(
        args.input_dir,
        args.output_dir,
        workers=args.workers,
        skip_existing=not args.overwrite,
        heartbeat_seconds=args.heartbeat_seconds,
    )

    logger.info("Final: %d/%d files converted", success, total)
    return 0 if success == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
