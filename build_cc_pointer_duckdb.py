#!/usr/bin/env python3
"""Build an incremental DuckDB pointer index from local Common Crawl CDXJ shards.

Goal
- Create a fast, persistent lookup table for URL/domain -> WARC pointer fields.
- Scale to all collections over time (incremental/resumable).

This reads local shards like:
  /storage/ccindex/<collection>/cdx-00000.gz

and builds a DB with:
  cc_pointers(collection, shard_file, surt, timestamp, url, host, host_rev,
              status, mime, digest, warc_filename, warc_offset, warc_length)

It also tracks ingested shard files in:
  cc_ingested_files(path, size_bytes, mtime_ns, ingested_at, rows)

Example (small test)
  /home/barberb/municipal_scrape_workspace/.venv/bin/python build_cc_pointer_duckdb.py \
    --input-root /storage/ccindex \
    --db /storage/ccindex_duckdb/cc_pointers.duckdb \
    --collections-regex 'CC-MAIN-2025-.*' \
    --max-files 4
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover
    orjson = None


@dataclass
class ParsedRow:
    collection: str
    shard_file: str
    surt: Optional[str]
    timestamp: Optional[str]
    url: Optional[str]
    host: Optional[str]
    host_rev: Optional[str]
    status: Optional[int]
    mime: Optional[str]
    digest: Optional[str]
    warc_filename: Optional[str]
    warc_offset: Optional[int]
    warc_length: Optional[int]


CC_POINTERS_SCHEMA = pa.schema(
    [
        ("collection", pa.string()),
        ("shard_file", pa.string()),
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


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def _progress_path(progress_dir: Path, shard_key: str) -> Path:
    # Keep filenames safe.
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", shard_key or "all")
    return progress_dir / f"progress_{safe}.json"


def _update_progress_snapshot(
    *,
    progress_dir: Optional[Path],
    shard_key: str,
    started_at_epoch: float,
    totals_by_shard: Dict[str, Dict[str, Any]],
    note: Optional[str] = None,
) -> None:
    if progress_dir is None:
        return

    now = time.time()
    rec = totals_by_shard.get(shard_key) or {}
    elapsed = max(0.0, now - started_at_epoch)

    payload: Dict[str, Any] = {
        "shard_key": shard_key,
        "year": rec.get("year"),
        "collection": rec.get("collection"),
        "started_at": datetime.fromtimestamp(started_at_epoch, tz=timezone.utc).isoformat(),
        "updated_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "ingested_files": int(rec.get("ingested_files", 0)),
        "ingested_rows": int(rec.get("ingested_rows", 0)),
        "last_collection": rec.get("last_collection"),
        "last_shard_file": rec.get("last_shard_file"),
        "last_shard_path": rec.get("last_shard_path"),
        "last_event": rec.get("last_event"),
    }
    if note:
        payload["note"] = str(note)

    try:
        _atomic_write_json(_progress_path(progress_dir, shard_key), payload)
    except Exception:
        # Best-effort only; never fail the ingestion due to progress reporting.
        return


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iter_index_files(input_root: Path, collections: Optional[Sequence[str]] = None) -> Iterator[Path]:
    if collections:
        for c in collections:
            col_dir = input_root / str(c)
            if not col_dir.exists() or not col_dir.is_dir():
                continue
            yield from sorted(p for p in col_dir.glob("cdx-*.gz") if p.is_file())
        return

    yield from sorted(p for p in input_root.rglob("cdx-*.gz") if p.is_file())


def _guess_collection_from_path(p: Path) -> str:
    try:
        return p.parent.name
    except Exception:
        return ""


def _collection_year(collection: str) -> Optional[int]:
    m = re.match(r"^CC-MAIN-(\d{4})-\d+", collection or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _extract_host(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
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


def _host_to_rev(host: str) -> Optional[str]:
    if not host:
        return None
    parts = [p for p in host.lower().split(".") if p]
    if not parts:
        return None
    return ",".join(reversed(parts))


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _parse_cdxj_line(line: str) -> Optional[Tuple[str, Optional[str], Optional[str], Dict[str, Any]]]:
    """Return (surt, timestamp, url, meta)."""
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
            if orjson is not None:
                meta = orjson.loads(json_str)
            else:
                meta = json.loads(json_str)
        except Exception:
            meta = {}

    parts = pre.split()
    surt = parts[0] if len(parts) >= 1 else ""
    ts = parts[1] if len(parts) >= 2 else None

    # Many CC shards put URL in JSON; some include it in the preamble.
    url: Optional[str] = None
    if len(parts) >= 3:
        # CDXJ: <surt> <timestamp> <json> OR <surt> <timestamp> <url> <json>
        if parts[2].startswith("{"):
            url = meta.get("url") if isinstance(meta, dict) else None
        else:
            url = parts[2]
    else:
        url = meta.get("url") if isinstance(meta, dict) else None

    return surt, ts, url, meta


def _connect(db_path: Path, threads: int) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={max(1, int(threads))}")
    con.execute("PRAGMA enable_object_cache")
    return con


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cc_pointers (
            collection VARCHAR,
            shard_file VARCHAR,
            surt VARCHAR,
            ts VARCHAR,
            url VARCHAR,
            host VARCHAR,
            host_rev VARCHAR,
            status INTEGER,
            mime VARCHAR,
            digest VARCHAR,
            warc_filename VARCHAR,
            warc_offset BIGINT,
            warc_length BIGINT
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cc_ingested_files (
            path VARCHAR PRIMARY KEY,
            size_bytes BIGINT,
            mtime_ns BIGINT,
            ingested_at VARCHAR,
            rows BIGINT
        );
        """
    )


def _already_ingested(con: duckdb.DuckDBPyConnection, path: str, size_bytes: int, mtime_ns: int) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM cc_ingested_files
        WHERE path = ? AND size_bytes = ? AND mtime_ns = ?
        LIMIT 1
        """,
        [path, int(size_bytes), int(mtime_ns)],
    ).fetchone()
    return row is not None


def _record_ingested(con: duckdb.DuckDBPyConnection, path: str, size_bytes: int, mtime_ns: int, rows: int) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO cc_ingested_files(path, size_bytes, mtime_ns, ingested_at, rows)
        VALUES (?, ?, ?, ?, ?)
        """,
        [path, int(size_bytes), int(mtime_ns), _utc_now_iso(), int(rows)],
    )


def _rows_to_arrow(rows: Sequence[ParsedRow]) -> pa.Table:
    batch = {
        "collection": [r.collection for r in rows],
        "shard_file": [r.shard_file for r in rows],
        "surt": [r.surt for r in rows],
        "ts": [r.timestamp for r in rows],
        "url": [r.url for r in rows],
        "host": [r.host for r in rows],
        "host_rev": [r.host_rev for r in rows],
        "status": [r.status for r in rows],
        "mime": [r.mime for r in rows],
        "digest": [r.digest for r in rows],
        "warc_filename": [r.warc_filename for r in rows],
        "warc_offset": [r.warc_offset for r in rows],
        "warc_length": [r.warc_length for r in rows],
    }

    return pa.Table.from_pydict(batch, schema=CC_POINTERS_SCHEMA)


def _new_columns() -> Dict[str, List[Any]]:
    # Columnar buffer for one insert batch.
    return {
        "collection": [],
        "shard_file": [],
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


def _columns_to_arrow(cols: Dict[str, List[Any]]) -> pa.Table:
    return pa.Table.from_pydict(cols, schema=CC_POINTERS_SCHEMA)


def _insert_and_write_table(
    con: duckdb.DuckDBPyConnection,
    tbl: pa.Table,
    parquet_writer: Optional[pq.ParquetWriter],
) -> None:
    con.register("_cc_batch", tbl)
    con.execute("INSERT INTO cc_pointers SELECT * FROM _cc_batch")
    con.unregister("_cc_batch")
    if parquet_writer is not None:
        parquet_writer.write_table(tbl)


def _insert_and_write_batch(
    con: duckdb.DuckDBPyConnection,
    rows: Sequence[ParsedRow],
    parquet_writer: Optional[pq.ParquetWriter],
    parquet_compression: str,
    parquet_compression_level: Optional[int],
) -> Optional[pq.ParquetWriter]:
    if not rows:
        return parquet_writer

    tbl = _rows_to_arrow(rows)

    # 1) Insert into DuckDB
    con.register("_cc_batch", tbl)
    con.execute("INSERT INTO cc_pointers SELECT * FROM _cc_batch")
    con.unregister("_cc_batch")

    # 2) Append to Parquet (if enabled)
    if parquet_writer is not None:
        parquet_writer.write_table(tbl)
        return parquet_writer

    return None


def _parquet_out_path(parquet_root: Path, year: int, collection: str, shard_file: str) -> Path:
    # Layout: <root>/<year>/<collection>/<shard_file>.parquet
    return parquet_root / str(int(year)) / collection / f"{shard_file}.parquet"


def _open_parquet_writer(
    out_path_tmp: Path,
    schema: pa.Schema,
    compression: str,
    compression_level: Optional[int],
) -> pq.ParquetWriter:
    out_path_tmp.parent.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(
        out_path_tmp,
        schema,
        compression=str(compression),
        compression_level=compression_level,
        use_dictionary=True,
    )


def _maybe_create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    # Index support and behavior can vary by DuckDB version.
    # If index creation fails, we continue (the table still works; zonemaps help).
    for stmt in [
        "CREATE INDEX idx_cc_pointers_host_rev ON cc_pointers(host_rev)",
        "CREATE INDEX idx_cc_pointers_host ON cc_pointers(host)",
        "CREATE INDEX idx_cc_pointers_url ON cc_pointers(url)",
    ]:
        try:
            con.execute(stmt)
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-root", required=True, type=str, help="Root folder containing CC shards (e.g. /storage/ccindex)")
    ap.add_argument(
        "--db",
        required=True,
        type=str,
        help="Output DuckDB file, or an output directory when using --shard-by-year",
    )
    ap.add_argument(
        "--shard-by-year",
        action="store_true",
        default=False,
        help="Write one DuckDB per year (CC-MAIN-YYYY-*), named cc_pointers_YYYY.duckdb under --db dir",
    )
    ap.add_argument(
        "--shard-by-collection",
        action="store_true",
        default=False,
        help="Write one DuckDB per collection (CC-MAIN-YYYY-WW), named cc_pointers_<collection>.duckdb under --db dir",
    )
    ap.add_argument(
        "--collections",
        action="append",
        default=None,
        help="Ingest only these collections (repeatable). Avoids scanning all of --input-root.",
    )
    ap.add_argument("--collections-regex", type=str, default=None, help="Only ingest collections matching this regex")
    ap.add_argument("--max-files", type=int, default=None, help="Cap number of shard files ingested (for testing)")
    ap.add_argument("--max-lines-per-file", type=int, default=None, help="Cap lines read per shard file (for testing)")
    ap.add_argument("--batch-rows", type=int, default=200_000, help="Rows per insert batch")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 8, help="DuckDB threads")
    ap.add_argument("--create-indexes", action="store_true", default=False, help="Attempt to create helpful indexes")
    ap.add_argument(
        "--parquet-out",
        type=str,
        default=None,
        help="Optional output directory to write Parquet pointer shards while ingesting (one Parquet per input shard)",
    )
    ap.add_argument(
        "--parquet-compression",
        type=str,
        default="zstd",
        choices=["zstd", "snappy", "gzip"],
        help="Parquet compression codec",
    )
    ap.add_argument(
        "--parquet-compression-level",
        type=int,
        default=None,
        help="Parquet compression level (codec-dependent)",
    )
    ap.add_argument(
        "--progress-dir",
        type=str,
        default=None,
        help="Optional directory to write progress_YYYY.json snapshots (defaults to DB directory)",
    )
    ap.add_argument(
        "--progress-interval-seconds",
        type=int,
        default=30,
        help="Minimum seconds between snapshot updates per shard",
    )
    args = ap.parse_args()

    if bool(args.shard_by_year) and bool(args.shard_by_collection):
        raise SystemExit("Use only one of --shard-by-year or --shard-by-collection")

    input_root = Path(args.input_root)
    db_target = Path(args.db)
    rx = re.compile(args.collections_regex) if args.collections_regex else None

    # Connections keyed by shard key (or 'all' when not sharding).
    cons: Dict[str, duckdb.DuckDBPyConnection] = {}

    started_at_epoch = time.time()
    last_progress_write: Dict[str, float] = {}
    totals_by_shard: Dict[str, Dict[str, Any]] = {}

    if args.progress_dir:
        progress_dir = Path(args.progress_dir).expanduser().resolve()
    else:
        # Default: next to the DB outputs.
        progress_dir = (
            db_target.expanduser().resolve()
            if bool(args.shard_by_year) or bool(args.shard_by_collection)
            else db_target.expanduser().resolve().parent
        )

    def maybe_write_progress(shard_key: str, *, force: bool = False, note: Optional[str] = None) -> None:
        if progress_dir is None:
            return
        now = time.time()
        prev = last_progress_write.get(shard_key, 0.0)
        interval = max(1, int(args.progress_interval_seconds))
        if (not force) and (now - prev) < float(interval):
            return
        last_progress_write[shard_key] = now
        _update_progress_snapshot(
            progress_dir=progress_dir,
            shard_key=shard_key,
            started_at_epoch=started_at_epoch,
            totals_by_shard=totals_by_shard,
            note=note,
        )

    def _db_path_for(collection: str, year: Optional[int]) -> Path:
        if bool(args.shard_by_year):
            if not db_target.exists():
                db_target.mkdir(parents=True, exist_ok=True)
            if not db_target.is_dir():
                raise SystemExit("--db must be a directory when using --shard-by-year")
            if year is None:
                raise SystemExit("Could not determine collection year; use --collections-regex to filter")
            return db_target / f"cc_pointers_{int(year)}.duckdb"
        if bool(args.shard_by_collection):
            if not db_target.exists():
                db_target.mkdir(parents=True, exist_ok=True)
            if not db_target.is_dir():
                raise SystemExit("--db must be a directory when using --shard-by-collection")
            if not collection:
                raise SystemExit("Could not determine collection name")
            return db_target / f"cc_pointers_{collection}.duckdb"
        return db_target

    def _shard_key_for(collection: str, year: Optional[int]) -> str:
        if bool(args.shard_by_year):
            return str(int(year)) if year is not None else "unknown"
        if bool(args.shard_by_collection):
            return collection or "unknown"
        return "all"

    def get_con(collection: str, year: Optional[int]) -> duckdb.DuckDBPyConnection:
        shard_key = _shard_key_for(collection, year)
        if shard_key in cons:
            return cons[shard_key]

        db_path = _db_path_for(collection, year)

        # Ensure counters exist before connect (so seeding can store into the right shard key).
        totals_by_shard.setdefault(shard_key, {"ingested_files": 0, "ingested_rows": 0})
        totals_by_shard[shard_key]["year"] = int(year) if year is not None else None
        totals_by_shard[shard_key]["collection"] = collection or None

        con = _connect(db_path, threads=int(args.threads))
        _init_schema(con)

        # Seed progress counters from existing ingested metadata so snapshots are cumulative across restarts.
        try:
            row_files = con.execute("SELECT count(*) FROM cc_ingested_files").fetchone()
            ing_files = (row_files[0] if row_files and row_files[0] is not None else 0) or 0
            row_rows = con.execute("SELECT COALESCE(sum(rows), 0) FROM cc_ingested_files").fetchone()
            ing_rows = (row_rows[0] if row_rows and row_rows[0] is not None else 0) or 0
            totals_by_shard[shard_key]["ingested_files"] = int(ing_files)
            totals_by_shard[shard_key]["ingested_rows"] = int(ing_rows)
        except Exception:
            pass

        cons[shard_key] = con
        return con

    collections_list: Optional[List[str]] = None
    if args.collections:
        collections_list = [c for c in args.collections if c]

    files = list(_iter_index_files(input_root, collections=collections_list))
    if rx:
        files = [p for p in files if rx.search(_guess_collection_from_path(p) or "")]

    if args.max_files is not None:
        files = files[: max(0, int(args.max_files))]

    if bool(args.shard_by_year) or bool(args.shard_by_collection):
        print(f"DB dir: {db_target}")
    else:
        print(f"DB: {db_target}")
    print(f"Files to consider: {len(files)}")

    total_files_ingested = 0
    total_rows_ingested = 0

    parquet_root = Path(args.parquet_out).expanduser().resolve() if args.parquet_out else None

    for idx, shard_path in enumerate(files, 1):
        st = shard_path.stat()
        path_str = shard_path.as_posix()
        collection = _guess_collection_from_path(shard_path)
        year = _collection_year(collection)
        con = get_con(collection, year)

        shard_key = _shard_key_for(collection, year)
        totals_by_shard.setdefault(shard_key, {"ingested_files": 0, "ingested_rows": 0})
        totals_by_shard[shard_key]["year"] = int(year) if year is not None else None
        totals_by_shard[shard_key]["collection"] = collection or None
        totals_by_shard[shard_key]["last_collection"] = collection
        totals_by_shard[shard_key]["last_shard_file"] = shard_path.name
        totals_by_shard[shard_key]["last_shard_path"] = shard_path.as_posix()
        totals_by_shard[shard_key]["last_event"] = "considering"
        maybe_write_progress(shard_key)

        if _already_ingested(con, path_str, st.st_size, st.st_mtime_ns):
            continue
        shard_file = shard_path.name
        t0 = time.perf_counter()

        # If writing Parquet, open a writer for this shard (tmp file then atomic rename).
        parquet_writer: Optional[pq.ParquetWriter] = None
        parquet_final: Optional[Path] = None
        parquet_tmp: Optional[Path] = None
        if parquet_root is not None:
            if year is None:
                raise SystemExit("Could not determine collection year for Parquet output")
            parquet_final = _parquet_out_path(parquet_root, year, collection, shard_file)
            parquet_tmp = parquet_final.with_suffix(parquet_final.suffix + ".tmp")
            # Ensure we don't keep a stale tmp from a prior interrupted run.
            try:
                if parquet_tmp.exists():
                    parquet_tmp.unlink()
            except Exception:
                pass

        cols = _new_columns()
        pending = 0
        file_rows = 0

        try:
            with gzip.open(shard_path, "rt", encoding="utf-8", errors="ignore") as f:
                for line_no, line in enumerate(f, 1):
                    if args.max_lines_per_file is not None and line_no > int(args.max_lines_per_file):
                        break

                    parsed = _parse_cdxj_line(line)
                    if not parsed:
                        continue

                    surt, ts, url, meta = parsed
                    if not url:
                        continue

                    host = _extract_host(url) or None
                    host_rev = _host_to_rev(host) if host else None

                    status = _to_int(meta.get("status")) if isinstance(meta, dict) else None
                    mime = (meta.get("mime") if isinstance(meta, dict) else None)
                    digest = (meta.get("digest") if isinstance(meta, dict) else None)
                    warc_filename = (meta.get("filename") if isinstance(meta, dict) else None)
                    warc_offset = _to_int(meta.get("offset")) if isinstance(meta, dict) else None
                    warc_length = _to_int(meta.get("length")) if isinstance(meta, dict) else None

                    cols["collection"].append(collection)
                    cols["shard_file"].append(shard_file)
                    cols["surt"].append(surt or None)
                    cols["ts"].append(ts)
                    cols["url"].append(url)
                    cols["host"].append(host)
                    cols["host_rev"].append(host_rev)
                    cols["status"].append(status)
                    cols["mime"].append(mime)
                    cols["digest"].append(digest)
                    cols["warc_filename"].append(warc_filename)
                    cols["warc_offset"].append(warc_offset)
                    cols["warc_length"].append(warc_length)
                    pending += 1
                    file_rows += 1

                    if pending >= int(args.batch_rows):
                        if parquet_root is not None and parquet_writer is None and parquet_tmp is not None:
                            parquet_writer = _open_parquet_writer(
                                parquet_tmp,
                                schema=CC_POINTERS_SCHEMA,
                                compression=str(args.parquet_compression),
                                compression_level=args.parquet_compression_level,
                            )
                        tbl = _columns_to_arrow(cols)
                        _insert_and_write_table(con, tbl, parquet_writer)
                        total_rows_ingested += pending
                        cols = _new_columns()
                        pending = 0

            if pending:
                if parquet_root is not None and parquet_writer is None and parquet_tmp is not None:
                    parquet_writer = _open_parquet_writer(
                        parquet_tmp,
                        schema=CC_POINTERS_SCHEMA,
                        compression=str(args.parquet_compression),
                        compression_level=args.parquet_compression_level,
                    )
                tbl = _columns_to_arrow(cols)
                _insert_and_write_table(con, tbl, parquet_writer)
                total_rows_ingested += pending
                cols = _new_columns()
                pending = 0

            if parquet_writer is not None:
                parquet_writer.close()
                parquet_writer = None
                if parquet_final is not None and parquet_tmp is not None:
                    parquet_final.parent.mkdir(parents=True, exist_ok=True)
                    parquet_tmp.replace(parquet_final)

            _record_ingested(con, path_str, st.st_size, st.st_mtime_ns, file_rows)
            total_files_ingested += 1

            totals_by_shard[shard_key]["ingested_files"] = int(totals_by_shard[shard_key].get("ingested_files", 0)) + 1
            totals_by_shard[shard_key]["ingested_rows"] = int(totals_by_shard[shard_key].get("ingested_rows", 0)) + int(file_rows)
            totals_by_shard[shard_key]["last_event"] = "ingested"
            maybe_write_progress(shard_key)

            dt = time.perf_counter() - t0
            if bool(args.shard_by_year):
                print(f"[{idx}/{len(files)}] ingested {collection}/{shard_file} -> {year}: rows={file_rows:,} in {dt:.1f}s")
            else:
                print(f"[{idx}/{len(files)}] ingested {collection}/{shard_file}: rows={file_rows:,} in {dt:.1f}s")

        except KeyboardInterrupt:
            print(f"[{idx}/{len(files)}] INTERRUPTED {collection}/{shard_file}: cleaning up")
            try:
                if parquet_writer is not None:
                    parquet_writer.close()
            except Exception:
                pass
            try:
                if parquet_tmp is not None and parquet_tmp.exists():
                    parquet_tmp.unlink()
            except Exception:
                pass
            # Do not record as ingested; exit the loop so we close DBs cleanly.
            totals_by_shard[shard_key]["last_event"] = "interrupted"
            maybe_write_progress(shard_key, force=True, note="interrupted")
            break
        except Exception as e:
            print(f"[{idx}/{len(files)}] ERROR {collection}/{shard_file}: {type(e).__name__}: {e}")
            try:
                if parquet_writer is not None:
                    parquet_writer.close()
            except Exception:
                pass
            try:
                if parquet_tmp is not None and parquet_tmp.exists():
                    parquet_tmp.unlink()
            except Exception:
                pass
            # Do not record as ingested.
            totals_by_shard[shard_key]["last_event"] = "error"
            maybe_write_progress(shard_key, force=True, note=f"{type(e).__name__}: {e}")
            continue

    if args.create_indexes:
        for con in cons.values():
            _maybe_create_indexes(con)

    for con in cons.values():
        con.close()

    print("")
    print(f"Ingested files this run: {total_files_ingested}")
    print(f"Ingested rows this run:  {total_rows_ingested:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
