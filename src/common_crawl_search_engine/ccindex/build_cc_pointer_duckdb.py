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

Optional: domain-only DuckDB index
- If --duckdb-index-mode domain is set, DuckDB stores only a compact mapping of
    domains (hosts) -> which shard/parquet file(s) contain URLs for that domain.
- In this mode, the full per-URL pointer rows are expected to live in Parquet,
    and DuckDB is used only as a fast directory/lookup layer.

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


CC_DOMAIN_SHARDS_SCHEMA = pa.schema(
    [
        ("source_path", pa.string()),
        ("collection", pa.string()),
        ("year", pa.int32()),
        ("shard_file", pa.string()),
        ("parquet_relpath", pa.string()),
        ("host", pa.string()),
        ("host_rev", pa.string()),
    ]
)


CC_PARQUET_ROWGROUPS_SCHEMA = pa.schema(
    [
        ("source_path", pa.string()),
        ("collection", pa.string()),
        ("year", pa.int32()),
        ("shard_file", pa.string()),
        ("parquet_relpath", pa.string()),
        ("row_group", pa.int32()),
        ("row_start", pa.int64()),
        ("row_end", pa.int64()),
        ("host_rev_min", pa.string()),
        ("host_rev_max", pa.string()),
    ]
)


_EXPECTED_POINTER_PARQUET_COLS = [f.name for f in CC_POINTERS_SCHEMA]


def _parquet_is_complete(path: Path, *, expected_cols: Optional[Sequence[str]] = None) -> bool:
    """Best-effort Parquet integrity check.

    Goal: distinguish a fully-written Parquet file from a truncated/partial one.

    This intentionally avoids reading row data; it checks the footer magic and
    ensures Parquet metadata is readable.
    """
    try:
        if not path.exists():
            return False
        st = path.stat()
        # Parquet footer + metadata requires at least a tiny file.
        if st.st_size < 12:
            return False

        # Parquet files end with the magic bytes PAR1.
        with path.open("rb") as f:
            f.seek(-4, os.SEEK_END)
            if f.read(4) != b"PAR1":
                return False

        pf = pq.ParquetFile(path)
        md = pf.metadata
        if md is None:
            return False
        if int(md.num_row_groups or 0) <= 0:
            return False
        if int(md.num_rows or 0) <= 0:
            return False

        if expected_cols:
            try:
                names = set(pf.schema_arrow.names)
            except Exception:
                names = set(pf.schema.names)
            for c in expected_cols:
                if c not in names:
                    return False

        return True
    except Exception:
        return False


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


_CDX_SHARD_RX = re.compile(r"^cdx-(\d{5})\.gz$")


def _cdx_shard_number(name: str) -> Optional[int]:
    m = _CDX_SHARD_RX.match(name or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


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


def _connect(db_path: Path, threads: int, *, memory_limit_gib: Optional[float] = None) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={max(1, int(threads))}")
    con.execute("PRAGMA enable_object_cache")
    if memory_limit_gib is not None:
        try:
            gib = float(memory_limit_gib)
            if gib > 0:
                con.execute(f"PRAGMA memory_limit='{gib:.3f}GB'")
        except Exception:
            # Best-effort; keep going.
            pass
    return con


def _init_schema(con: duckdb.DuckDBPyConnection, *, duckdb_index_mode: str) -> None:
    if str(duckdb_index_mode) == "url":
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
    elif str(duckdb_index_mode) == "domain":
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cc_domain_shards (
                source_path VARCHAR,
                collection VARCHAR,
                year INTEGER,
                shard_file VARCHAR,
                parquet_relpath VARCHAR,
                host VARCHAR,
                host_rev VARCHAR
            );
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cc_parquet_rowgroups (
                source_path VARCHAR,
                collection VARCHAR,
                year INTEGER,
                shard_file VARCHAR,
                parquet_relpath VARCHAR,
                row_group INTEGER,
                row_start BIGINT,
                row_end BIGINT,
                host_rev_min VARCHAR,
                host_rev_max VARCHAR
            );
            """
        )
    else:
        raise SystemExit(f"Unknown --duckdb-index-mode: {duckdb_index_mode}")

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
    *,
    duckdb_index_mode: str,
    insert_duckdb: bool = True,
) -> None:
    if insert_duckdb and str(duckdb_index_mode) == "url":
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
    *,
    duckdb_index_mode: str,
) -> Optional[pq.ParquetWriter]:
    if not rows:
        return parquet_writer

    tbl = _rows_to_arrow(rows)

    # 1) Insert into DuckDB (url mode only)
    if str(duckdb_index_mode) == "url":
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


def _sort_parquet_with_duckdb(
    src_parquet: Path,
    dst_parquet: Path,
    *,
    compression: str,
    compression_level: Optional[int],
) -> None:
    """Rewrite a Parquet file in sorted order (expensive)."""
    dst_parquet.parent.mkdir(parents=True, exist_ok=True)
    try:
        if dst_parquet.exists():
            dst_parquet.unlink()
    except Exception:
        pass

    con = duckdb.connect(database=":memory:")
    try:
        opts = ["FORMAT 'parquet'", f"COMPRESSION '{str(compression)}'"]
        if compression_level is not None:
            opts.append(f"COMPRESSION_LEVEL {int(compression_level)}")
        opt_sql = ", ".join(opts)
        # DuckDB parameter binding with COPY ... TO ? can be finicky across versions.
        # Keep the input parameterized, but embed the output path as a literal.
        out_sql = str(dst_parquet).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet(?)
                ORDER BY host_rev, url, ts
            )
            TO '{out_sql}' ({opt_sql});
            """,
            [str(src_parquet)],
        )
    finally:
        con.close()


def _decode_stat(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", errors="ignore")
        except Exception:
            return None
    try:
        return str(v)
    except Exception:
        return None


def _extract_parquet_rowgroup_host_rev_ranges(parquet_path: Path) -> List[Tuple[int, int, int, Optional[str], Optional[str]]]:
    """Return per-row-group (rg_idx, row_start, row_end, host_rev_min, host_rev_max)."""
    pf = pq.ParquetFile(parquet_path)
    md = pf.metadata
    if md is None:
        return []

    host_rev_col_idx: Optional[int] = None
    try:
        host_rev_col_idx = list(pf.schema_arrow.names).index("host_rev")
    except Exception:
        try:
            host_rev_col_idx = list(pf.schema.names).index("host_rev")
        except Exception:
            host_rev_col_idx = None

    out: List[Tuple[int, int, int, Optional[str], Optional[str]]] = []
    row_start = 0
    for rg_idx in range(int(md.num_row_groups or 0)):
        rg = md.row_group(rg_idx)
        n = int(rg.num_rows or 0)
        row_end = row_start + n

        mn: Optional[str] = None
        mx: Optional[str] = None
        if host_rev_col_idx is not None:
            try:
                col = rg.column(int(host_rev_col_idx))
                stats = getattr(col, "statistics", None)
                if stats is not None:
                    mn = _decode_stat(getattr(stats, "min", None))
                    mx = _decode_stat(getattr(stats, "max", None))
            except Exception:
                mn = None
                mx = None

        out.append((int(rg_idx), int(row_start), int(row_end), mn, mx))
        row_start = row_end

    return out


def _rebuild_cc_parquet_rowgroups_for_shard(
    con: duckdb.DuckDBPyConnection,
    *,
    source_path: str,
    collection: str,
    year: Optional[int],
    shard_file: str,
    parquet_relpath: Optional[str],
    parquet_path: Path,
) -> None:
    try:
        con.execute("DELETE FROM cc_parquet_rowgroups WHERE source_path = ?", [str(source_path)])
    except Exception:
        return

    rows = _extract_parquet_rowgroup_host_rev_ranges(parquet_path)
    if not rows:
        return

    tbl = pa.Table.from_pydict(
        {
            "source_path": [str(source_path)] * len(rows),
            "collection": [str(collection)] * len(rows),
            "year": [int(year) if year is not None else None] * len(rows),
            "shard_file": [str(shard_file)] * len(rows),
            "parquet_relpath": [str(parquet_relpath) if parquet_relpath else None] * len(rows),
            "row_group": [r[0] for r in rows],
            "row_start": [r[1] for r in rows],
            "row_end": [r[2] for r in rows],
            "host_rev_min": [r[3] for r in rows],
            "host_rev_max": [r[4] for r in rows],
        },
        schema=CC_PARQUET_ROWGROUPS_SCHEMA,
    )
    con.register("_cc_rowgroups", tbl)
    con.execute("INSERT INTO cc_parquet_rowgroups SELECT * FROM _cc_rowgroups")
    con.unregister("_cc_rowgroups")


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


def _maybe_create_domain_indexes(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in [
        "CREATE INDEX idx_cc_domain_shards_host_rev ON cc_domain_shards(host_rev)",
        "CREATE INDEX idx_cc_domain_shards_host ON cc_domain_shards(host)",
        "CREATE INDEX idx_cc_domain_shards_collection ON cc_domain_shards(collection)",
        "CREATE INDEX idx_cc_parquet_rowgroups_host_rev_min ON cc_parquet_rowgroups(host_rev_min)",
        "CREATE INDEX idx_cc_parquet_rowgroups_host_rev_max ON cc_parquet_rowgroups(host_rev_max)",
        "CREATE INDEX idx_cc_parquet_rowgroups_collection ON cc_parquet_rowgroups(collection)",
    ]:
        try:
            con.execute(stmt)
        except Exception:
            pass


def _parquet_relpath(parquet_root: Optional[Path], year: Optional[int], collection: str, shard_file: str) -> Optional[str]:
    if parquet_root is None or year is None:
        return None
    p = _parquet_out_path(parquet_root, int(year), collection, shard_file)
    try:
        return p.relative_to(parquet_root).as_posix()
    except Exception:
        return p.as_posix()


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
    ap.add_argument(
        "--memory-limit-gib",
        type=float,
        default=None,
        help="Optional DuckDB memory_limit per process in GiB (spills/limits memory; helps avoid OOM)",
    )
    ap.add_argument(
        "--cdx-shard-mod",
        type=int,
        default=None,
        help="If set, only ingest shard files where shard_number %% mod == rem (use with --cdx-shard-rem)",
    )
    ap.add_argument(
        "--cdx-shard-rem",
        type=int,
        default=None,
        help="Remainder for --cdx-shard-mod partitioning",
    )
    ap.add_argument("--create-indexes", action="store_true", default=False, help="Attempt to create helpful indexes")
    ap.add_argument(
        "--duckdb-index-mode",
        type=str,
        default="url",
        choices=["url", "domain"],
        help="What to store in DuckDB: 'url' stores per-URL pointers; 'domain' stores only domain->shard/parquet mapping",
    )
    ap.add_argument(
        "--domain-index-action",
        type=str,
        default="append",
        choices=["append", "rebuild"],
        help="Only used with --duckdb-index-mode domain. 'append' keeps existing rows; 'rebuild' clears and rebuilds cc_domain_shards",
    )
    ap.add_argument(
        "--parquet-out",
        type=str,
        default=None,
        help="Optional output directory to write Parquet pointer shards while ingesting (one Parquet per input shard)",
    )
    ap.add_argument(
        "--parquet-action",
        type=str,
        default="write",
        choices=["write", "skip-if-exists", "skip"],
        help=(
            "Whether to write Parquet shards. 'write' overwrites by atomic replace; "
            "'skip-if-exists' will not rewrite an existing non-empty Parquet shard; "
            "'skip' disables Parquet writing entirely"
        ),
    )
    ap.add_argument(
        "--parquet-validate",
        type=str,
        default="quick",
        choices=["quick", "none"],
        help=(
            "When deciding whether an existing Parquet shard can be skipped, "
            "validate it. 'quick' reads footer+metadata (detects truncation); "
            "'none' only checks file existence/size."
        ),
    )
    ap.add_argument(
        "--resume-require-parquet",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="When --parquet-out is set, only skip an already-ingested shard if its Parquet file exists (default: true when --parquet-out is set)",
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
        "--parquet-sort",
        type=str,
        default="none",
        choices=["none", "duckdb"],
        help=(
            "Optional: rewrite each Parquet shard in sorted order after writing. "
            "'duckdb' performs ORDER BY host_rev,url,ts via DuckDB (expensive)."
        ),
    )
    ap.add_argument(
        "--domain-range-index",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "When using --duckdb-index-mode domain, also store Parquet row-group range/offset metadata "
            "in cc_parquet_rowgroups (host_rev min/max + row start/end per row group)."
        ),
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

    if str(args.duckdb_index_mode) != "domain" and str(args.domain_index_action) != "append":
        raise SystemExit("--domain-index-action is only valid with --duckdb-index-mode domain")

    if bool(args.domain_range_index) and str(args.duckdb_index_mode) != "domain":
        raise SystemExit("--domain-range-index is only valid with --duckdb-index-mode domain")

    if bool(args.shard_by_year) and bool(args.shard_by_collection):
        raise SystemExit("Use only one of --shard-by-year or --shard-by-collection")

    if (args.cdx_shard_mod is None) != (args.cdx_shard_rem is None):
        raise SystemExit("Use --cdx-shard-mod together with --cdx-shard-rem")
    if args.cdx_shard_mod is not None:
        mod = int(args.cdx_shard_mod)
        rem = int(args.cdx_shard_rem)
        if mod <= 0:
            raise SystemExit("--cdx-shard-mod must be > 0")
        if rem < 0 or rem >= mod:
            raise SystemExit("--cdx-shard-rem must satisfy 0 <= rem < mod")

    input_root = Path(args.input_root)
    db_target = Path(args.db)
    rx = re.compile(args.collections_regex) if args.collections_regex else None

    # Connections keyed by shard key (or 'all' when not sharding).
    cons: Dict[str, duckdb.DuckDBPyConnection] = {}
    domain_tables_cleared: set[str] = set()

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
        part_suffix = ""
        if args.cdx_shard_mod is not None:
            part_suffix = f"__m{int(args.cdx_shard_mod)}r{int(args.cdx_shard_rem)}"
        if bool(args.shard_by_year):
            if not db_target.exists():
                db_target.mkdir(parents=True, exist_ok=True)
            if not db_target.is_dir():
                raise SystemExit("--db must be a directory when using --shard-by-year")
            if year is None:
                raise SystemExit("Could not determine collection year; use --collections-regex to filter")
            return db_target / f"cc_pointers_{int(year)}{part_suffix}.duckdb"
        if bool(args.shard_by_collection):
            if not db_target.exists():
                db_target.mkdir(parents=True, exist_ok=True)
            if not db_target.is_dir():
                raise SystemExit("--db must be a directory when using --shard-by-collection")
            if not collection:
                raise SystemExit("Could not determine collection name")
            return db_target / f"cc_pointers_{collection}{part_suffix}.duckdb"
        return db_target

    def _shard_key_for(collection: str, year: Optional[int]) -> str:
        part_suffix = ""
        if args.cdx_shard_mod is not None:
            part_suffix = f"__m{int(args.cdx_shard_mod)}r{int(args.cdx_shard_rem)}"
        if bool(args.shard_by_year):
            return (str(int(year)) if year is not None else "unknown") + part_suffix
        if bool(args.shard_by_collection):
            return (collection or "unknown") + part_suffix
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

        con = _connect(db_path, threads=int(args.threads), memory_limit_gib=args.memory_limit_gib)
        _init_schema(con, duckdb_index_mode=str(args.duckdb_index_mode))

        # Domain rebuild: clear cc_domain_shards once per DB file.
        if str(args.duckdb_index_mode) == "domain" and str(args.domain_index_action) == "rebuild":
            key = str(db_path)
            if key not in domain_tables_cleared:
                try:
                    con.execute("DELETE FROM cc_domain_shards")
                except Exception:
                    pass
                try:
                    con.execute("DELETE FROM cc_parquet_rowgroups")
                except Exception:
                    pass
                domain_tables_cleared.add(key)

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

    if args.cdx_shard_mod is not None:
        mod = int(args.cdx_shard_mod)
        rem = int(args.cdx_shard_rem)
        filtered: List[Path] = []
        for p in files:
            n = _cdx_shard_number(p.name)
            if n is None:
                continue
            if (n % mod) == rem:
                filtered.append(p)
        files = filtered

    if args.max_files is not None:
        files = files[: max(0, int(args.max_files))]

    if bool(args.shard_by_year) or bool(args.shard_by_collection):
        print(f"DB dir: {db_target}")
    else:
        print(f"DB: {db_target}")
    print(f"Files to consider: {len(files)}")

    processed_files = 0
    processed_rows = 0

    total_files_ingested = 0
    total_rows_ingested = 0

    parquet_root = Path(args.parquet_out).expanduser().resolve() if args.parquet_out else None
    resume_require_parquet = bool(parquet_root) if args.resume_require_parquet is None else bool(args.resume_require_parquet)
    parquet_action = str(args.parquet_action)
    parquet_validate = str(args.parquet_validate)

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

        shard_file = shard_path.name

        # Determine expected Parquet output (used for resume decisions).
        parquet_final: Optional[Path] = None
        parquet_tmp: Optional[Path] = None
        if parquet_root is not None:
            if year is None:
                raise SystemExit("Could not determine collection year for Parquet output")
            parquet_final = _parquet_out_path(parquet_root, int(year), collection, shard_file)
            parquet_tmp = parquet_final.with_suffix(parquet_final.suffix + ".tmp")

        already = _already_ingested(con, path_str, st.st_size, st.st_mtime_ns)
        parquet_ok = True
        if resume_require_parquet and parquet_final is not None:
            try:
                if parquet_validate == "quick":
                    parquet_ok = _parquet_is_complete(parquet_final, expected_cols=_EXPECTED_POINTER_PARQUET_COLS)
                else:
                    parquet_ok = parquet_final.exists() and parquet_final.stat().st_size > 0
            except Exception:
                parquet_ok = False

        # Decide whether we should write Parquet for this shard.
        parquet_exists_ok = False
        if parquet_final is not None:
            try:
                if parquet_validate == "quick":
                    parquet_exists_ok = _parquet_is_complete(parquet_final, expected_cols=_EXPECTED_POINTER_PARQUET_COLS)
                else:
                    parquet_exists_ok = parquet_final.exists() and parquet_final.stat().st_size > 0
            except Exception:
                parquet_exists_ok = False

        domain_rebuild = str(args.duckdb_index_mode) == "domain" and str(args.domain_index_action) == "rebuild"

        # If already ingested and (if enabled) Parquet exists, skip.
        # Exception: domain rebuild wants to recompute cc_domain_shards even for already-ingested shards.
        if already and (not resume_require_parquet or parquet_ok) and (not domain_rebuild):
            if (
                bool(args.domain_range_index)
                and str(args.duckdb_index_mode) == "domain"
                and parquet_final is not None
                and parquet_ok
                and parquet_final.exists()
            ):
                try:
                    rel = _parquet_relpath(parquet_root, year, collection, shard_file)
                    _rebuild_cc_parquet_rowgroups_for_shard(
                        con,
                        source_path=path_str,
                        collection=collection,
                        year=year,
                        shard_file=shard_file,
                        parquet_relpath=rel,
                        parquet_path=parquet_final,
                    )
                except Exception:
                    pass
            continue

        domain_rebuild_only = domain_rebuild and already and (not resume_require_parquet or parquet_ok)

        # In URL mode, if we are reprocessing only because the Parquet output is missing,
        # do NOT re-insert into DuckDB (would duplicate cc_pointers rows).
        parquet_rebuild_only = (
            str(args.duckdb_index_mode) == "url"
            and already
            and bool(resume_require_parquet)
            and parquet_final is not None
            and (not parquet_ok)
        )
        t0 = time.perf_counter()

        processed_files += 1

        # Domain-only DuckDB index: map host/host_rev -> shard/parquet
        # (keeps DuckDB compact and leaves full per-URL rows to Parquet).
        domain_map: Optional[Dict[str, str]] = None
        if str(args.duckdb_index_mode) == "domain":
            domain_map = {}

        # If writing Parquet, open a writer for this shard (tmp file then atomic rename).
        parquet_writer: Optional[pq.ParquetWriter] = None

        # In domain rebuild mode we may choose to only rebuild the domain table (no parquet rewrite).
        write_parquet = (parquet_root is not None) and (not domain_rebuild_only)
        if write_parquet:
            if parquet_action == "skip":
                write_parquet = False
            elif parquet_action == "skip-if-exists" and parquet_exists_ok:
                write_parquet = False
        write_duckdb_rows = (str(args.duckdb_index_mode) == "url") and (not domain_rebuild_only) and (not parquet_rebuild_only)

        if write_parquet and parquet_tmp is not None:
            # Ensure we don't keep a stale tmp from a prior interrupted run.
            try:
                if parquet_tmp.exists():
                    parquet_tmp.unlink()
            except Exception:
                pass
        cols = _new_columns() if (write_parquet or write_duckdb_rows) else {}
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

                    if domain_map is not None and host and host_rev:
                        # Keep one entry per host_rev (host value retained for display).
                        domain_map.setdefault(host_rev, host)

                    status = _to_int(meta.get("status")) if isinstance(meta, dict) else None
                    mime = (meta.get("mime") if isinstance(meta, dict) else None)
                    digest = (meta.get("digest") if isinstance(meta, dict) else None)
                    warc_filename = (meta.get("filename") if isinstance(meta, dict) else None)
                    warc_offset = _to_int(meta.get("offset")) if isinstance(meta, dict) else None
                    warc_length = _to_int(meta.get("length")) if isinstance(meta, dict) else None

                    if write_parquet or write_duckdb_rows:
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
                        if write_parquet and parquet_writer is None and parquet_tmp is not None:
                            parquet_writer = _open_parquet_writer(
                                parquet_tmp,
                                schema=CC_POINTERS_SCHEMA,
                                compression=str(args.parquet_compression),
                                compression_level=args.parquet_compression_level,
                            )
                        tbl = _columns_to_arrow(cols)
                        _insert_and_write_table(
                            con,
                            tbl,
                            parquet_writer,
                            duckdb_index_mode=str(args.duckdb_index_mode),
                            insert_duckdb=bool(write_duckdb_rows),
                        )
                        total_rows_ingested += pending
                        cols = _new_columns() if (write_parquet or write_duckdb_rows) else {}
                        pending = 0

            if pending:
                if write_parquet and parquet_writer is None and parquet_tmp is not None:
                    parquet_writer = _open_parquet_writer(
                        parquet_tmp,
                        schema=CC_POINTERS_SCHEMA,
                        compression=str(args.parquet_compression),
                        compression_level=args.parquet_compression_level,
                    )
                tbl = _columns_to_arrow(cols)
                _insert_and_write_table(
                    con,
                    tbl,
                    parquet_writer,
                    duckdb_index_mode=str(args.duckdb_index_mode),
                    insert_duckdb=bool(write_duckdb_rows),
                )
                total_rows_ingested += pending
                cols = _new_columns() if (write_parquet or write_duckdb_rows) else {}
                pending = 0

            if parquet_writer is not None:
                parquet_writer.close()
                parquet_writer = None
                if parquet_final is not None and parquet_tmp is not None:
                    parquet_final.parent.mkdir(parents=True, exist_ok=True)
                    parquet_tmp.replace(parquet_final)

                    if str(args.parquet_sort) == "duckdb":
                        sorted_tmp = parquet_final.with_suffix(parquet_final.suffix + ".sorted.tmp")
                        _sort_parquet_with_duckdb(
                            parquet_final,
                            sorted_tmp,
                            compression=str(args.parquet_compression),
                            compression_level=args.parquet_compression_level,
                        )
                        sorted_tmp.replace(parquet_final)

            # Domain-only index: store host -> shard/parquet mapping in DuckDB.
            if domain_map is not None:
                # Make re-ingests idempotent: remove any prior entries for this source path.
                try:
                    con.execute("DELETE FROM cc_domain_shards WHERE source_path = ?", [path_str])
                except Exception:
                    pass

                rel = _parquet_relpath(parquet_root, year, collection, shard_file)
                if domain_map:
                    hosts = list(domain_map.items())
                    dom_tbl = pa.Table.from_pydict(
                        {
                            "source_path": [path_str] * len(hosts),
                            "collection": [collection] * len(hosts),
                            "year": [int(year) if year is not None else None] * len(hosts),
                            "shard_file": [shard_file] * len(hosts),
                            "parquet_relpath": [rel] * len(hosts),
                            "host": [h for (_hr, h) in hosts],
                            "host_rev": [hr for (hr, _h) in hosts],
                        },
                        schema=CC_DOMAIN_SHARDS_SCHEMA,
                    )
                    con.register("_cc_domains", dom_tbl)
                    con.execute("INSERT INTO cc_domain_shards SELECT * FROM _cc_domains")
                    con.unregister("_cc_domains")

                if (
                    bool(args.domain_range_index)
                    and parquet_final is not None
                    and parquet_final.exists()
                    and _parquet_is_complete(parquet_final, expected_cols=_EXPECTED_POINTER_PARQUET_COLS)
                ):
                    _rebuild_cc_parquet_rowgroups_for_shard(
                        con,
                        source_path=path_str,
                        collection=collection,
                        year=year,
                        shard_file=shard_file,
                        parquet_relpath=rel,
                        parquet_path=parquet_final,
                    )

            if not domain_rebuild_only:
                _record_ingested(con, path_str, st.st_size, st.st_mtime_ns, file_rows)
                total_files_ingested += 1

                totals_by_shard[shard_key]["ingested_files"] = int(totals_by_shard[shard_key].get("ingested_files", 0)) + 1
                totals_by_shard[shard_key]["ingested_rows"] = int(totals_by_shard[shard_key].get("ingested_rows", 0)) + int(file_rows)
                totals_by_shard[shard_key]["last_event"] = "ingested"
                maybe_write_progress(shard_key)

            processed_rows += int(file_rows)

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
            if str(args.duckdb_index_mode) == "url":
                _maybe_create_indexes(con)
            else:
                _maybe_create_domain_indexes(con)

    for con in cons.values():
        con.close()

    print("")
    print(f"Processed shard files this run: {processed_files}")
    print(f"Processed rows this run:       {processed_rows:,}")
    print(f"New ingested files this run:  {total_files_ingested}")
    print(f"New ingested rows this run:   {total_rows_ingested:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
