#!/usr/bin/env python3
"""Monitor a running Common Crawl pointer-index build.

This is meant to be used while build_cc_pointer_duckdb.py is running and holding
a DuckDB write lock. It relies on the builder's progress_*.json snapshots.

Typical usage
  /home/barberb/municipal_scrape_workspace/.venv/bin/python monitor_cc_pointer_build.py \
    --db-dir /storage/ccindex_duckdb/cc_pointers_by_year \
    --input-root /storage/ccindex \
    --collections-regex 'CC-MAIN-202[4-5]-.*'

Optional (process health):
  --pid-file /storage/ccindex_duckdb/cc_pointers_by_year/build_2024_2025.pid
  --log-file /storage/ccindex_duckdb/cc_pointers_by_year/build_2024_2025.log

Notes
- Uses only stdlib.
- Prints one block per interval; use `watch -n 5 ... --once` if preferred.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class Snapshot:
    shard_key: Optional[str]
    year: Optional[int]
    path: Path
    started_at: Optional[str]
    updated_at: Optional[str]
    elapsed_seconds: Optional[float]
    ingested_files: int
    ingested_rows: int
    last_collection: Optional[str]
    last_shard_file: Optional[str]
    last_shard_path: Optional[str]
    last_event: Optional[str]


def _fmt_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.2f} {u}"
        f /= 1024.0
    return f"{f:.2f} TiB"


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _read_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _read_snapshot(path: Path) -> Optional[Snapshot]:
    data = _read_json(path)
    if not isinstance(data, dict):
        return None

    shard_key = data.get("shard_key")
    year = data.get("year")
    try:
        year_int = int(year) if year is not None else None
    except Exception:
        year_int = None

    def _maybe_float(v: object) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)  # type: ignore[arg-type]
        except Exception:
            return None

    return Snapshot(
        shard_key=str(shard_key) if shard_key else None,
        year=year_int,
        path=path,
        started_at=str(data.get("started_at")) if data.get("started_at") else None,
        updated_at=str(data.get("updated_at")) if data.get("updated_at") else None,
        elapsed_seconds=_maybe_float(data.get("elapsed_seconds")),
        ingested_files=int(data.get("ingested_files", 0) or 0),
        ingested_rows=int(data.get("ingested_rows", 0) or 0),
        last_collection=str(data.get("last_collection")) if data.get("last_collection") else None,
        last_shard_file=str(data.get("last_shard_file")) if data.get("last_shard_file") else None,
        last_shard_path=str(data.get("last_shard_path")) if data.get("last_shard_path") else None,
        last_event=str(data.get("last_event")) if data.get("last_event") else None,
    )


def _iter_progress_files(db_dir: Path) -> List[Path]:
    # progress_*.json (ignore progress_all.json for now)
    return sorted(p for p in db_dir.glob("progress_*.json") if p.is_file() and p.name != "progress_all.json")


def _db_path_for_snapshot(db_dir: Path, s: Snapshot) -> Path:
    # Year-sharded: cc_pointers_2024.duckdb
    # Collection-sharded: cc_pointers_CC-MAIN-2024-10.duckdb
    if s.shard_key:
        if re.fullmatch(r"\d{4}", s.shard_key):
            return db_dir / f"cc_pointers_{s.shard_key}.duckdb"
        return db_dir / f"cc_pointers_{s.shard_key}.duckdb"
    if s.year is not None:
        return db_dir / f"cc_pointers_{int(s.year)}.duckdb"
    return db_dir / "cc_pointers.duckdb"


def _iter_collections(input_root: Path) -> Iterable[Path]:
    for entry in sorted(input_root.iterdir()):
        if entry.is_dir():
            yield entry


def _count_expected_shards_by_year(input_root: Path, collections_regex: Optional[str]) -> Dict[int, int]:
    rx = re.compile(collections_regex) if collections_regex else None

    counts: Dict[int, int] = {}
    for col_dir in _iter_collections(input_root):
        collection = col_dir.name
        if rx and not rx.search(collection):
            continue

        m = re.match(r"^CC-MAIN-(\d{4})-\d+", collection)
        if not m:
            continue
        year = int(m.group(1))

        n = 0
        try:
            for f in col_dir.iterdir():
                if f.is_file() and f.name.startswith("cdx-") and f.name.endswith(".gz"):
                    n += 1
        except Exception:
            continue

        counts[year] = counts.get(year, 0) + n

    return counts


def _count_expected_shards_by_collection(input_root: Path, collections_regex: Optional[str]) -> Dict[str, int]:
    rx = re.compile(collections_regex) if collections_regex else None

    counts: Dict[str, int] = {}
    for col_dir in _iter_collections(input_root):
        collection = col_dir.name
        if rx and not rx.search(collection):
            continue

        n = 0
        try:
            for f in col_dir.iterdir():
                if f.is_file() and f.name.startswith("cdx-") and f.name.endswith(".gz"):
                    n += 1
        except Exception:
            continue

        counts[collection] = n

    return counts


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_pid(pid_file: Path) -> Optional[int]:
    try:
        s = pid_file.read_text(encoding="utf-8").strip()
        return int(s)
    except Exception:
        return None


def _tail_last_line(path: Path) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end == 0:
                return None
            # read last ~64KiB
            size = min(65536, end)
            f.seek(end - size)
            chunk = f.read(size)
        lines = chunk.splitlines()
        if not lines:
            return None
        return lines[-1].decode("utf-8", errors="ignore")
    except Exception:
        return None


def _fmt_pct(n: int, d: int) -> str:
    if d <= 0:
        return "-"
    return f"{(100.0 * float(n) / float(d)):.1f}%"


def _render_once(
    *,
    db_dir: Path,
    input_root: Optional[Path],
    collections_regex: Optional[str],
    pid_file: Optional[Path],
    log_file: Optional[Path],
) -> int:
    now = datetime.now(timezone.utc).isoformat()

    print(f"time_utc\t{now}")
    print(f"db_dir\t{db_dir}")

    # Process health
    if pid_file is not None:
        pid = _read_pid(pid_file)
        if pid is None:
            print(f"pid\t-\t(pid file unreadable: {pid_file})")
        else:
            alive = _pid_alive(pid)
            print(f"pid\t{pid}\t{'alive' if alive else 'DEAD'}")

    if log_file is not None:
        last = _tail_last_line(log_file)
        if last:
            print(f"log_last\t{last}")

    print("")

    expected_by_year: Dict[int, int] = {}
    expected_by_collection: Dict[str, int] = {}
    if input_root is not None:
        expected_by_year = _count_expected_shards_by_year(input_root, collections_regex)
        expected_by_collection = _count_expected_shards_by_collection(input_root, collections_regex)
        print(f"input_root\t{input_root}")
        if collections_regex:
            print(f"collections_regex\t{collections_regex}")
        print("")

    snaps: List[Snapshot] = []
    for p in _iter_progress_files(db_dir):
        s = _read_snapshot(p)
        if s is not None:
            snaps.append(s)

    if not snaps:
        print("No progress_*.json snapshots found.")
        return 2

    header = ["shard", "year", "db_size", "files", "expected", "pct", "rows", "rows_per_sec", "updated_at", "last"]
    print("\t".join(header))

    total_rows = 0
    total_files = 0
    total_expected = 0
    total_db_bytes = 0

    for s in sorted(snaps, key=lambda x: ((x.year or 0), (x.shard_key or ""))):
        year = s.year
        shard_label = s.shard_key or (str(year) if year is not None else "-")
        db_path = _db_path_for_snapshot(db_dir, s)
        db_bytes = db_path.stat().st_size if db_path.exists() else 0
        total_db_bytes += int(db_bytes)

        exp = None
        if expected_by_collection and s.shard_key and not re.fullmatch(r"\d{4}", s.shard_key):
            exp = expected_by_collection.get(s.shard_key)
        if exp is None and expected_by_year and year is not None:
            exp = expected_by_year.get(int(year))
        if exp is not None:
            total_expected += int(exp)

        total_files += int(s.ingested_files)
        total_rows += int(s.ingested_rows)

        started = _parse_iso(s.started_at)
        updated = _parse_iso(s.updated_at)
        rows_per_sec = "-"
        if started and updated:
            dt = max(1.0, (updated - started).total_seconds())
            rows_per_sec = f"{(float(s.ingested_rows) / dt):.0f}"

        last = "/".join(x for x in [s.last_collection, s.last_shard_file, s.last_event] if x)

        row = [
            shard_label,
            str(year) if year is not None else "-",
            _fmt_bytes(db_bytes),
            str(s.ingested_files),
            str(exp) if exp is not None else "-",
            _fmt_pct(s.ingested_files, exp) if exp is not None else "-",
            f"{s.ingested_rows:,}",
            rows_per_sec,
            s.updated_at or "-",
            last or "-",
        ]
        print("\t".join(row))

    print("")
    if total_expected > 0:
        print(
            f"TOTAL\tdb={_fmt_bytes(total_db_bytes)}\tfiles={total_files}/{total_expected} ({_fmt_pct(total_files, total_expected)})\trows={total_rows:,}"
        )
    else:
        print(f"TOTAL\tdb={_fmt_bytes(total_db_bytes)}\tfiles={total_files}\trows={total_rows:,}")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-dir", required=True, type=str, help="Directory containing cc_pointers_*.duckdb + progress_*.json")
    ap.add_argument("--input-root", type=str, default=None, help="Optional CC index root to compute expected shard counts")
    ap.add_argument("--collections-regex", type=str, default=None, help="Regex filter for collections when counting expected shards")
    ap.add_argument("--pid-file", type=str, default=None, help="Optional PID file to check process health")
    ap.add_argument("--log-file", type=str, default=None, help="Optional build log file to show last line")
    ap.add_argument("--interval", type=int, default=30, help="Seconds between updates")
    ap.add_argument("--once", action="store_true", default=False, help="Print once and exit")
    args = ap.parse_args()

    db_dir = Path(args.db_dir).expanduser().resolve()
    if not db_dir.exists() or not db_dir.is_dir():
        raise SystemExit(f"Not a directory: {db_dir}")

    input_root = Path(args.input_root).expanduser().resolve() if args.input_root else None
    pid_file = Path(args.pid_file).expanduser().resolve() if args.pid_file else None
    log_file = Path(args.log_file).expanduser().resolve() if args.log_file else None

    interval = max(1, int(args.interval))

    if args.once:
        return _render_once(
            db_dir=db_dir,
            input_root=input_root,
            collections_regex=args.collections_regex,
            pid_file=pid_file,
            log_file=log_file,
        )

    # Loop
    while True:
        try:
            rc = _render_once(
                db_dir=db_dir,
                input_root=input_root,
                collections_regex=args.collections_regex,
                pid_file=pid_file,
                log_file=log_file,
            )
            # If something is badly wrong (no snapshots), still keep looping.
            _ = rc
            sys.stdout.flush()
            time.sleep(interval)
            print("\n" + ("-" * 100) + "\n")
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
