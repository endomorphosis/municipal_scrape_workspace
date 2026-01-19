#!/usr/bin/env python3
"""Status helper for Common Crawl pointer-index builds.

Reads DuckDB pointer DBs (cc_pointers_*.duckdb) and reports:
- ingested shard files count
- ingested rows (sum of per-file parsed rows)
- latest ingested timestamp
- DB file size

Optionally, if you pass --input-root, it also counts how many cdx-*.gz shards
exist on disk per year (filtered by --collections-regex) so you can estimate
remaining work.

Example
    /home/barberb/municipal_scrape_workspace/.venv/bin/python cc_pointer_status.py \
        --db-dir /storage/ccindex_duckdb/cc_pointers_by_year \
        --input-root /storage/ccindex \
        --collections-regex 'CC-MAIN-202[4-5]-.*'
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import duckdb


_PART_RX = re.compile(r"^(?P<base>CC-MAIN-\d{4}-\d{2})__m(?P<mod>\d+)r(?P<rem>\d+)$")
_CDX_RX = re.compile(r"^cdx-(\d{5})\.gz$")


def _split_part_suffix(shard_key: str) -> Tuple[str, Optional[int], Optional[int]]:
    m = _PART_RX.match(shard_key or "")
    if not m:
        return shard_key, None, None
    base = str(m.group("base"))
    try:
        mod = int(m.group("mod"))
        rem = int(m.group("rem"))
        return base, mod, rem
    except Exception:
        return shard_key, None, None


def _count_expected_part_shards(col_dir: Path, mod: int, rem: int) -> int:
    n = 0
    try:
        for f in col_dir.iterdir():
            if not f.is_file():
                continue
            m = _CDX_RX.match(f.name)
            if not m:
                continue
            try:
                i = int(m.group(1))
            except Exception:
                continue
            if (i % int(mod)) == int(rem):
                n += 1
    except Exception:
        return 0
    return n


@dataclass
class ShardStatus:
    shard_key: str
    year: Optional[int]
    collection: Optional[str]
    db_path: Path
    db_bytes: int
    ingested_files: int
    ingested_rows: int
    latest_ingested_at: Optional[str]
    expected_files: Optional[int] = None
    locked: bool = False
    lock_error: Optional[str] = None
    from_snapshot: bool = False


def _fmt_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.2f} {u}"
        f /= 1024.0
    return f"{f:.2f} TiB"


def _parse_year_from_text(text: str) -> Optional[int]:
    m = re.search(r"(\d{4})", text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _parse_shard_key_from_db_name(name: str) -> Optional[str]:
    # cc_pointers_2024.duckdb OR cc_pointers_CC-MAIN-2024-10.duckdb
    m = re.match(r"^cc_pointers_(.+)\.duckdb$", name)
    if m:
        return m.group(1)
    if name == "cc_pointers.duckdb":
        return "all"
    return None


def _read_shard_db(db_path: Path) -> ShardStatus:
    shard_key = _parse_shard_key_from_db_name(db_path.name)
    if shard_key is None:
        raise SystemExit(f"Could not parse shard key from DB filename: {db_path.name}")

    base_key, _mod, _rem = _split_part_suffix(shard_key)

    year = int(base_key) if re.fullmatch(r"\d{4}", base_key) else _parse_year_from_text(base_key)
    collection = None if re.fullmatch(r"\d{4}", base_key) else base_key

    db_bytes = db_path.stat().st_size if db_path.exists() else 0

    # During an active build, DuckDB may hold a lock. We try a best-effort read
    # without taking a lock; if that fails, report the lock state.
    try:
        con = duckdb.connect(str(db_path), read_only=True, config={"lock_configuration": "none"})
        try:
            row_files = con.execute("SELECT count(*) FROM cc_ingested_files").fetchone()
            ing_files = (row_files[0] if row_files and row_files[0] is not None else 0) or 0
            row_rows = con.execute("SELECT COALESCE(sum(rows), 0) FROM cc_ingested_files").fetchone()
            ing_rows = (row_rows[0] if row_rows and row_rows[0] is not None else 0) or 0
            row_latest = con.execute("SELECT max(ingested_at) FROM cc_ingested_files").fetchone()
            latest = row_latest[0] if row_latest and row_latest[0] is not None else None
        finally:
            con.close()
        return ShardStatus(
            shard_key=shard_key,
            year=year,
            collection=collection,
            db_path=db_path,
            db_bytes=int(db_bytes),
            ingested_files=int(ing_files),
            ingested_rows=int(ing_rows),
            latest_ingested_at=str(latest) if latest else None,
        )
    except Exception as e:
        # Locked or unreadable right now.
        return ShardStatus(
            shard_key=shard_key,
            year=year,
            collection=collection,
            db_path=db_path,
            db_bytes=int(db_bytes),
            ingested_files=0,
            ingested_rows=0,
            latest_ingested_at=None,
            locked=True,
            lock_error=f"{type(e).__name__}: {e}",
        )


def _read_progress_snapshot_by_shard_key(db_dir: Path, shard_key: str) -> Optional[dict]:
    for snap_path in sorted(db_dir.glob("progress_*.json")):
        if not snap_path.is_file() or snap_path.name == "progress_all.json":
            continue
        try:
            with open(snap_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                continue
            if str(data.get("shard_key")) == str(shard_key):
                return data
        except Exception:
            continue
    return None


def _iter_collections(input_root: Path) -> Iterable[Path]:
    # Layout: /storage/ccindex/<collection>/cdx-*.gz
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

        # Count cdx-*.gz in this collection directory.
        try:
            n = 0
            for f in col_dir.iterdir():
                if f.is_file() and f.name.startswith("cdx-") and f.name.endswith(".gz"):
                    n += 1
            counts[year] = counts.get(year, 0) + n
        except Exception:
            continue

    return counts


def _count_expected_shards_by_collection(input_root: Path, collections_regex: Optional[str]) -> Dict[str, int]:
    rx = re.compile(collections_regex) if collections_regex else None

    counts: Dict[str, int] = {}
    for col_dir in _iter_collections(input_root):
        collection = col_dir.name
        if rx and not rx.search(collection):
            continue

        try:
            n = 0
            for f in col_dir.iterdir():
                if f.is_file() and f.name.startswith("cdx-") and f.name.endswith(".gz"):
                    n += 1
            counts[collection] = n
        except Exception:
            continue

    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-dir", required=True, type=str, help="Directory containing cc_pointers_*.duckdb files")
    ap.add_argument("--input-root", type=str, default=None, help="Optional CC index root to compute expected shard counts")
    ap.add_argument("--collections-regex", type=str, default=None, help="Regex filter for collections when counting expected shards")
    args = ap.parse_args()

    db_dir = Path(args.db_dir).expanduser().resolve()
    if not db_dir.exists() or not db_dir.is_dir():
        raise SystemExit(f"Not a directory: {db_dir}")

    db_files = sorted(p for p in db_dir.glob("*.duckdb") if p.is_file())
    if not db_files:
        raise SystemExit(f"No .duckdb files found in: {db_dir}")

    statuses: List[ShardStatus] = []
    for db in db_files:
        st = _read_shard_db(db)
        if st.locked:
            snap = _read_progress_snapshot_by_shard_key(db_dir, st.shard_key)
            if isinstance(snap, dict):
                st.ingested_files = int(snap.get("ingested_files", 0) or 0)
                st.ingested_rows = int(snap.get("ingested_rows", 0) or 0)
                latest = snap.get("updated_at") or snap.get("last_update")
                st.latest_ingested_at = str(latest) if latest else None
                st.from_snapshot = True
        statuses.append(st)

    expected_by_year: Dict[int, int] = {}
    expected_by_collection: Dict[str, int] = {}
    expected_part_cache: Dict[Tuple[str, int, int], int] = {}
    if args.input_root:
        input_root = Path(args.input_root).expanduser().resolve()
        expected_by_year = _count_expected_shards_by_year(input_root, args.collections_regex)
        expected_by_collection = _count_expected_shards_by_collection(input_root, args.collections_regex)
        for st in statuses:
            base, mod, rem = _split_part_suffix(st.shard_key)
            if st.collection and mod is not None and rem is not None:
                key = (st.collection, int(mod), int(rem))
                if key not in expected_part_cache:
                    expected_part_cache[key] = _count_expected_part_shards(input_root / st.collection, int(mod), int(rem))
                st.expected_files = expected_part_cache.get(key)
            elif st.collection and st.collection in expected_by_collection:
                st.expected_files = expected_by_collection[st.collection]
            elif st.year is not None and st.year in expected_by_year:
                st.expected_files = expected_by_year[st.year]

    # Print report
    print(f"DB dir: {db_dir}")
    if args.input_root:
        print(f"Input root: {Path(args.input_root).expanduser().resolve()}")
        if args.collections_regex:
            print(f"Collections regex: {args.collections_regex}")

    print("")
    header = ["shard", "year", "db_size", "ing_files", "exp_files", "pct", "ing_rows", "latest_ingested_at"]
    print("\t".join(header))

    total_db = 0
    total_ing_files = 0
    total_exp_files = 0
    total_rows = 0

    def _sort_key(s: ShardStatus) -> Tuple[int, str]:
        return (s.year or 0, s.shard_key)

    for st in sorted(statuses, key=_sort_key):
        total_db += st.db_bytes
        total_ing_files += st.ingested_files
        total_rows += st.ingested_rows
        exp = st.expected_files
        if exp is not None:
            total_exp_files += exp
            pct = (100.0 * st.ingested_files / exp) if exp > 0 else 0.0
            pct_s = f"{pct:.1f}%"
            exp_s = str(exp)
        else:
            pct_s = "-"
            exp_s = "-"

        if st.locked and not st.from_snapshot:
            row = [st.shard_key, str(st.year) if st.year is not None else "-", _fmt_bytes(st.db_bytes), "LOCKED", exp_s, "-", "-", "-"]
        else:
            ing_files_s = str(st.ingested_files)
            ing_rows_s = f"{st.ingested_rows:,}" if st.ingested_rows is not None else "-"
            latest_s = st.latest_ingested_at or "-"
            if st.from_snapshot:
                ing_files_s = ing_files_s + "*"
            row = [
                st.shard_key,
                str(st.year) if st.year is not None else "-",
                _fmt_bytes(st.db_bytes),
                ing_files_s,
                exp_s,
                pct_s,
                ing_rows_s,
                latest_s,
            ]
        print("\t".join(row))

    any_snapshot = any(s.from_snapshot for s in statuses)

    print("")
    if total_exp_files > 0:
        total_pct = 100.0 * total_ing_files / total_exp_files
        print(f"TOTAL: db={_fmt_bytes(total_db)}, files={total_ing_files}/{total_exp_files} ({total_pct:.1f}%), rows={total_rows:,}")
    else:
        print(f"TOTAL: db={_fmt_bytes(total_db)}, files={total_ing_files}, rows={total_rows:,}")

    if any_snapshot:
        print("NOTE: '*' indicates counts read from progress_*.json snapshots (DuckDB was locked).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
