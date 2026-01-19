#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Iterator, Optional, Sequence, Tuple

import duckdb


def _iter_parquet_files(
    parquet_root: Path,
    collections: Optional[Sequence[str]],
    collections_regex: Optional[str],
) -> Iterator[Path]:
    rx = re.compile(collections_regex) if collections_regex else None

    if collections:
        for col in collections:
            parts = col.split("-")
            if len(parts) < 3:
                raise SystemExit(f"Invalid collection name: {col}")
            year = parts[2]
            col_dir = parquet_root / year / col
            if not col_dir.is_dir():
                continue
            yield from sorted(col_dir.glob("cdx-*.gz.parquet"))
        return

    # Enumerate <year>/<collection>/cdx-*.gz.parquet
    for year_dir in sorted(parquet_root.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for col_dir in sorted(year_dir.iterdir()):
            if not col_dir.is_dir():
                continue
            if rx and not rx.search(col_dir.name):
                continue
            yield from sorted(col_dir.glob("cdx-*.gz.parquet"))


def _format_gb(nbytes: int) -> str:
    return f"{nbytes / (1024**3):.1f}GB"


def _should_skip(path: Path, force: bool) -> bool:
    if force:
        return False
    marker = path.with_suffix(path.suffix + ".sorted")
    return marker.exists()


def _write_sorted_marker(path: Path) -> None:
    marker = path.with_suffix(path.suffix + ".sorted")
    marker.write_text(f"sorted_at={time.time()}\n", encoding="utf-8")


def sort_one(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    min_free_gb: float,
    temp_dir: Optional[Path],
    force: bool,
    dry_run: bool,
) -> Tuple[bool, str]:
    if _should_skip(parquet_path, force=force):
        return False, "skip(already_marked_sorted)"

    out_dir = parquet_path.parent
    old_size = parquet_path.stat().st_size
    min_free_bytes = int(min_free_gb * 1024**3)

    free_bytes = shutil.disk_usage(str(out_dir)).free
    # Conservative headroom: we need to hold old + new simultaneously; free space must cover new.
    if free_bytes < (min_free_bytes + old_size):
        return False, (
            f"skip(insufficient_free free={_format_gb(free_bytes)} need>={_format_gb(min_free_bytes + old_size)})"
        )

    tmp_path = parquet_path.with_suffix(parquet_path.suffix + ".sorting.part")

    if dry_run:
        return True, f"dry_run(would_write {tmp_path.name} then replace)"

    if tmp_path.exists():
        tmp_path.unlink()

    if temp_dir:
        con.execute(f"PRAGMA temp_directory='{str(temp_dir)}'")

    # Sort by the key we use for range/offset lookups.
    q = (
        "COPY ("
        "  SELECT * FROM read_parquet(?) "
        "  ORDER BY host_rev, url, ts"
        ") TO '" + str(tmp_path).replace("'", "''") + "' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    con.execute(q, [str(parquet_path)])

    os.replace(str(tmp_path), str(parquet_path))
    _write_sorted_marker(parquet_path)
    return True, "sorted"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Rewrite CC Parquet shards sorted by (host_rev, url, ts) safely in-place.")
    ap.add_argument(
        "--parquet-root",
        required=True,
        type=str,
        help="Root like /storage/ccindex_parquet/cc_pointers_by_collection",
    )
    ap.add_argument("--collections", action="append", default=None, help="Repeatable: only process these collections")
    ap.add_argument("--collections-regex", type=str, default=None, help="Regex to select collections (when enumerating)")
    ap.add_argument("--max-files", type=int, default=None, help="Stop after sorting this many files")
    ap.add_argument(
        "--min-free-gb",
        type=float,
        default=250.0,
        help="Skip a file if free space is less than (min-free + file_size)",
    )
    ap.add_argument("--duckdb-tmp", type=str, default=None, help="Optional DuckDB temp spill directory")
    ap.add_argument("--threads", type=int, default=2, help="DuckDB threads to use")
    ap.add_argument(
        "--zfs-dataset",
        type=str,
        default=None,
        help=(
            "Optional: ZFS dataset name (e.g. storage/ccindex_parquet). "
            "If provided, can refuse to run when snapshots exist to avoid retained-block space blowups."
        ),
    )
    ap.add_argument(
        "--refuse-if-snapshots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When --zfs-dataset is set, refuse to sort if any snapshots exist on that dataset.",
    )
    ap.add_argument("--force", action="store_true", default=False, help="Re-sort even if marker exists")
    ap.add_argument("--dry-run", action="store_true", default=False, help="Print what would be done without writing")

    args = ap.parse_args(argv)

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    tmp_dir = Path(args.duckdb_tmp).expanduser().resolve() if args.duckdb_tmp else None

    if not parquet_root.is_dir():
        raise SystemExit(f"--parquet-root does not exist: {parquet_root}")

    def _snapshot_count() -> int:
        if not args.zfs_dataset:
            return 0
        cmd = ["zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-r", str(args.zfs_dataset)]
        try:
            res = subprocess.run(cmd, check=False, capture_output=True, text=True)
            snaps = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
            return len(snaps)
        except Exception:
            return 0

    if args.zfs_dataset and bool(args.refuse_if_snapshots):
        n = _snapshot_count()
        if n:
            raise SystemExit(
                f"Refusing to sort because dataset {args.zfs_dataset} has {n} snapshots. "
                "Delete/prune snapshots first or pass --no-refuse-if-snapshots."
            )

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)}")

    total = 0
    did = 0
    skipped = 0
    for p in _iter_parquet_files(parquet_root, args.collections, args.collections_regex):
        if args.zfs_dataset and bool(args.refuse_if_snapshots):
            n = _snapshot_count()
            if n:
                raise SystemExit(
                    f"Refusing to continue sorting because dataset {args.zfs_dataset} now has {n} snapshots. "
                    "Prune them and re-run."
                )

        total += 1
        ok, status = sort_one(
            con,
            p,
            min_free_gb=float(args.min_free_gb),
            temp_dir=tmp_dir,
            force=bool(args.force),
            dry_run=bool(args.dry_run),
        )
        if ok:
            did += 1
        else:
            skipped += 1

        if total % 50 == 0:
            print(f"seen={total} did={did} skipped={skipped} last={p.name} status={status}")

        if args.max_files and did >= int(args.max_files):
            break

    print(f"done seen={total} sorted={did} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
