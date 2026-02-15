#!/usr/bin/env python3

import argparse
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pyarrow.parquet as pq


_KYLE_FILENAME_RE = re.compile(r"^(?P<gnis>\d+?)_(?P<kind>html|citation|embeddings)\.parquet$")


@dataclass(frozen=True)
class FileStat:
    path: str
    bytes: int
    rows: Optional[int]


def _iter_parquet_files(data_dir: Path) -> Iterable[Path]:
    for entry in sorted(data_dir.iterdir()):
        if entry.is_file() and entry.suffix == ".parquet":
            yield entry


def _safe_num_rows(parquet_path: Path) -> Optional[int]:
    try:
        pf = pq.ParquetFile(parquet_path)
        md = pf.metadata
        if md is None:
            return None
        return md.num_rows
    except Exception:
        return None


def build_inventory(data_dir: Path, with_rows: bool) -> Dict[str, Any]:
    per_gnis: Dict[str, Dict[str, Any]] = {}

    counts_by_kind = {"html": 0, "citation": 0, "embeddings": 0, "other": 0}
    bytes_by_kind = {"html": 0, "citation": 0, "embeddings": 0, "other": 0}

    total_files = 0

    for parquet_path in _iter_parquet_files(data_dir):
        total_files += 1
        m = _KYLE_FILENAME_RE.match(parquet_path.name)
        if not m:
            counts_by_kind["other"] += 1
            try:
                bytes_by_kind["other"] += parquet_path.stat().st_size
            except OSError:
                pass
            continue

        gnis = m.group("gnis")
        kind = m.group("kind")

        try:
            file_bytes = parquet_path.stat().st_size
        except OSError:
            file_bytes = 0

        file_rows = _safe_num_rows(parquet_path) if with_rows else None

        counts_by_kind[kind] += 1
        bytes_by_kind[kind] += file_bytes

        rec = per_gnis.setdefault(
            gnis,
            {
                "gnis": gnis,
                "has_html": False,
                "has_citation": False,
                "has_embeddings": False,
                "files": {},
            },
        )

        rec[f"has_{kind}"] = True
        rec["files"][kind] = asdict(
            FileStat(path=str(parquet_path.relative_to(data_dir)), bytes=file_bytes, rows=file_rows)
        )

    gnis_all = set(per_gnis.keys())
    gnis_with_html = {g for g, r in per_gnis.items() if r.get("has_html")}
    gnis_with_citation = {g for g, r in per_gnis.items() if r.get("has_citation")}
    gnis_with_embeddings = {g for g, r in per_gnis.items() if r.get("has_embeddings")}

    gnis_with_all_three = gnis_with_html & gnis_with_citation & gnis_with_embeddings

    return {
        "generated_at": datetime.now().isoformat(),
        "data_dir": str(data_dir),
        "total_files": total_files,
        "counts_by_kind": counts_by_kind,
        "bytes_by_kind": bytes_by_kind,
        "gnis_counts": {
            "any": len(gnis_all),
            "html": len(gnis_with_html),
            "citation": len(gnis_with_citation),
            "embeddings": len(gnis_with_embeddings),
            "all_three": len(gnis_with_all_three),
        },
        "per_gnis": per_gnis,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Inventory Kyle slop Parquet files by GNIS/kind")
    ap.add_argument(
        "--data-dir",
        default="data/kyle_slop/american_municipal_law/american_law/data",
        help="Directory containing Kyle per-GNIS parquet files",
    )
    ap.add_argument(
        "--out",
        default=None,
        help="Output JSON path (default: artifacts/kyle_slop_inventory_<timestamp>.json)",
    )
    ap.add_argument(
        "--with-rows",
        action="store_true",
        help="Include per-file row counts using Parquet metadata (fast, no full reads)",
    )

    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists() or not data_dir.is_dir():
        raise SystemExit(f"data-dir not found or not a directory: {data_dir}")

    out_path = Path(args.out) if args.out else Path("artifacts") / f"kyle_slop_inventory_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    inventory = build_inventory(data_dir=data_dir, with_rows=bool(args.with_rows))

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, sort_keys=True)
    os.replace(tmp_path, out_path)

    print(f"Wrote inventory: {out_path}")
    print(f"GNIS any={inventory['gnis_counts']['any']} html={inventory['gnis_counts']['html']} citation={inventory['gnis_counts']['citation']} embeddings={inventory['gnis_counts']['embeddings']} all_three={inventory['gnis_counts']['all_three']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
