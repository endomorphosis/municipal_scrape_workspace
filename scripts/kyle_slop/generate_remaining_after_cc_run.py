#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Set

import pyarrow.parquet as pq


def _iter_csv_rows(csv_path: Path) -> Iterable[dict]:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def _load_cc_success_gnis(crawl_runs_parquet: Path) -> Set[str]:
    table = pq.read_table(crawl_runs_parquet)
    df = table.to_pandas()

    if "gnis" not in df.columns:
        return set()

    saved_col = "saved" if "saved" in df.columns else None
    status_col = "status" if "status" in df.columns else None
    cc_status_col = "cc_status" if "cc_status" in df.columns else None

    ok = None
    if saved_col:
        ok = df[saved_col].fillna(0).astype(int) > 0
    if status_col:
        ok = ok & (df[status_col].fillna("").astype(str) == "ok") if ok is not None else (df[status_col] == "ok")
    if cc_status_col:
        ok = ok & (df[cc_status_col].fillna("").astype(str) == "cc_ok") if ok is not None else (df[cc_status_col] == "cc_ok")

    if ok is None:
        return set()

    return {str(x).strip() for x in df.loc[ok, "gnis"].tolist() if str(x).strip()}


def main() -> int:
    ap = argparse.ArgumentParser(description="Compute remaining targets after a CC-only run")
    ap.add_argument("--remaining-csv", required=True, help="Input remaining targets CSV")
    ap.add_argument("--crawl-runs-parquet", required=True, help="crawl_runs.parquet from the CC-only run")
    ap.add_argument("--out-csv", required=True, help="Output CSV containing still-remaining rows")

    args = ap.parse_args()

    remaining_csv = Path(args.remaining_csv)
    crawl_runs = Path(args.crawl_runs_parquet)
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    done_gnis = _load_cc_success_gnis(crawl_runs)

    rows_out: List[Dict[str, str]] = []
    total = 0
    for row in _iter_csv_rows(remaining_csv):
        total += 1
        gnis = (row.get("gnis") or "").strip()
        if not gnis:
            continue
        if gnis in done_gnis:
            continue
        rows_out.append(row)

    tmp = out_csv.with_suffix(out_csv.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        if rows_out:
            fieldnames = list(rows_out[0].keys())
        else:
            # preserve expected schema if empty
            fieldnames = ["gnis", "place_name", "state_code", "source_url", "primary_url", "status"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    tmp.replace(out_csv)

    print(f"Input targets: {total}")
    print(f"CC-success GNIS: {len(done_gnis)}")
    print(f"Still remaining: {len(rows_out)}")
    print(f"Wrote: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
