#!/usr/bin/env python3

import argparse
import csv
import json
import os
from pathlib import Path
from typing import List, Optional


def _split_urls(source_url_field: Optional[str]) -> List[str]:
    # The CSV stores multiple URLs in a single field separated by commas.
    # Keep this intentionally simple: split, strip, drop empties.
    if not source_url_field:
        return []
    urls = [u.strip().strip('"') for u in source_url_field.split(",")]
    return [u for u in urls if u]


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert us_towns_and_counties_urls.csv to JSONL")
    ap.add_argument(
        "--in-csv",
        default="data/us_towns_and_counties_urls.csv",
        help="Input CSV path",
    )
    ap.add_argument(
        "--out-jsonl",
        default="data/us_towns_and_counties_urls.jsonl",
        help="Output JSONL path",
    )
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    out_path = Path(args.out_jsonl)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    rows_written = 0
    with open(in_path, "r", encoding="utf-8", newline="") as fin, open(
        tmp_path, "w", encoding="utf-8"
    ) as fout:
        reader = csv.DictReader(fin)
        for row in reader:
            gnis_raw = (row.get("gnis") or "").strip()
            gnis: str | int
            try:
                gnis = int(gnis_raw) if gnis_raw else ""
            except ValueError:
                gnis = gnis_raw

            source_url_raw = (row.get("source_url") or "").strip()
            obj = {
                "gnis": gnis,
                "place_name": (row.get("place_name") or "").strip(),
                "state_code": (row.get("state_code") or "").strip(),
                "source_url": source_url_raw,
                "source_urls": _split_urls(source_url_raw),
                "status": (row.get("status") or "").strip(),
            }
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            rows_written += 1

    os.replace(tmp_path, out_path)

    print(f"Wrote {rows_written} JSONL rows: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
