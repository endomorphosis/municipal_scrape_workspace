#!/usr/bin/env python3

import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def _split_urls(source_url_field: str) -> List[str]:
    # The CSV stores multiple URLs in a single field separated by commas.
    # Keep this intentionally simple: split, strip, drop empties.
    if source_url_field is None:
        return []
    urls = [u.strip().strip('"') for u in source_url_field.split(',')]
    return [u for u in urls if u]


def _load_inventory_html_set(inventory_json_path: Path) -> Tuple[Dict[str, dict], set]:
    with open(inventory_json_path, "r", encoding="utf-8") as f:
        inv = json.load(f)
    per_gnis = inv.get("per_gnis", {})
    html_set = {g for g, r in per_gnis.items() if r.get("has_html")}
    return per_gnis, html_set


def _iter_targets_csv(csv_path: Path) -> Iterable[dict]:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate remaining scrape targets by GNIS")
    ap.add_argument(
        "--targets-csv",
        default="data/us_towns_and_counties_urls.csv",
        help="CSV with columns: gnis, place_name, state_code, source_url, status",
    )
    ap.add_argument(
        "--inventory-json",
        required=True,
        help="Inventory JSON produced by inventory_kyle_slop.py",
    )
    ap.add_argument(
        "--out-csv",
        default=None,
        help="Output CSV path (default: artifacts/remaining_targets_<timestamp>.csv)",
    )
    ap.add_argument(
        "--mode",
        choices=["missing_html", "missing_any"],
        default="missing_html",
        help="missing_html: gnis not present in Kyle html; missing_any: gnis not present in any Kyle file",
    )

    args = ap.parse_args()

    targets_csv = Path(args.targets_csv)
    inv_path = Path(args.inventory_json)

    per_gnis, html_set = _load_inventory_html_set(inv_path)
    any_set = set(per_gnis.keys())

    out_path = Path(args.out_csv) if args.out_csv else Path("artifacts") / f"remaining_targets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    remaining_rows: List[dict] = []

    total = 0
    for row in _iter_targets_csv(targets_csv):
        total += 1
        gnis = (row.get("gnis") or "").strip()
        if not gnis:
            continue

        if args.mode == "missing_html":
            remaining = gnis not in html_set
        else:
            remaining = gnis not in any_set

        if not remaining:
            continue

        urls = _split_urls(row.get("source_url") or "")
        remaining_rows.append(
            {
                "gnis": gnis,
                "place_name": (row.get("place_name") or "").strip(),
                "state_code": (row.get("state_code") or "").strip(),
                "source_url": (row.get("source_url") or "").strip(),
                "primary_url": urls[0] if urls else "",
                "status": (row.get("status") or "").strip(),
            }
        )

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["gnis", "place_name", "state_code", "source_url", "primary_url", "status"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in remaining_rows:
            w.writerow(r)

    os.replace(tmp_path, out_path)

    print(f"Wrote remaining targets: {out_path}")
    print(f"Targets total={total} remaining={len(remaining_rows)} mode={args.mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
