#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


OAR_KEY_RE = re.compile(r"\b\d{3}-\d{3}-\d{4}\b")


def _extract_oar_key(*values: Any) -> str:
    for value in values:
        text = str(value or "")
        match = OAR_KEY_RE.search(text)
        if match:
            return match.group(0)
    return ""


def build_index(
    input_parquet: Path,
    output_parquet: Path,
    output_json: Path | None = None,
    *,
    canonical_only: bool = False,
) -> dict[str, int]:
    table = pq.read_table(
        input_parquet,
        columns=["cid", "record_type", "node_type", "node_id", "label"],
    )

    rows = table.to_pylist()

    best_by_key: dict[str, dict[str, Any]] = {}
    scanned = 0
    matched_nodes = 0
    canonical_skipped = 0

    for row in rows:
        scanned += 1
        if str(row.get("record_type") or "") != "node":
            continue
        if str(row.get("node_type") or "") != "oar_rule":
            continue

        cid = str(row.get("cid") or "").strip()
        node_id = str(row.get("node_id") or "").strip()
        label = str(row.get("label") or "").strip()
        if not cid:
            continue

        if canonical_only and not node_id.startswith("oar:OAR "):
            canonical_skipped += 1
            continue

        oar_key = _extract_oar_key(label, node_id)
        if not oar_key:
            continue

        matched_nodes += 1

        # Prefer canonical full-rule node IDs: oar:OAR xxx-xxx-xxxx over cited stubs: oar:xxx-xxx-xxxx
        priority = 2 if node_id.startswith("oar:OAR ") else 1

        existing = best_by_key.get(oar_key)
        if existing is None or priority > int(existing["priority"]):
            best_by_key[oar_key] = {
                "oar_key": oar_key,
                "cid": cid,
                "node_id": node_id,
                "label": label,
                "priority": priority,
            }

    index_rows = [best_by_key[key] for key in sorted(best_by_key.keys())]

    schema = pa.schema(
        [
            ("oar_key", pa.string()),
            ("cid", pa.string()),
            ("node_id", pa.string()),
            ("label", pa.string()),
        ]
    )

    out_data = {
        "oar_key": [r["oar_key"] for r in index_rows],
        "cid": [r["cid"] for r in index_rows],
        "node_id": [r["node_id"] for r in index_rows],
        "label": [r["label"] for r in index_rows],
    }

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pydict(out_data, schema=schema), output_parquet, compression="zstd")

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        mapping = {r["oar_key"]: r["cid"] for r in index_rows}
        output_json.write_text(json.dumps(mapping, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    return {
        "rows_scanned": scanned,
        "matched_oar_rule_nodes": matched_nodes,
        "canonical_skipped": canonical_skipped,
        "canonical_only": int(canonical_only),
        "index_entries": len(index_rows),
        "unique_keys": len(index_rows),
        "unique_cids": len({r["cid"] for r in index_rows}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build OAR key->CID index from KG parquet")
    parser.add_argument("--input", required=True, help="Path to KG parquet (nodes + edges)")
    parser.add_argument("--output", required=True, help="Path to output OAR key->CID parquet")
    parser.add_argument("--output-json", required=False, help="Optional JSON mapping output")
    parser.add_argument(
        "--canonical-only",
        action="store_true",
        help="Only index canonical OAR rule nodes with node_id prefix 'oar:OAR '",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_json = Path(args.output_json) if args.output_json else None

    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")

    stats = build_index(
        input_path,
        output_path,
        output_json,
        canonical_only=args.canonical_only,
    )
    print("done", json.dumps(stats, ensure_ascii=False))
    print(f"output_parquet={output_path}")
    if output_json is not None:
        print(f"output_json={output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
