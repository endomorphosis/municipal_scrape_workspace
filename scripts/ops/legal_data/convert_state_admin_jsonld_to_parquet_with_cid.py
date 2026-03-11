#!/usr/bin/env python3
"""Convert per-state JSONLD line files into CID-keyed Parquet datasets."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq

STATE_FILE_RE = re.compile(r"^STATE-([A-Z]{2})\.jsonld$")


def _uvarint(value: int) -> bytes:
    out = bytearray()
    while True:
        to_write = value & 0x7F
        value >>= 7
        if value:
            out.append(to_write | 0x80)
        else:
            out.append(to_write)
            break
    return bytes(out)


def cid_v1_from_json_obj(obj: Dict[str, Any], codec: int = 0x0129) -> str:
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    digest = hashlib.sha256(canonical).digest()
    multihash = _uvarint(0x12) + _uvarint(len(digest)) + digest
    cid_bytes = _uvarint(1) + _uvarint(codec) + multihash
    return "b" + base64.b32encode(cid_bytes).decode("ascii").lower().rstrip("=")


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _iter_jsonld_lines(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                yield item


def _canonical_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in item.items() if k not in {"ipfs_cid", "cid"}}


def _state_files(input_dir: Path) -> List[Path]:
    out: List[Path] = []
    for path in sorted(input_dir.glob("STATE-*.jsonld")):
        if STATE_FILE_RE.match(path.name):
            out.append(path)
    return out


def _schema() -> pa.Schema:
    return pa.schema(
        [
            ("ipfs_cid", pa.string()),
            ("state_code", pa.string()),
            ("source_id", pa.string()),
            ("identifier", pa.string()),
            ("name", pa.string()),
            ("legislation_type", pa.string()),
            ("legislation_jurisdiction", pa.string()),
            ("source_url", pa.string()),
            ("text", pa.string()),
            ("jsonld", pa.string()),
        ]
    )


def _rows_from_file(path: Path, state_code: str) -> Dict[str, Any]:
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()
    dropped_duplicates = 0
    input_rows = 0

    for item in _iter_jsonld_lines(path):
        input_rows += 1
        payload = _canonical_payload(item)
        ipfs_cid = cid_v1_from_json_obj(payload)
        if ipfs_cid in seen:
            dropped_duplicates += 1
            continue
        seen.add(ipfs_cid)

        source_url = _norm(item.get("sourceUrl") or item.get("url") or item.get("sameAs"))
        row = {
            "ipfs_cid": ipfs_cid,
            "state_code": state_code,
            "source_id": _norm(item.get("@id")),
            "identifier": _norm(item.get("identifier") or item.get("ruleIdentifier")),
            "name": _norm(item.get("name")),
            "legislation_type": _norm(item.get("legislationType")),
            "legislation_jurisdiction": _norm(item.get("legislationJurisdiction")),
            "source_url": source_url,
            "text": _norm(item.get("text")),
            "jsonld": json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        }
        rows.append(row)

    rows.sort(key=lambda r: r["ipfs_cid"])
    return {
        "rows": rows,
        "input_rows": input_rows,
        "rows_written": len(rows),
        "duplicates_dropped": dropped_duplicates,
        "unique_cids": len(seen),
    }


def _write_parquet(rows: List[Dict[str, str]], out_path: Path, schema: pa.Schema) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    columns = {field.name: [row[field.name] for row in rows] for field in schema}
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, out_path, compression="zstd")


def convert(input_dir: Path, output_dir: Path, combined_filename: str) -> Dict[str, Any]:
    files = _state_files(input_dir)
    if not files:
        raise FileNotFoundError(f"no STATE-XX.jsonld files found in {input_dir}")

    schema = _schema()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_states: Dict[str, Dict[str, Any]] = {}
    combined: List[Dict[str, str]] = []
    combined_seen: set[str] = set()
    combined_dupes = 0

    for path in files:
        m = STATE_FILE_RE.match(path.name)
        if not m:
            continue
        state_code = m.group(1)
        stats = _rows_from_file(path, state_code=state_code)
        rows = stats["rows"]

        state_out = output_dir / f"STATE-{state_code}.parquet"
        _write_parquet(rows, state_out, schema=schema)

        for row in rows:
            cid = row["ipfs_cid"]
            if cid in combined_seen:
                combined_dupes += 1
                continue
            combined_seen.add(cid)
            combined.append(row)

        manifest_states[state_code] = {
            "input_file": str(path),
            "output_file": str(state_out),
            "input_rows": stats["input_rows"],
            "rows_written": stats["rows_written"],
            "duplicates_dropped": stats["duplicates_dropped"],
            "unique_cids": stats["unique_cids"],
        }

    combined.sort(key=lambda r: r["ipfs_cid"])
    combined_out = output_dir / combined_filename
    _write_parquet(combined, combined_out, schema=schema)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "state_count": len(manifest_states),
        "states": sorted(manifest_states.keys()),
        "combined_output": str(combined_out),
        "combined_rows_written": len(combined),
        "combined_unique_cids": len(combined_seen),
        "combined_duplicates_dropped": combined_dupes,
        "per_state": manifest_states,
    }
    (output_dir / "manifest.parquet.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert state-admin JSONLD to CID-keyed Parquet")
    parser.add_argument("--input-dir", required=True, help="Directory containing STATE-XX.jsonld files")
    parser.add_argument("--output-dir", required=True, help="Directory to write parquet files")
    parser.add_argument(
        "--combined-filename",
        default="state_admin_rules_all_states.parquet",
        help="Filename for combined all-state parquet",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input dir not found: {input_dir}")

    manifest = convert(input_dir=input_dir, output_dir=output_dir, combined_filename=args.combined_filename)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
