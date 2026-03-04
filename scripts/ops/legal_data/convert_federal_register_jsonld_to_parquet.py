#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
from typing import Any

import ijson
import pyarrow as pa
import pyarrow.parquet as pq


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


def cid_v1_from_json_obj(obj: dict[str, Any], codec: int = 0x0129) -> str:
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    digest = hashlib.sha256(canonical).digest()
    multihash = _uvarint(0x12) + _uvarint(len(digest)) + digest
    cid_bytes = _uvarint(1) + _uvarint(codec) + multihash
    return "b" + base64.b32encode(cid_bytes).decode("ascii").lower().rstrip("=")


def _extract(row: dict[str, Any], key: str, default: str = "") -> str:
    value = row.get(key, default)
    return "" if value is None else str(value)


def convert_jsonld_to_parquet(input_path: Path, output_path: Path, chunk_size: int = 20_000) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            ("cid", pa.string()),
            ("source_id", pa.string()),
            ("identifier", pa.string()),
            ("name", pa.string()),
            ("legislation_type", pa.string()),
            ("date_published", pa.string()),
            ("effective_date", pa.string()),
            ("agency", pa.string()),
            ("source_url", pa.string()),
            ("jsonld", pa.string()),
        ]
    )

    writer: pq.ParquetWriter | None = None
    batch = {field.name: [] for field in schema}
    total = 0
    seen_cids: set[str] = set()

    with input_path.open("rb") as handle:
        items = ijson.items(handle, "hasPart.item")
        for item in items:
            if not isinstance(item, dict):
                continue

            cid = cid_v1_from_json_obj(item)
            if cid in seen_cids:
                continue
            seen_cids.add(cid)

            source_org = item.get("sourceOrganization") or {}
            agency = source_org.get("name") if isinstance(source_org, dict) else ""

            batch["cid"].append(cid)
            batch["source_id"].append(_extract(item, "@id"))
            batch["identifier"].append(_extract(item, "identifier"))
            batch["name"].append(_extract(item, "name"))
            batch["legislation_type"].append(_extract(item, "legislationType"))
            batch["date_published"].append(_extract(item, "datePublished"))
            batch["effective_date"].append(_extract(item, "legislationDate"))
            batch["agency"].append("" if agency is None else str(agency))
            batch["source_url"].append(_extract(item, "sourceUrl"))
            batch["jsonld"].append(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))

            if len(batch["cid"]) >= chunk_size:
                table = pa.Table.from_pydict(batch, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(str(output_path), schema=schema, compression="zstd")
                writer.write_table(table)
                total += len(batch["cid"])
                print(f"wrote_rows={total}")
                batch = {field.name: [] for field in schema}

    if batch["cid"]:
        table = pa.Table.from_pydict(batch, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(str(output_path), schema=schema, compression="zstd")
        writer.write_table(table)
        total += len(batch["cid"])

    if writer is not None:
        writer.close()

    print(f"done rows={total} output={output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert Federal Register JSON-LD to Parquet with CID primary key")
    parser.add_argument("--input", required=True, help="Path to federal_register.jsonld")
    parser.add_argument("--output", required=True, help="Path to output parquet")
    parser.add_argument("--chunk-size", type=int, default=20_000, help="Rows per parquet write chunk")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")

    convert_jsonld_to_parquet(input_path=input_path, output_path=output_path, chunk_size=args.chunk_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
