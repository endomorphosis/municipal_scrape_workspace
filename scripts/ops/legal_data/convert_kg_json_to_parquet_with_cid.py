#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
from typing import Any

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


def _norm(value: Any) -> str:
    return str(value or "").strip()


def convert_kg_json_to_parquet(input_json: Path, output_parquet: Path) -> dict[str, int]:
    graph = json.loads(input_json.read_text(encoding="utf-8"))
    nodes = graph.get("nodes") or []
    edges = graph.get("edges") or []

    graph_name = _norm(graph.get("graph_name"))
    source = _norm(graph.get("source"))
    generated_at = _norm(graph.get("generated_at"))

    schema = pa.schema(
        [
            ("cid", pa.string()),
            ("record_type", pa.string()),
            ("graph_name", pa.string()),
            ("source", pa.string()),
            ("generated_at", pa.string()),
            ("node_id", pa.string()),
            ("node_type", pa.string()),
            ("label", pa.string()),
            ("source_id", pa.string()),
            ("target_id", pa.string()),
            ("edge_type", pa.string()),
            ("properties_json", pa.string()),
            ("payload_json", pa.string()),
        ]
    )

    rows = {field.name: [] for field in schema}

    for node in nodes:
        if not isinstance(node, dict):
            continue
        payload = {
            "record_type": "node",
            "graph_name": graph_name,
            "node": node,
        }
        cid = cid_v1_from_json_obj(payload)

        rows["cid"].append(cid)
        rows["record_type"].append("node")
        rows["graph_name"].append(graph_name)
        rows["source"].append(source)
        rows["generated_at"].append(generated_at)
        rows["node_id"].append(_norm(node.get("id")))
        rows["node_type"].append(_norm(node.get("type")))
        rows["label"].append(_norm(node.get("label")))
        rows["source_id"].append("")
        rows["target_id"].append("")
        rows["edge_type"].append("")
        rows["properties_json"].append(
            json.dumps(node.get("properties") or {}, ensure_ascii=False, separators=(",", ":"))
        )
        rows["payload_json"].append(json.dumps(node, ensure_ascii=False, separators=(",", ":")))

    for edge in edges:
        if not isinstance(edge, dict):
            continue
        payload = {
            "record_type": "edge",
            "graph_name": graph_name,
            "edge": edge,
        }
        cid = cid_v1_from_json_obj(payload)

        rows["cid"].append(cid)
        rows["record_type"].append("edge")
        rows["graph_name"].append(graph_name)
        rows["source"].append(source)
        rows["generated_at"].append(generated_at)
        rows["node_id"].append("")
        rows["node_type"].append("")
        rows["label"].append("")
        rows["source_id"].append(_norm(edge.get("source")))
        rows["target_id"].append(_norm(edge.get("target")))
        rows["edge_type"].append(_norm(edge.get("type")))
        rows["properties_json"].append("{}")
        rows["payload_json"].append(json.dumps(edge, ensure_ascii=False, separators=(",", ":")))

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(rows, schema=schema)
    pq.write_table(table, output_parquet, compression="zstd")

    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "rows": len(rows["cid"]),
        "unique_cids": len(set(rows["cid"])),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert knowledge-graph JSON (nodes+edges) to Parquet with CID primary key"
    )
    parser.add_argument("--input", required=True, help="Path to KG JSON file")
    parser.add_argument("--output", required=True, help="Path to output Parquet")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")

    stats = convert_kg_json_to_parquet(input_path, output_path)
    print("done", json.dumps(stats, ensure_ascii=False))
    print(f"output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
