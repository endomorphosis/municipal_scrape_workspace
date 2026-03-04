#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sentence_transformers import SentenceTransformer


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except Exception:
            return {}
    return {}


def build_semantic_text(row: dict[str, Any]) -> str:
    parts: list[str] = []

    for key in ("official_cite", "section_number", "section_name", "short_title", "chapter_name", "title_name"):
        value = _norm(row.get(key))
        if value:
            parts.append(value)

    summary = _norm(row.get("summary"))
    if summary:
        parts.append(summary)

    full_text = _norm(row.get("full_text"))
    if full_text:
        parts.append(full_text)

    structured = _json_dict(row.get("structured_data"))
    for key in ("preamble", "authority", "implemented"):
        value = _norm(structured.get(key))
        if value:
            parts.append(value)

    return "\n".join(parts).strip()


def build_embeddings_parquet(
    input_parquet: Path,
    output_parquet: Path,
    model_name: str,
    batch_size: int,
) -> dict[str, Any]:
    table = pq.read_table(
        input_parquet,
        columns=[
            "cid",
            "oar_number",
            "section_number",
            "official_cite",
            "source_url",
            "section_name",
            "short_title",
            "chapter_name",
            "title_name",
            "summary",
            "full_text",
            "structured_data",
        ],
    )
    rows = table.to_pylist()

    model = SentenceTransformer(model_name)

    out_schema = pa.schema(
        [
            ("cid", pa.string()),
            ("oar_number", pa.string()),
            ("section_number", pa.string()),
            ("official_cite", pa.string()),
            ("source_url", pa.string()),
            ("semantic_text", pa.string()),
            ("embedding_model", pa.string()),
            ("embedding", pa.list_(pa.float32())),
        ]
    )

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(str(output_parquet), out_schema, compression="zstd")

    total_rows = 0
    total_embedded = 0
    cids: set[str] = set()
    dim = 0

    for start in range(0, len(rows), batch_size):
        batch_rows = rows[start : start + batch_size]

        semantic_texts: list[str] = []
        meta_rows: list[dict[str, str]] = []

        for row in batch_rows:
            total_rows += 1
            cid = _norm(row.get("cid"))
            if not cid:
                continue
            if cid in cids:
                continue

            semantic_text = build_semantic_text(row)
            if not semantic_text:
                continue

            cids.add(cid)
            semantic_texts.append(semantic_text)
            meta_rows.append(
                {
                    "cid": cid,
                    "oar_number": _norm(row.get("oar_number")),
                    "section_number": _norm(row.get("section_number")),
                    "official_cite": _norm(row.get("official_cite")),
                    "source_url": _norm(row.get("source_url")),
                }
            )

        if not semantic_texts:
            continue

        embeddings = model.encode(
            semantic_texts,
            batch_size=min(256, len(semantic_texts)),
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        ).astype(np.float32)

        if dim == 0 and embeddings.size > 0:
            dim = int(embeddings.shape[1])

        out = {
            "cid": [m["cid"] for m in meta_rows],
            "oar_number": [m["oar_number"] for m in meta_rows],
            "section_number": [m["section_number"] for m in meta_rows],
            "official_cite": [m["official_cite"] for m in meta_rows],
            "source_url": [m["source_url"] for m in meta_rows],
            "semantic_text": semantic_texts,
            "embedding_model": [model_name] * len(meta_rows),
            "embedding": [vec.tolist() for vec in embeddings],
        }

        writer.write_table(pa.Table.from_pydict(out, schema=out_schema))
        total_embedded += len(meta_rows)
        print(f"embedded_rows={total_embedded}")

    writer.close()

    return {
        "source_rows": total_rows,
        "embedded_rows": total_embedded,
        "unique_cids": len(cids),
        "embedding_dim": dim,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build OAR semantic embeddings parquet keyed by CID")
    parser.add_argument("--input", required=True, help="Input OAR parquet path")
    parser.add_argument("--output", required=True, help="Output embeddings parquet path")
    parser.add_argument("--model", default="thenlper/gte-small", help="SentenceTransformer model")
    parser.add_argument("--batch-size", type=int, default=2000, help="Source row batch size")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")

    stats = build_embeddings_parquet(
        input_parquet=input_path,
        output_parquet=output_path,
        model_name=args.model,
        batch_size=args.batch_size,
    )
    print("done", json.dumps(stats, ensure_ascii=False))
    print(f"output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
