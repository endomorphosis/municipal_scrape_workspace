#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import faiss
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def _to_float32_matrix(values: list) -> np.ndarray:
    matrix = np.array(values, dtype=np.float32)
    if matrix.ndim != 2:
        raise ValueError(f"embedding batch is not 2D (shape={matrix.shape})")
    return matrix


def build_index(
    input_parquet: Path,
    output_index: Path,
    output_metadata: Path,
    *,
    read_batch_size: int = 5000,
    normalize_vectors: bool = False,
) -> dict[str, int]:
    parquet_file = pq.ParquetFile(input_parquet)

    base_columns = [
        "cid",
        "oar_number",
        "section_number",
        "official_cite",
        "source_url",
        "semantic_text",
        "embedding_model",
        "embedding",
    ]

    dim = 0
    total_vectors = 0
    index: faiss.IndexFlatIP | None = None

    metadata_schema = pa.schema(
        [
            ("vector_id", pa.int64()),
            ("cid", pa.string()),
            ("oar_number", pa.string()),
            ("section_number", pa.string()),
            ("official_cite", pa.string()),
            ("source_url", pa.string()),
            ("semantic_text", pa.string()),
            ("embedding_model", pa.string()),
        ]
    )

    output_index.parent.mkdir(parents=True, exist_ok=True)
    output_metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata_writer = pq.ParquetWriter(str(output_metadata), metadata_schema, compression="zstd")

    for batch in parquet_file.iter_batches(batch_size=read_batch_size, columns=base_columns):
        rows = batch.to_pylist()
        if not rows:
            continue

        embeddings = [row.get("embedding") for row in rows if row.get("embedding")]
        kept_rows = [row for row in rows if row.get("embedding")]
        if not embeddings:
            continue

        matrix = _to_float32_matrix(embeddings)
        if normalize_vectors:
            faiss.normalize_L2(matrix)

        if index is None:
            dim = int(matrix.shape[1])
            index = faiss.IndexFlatIP(dim)

        index.add(matrix)

        metadata_rows = {
            "vector_id": list(range(total_vectors, total_vectors + len(kept_rows))),
            "cid": [str(r.get("cid") or "") for r in kept_rows],
            "oar_number": [str(r.get("oar_number") or "") for r in kept_rows],
            "section_number": [str(r.get("section_number") or "") for r in kept_rows],
            "official_cite": [str(r.get("official_cite") or "") for r in kept_rows],
            "source_url": [str(r.get("source_url") or "") for r in kept_rows],
            "semantic_text": [str(r.get("semantic_text") or "") for r in kept_rows],
            "embedding_model": [str(r.get("embedding_model") or "") for r in kept_rows],
        }
        metadata_writer.write_table(pa.Table.from_pydict(metadata_rows, schema=metadata_schema))

        total_vectors += len(kept_rows)
        print(f"indexed_vectors={total_vectors}")

    metadata_writer.close()

    if index is None:
        raise RuntimeError("no embeddings were found in input parquet")

    faiss.write_index(index, str(output_index))
    return {
        "vectors": total_vectors,
        "dimension": dim,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build FAISS index from embeddings parquet")
    parser.add_argument("--input-parquet", required=True)
    parser.add_argument("--output-index", required=True)
    parser.add_argument("--output-metadata", required=True)
    parser.add_argument("--read-batch-size", type=int, default=5000)
    parser.add_argument("--normalize-vectors", action="store_true")
    args = parser.parse_args()

    stats = build_index(
        input_parquet=Path(args.input_parquet),
        output_index=Path(args.output_index),
        output_metadata=Path(args.output_metadata),
        read_batch_size=args.read_batch_size,
        normalize_vectors=args.normalize_vectors,
    )
    print("done", json.dumps(stats, ensure_ascii=False))
    print(f"index={args.output_index}")
    print(f"metadata={args.output_metadata}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
