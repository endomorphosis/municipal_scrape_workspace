#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import faiss
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sentence_transformers import SentenceTransformer


SEMANTIC_JSONLD_KEYS = (
    "text",
    "description",
    "summary",
    "abstract",
    "preamble",
)


def extract_semantic_text(
    name: str,
    legislation_type: str,
    agency: str,
    jsonld_str: str,
) -> str:
    parts: list[str] = []

    if name:
        parts.append(name.strip())
    if legislation_type:
        parts.append(f"Type: {legislation_type.strip()}")
    if agency:
        parts.append(f"Agency: {agency.strip()}")

    if jsonld_str:
        try:
            obj = json.loads(jsonld_str)
        except Exception:
            obj = {}
        for key in SEMANTIC_JSONLD_KEYS:
            value = obj.get(key)
            if isinstance(value, str):
                value = value.strip()
                if value:
                    parts.append(value)

    return "\n".join(parts).strip()


def write_metadata_rows(writer: pq.ParquetWriter, rows: dict[str, list]) -> None:
    table = pa.Table.from_pydict(
        rows,
        schema=pa.schema(
            [
                ("vector_id", pa.int64()),
                ("cid", pa.string()),
                ("identifier", pa.string()),
                ("name", pa.string()),
                ("agency", pa.string()),
                ("legislation_type", pa.string()),
                ("date_published", pa.string()),
                ("semantic_text", pa.string()),
            ]
        ),
    )
    writer.write_table(table)


def iter_parquet_batches(parquet_path: Path, batch_size: int) -> Iterable[pa.RecordBatch]:
    parquet_file = pq.ParquetFile(parquet_path)
    return parquet_file.iter_batches(
        batch_size=batch_size,
        columns=[
            "cid",
            "identifier",
            "name",
            "agency",
            "legislation_type",
            "date_published",
            "jsonld",
        ],
    )


def build_index(
    input_parquet: Path,
    output_index: Path,
    output_metadata: Path,
    model_name: str,
    encode_batch_size: int,
    read_batch_size: int,
) -> None:
    model = SentenceTransformer(model_name)

    # Determine embedding dimension once.
    probe = model.encode(["probe"], normalize_embeddings=True)
    dim = int(probe.shape[1])

    index = faiss.IndexFlatIP(dim)

    output_index.parent.mkdir(parents=True, exist_ok=True)
    output_metadata.parent.mkdir(parents=True, exist_ok=True)

    metadata_schema = pa.schema(
        [
            ("vector_id", pa.int64()),
            ("cid", pa.string()),
            ("identifier", pa.string()),
            ("name", pa.string()),
            ("agency", pa.string()),
            ("legislation_type", pa.string()),
            ("date_published", pa.string()),
            ("semantic_text", pa.string()),
        ]
    )
    metadata_writer = pq.ParquetWriter(str(output_metadata), metadata_schema, compression="zstd")

    total_vectors = 0

    for record_batch in iter_parquet_batches(input_parquet, batch_size=read_batch_size):
        rows = record_batch.to_pylist()

        semantic_texts: list[str] = []
        meta = {
            "vector_id": [],
            "cid": [],
            "identifier": [],
            "name": [],
            "agency": [],
            "legislation_type": [],
            "date_published": [],
            "semantic_text": [],
        }

        for row in rows:
            cid = str(row.get("cid") or "").strip()
            if not cid:
                continue

            name = str(row.get("name") or "")
            agency = str(row.get("agency") or "")
            legislation_type = str(row.get("legislation_type") or "")
            date_published = str(row.get("date_published") or "")
            identifier = str(row.get("identifier") or "")
            jsonld_str = str(row.get("jsonld") or "")

            semantic_text = extract_semantic_text(
                name=name,
                legislation_type=legislation_type,
                agency=agency,
                jsonld_str=jsonld_str,
            )
            if not semantic_text:
                continue

            semantic_texts.append(semantic_text)
            meta["vector_id"].append(total_vectors + len(meta["vector_id"]))
            meta["cid"].append(cid)
            meta["identifier"].append(identifier)
            meta["name"].append(name)
            meta["agency"].append(agency)
            meta["legislation_type"].append(legislation_type)
            meta["date_published"].append(date_published)
            meta["semantic_text"].append(semantic_text)

        if not semantic_texts:
            continue

        vectors = model.encode(
            semantic_texts,
            batch_size=encode_batch_size,
            show_progress_bar=False,
            normalize_embeddings=True,
            convert_to_numpy=True,
        ).astype(np.float32)

        index.add(vectors)
        write_metadata_rows(metadata_writer, meta)

        total_vectors += vectors.shape[0]
        print(f"indexed_vectors={total_vectors}")

    metadata_writer.close()
    faiss.write_index(index, str(output_index))

    print(f"done vectors={total_vectors}")
    print(f"index={output_index}")
    print(f"metadata={output_metadata}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Federal Register vector index using gte-small")
    parser.add_argument("--input-parquet", required=True)
    parser.add_argument("--output-index", required=True)
    parser.add_argument("--output-metadata", required=True)
    parser.add_argument("--model", default="thenlper/gte-small", help="Embedding model (default: thenlper/gte-small)")
    parser.add_argument("--encode-batch-size", type=int, default=256)
    parser.add_argument("--read-batch-size", type=int, default=5000)
    args = parser.parse_args()

    build_index(
        input_parquet=Path(args.input_parquet),
        output_index=Path(args.output_index),
        output_metadata=Path(args.output_metadata),
        model_name=args.model,
        encode_batch_size=args.encode_batch_size,
        read_batch_size=args.read_batch_size,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
