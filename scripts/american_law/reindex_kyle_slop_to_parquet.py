#!/usr/bin/env python3
"""Reindex Kyle's american_municipal_law parquet dump into HF-friendly shards.

This script is intentionally dependency-light (pyarrow only) and focuses on:
- Dropping pandas artifact column: __index_level_0__
- Normalizing a few problematic types (notably all-null citation fields)
- Adding `gnis` derived from filename to html/citation tables
- Optionally enriching html rows with (place_name, state_code) from metadata JSON
- Writing larger Parquet shards to reduce file-count for downstream ingestion

Input layout (as checked into this repo):
  data/kyle_slop/american_municipal_law/american_law/
    data/*.parquet
    metadata/*.json

Output layout (default):
  datasets/american_law_parquet/
    places.parquet
    html/part-00000.parquet ...
    citation/part-00000.parquet ...
    embeddings/part-00000.parquet ...

Notes:
- This does NOT regenerate embeddings; it only repackages existing data.
- By default it skips `test_embeddings.parquet`.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


PANDAS_INDEX_COL = "__index_level_0__"


@dataclass(frozen=True)
class PlaceMeta:
    gnis: str
    place_name: str | None
    state_code: str | None
    total_sections: int | None
    last_updated_ms: int | None


def _read_place_metadata(metadata_dir: Path) -> dict[str, PlaceMeta]:
    out: dict[str, PlaceMeta] = {}
    for p in sorted(metadata_dir.glob("*.json")):
        gnis = p.stem
        try:
            data = json.loads(p.read_text())
        except Exception:
            data = {}
        out[gnis] = PlaceMeta(
            gnis=gnis,
            place_name=data.get("place_name"),
            state_code=data.get("state_code"),
            total_sections=data.get("total_sections"),
            last_updated_ms=data.get("last_updated"),
        )
    return out


def _write_places_parquet(place_meta: dict[str, PlaceMeta], output_path: Path) -> None:
    rows = []
    for m in place_meta.values():
        rows.append(
            {
                "gnis": m.gnis,
                "place_name": m.place_name,
                "state_code": m.state_code,
                "total_sections": m.total_sections,
                "last_updated_ms": m.last_updated_ms,
            }
        )

    table = pa.Table.from_pylist(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="zstd")


def _drop_pandas_index(table: pa.Table) -> pa.Table:
    if PANDAS_INDEX_COL in table.column_names:
        return table.drop([PANDAS_INDEX_COL])
    return table


def _add_const_string_col(table: pa.Table, name: str, value: str | None) -> pa.Table:
    arr = pa.array([value] * table.num_rows, type=pa.string())
    return table.append_column(name, arr)


def _ensure_nullable_string(table: pa.Table, column_name: str) -> pa.Table:
    if column_name not in table.column_names:
        return table
    col = table[column_name]
    if pa.types.is_null(col.type):
        arr = pa.array([None] * table.num_rows, type=pa.string())
        idx = table.column_names.index(column_name)
        return table.set_column(idx, column_name, arr)
    if pa.types.is_string(col.type):
        return table
    try:
        casted = pc.cast(col, pa.string())
        idx = table.column_names.index(column_name)
        return table.set_column(idx, column_name, casted)
    except Exception:
        return table


def _cast_int32(table: pa.Table, column_name: str) -> pa.Table:
    if column_name not in table.column_names:
        return table
    col = table[column_name]
    if pa.types.is_int32(col.type):
        return table
    try:
        casted = pc.cast(col, pa.int32())
        idx = table.column_names.index(column_name)
        return table.set_column(idx, column_name, casted)
    except Exception:
        return table


def _cast_list_float32(table: pa.Table, column_name: str) -> pa.Table:
    if column_name not in table.column_names:
        return table
    col = table[column_name]
    target = pa.list_(pa.float32())
    if col.type == target:
        return table
    try:
        casted = pc.cast(col, target)
        idx = table.column_names.index(column_name)
        return table.set_column(idx, column_name, casted)
    except Exception:
        return table


def _infer_gnis_from_filename(p: Path) -> str:
    # expected: {gnis}_{kind}.parquet
    stem = p.stem
    if stem.endswith("_html"):
        return stem[: -len("_html")]
    if stem.endswith("_citation"):
        return stem[: -len("_citation")]
    if stem.endswith("_embeddings"):
        return stem[: -len("_embeddings")]
    raise ValueError(f"Unrecognized parquet name: {p.name}")


def _iter_inputs(data_dir: Path, kind: str, include_test: bool) -> Iterable[Path]:
    suffix = f"_{kind}.parquet" if kind != "embeddings" else "_embeddings.parquet"
    if kind == "html":
        suffix = "_html.parquet"
    elif kind == "citation":
        suffix = "_citation.parquet"

    for p in sorted(data_dir.glob(f"*{suffix}")):
        if not include_test and p.name == "test_embeddings.parquet":
            continue
        yield p


def _normalize_table(
    *,
    kind: str,
    table: pa.Table,
    gnis: str,
    place_meta: dict[str, PlaceMeta],
    enrich_places: bool,
    cast_embedding_float32: bool,
) -> pa.Table:
    table = _drop_pandas_index(table)

    # Attach gnis where absent (html + citation need it; embeddings already has it but we keep)
    if "gnis" not in table.column_names:
        table = _add_const_string_col(table, "gnis", gnis)

    if kind == "html":
        table = _cast_int32(table, "doc_order")
        if enrich_places:
            meta = place_meta.get(gnis)
            table = _add_const_string_col(table, "place_name", meta.place_name if meta else None)
            table = _add_const_string_col(table, "state_code", meta.state_code if meta else None)

    if kind == "citation":
        # Kyle's citations currently encode some fields as all-null columns.
        for col in ("ordinance", "section", "enacted", "year"):
            table = _ensure_nullable_string(table, col)
        if enrich_places:
            # citations already have place_name/state_code but we keep them as-is.
            pass

    if kind == "embeddings":
        table = _cast_int32(table, "text_chunk_order")
        if cast_embedding_float32:
            table = _cast_list_float32(table, "embedding")

    return table


class ShardedParquetWriter:
    def __init__(
        self,
        output_dir: Path,
        base_name: str,
        shard_size_bytes: int,
        compression: str = "zstd",
    ) -> None:
        self.output_dir = output_dir
        self.base_name = base_name
        self.shard_size_bytes = shard_size_bytes
        self.compression = compression

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._writer: pq.ParquetWriter | None = None
        self._schema: pa.Schema | None = None
        self._shard_index = 0
        self._bytes_in_shard = 0

    def _next_path(self) -> Path:
        return self.output_dir / f"{self.base_name}-{self._shard_index:05d}.parquet"

    def _open(self, schema: pa.Schema) -> None:
        path = self._next_path()
        self._writer = pq.ParquetWriter(path, schema=schema, compression=self.compression)
        self._schema = schema
        self._bytes_in_shard = 0

    def write(self, table: pa.Table) -> None:
        if self._writer is None:
            self._open(table.schema)

        assert self._schema is not None
        if table.schema != self._schema:
            table = table.cast(self._schema)

        # Roll shard if needed (avoid lots of small shards; only roll if current shard already has some data)
        estimated = table.nbytes
        if self._bytes_in_shard > 0 and (self._bytes_in_shard + estimated) >= self.shard_size_bytes:
            self.close()
            self._shard_index += 1
            self._open(self._schema)

        assert self._writer is not None
        self._writer.write_table(table)
        self._bytes_in_shard += estimated

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input-root",
        type=Path,
        default=Path("data/kyle_slop/american_municipal_law/american_law"),
        help="Root containing data/ and metadata/",
    )
    ap.add_argument(
        "--output-root",
        type=Path,
        default=Path("datasets/american_law_parquet"),
        help="Output folder for normalized parquet shards",
    )
    ap.add_argument(
        "--shard-size-mb",
        type=int,
        default=512,
        help="Target shard size (approx, based on Arrow table.nbytes)",
    )
    ap.add_argument(
        "--include-test",
        action="store_true",
        help="Include test parquet like test_embeddings.parquet",
    )
    ap.add_argument(
        "--no-enrich-places",
        action="store_true",
        help="Do not add (place_name,state_code) columns to html output",
    )
    ap.add_argument(
        "--no-embedding-float32",
        action="store_true",
        help="Keep embeddings as float64 (bigger), do not cast to float32",
    )
    ap.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="For debugging: process only the first N files per kind (0=all)",
    )
    args = ap.parse_args()

    input_root: Path = args.input_root
    data_dir = input_root / "data"
    metadata_dir = input_root / "metadata"

    if not data_dir.exists():
        raise SystemExit(f"Missing {data_dir}")
    if not metadata_dir.exists():
        raise SystemExit(f"Missing {metadata_dir}")

    output_root: Path = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)

    shard_size_bytes = args.shard_size_mb * 1024 * 1024

    place_meta = _read_place_metadata(metadata_dir)
    _write_places_parquet(place_meta, output_root / "places.parquet")

    enrich_places = not args.no_enrich_places
    cast_embedding_float32 = not args.no_embedding_float32

    for kind in ("html", "citation", "embeddings"):
        writer = ShardedParquetWriter(
            output_dir=output_root / kind,
            base_name="part",
            shard_size_bytes=shard_size_bytes,
        )
        n = 0
        for p in _iter_inputs(data_dir, kind, include_test=args.include_test):
            if args.limit_files and n >= args.limit_files:
                break
            gnis = _infer_gnis_from_filename(p) if p.name != "test_embeddings.parquet" else "test"
            table = pq.read_table(p)
            table = _normalize_table(
                kind=kind,
                table=table,
                gnis=gnis,
                place_meta=place_meta,
                enrich_places=enrich_places,
                cast_embedding_float32=cast_embedding_float32,
            )
            writer.write(table)
            n += 1
        writer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
