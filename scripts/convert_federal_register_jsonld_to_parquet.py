#!/usr/bin/env python3
"""Convert Federal Register JSON-LD dataset to Parquet with IPFS CID primary key.

This script streams `hasPart` entries from a large JSON-LD file using `jq`,
computes deterministic CIDs per document node, and writes one parquet row per
Federal Register document.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ipfs_datasets_py.utils.cid_utils import cid_for_obj


@dataclass
class Stats:
    rows_seen: int = 0
    rows_written: int = 0
    parse_errors: int = 0
    duplicate_cids: int = 0


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _row_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    source_org = doc.get("sourceOrganization") if isinstance(doc.get("sourceOrganization"), dict) else {}
    return {
        "ipfs_cid": cid_for_obj(doc),
        "jsonld_id": str(doc.get("@id") or ""),
        "identifier": str(doc.get("identifier") or ""),
        "name": str(doc.get("name") or ""),
        "document_type": str(doc.get("legislationType") or ""),
        "publication_date": str(doc.get("datePublished") or ""),
        "effective_date": str(doc.get("legislationDate") or ""),
        "agency_name": str(source_org.get("name") or ""),
        "url": str(doc.get("url") or ""),
        "text": str(doc.get("text") or ""),
        "additional_property_json": _json_dumps(doc.get("additionalProperty") or []),
        "source_organization_json": _json_dumps(source_org),
        "raw_json": _json_dumps(doc),
    }


def convert_jsonld_to_parquet(
    *,
    input_jsonld: Path,
    output_dir: Path,
    output_file: str = "laws.parquet",
    chunk_size: int = 10000,
    row_group_size: int = 71680,
) -> Stats:
    if not input_jsonld.exists():
        raise FileNotFoundError(f"Input not found: {input_jsonld}")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_laws = output_dir / output_file
    out_index = output_dir / "cid_index.parquet"

    stats = Stats()
    seen_cids = set()
    writer: Optional[pq.ParquetWriter] = None
    buffer: List[Dict[str, Any]] = []

    cmd = ["jq", "-c", ".hasPart[]", str(input_jsonld)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            stats.rows_seen += 1
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                stats.parse_errors += 1
                continue

            if not isinstance(doc, dict):
                stats.parse_errors += 1
                continue

            row = _row_from_doc(doc)
            cid = row["ipfs_cid"]
            if cid in seen_cids:
                stats.duplicate_cids += 1
                continue
            seen_cids.add(cid)

            buffer.append(row)
            if len(buffer) >= int(chunk_size):
                table = pa.Table.from_pylist(buffer)
                if writer is None:
                    writer = pq.ParquetWriter(str(out_laws), table.schema, compression="snappy", use_dictionary=True)
                writer.write_table(table, row_group_size=int(row_group_size))
                stats.rows_written += len(buffer)
                buffer.clear()

        if buffer:
            table = pa.Table.from_pylist(buffer)
            if writer is None:
                writer = pq.ParquetWriter(str(out_laws), table.schema, compression="snappy", use_dictionary=True)
            writer.write_table(table, row_group_size=int(row_group_size))
            stats.rows_written += len(buffer)
            buffer.clear()
    finally:
        if writer is not None:
            writer.close()

        stderr = ""
        if proc.stderr is not None:
            stderr = proc.stderr.read().strip()
        rc = proc.wait(timeout=30)
        if rc != 0:
            raise RuntimeError(f"jq failed with code {rc}: {stderr[:500]}")

    if stats.rows_written <= 0:
        raise RuntimeError("No rows written to parquet")

    # Build a small CID index parquet for quick point lookups.
    laws_table = pq.read_table(str(out_laws), columns=["ipfs_cid", "identifier", "publication_date"])
    pq.write_table(laws_table, str(out_index), compression="snappy", use_dictionary=True)

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert Federal Register JSON-LD to parquet with CID primary key")
    parser.add_argument(
        "--input-jsonld",
        default="data/federal_laws/federal_register/federal_register.jsonld",
        help="Path to Federal Register JSON-LD dataset",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path.home() / ".ipfs_datasets" / "federal_register" / "federal_register_parquet"),
        help="Destination directory for parquet outputs",
    )
    parser.add_argument(
        "--output-file",
        default="laws.parquet",
        help="Main parquet filename",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Rows per parquet write chunk",
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=71680,
        help="Target rows per parquet row group",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = convert_jsonld_to_parquet(
        input_jsonld=Path(args.input_jsonld).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        output_file=args.output_file,
        chunk_size=max(1000, int(args.chunk_size)),
        row_group_size=max(1000, int(args.row_group_size)),
    )

    out_dir = Path(args.output_dir).expanduser().resolve()
    print(f"rows_seen={stats.rows_seen}")
    print(f"rows_written={stats.rows_written}")
    print(f"parse_errors={stats.parse_errors}")
    print(f"duplicate_cids={stats.duplicate_cids}")
    print(f"row_group_size={max(1000, int(args.row_group_size))}")
    print(f"laws_parquet={out_dir / args.output_file}")
    print(f"cid_index_parquet={out_dir / 'cid_index.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
