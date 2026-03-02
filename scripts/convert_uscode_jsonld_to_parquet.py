#!/usr/bin/env python3
"""Convert US Code JSON-LD line records to Parquet with CID primary key.

Input format expected:
- Directory of files named like USCODE-YYYY-titleNN.jsonld
- Each file is newline-delimited JSON (one law record per line)

Output:
- Parquet file with one row per law record.
- `ipfs_cid` column computed from canonical JSON content and intended as the
  primary key for content-addressed lookup.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List

import pyarrow as pa
import pyarrow.parquet as pq

from ipfs_datasets_py.utils.cid_utils import cid_for_obj


TITLE_FILE_RE = re.compile(r"USCODE-(?P<year>\d+)-title(?P<title>\d+)\.jsonld$")


@dataclass
class ConversionStats:
    files_seen: int = 0
    rows_seen: int = 0
    rows_written: int = 0
    parse_errors: int = 0
    duplicate_cids: int = 0


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _iter_source_files(input_dir: Path) -> List[Path]:
    files = sorted(input_dir.glob("USCODE-*-title*.jsonld"))
    return [p for p in files if p.is_file()]


def _normalize_row(record: Dict[str, Any], *, source_file: Path, year_hint: str, title_hint: str) -> Dict[str, Any]:
    title_number = str(record.get("titleNumber") or record.get("title_number") or title_hint or "")
    section_number = str(record.get("sectionNumber") or record.get("section_number") or "")

    row: Dict[str, Any] = {
        "ipfs_cid": cid_for_obj(record),
        "title_number": title_number,
        "title_name": str(record.get("titleName") or record.get("title_name") or ""),
        "section_number": section_number,
        "law_name": str(record.get("name") or ""),
        "jsonld_id": str(record.get("@id") or ""),
        "date_modified": str(record.get("dateModified") or record.get("year") or year_hint or ""),
        "source_url": str(record.get("sourceUrl") or record.get("source_url") or ""),
        "text": str(record.get("text") or ""),
        "citations_json": _json_dumps(record.get("citations") or {}),
        "chapter_json": _json_dumps(record.get("chapter") or {}),
        "legislative_history_json": _json_dumps(record.get("legislativeHistory") or record.get("legislative_history") or {}),
        "subsections_json": _json_dumps(record.get("subsections") or []),
        "parser_warnings_json": _json_dumps(record.get("parser_warnings") or []),
        "is_part_of_json": _json_dumps(record.get("isPartOf") or {}),
        "raw_json": _json_dumps(record),
        "source_file": str(source_file),
    }
    return row


def _iter_rows(input_dir: Path, stats: ConversionStats) -> Iterator[Dict[str, Any]]:
    for source_file in _iter_source_files(input_dir):
        stats.files_seen += 1
        m = TITLE_FILE_RE.match(source_file.name)
        year_hint = m.group("year") if m else ""
        title_hint = m.group("title") if m else ""

        with source_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue

                stats.rows_seen += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    stats.parse_errors += 1
                    continue

                if not isinstance(record, dict):
                    stats.parse_errors += 1
                    continue

                yield _normalize_row(record, source_file=source_file, year_hint=year_hint, title_hint=title_hint)


def convert_uscode_jsonld_to_parquet(input_dir: Path, output_dir: Path, output_file: str = "laws.parquet") -> ConversionStats:
    stats = ConversionStats()
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = list(_iter_rows(input_dir, stats))

    unique_rows: List[Dict[str, Any]] = []
    seen_cids = set()
    for row in rows:
        cid = row["ipfs_cid"]
        if cid in seen_cids:
            stats.duplicate_cids += 1
            continue
        seen_cids.add(cid)
        unique_rows.append(row)

    if not unique_rows:
        raise RuntimeError("No rows produced from input JSON-LD files")

    table = pa.Table.from_pylist(unique_rows)
    pq.write_table(
        table,
        output_dir / output_file,
        compression="snappy",
        use_dictionary=True,
    )

    # Optional lightweight index for quick CID lookups without scanning full row payloads.
    cid_index = pa.Table.from_pylist(
        [{"ipfs_cid": r["ipfs_cid"], "title_number": r["title_number"], "section_number": r["section_number"]} for r in unique_rows]
    )
    pq.write_table(
        cid_index,
        output_dir / "cid_index.parquet",
        compression="snappy",
        use_dictionary=True,
    )

    stats.rows_written = len(unique_rows)
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert US Code JSON-LD to one-row-per-law parquet with IPFS CID key")
    parser.add_argument(
        "--input-dir",
        default=str(Path.home() / ".ipfs_datasets" / "us_code" / "uscode_jsonld"),
        help="Directory containing USCODE-*-title*.jsonld files",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path.home() / ".ipfs_datasets" / "us_code" / "uscode_parquet"),
        help="Destination directory for parquet output",
    )
    parser.add_argument(
        "--output-file",
        default="laws.parquet",
        help="Parquet filename for full law rows",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    stats = convert_uscode_jsonld_to_parquet(input_dir=input_dir, output_dir=output_dir, output_file=args.output_file)

    print(f"input_dir={input_dir}")
    print(f"output_dir={output_dir}")
    print(f"files_seen={stats.files_seen}")
    print(f"rows_seen={stats.rows_seen}")
    print(f"rows_written={stats.rows_written}")
    print(f"parse_errors={stats.parse_errors}")
    print(f"duplicate_cids={stats.duplicate_cids}")
    print(f"laws_parquet={output_dir / args.output_file}")
    print(f"cid_index_parquet={output_dir / 'cid_index.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
