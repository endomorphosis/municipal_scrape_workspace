#!/usr/bin/env python3
"""Convert canonical state court-rules JSONLD into CID-keyed parquet shards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _bootstrap_pythonpath() -> None:
    root = _repo_root()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


def _default_input_dir() -> Path:
    return _repo_root() / "artifacts" / "state_court_rules" / "canonical_merged_20260311_071728" / "state_court_rules_jsonld"


def _default_output_dir() -> Path:
    return _default_input_dir().parent / "state_court_rules_parquet_cid"


def convert(input_dir: Path, output_dir: Path, combined_filename: str) -> Dict[str, Any]:
    _bootstrap_pythonpath()
    from scripts.ops.legal_data.convert_state_admin_jsonld_to_parquet_with_cid import convert as base_convert

    return base_convert(
        input_dir=input_dir,
        output_dir=output_dir,
        combined_filename=combined_filename,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert state court-rules JSONLD to CID-keyed parquet")
    parser.add_argument("--input-dir", default=str(_default_input_dir()))
    parser.add_argument("--output-dir", default=str(_default_output_dir()))
    parser.add_argument("--combined-filename", default="state_court_rules_all_states.parquet")
    args = parser.parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input dir not found: {input_dir}")

    manifest = convert(
        input_dir=input_dir,
        output_dir=output_dir,
        combined_filename=str(args.combined_filename),
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())