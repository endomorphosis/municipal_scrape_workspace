"""Regression checker for parser warnings in Oregon ORS JSON-LD outputs.

Exits with code 0 when no parser warnings are present.
Exits with code 1 when any parser warnings are found.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jsonld-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "parsed" / "jsonld",
        help="Directory containing chapter JSON-LD files",
    )
    parser.add_argument(
        "--show-examples",
        type=int,
        default=10,
        help="Number of section examples to print when warnings are found",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    jsonld_dir = args.jsonld_dir.resolve()

    if not jsonld_dir.is_dir():
        print(json.dumps({"error": "jsonld_dir_not_found", "jsonld_dir": str(jsonld_dir)}, indent=2))
        return 2

    files = sorted(jsonld_dir.glob("*.jsonld"))
    warning_counts: Counter[str] = Counter()
    examples: list[dict[str, object]] = []
    files_with_warnings = 0
    sections_with_warnings = 0
    sections_total = 0

    for path in files:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            warning_counts.update(["invalid_jsonld_file"])
            examples.append(
                {
                    "file": path.name,
                    "section": None,
                    "warnings": [f"invalid_jsonld_file: {exc}"],
                }
            )
            files_with_warnings += 1
            continue

        chapter_has_warnings = False
        for section in doc.get("hasPart", []):
            sections_total += 1
            warnings = section.get("parser_warnings") or []
            if not warnings:
                continue

            chapter_has_warnings = True
            sections_with_warnings += 1
            warning_counts.update(warnings)
            if len(examples) < max(args.show_examples, 0):
                examples.append(
                    {
                        "file": path.name,
                        "section": section.get("identifier") or section.get("@id"),
                        "warnings": warnings,
                    }
                )

        if chapter_has_warnings:
            files_with_warnings += 1

    payload = {
        "jsonld_dir": str(jsonld_dir),
        "files_total": len(files),
        "files_with_warnings": files_with_warnings,
        "sections_total": sections_total,
        "sections_with_warnings": sections_with_warnings,
        "warning_breakdown": warning_counts.most_common(),
        "examples": examples,
    }
    print(json.dumps(payload, indent=2))

    return 1 if files_with_warnings or sections_with_warnings else 0


if __name__ == "__main__":
    raise SystemExit(main())
