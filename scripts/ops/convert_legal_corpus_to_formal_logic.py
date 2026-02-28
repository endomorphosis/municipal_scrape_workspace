#!/usr/bin/env python3
"""Convert legal corpus documents into formal logic artifacts.

This utility is designed as a scalable bridge from corpus text (JSON-LD/JSON/JSONL/TXT)
into theorem-ready outputs. It performs:
1) text extraction and normalization,
2) sentence/chunk segmentation,
3) deontic + FOL conversion with non-deprecated converters,
4) theorem-candidate emission, and
5) optional theorem-store ingestion.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from ipfs_datasets_py.logic.deontic.converter import DeonticConverter
from ipfs_datasets_py.logic.fol.converter import FOLConverter
from ipfs_datasets_py.logic.integration.domain.temporal_deontic_api import (
    add_theorem_from_parameters,
)


@dataclass
class Segment:
    source_path: str
    source_id: str
    text: str


@dataclass
class ConversionRecord:
    source_path: str
    source_id: str
    text: str
    deontic_success: bool
    deontic_operator: Optional[str]
    deontic_formula: Optional[str]
    deontic_confidence: float
    deontic_errors: List[str]
    fol_success: bool
    fol_formula: Optional[str]
    fol_confidence: float
    fol_errors: List[str]
    theorem_candidate: Optional[Dict[str, Any]]
    theorem_ingest: Optional[Dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert legal corpus documents into theorem-ready formal logic outputs.",
    )
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Input paths (files and/or directories).",
    )
    parser.add_argument(
        "--glob",
        default="**/*",
        help="Glob pattern used when an input path is a directory (default: **/*).",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Optional file limit for quick test runs (0 = no limit).",
    )
    parser.add_argument(
        "--limit-segments",
        type=int,
        default=0,
        help="Optional segment limit after chunking (0 = no limit).",
    )
    parser.add_argument(
        "--max-sentences-per-segment",
        type=int,
        default=2,
        help="Maximum sentences per segment for conversion quality.",
    )
    parser.add_argument(
        "--max-chars-per-segment",
        type=int,
        default=420,
        help="Hard cap on segment size; long sentences are split recursively.",
    )
    parser.add_argument(
        "--jurisdiction",
        default="Federal",
        help="Theorem metadata jurisdiction (default: Federal).",
    )
    parser.add_argument(
        "--legal-domain",
        default="general",
        help="Theorem metadata legal domain (default: general).",
    )
    parser.add_argument(
        "--source-case",
        default="Corpus Conversion",
        help="Theorem metadata source case/document label.",
    )
    parser.add_argument(
        "--precedent-strength",
        type=float,
        default=0.7,
        help="Theorem metadata precedent strength (default: 0.7).",
    )
    parser.add_argument(
        "--add-to-theorem-store",
        action="store_true",
        help="If set, ingest theorem candidates into temporal deontic theorem store.",
    )
    parser.add_argument(
        "--deontic-use-ml",
        action="store_true",
        help="Enable ML confidence scoring in DeonticConverter (off by default for stability).",
    )
    parser.add_argument(
        "--fol-use-ml",
        action="store_true",
        help="Enable ML confidence scoring in FOLConverter (off by default for stability).",
    )
    parser.add_argument(
        "--output-json",
        default="artifacts/federal_laws/corpus_formal_logic_conversion_report.json",
        help="Path to JSON report output.",
    )
    parser.add_argument(
        "--output-jsonl",
        default="artifacts/federal_laws/corpus_formal_logic_conversion_records.jsonl",
        help="Path to per-record JSONL output.",
    )
    return parser.parse_args()


def iter_input_files(paths: Sequence[str], pattern: str) -> Iterator[Path]:
    for raw in paths:
        p = Path(raw)
        if p.is_file():
            yield p
            continue
        if p.is_dir():
            for child in sorted(p.glob(pattern)):
                if child.is_file():
                    yield child


def _normalize_text(text: str) -> str:
    # Normalize punctuation variants and whitespace for parser stability.
    text = text.replace("\u2014", " - ").replace("\u2013", " - ")
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = text.replace(";", ". ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_texts(obj: Any, path_prefix: str = "root") -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    if isinstance(obj, dict):
        for key in ("text", "preamble", "name", "title", "description"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                rows.append((f"{path_prefix}.{key}", _normalize_text(val)))
        has_part = obj.get("hasPart")
        if isinstance(has_part, list):
            for i, child in enumerate(has_part):
                rows.extend(_extract_texts(child, f"{path_prefix}.hasPart[{i}]"))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            rows.extend(_extract_texts(item, f"{path_prefix}[{i}]"))
    elif isinstance(obj, str) and obj.strip():
        rows.append((path_prefix, _normalize_text(obj)))
    return rows


def _split_sentences(text: str) -> List[str]:
    parts = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
    if not parts and text.strip():
        return [text.strip()]
    return parts


def _split_long_piece(piece: str, max_chars: int) -> List[str]:
    if len(piece) <= max_chars:
        return [piece]
    chunks: List[str] = []
    words = piece.split()
    acc: List[str] = []
    for w in words:
        trial = " ".join(acc + [w]).strip()
        if acc and len(trial) > max_chars:
            chunks.append(" ".join(acc).strip())
            acc = [w]
        else:
            acc.append(w)
    if acc:
        chunks.append(" ".join(acc).strip())
    return [c for c in chunks if c]


def chunk_text(
    source_path: str,
    source_id: str,
    text: str,
    max_sentences: int,
    max_chars: int,
) -> List[Segment]:
    sentences = _split_sentences(text)
    chunks: List[str] = []
    current: List[str] = []

    for s in sentences:
        current.append(s)
        trial = " ".join(current).strip()
        if len(current) >= max_sentences or len(trial) > max_chars:
            chunks.append(trial)
            current = []
    if current:
        chunks.append(" ".join(current).strip())

    normalized_chunks: List[str] = []
    for chunk in chunks:
        normalized_chunks.extend(_split_long_piece(chunk, max_chars=max_chars))

    segments: List[Segment] = []
    for i, c in enumerate(normalized_chunks, start=1):
        if c:
            segments.append(
                Segment(
                    source_path=source_path,
                    source_id=f"{source_id}#seg{i}",
                    text=c,
                )
            )
    return segments


def load_segments_from_file(path: Path, max_sentences: int, max_chars: int) -> List[Segment]:
    ext = path.suffix.lower()
    if ext not in {".json", ".jsonld", ".jsonl", ".txt", ".md"}:
        return []
    raw = path.read_text(encoding="utf-8", errors="ignore")

    extracted: List[Tuple[str, str]] = []
    if ext in {".json", ".jsonld"}:
        data = json.loads(raw)
        extracted = _extract_texts(data)
    elif ext == ".jsonl":
        for i, line in enumerate(raw.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                extracted.extend(_extract_texts(data, f"line[{i}]"))
            except json.JSONDecodeError:
                extracted.append((f"line[{i}]", _normalize_text(line)))
    elif ext in {".txt", ".md"}:
        extracted = [("text", _normalize_text(raw))]
    else:
        return []

    segments: List[Segment] = []
    for src_id, text in extracted:
        segments.extend(
            chunk_text(
                source_path=str(path),
                source_id=src_id,
                text=text,
                max_sentences=max_sentences,
                max_chars=max_chars,
            )
        )
    return segments


def operator_for_theorem(op_name: Optional[str]) -> str:
    if not op_name:
        return "OBLIGATION"
    name = op_name.upper()
    if name in {"OBLIGATION", "PERMISSION", "PROHIBITION"}:
        return name
    if name in {"O", "P", "F"}:
        return {"O": "OBLIGATION", "P": "PERMISSION", "F": "PROHIBITION"}[name]
    return "OBLIGATION"


async def maybe_ingest_theorem(
    enabled: bool,
    theorem_candidate: Optional[Dict[str, Any]],
    jurisdiction: str,
    legal_domain: str,
    source_case: str,
    precedent_strength: float,
) -> Optional[Dict[str, Any]]:
    if not enabled or theorem_candidate is None:
        return None

    params = {
        "operator": theorem_candidate["operator"],
        "proposition": theorem_candidate["proposition"],
        "agent_name": theorem_candidate.get("agent_name", "Unspecified Party"),
        "jurisdiction": jurisdiction,
        "legal_domain": legal_domain,
        "source_case": source_case,
        "precedent_strength": precedent_strength,
    }
    return await add_theorem_from_parameters(params)


def theorem_ingestion_preflight(enabled: bool) -> Tuple[bool, Optional[str]]:
    if not enabled:
        return False, None
    try:
        __import__("numpy")
    except Exception as exc:
        return False, f"Theorem ingestion disabled: missing dependency ({exc})."
    return True, None


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    input_files = list(iter_input_files(args.input, args.glob))
    if args.limit_files > 0:
        input_files = input_files[: args.limit_files]

    deontic = DeonticConverter(
        use_cache=True,
        use_ml=bool(args.deontic_use_ml),
        enable_monitoring=False,
        jurisdiction="us",
        document_type="statute",
    )
    fol = FOLConverter(
        use_cache=True,
        use_ml=bool(args.fol_use_ml),
        use_nlp=True,
        enable_monitoring=False,
    )

    segments: List[Segment] = []
    for f in input_files:
        segments.extend(
            load_segments_from_file(
                f,
                max_sentences=args.max_sentences_per_segment,
                max_chars=args.max_chars_per_segment,
            )
        )

    if args.limit_segments > 0:
        segments = segments[: args.limit_segments]

    records: List[ConversionRecord] = []
    theorem_candidates = 0
    ingested_theorems = 0
    theorem_ingest_enabled, theorem_ingest_blocker = theorem_ingestion_preflight(
        args.add_to_theorem_store
    )

    for seg in segments:
        d_res = deontic.convert(seg.text)
        f_res = fol.convert(seg.text)

        d_formula = d_res.output if d_res.success and d_res.output is not None else None
        f_formula = f_res.output if f_res.success and f_res.output is not None else None

        operator_name = None
        deontic_formula_string = None
        theorem_candidate = None
        if d_formula is not None:
            operator_name = getattr(getattr(d_formula, "operator", None), "name", None)
            deontic_formula_string = d_formula.to_fol_string()
            proposition = getattr(d_formula, "proposition", "") or ""
            if proposition.strip():
                theorem_candidate = {
                    "operator": operator_for_theorem(operator_name),
                    "proposition": proposition,
                    "agent_name": (
                        d_formula.agent.name if getattr(d_formula, "agent", None) else "Unspecified Party"
                    ),
                    "source_text": seg.text,
                }
                theorem_candidates += 1

        theorem_ingest = await maybe_ingest_theorem(
            enabled=theorem_ingest_enabled,
            theorem_candidate=theorem_candidate,
            jurisdiction=args.jurisdiction,
            legal_domain=args.legal_domain,
            source_case=args.source_case,
            precedent_strength=args.precedent_strength,
        )
        if theorem_ingest and theorem_ingest.get("success"):
            ingested_theorems += 1

        rec = ConversionRecord(
            source_path=seg.source_path,
            source_id=seg.source_id,
            text=seg.text,
            deontic_success=d_res.success,
            deontic_operator=operator_name,
            deontic_formula=deontic_formula_string,
            deontic_confidence=float(d_res.confidence),
            deontic_errors=list(d_res.errors),
            fol_success=f_res.success,
            fol_formula=f_formula.formula_string if f_formula is not None else None,
            fol_confidence=float(f_res.confidence),
            fol_errors=list(f_res.errors),
            theorem_candidate=theorem_candidate,
            theorem_ingest=theorem_ingest,
        )
        records.append(rec)

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl = Path(args.output_jsonl)
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)

    with out_jsonl.open("w", encoding="utf-8") as fp:
        for r in records:
            fp.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")

    deontic_success_count = sum(1 for r in records if r.deontic_success)
    fol_success_count = sum(1 for r in records if r.fol_success)

    summary = {
        "inputs": [str(p) for p in input_files],
        "input_file_count": len(input_files),
        "segment_count": len(segments),
        "deontic_success_count": deontic_success_count,
        "fol_success_count": fol_success_count,
        "theorem_candidate_count": theorem_candidates,
        "theorems_ingested_count": ingested_theorems,
        "add_to_theorem_store": bool(args.add_to_theorem_store),
        "theorem_ingestion_enabled": theorem_ingest_enabled,
        "theorem_ingestion_blocker": theorem_ingest_blocker,
        "output_json": str(out_json),
        "output_jsonl": str(out_jsonl),
    }
    report = {
        "summary": summary,
        "records": [asdict(r) for r in records],
    }
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return summary


def main() -> None:
    args = parse_args()
    summary = asyncio.run(run(args))
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
