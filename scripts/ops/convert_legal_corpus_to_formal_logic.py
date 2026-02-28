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
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from ipfs_datasets_py.logic.deontic.converter import DeonticConverter
from ipfs_datasets_py.logic.fol.converter import FOLConverter
from ipfs_datasets_py.logic.integration.domain.temporal_deontic_api import (
    add_theorem_from_parameters,
)
from ipfs_datasets_py.optimizers.logic_theorem_optimizer.prompt_optimizer import (
    OptimizationStrategy as PromptOptimizationStrategy,
    PromptOptimizer,
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
    structured_role_tuple: Optional[Dict[str, Any]]
    tdfol_success: bool
    tdfol_formula: Optional[str]
    tdfol_formula_origin: Optional[str]
    tdfol_errors: List[str]
    cec_bridge_success: bool
    cec_bridge_formula: Optional[str]
    cec_bridge_formula_origin: Optional[str]
    cec_compile_success: bool
    cec_formula_count: int
    cec_errors: List[str]
    deontic_roundtrip_text: Optional[str]
    fol_roundtrip_text: Optional[str]
    tdfol_roundtrip_text: Optional[str]
    cec_bridge_roundtrip_text: Optional[str]
    cec_compile_roundtrip_text: Optional[str]
    semantic_similarity_deontic: Optional[float]
    semantic_similarity_fol: Optional[float]
    semantic_similarity_tdfol: Optional[float]
    semantic_similarity_cec_bridge: Optional[float]
    semantic_similarity_cec_compile: Optional[float]
    theorem_filter_passed: bool
    theorem_filter_reasons: List[str]
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
        "--enable-clause-decomposition",
        action="store_true",
        help="Decompose long legal segments into smaller normative clauses before conversion.",
    )
    parser.add_argument(
        "--clause-min-chars",
        type=int,
        default=45,
        help="Minimum chars for a clause fragment to be treated as a segment (default: 45).",
    )
    parser.add_argument(
        "--clause-max-chars",
        type=int,
        default=260,
        help="Target max chars for clause-level decomposition (default: 260).",
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
        "--enable-tdfol",
        action="store_true",
        help="Enable TDFOL parsing bridge for each segment.",
    )
    parser.add_argument(
        "--enable-cec",
        action="store_true",
        help="Enable CEC conversions (bridge and NL compiler) for each segment.",
    )
    parser.add_argument(
        "--enable-semantic-roundtrip",
        action="store_true",
        help="Compute embedding similarity between original text and decoded logic roundtrip text.",
    )
    parser.add_argument(
        "--embedding-dim",
        type=int,
        default=1024,
        help="Embedding dimension for hashing-based semantic similarity (default: 1024).",
    )
    parser.add_argument(
        "--embedding-backend",
        choices=["hash", "sentence-transformers"],
        default="hash",
        help="Vector embedding backend for semantic scoring (default: hash).",
    )
    parser.add_argument(
        "--embedding-model",
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="Embedding model when using sentence-transformers backend.",
    )
    parser.add_argument(
        "--strict-embedding-backend",
        action="store_true",
        help="Fail the run if the requested embedding backend cannot be used.",
    )
    parser.add_argument(
        "--enable-roundtrip-optimizer",
        action="store_true",
        help="Try multiple roundtrip decode strategies and keep the highest-similarity variant.",
    )
    parser.add_argument(
        "--roundtrip-optimizer-min-uses",
        type=int,
        default=3,
        help="Minimum observations before reporting global best prompt by modality (default: 3).",
    )
    parser.add_argument(
        "--roundtrip-optimizer-exploration-rate",
        type=float,
        default=0.0,
        help="Exploration rate for prompt optimizer selection (default: 0.0 for deterministic runs).",
    )
    parser.add_argument(
        "--roundtrip-optimizer-export",
        default="",
        help="Optional path to export optimizer prompt-library metrics JSON.",
    )
    parser.add_argument(
        "--allow-source-conditioned-roundtrip",
        action="store_true",
        help="Allow decoder candidates that reuse source text (off by default to avoid evaluation leakage).",
    )
    parser.add_argument(
        "--semantic-threshold-deontic",
        type=float,
        default=-1.0,
        help="Minimum deontic roundtrip similarity for theorem acceptance (-1 disables).",
    )
    parser.add_argument(
        "--semantic-threshold-fol",
        type=float,
        default=-1.0,
        help="Minimum FOL roundtrip similarity for theorem acceptance (-1 disables).",
    )
    parser.add_argument(
        "--semantic-threshold-tdfol",
        type=float,
        default=-1.0,
        help="Minimum TDFOL roundtrip similarity for theorem acceptance (-1 disables).",
    )
    parser.add_argument(
        "--semantic-threshold-cec-bridge",
        type=float,
        default=-1.0,
        help="Minimum CEC-bridge roundtrip similarity for theorem acceptance (-1 disables).",
    )
    parser.add_argument(
        "--semantic-threshold-cec-compile",
        type=float,
        default=-1.0,
        help="Minimum CEC-compile roundtrip similarity for theorem acceptance (-1 disables).",
    )
    parser.add_argument(
        "--theorem-min-text-chars",
        type=int,
        default=60,
        help="Minimum source text length for theorem candidacy (default: 60).",
    )
    parser.add_argument(
        "--theorem-min-proposition-chars",
        type=int,
        default=20,
        help="Minimum deontic proposition length for theorem candidacy (default: 20).",
    )
    parser.add_argument(
        "--theorem-min-deontic-confidence",
        type=float,
        default=0.55,
        help="Minimum deontic confidence for theorem candidacy (default: 0.55).",
    )
    parser.add_argument(
        "--allow-non-normative-theorems",
        action="store_true",
        help="Allow theorem candidates without normative cue words (off by default).",
    )
    parser.add_argument(
        "--enable-fragment-merging",
        action="store_true",
        help="Allow small proposition fragments to be merged with nearby formula context.",
    )
    parser.add_argument(
        "--fragment-merge-max-prior",
        type=int,
        default=1,
        help="How many prior merged propositions to keep per segment stream (default: 1).",
    )
    parser.add_argument(
        "--allow-missing-semantic-modalities",
        default="tdfol,cec_bridge",
        help="Comma-separated modalities allowed to be missing for theorem gating (default: tdfol,cec_bridge).",
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
    parser.add_argument(
        "--output-logic-jsonld",
        default="artifacts/federal_laws/corpus_formal_logic_conversion_logic.jsonld",
        help="Path to JSON-LD output containing logic assertions.",
    )
    parser.add_argument(
        "--enable-focused-retry-optimizer",
        action="store_true",
        help="Retry deontic/FOL conversion on a focused normative sentence when initial output is weak.",
    )
    parser.add_argument(
        "--enable-encoder-quality-retry",
        action="store_true",
        help="Retry weak deontic/FOL encodes using focused and prior-context windows.",
    )
    parser.add_argument(
        "--encoder-context-window-prior",
        type=int,
        default=1,
        help="Number of prior segment texts to include in encoder retry windows.",
    )
    parser.add_argument(
        "--encoder-retry-max-attempts",
        type=int,
        default=3,
        help="Maximum retry candidate texts for encoder quality retry.",
    )
    parser.add_argument(
        "--semantic-floor-deontic",
        type=float,
        default=-1.0,
        help="Optional floor target for deontic similarity mean (-1 disables).",
    )
    parser.add_argument(
        "--semantic-floor-fol",
        type=float,
        default=-1.0,
        help="Optional floor target for FOL similarity mean (-1 disables).",
    )
    parser.add_argument(
        "--semantic-floor-cec-compile",
        type=float,
        default=-1.0,
        help="Optional floor target for CEC-compile similarity mean (-1 disables).",
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
    # Include clause boundaries to improve legal-text segmentation quality.
    parts = [s.strip() for s in re.split(r"(?<=[.!?;:])\s+", text) if s.strip()]
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


def _should_decompose_segment(text: str, clause_max_chars: int) -> bool:
    t = f" {text.lower()} "
    normative_hits = t.count(" shall ") + t.count(" must ") + t.count(" may ")
    return len(text) > clause_max_chars or normative_hits >= 2


def _split_normative_clauses(text: str, min_chars: int, max_chars: int) -> List[str]:
    candidates: List[str] = []
    for sentence in _split_sentences(text):
        pieces = re.split(
            r"(?i),\s+(?=(?:and\s+)?(?:no\s+person|the\s+actual\s+enumeration|which\s+shall|who\s+shall|shall\s+not|shall|must|may)\b)",
            sentence,
        )
        if not pieces:
            pieces = [sentence]
        for piece in pieces:
            p = _normalize_text(piece)
            if not p:
                continue
            candidates.extend(_split_long_piece(p, max_chars=max_chars))

    out: List[str] = []
    seen = set()
    for c in candidates:
        cc = c.strip()
        if len(cc) < int(min_chars):
            continue
        key = cc.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cc)
    return out


def _expand_segments_by_clause(
    segments: List[Segment],
    *,
    min_chars: int,
    max_chars: int,
) -> Tuple[List[Segment], int]:
    expanded: List[Segment] = []
    created = 0
    for seg in segments:
        if not _should_decompose_segment(seg.text, clause_max_chars=max_chars):
            expanded.append(seg)
            continue
        clauses = _split_normative_clauses(seg.text, min_chars=min_chars, max_chars=max_chars)
        if len(clauses) <= 1:
            expanded.append(seg)
            continue
        for idx, clause in enumerate(clauses, start=1):
            expanded.append(
                Segment(
                    source_path=seg.source_path,
                    source_id=f"{seg.source_id}.cl{idx}",
                    text=clause,
                )
            )
        created += max(0, len(clauses) - 1)
    return expanded, created


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


def _has_normative_cue(text: str) -> bool:
    t = text.lower()
    cues = (
        " shall ",
        " must ",
        " may ",
        " prohibited ",
        " forbidden ",
        " required ",
        " shall not ",
        " must not ",
        " may not ",
        " entitled to ",
    )
    padded = f" {t} "
    return any(c in padded for c in cues)


def _extract_normative_focus_text(text: str) -> str:
    """Return the best sentence-like slice for retry conversions."""
    sentences = _split_sentences(text)
    if not sentences:
        return text
    cue_sentences = [s for s in sentences if _has_normative_cue(s)]
    base = " ".join(cue_sentences[:2]).strip() if cue_sentences else " ".join(sentences[:1]).strip()

    # If sentence-level focusing does not reduce scope, fall back to cue-bearing clauses.
    if base == text.strip():
        clauses = [c.strip() for c in re.split(r"[,;:]", text) if c.strip()]
        cue_clauses = [c for c in clauses if _has_normative_cue(c)]
        if cue_clauses:
            base = " ".join(cue_clauses[:2]).strip()
        elif clauses:
            base = clauses[0]
    return base or text


def _is_trivial_deontic_formula(formula: Optional[str]) -> bool:
    if not formula:
        return True
    s = formula.strip()
    return s in {"O()", "P()", "F()"}


def _extract_deontic_inner(formula: str) -> Optional[Tuple[str, str]]:
    """Extract (operator, inner_text) from O(...), O[tag](...), etc."""
    s = formula.strip()
    m = re.match(r"^\s*([OPF])(?:\[[^\]]+\])?\s*\((.*)\)\s*$", s)
    if not m:
        return None
    return m.group(1), (m.group(2) or "").strip()


def _extract_deontic_tag(formula: Optional[str]) -> Optional[str]:
    if not formula:
        return None
    m = re.match(r"^\s*[OPF]\[([^\]]+)\]", formula.strip())
    if not m:
        return None
    return (m.group(1) or "").strip() or None


def _is_weak_fol_formula(formula: Optional[str]) -> bool:
    if not formula:
        return True
    s = formula.strip()
    if len(s) < 18:
        return True
    if re.fullmatch(r"∃x\s+[A-Za-z0-9_]+\(x\)", s):
        return True
    return False


def _is_misaligned_negation_fol_formula(formula: Optional[str]) -> bool:
    if not formula:
        return False
    s = formula.strip()
    return bool(re.search(r"Not\(x\)\s*→", s))


def _formula_has_negation(formula: Optional[str]) -> bool:
    if not formula:
        return False
    s = formula.lower()
    return ("¬" in formula) or (" not " in f" {s} ") or ("not(" in s)


def _is_weak_tdfol_formula(formula: Optional[str]) -> bool:
    if not formula:
        return True
    s = formula.strip()
    if len(s) < 3:
        return True
    # Single lexical tokens from grammar fallbacks are not useful formulas.
    if re.fullmatch(r"[A-Za-z_]+", s):
        return True
    return False


def _is_informative_deontic_formula(formula: Optional[str]) -> bool:
    if _is_trivial_deontic_formula(formula):
        return False
    extracted = _extract_deontic_inner(formula or "")
    if not extracted:
        return False
    _, inner = extracted
    return bool(inner and len(inner.strip()) >= 8)


def _is_informative_fol_formula(formula: Optional[str]) -> bool:
    return not _is_weak_fol_formula(formula)


def _deontic_quality_score(result: Any) -> float:
    formula = result.output.to_fol_string() if getattr(result, "output", None) is not None else None
    proposition = getattr(getattr(result, "output", None), "proposition", "") or ""
    score = 0.0
    if _is_informative_deontic_formula(formula):
        score += 4.0
    score += min(len(proposition.strip()), 120) / 80.0
    score += float(getattr(result, "confidence", 0.0))
    return score


def _fol_quality_score(result: Any) -> float:
    formula = getattr(getattr(result, "output", None), "formula_string", None)
    decoded = _decode_fol_formula_to_text(formula) or ""
    score = 0.0
    if _is_informative_fol_formula(formula):
        score += 4.0
    score += min(len(decoded.strip()), 120) / 90.0
    score += float(getattr(result, "confidence", 0.0))
    return score


def _build_encoder_retry_texts(base_text: str, prior_texts: List[str], max_attempts: int) -> List[str]:
    candidates: List[str] = []
    focus = _extract_normative_focus_text(base_text)
    if focus and focus != base_text:
        candidates.append(focus)

    if prior_texts:
        prior_join = " ".join([p for p in prior_texts if p]).strip()
        if prior_join:
            candidates.append(f"{prior_join} {base_text}".strip())
            if focus and focus != base_text:
                candidates.append(f"{prior_join} {focus}".strip())

    # Clause-level candidate from legal punctuation.
    clauses = [c.strip() for c in re.split(r"[,;:]", base_text) if c.strip()]
    cue_clauses = [c for c in clauses if _has_normative_cue(c)]
    if cue_clauses:
        candidates.append(" ".join(cue_clauses[:2]).strip())

    out: List[str] = []
    seen = set()
    for c in candidates:
        cc = re.sub(r"\s+", " ", c).strip()
        if not cc or cc == base_text or cc in seen:
            continue
        seen.add(cc)
        out.append(cc)
        if len(out) >= max(1, int(max_attempts)):
            break
    return out


def _formula_tokens_for_overlap(formula: Optional[str]) -> List[str]:
    if not formula:
        return []
    text = _humanize_logic_text(_logic_formula_to_text(formula) or formula).lower()
    tokens = [t for t in re.findall(r"[a-z0-9]+", text) if len(t) >= 3]
    stop = {
        "there",
        "exists",
        "every",
        "entity",
        "such",
        "that",
        "for",
        "all",
        "and",
        "the",
        "not",
        "implies",
        "obligatory",
        "permitted",
        "forbidden",
    }
    return [t for t in tokens if t not in stop]


def _best_source_overlap_sentence(original_text: str, formula: Optional[str]) -> Optional[str]:
    tokens = _formula_tokens_for_overlap(formula)
    if not tokens:
        return None
    sentences = _split_sentences(original_text)
    if not sentences:
        return None
    best_score = -1
    best_sentence: Optional[str] = None
    token_set = set(tokens)
    for sent in sentences:
        stoks = set(re.findall(r"[a-z0-9]+", sent.lower()))
        score = len(token_set & stoks)
        if score > best_score:
            best_score = score
            best_sentence = sent.strip()
    if best_score <= 0:
        return None
    return best_sentence


def _is_heading_like(source_id: str, text: str) -> bool:
    sid = source_id.lower()
    if ".name#" in sid or ".title#" in sid or ".description#" in sid:
        return True
    # Common heading-only patterns like "Article I", "Section 1", "First Amendment"
    if re.fullmatch(r"(article|section)\s+[ivxlcdm0-9]+", text.strip(), flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[a-z]+\s+amendment", text.strip(), flags=re.IGNORECASE):
        return True
    return False


def _segment_stream_key(source_path: str, source_id: str) -> str:
    return f"{source_path}::{source_id.split('#seg', 1)[0]}"


def _normalize_prop_piece(piece: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (piece or "").strip())


def _merge_fragment_proposition(
    *,
    proposition: str,
    fol_formula: Optional[str],
    prior_props: List[str],
    min_prop_chars: int,
    enabled: bool,
) -> Tuple[str, bool]:
    base_prop = _normalize_prop_piece(proposition)
    if not enabled:
        return base_prop, False

    pieces: List[str] = []
    if base_prop:
        pieces.append(base_prop)

    fol_decoded = _decode_fol_formula_to_text(fol_formula)
    if fol_decoded:
        fol_piece = _normalize_prop_piece(fol_decoded)
        if fol_piece and fol_piece not in pieces:
            pieces.append(fol_piece)

    needs_merge = (not base_prop) or (len(base_prop) < int(min_prop_chars))
    if needs_merge:
        for p in prior_props:
            np = _normalize_prop_piece(p)
            if np and np not in pieces:
                pieces.append(np)

    if not pieces:
        return "", False
    merged = " and ".join(pieces)
    changed = merged != base_prop
    return merged, changed


def build_theorem_candidate(
    *,
    source_id: str,
    text: str,
    deontic_operator_name: Optional[str],
    deontic_proposition: str,
    deontic_proposition_canonical: Optional[str],
    agent_name: str,
    deontic_confidence: float,
    min_text_chars: int,
    min_prop_chars: int,
    min_confidence: float,
    require_normative_cue: bool,
    is_merged_fragment: bool = False,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    reasons: List[str] = []

    normalized_prop = deontic_proposition.strip()
    if not normalized_prop:
        reasons.append("empty_proposition")

    if normalized_prop in {"O()", "P()", "F()"}:
        reasons.append("trivial_deontic_formula")

    if len(text.strip()) < min_text_chars and not is_merged_fragment:
        reasons.append("text_too_short")

    if len(normalized_prop) < min_prop_chars:
        reasons.append("proposition_too_short")

    if deontic_confidence < min_confidence:
        reasons.append("confidence_below_threshold")

    if _is_heading_like(source_id, text) and not is_merged_fragment:
        reasons.append("heading_like_text")

    if require_normative_cue and not _has_normative_cue(text) and not is_merged_fragment:
        reasons.append("no_normative_cue")

    if reasons:
        return None, reasons

    return {
        "operator": operator_for_theorem(deontic_operator_name),
        "proposition": normalized_prop,
        "proposition_canonical": (deontic_proposition_canonical or "").strip(),
        "agent_name": agent_name,
        "source_text": text,
    }, []


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


def _tokenize_for_embedding(text: str) -> List[str]:
    return re.findall(r"[a-zA-Z0-9_]+", text.lower())


def _sparse_hash_embed(text: str, dims: int = 1024) -> Dict[int, float]:
    vec: Dict[int, float] = {}
    tokens = _tokenize_for_embedding(text)
    if not tokens:
        return vec
    for tok in tokens:
        h = hashlib.sha256(tok.encode("utf-8")).hexdigest()
        idx = int(h[:8], 16) % max(8, dims)
        sign = 1.0 if (int(h[8:10], 16) % 2 == 0) else -1.0
        vec[idx] = vec.get(idx, 0.0) + sign
    return vec


def _cosine_sparse(a: Dict[int, float], b: Dict[int, float]) -> float:
    if not a or not b:
        return 0.0
    common = set(a.keys()) & set(b.keys())
    dot = sum(a[k] * b[k] for k in common)
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return float(dot / (na * nb))


def _logic_formula_to_text(formula: Optional[str]) -> Optional[str]:
    if not formula:
        return None
    text = formula
    text = text.replace("∀", " for all ")
    text = text.replace("∃", " there exists ")
    text = text.replace("→", " implies ")
    text = text.replace("∧", " and ")
    text = text.replace("∨", " or ")
    text = text.replace("¬", " not ")
    text = text.replace("(", " ").replace(")", " ")
    text = text.replace("[", " ").replace("]", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _decode_fol_formula_to_text(formula: Optional[str]) -> Optional[str]:
    """Decode common FOL forms into a more natural sentence template."""
    if not formula:
        return None
    s = formula.strip()

    m_exists = re.fullmatch(r"∃x\s+([A-Za-z0-9_]+)\(x\)", s)
    if m_exists:
        pred = _humanize_logic_text(m_exists.group(1)).lower()
        # Single-token existential stubs carry almost no semantics.
        if pred in {"all", "article", "section", "constitution", "we"}:
            return None
        return f"there exists an entity such that {pred} holds"

    m_forall_impl = re.fullmatch(
        r"∀x\s*\(\s*([A-Za-z0-9_]+)\(x\)\s*→\s*([A-Za-z0-9_]+)\(x\)\s*\)",
        s,
    )
    if m_forall_impl:
        lhs = _humanize_logic_text(m_forall_impl.group(1)).lower()
        rhs = _humanize_logic_text(m_forall_impl.group(2)).lower()
        return f"for every entity, if {lhs} applies then {rhs} applies"

    return _humanize_logic_text(_logic_formula_to_text(s) or s)


def _decode_deontic_formula_to_text(formula: Optional[str]) -> Optional[str]:
    """Decode deontic formulas with support for indexed operators and nesting."""
    if not formula:
        return None
    extracted = _extract_deontic_inner(formula)
    if not extracted:
        return _humanize_logic_text(_logic_formula_to_text(formula) or formula)
    op, inner = extracted
    nested = _extract_deontic_inner(inner)
    if nested:
        inner_text = _decode_deontic_formula_to_text(inner)
    else:
        inner_text = _decode_fol_formula_to_text(inner) or _humanize_logic_text(
            _logic_formula_to_text(inner) or inner
        )
    if not inner_text:
        return None
    if op == "O":
        return f"it is obligatory that {inner_text}"
    if op == "P":
        return f"it is permitted that {inner_text}"
    if op == "F":
        return f"it is forbidden that {inner_text}"
    return inner_text


def _humanize_logic_text(text: str) -> str:
    out = text.replace("_", " ")
    out = re.sub(r"([a-z])([A-Z])", r"\1 \2", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _sanitize_symbol_token(value: str, fallback: str = "Term") -> str:
    token = re.sub(r"[^A-Za-z0-9 ]+", " ", value or "")
    token = re.sub(r"\s+", " ", token).strip()
    if not token:
        return fallback
    parts = [p.capitalize() for p in token.split() if p]
    out = "".join(parts)
    if not out:
        out = fallback
    if out[0].isdigit():
        out = f"N{out}"
    return out


def _canonicalize_proposition_text(value: str) -> str:
    text = _humanize_logic_text(value or "").lower()
    text = re.sub(r"\b(it is|that|there exists|an|a|the|for every entity if|applies|holds)\b", " ", text)
    text = re.sub(r"\b(and|or|implies|not)\b", " ", text)
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    tokens = [t for t in text.split() if len(t) >= 3]
    if not tokens:
        return ""
    # Canonical key ignores ordering noise by sorting unique tokens.
    return " ".join(sorted(set(tokens)))


def _extract_structured_role_tuple(text: str) -> Optional[Dict[str, Any]]:
    normalized = _normalize_text(text)
    m = re.search(r"\b(shall|must|may)\b(\s+not)?", normalized, flags=re.IGNORECASE)
    if not m:
        return None
    modal = m.group(1).lower()
    negated = bool(m.group(2))

    agent_raw = normalized[: m.start()].strip(" ,;:-")
    action_raw = normalized[m.end() :].strip(" ,;:-")
    if not agent_raw or not action_raw:
        return None

    # Capture downstream negation cues in long legal clauses.
    lower_action = action_raw.lower()
    if (" shall not " in f" {lower_action} ") or (" must not " in f" {lower_action} "):
        negated = True
    if lower_action.startswith("not "):
        negated = True

    action_raw = re.split(r"(?i)\b(and|but|provided|except)\b", action_raw, maxsplit=1)[0].strip(" ,;:-")
    action_words = action_raw.split()
    if len(action_words) > 14:
        action_raw = " ".join(action_words[:14])

    # Strip leading determiners to improve canonical predicate naming.
    agent_raw = re.sub(r"^(the|a|an|no)\s+", "", agent_raw, flags=re.IGNORECASE).strip()
    if len(agent_raw) < 2 or len(action_raw) < 3:
        return None

    return {
        "agent": agent_raw,
        "action": action_raw,
        "modality": modal,
        "negated": negated,
    }


def _build_structured_fol_formula(role_tuple: Optional[Dict[str, Any]]) -> Optional[str]:
    if not role_tuple:
        return None
    agent = _sanitize_symbol_token(str(role_tuple.get("agent") or ""), fallback="Agent")
    action = _sanitize_symbol_token(str(role_tuple.get("action") or ""), fallback="Action")
    negated = bool(role_tuple.get("negated"))
    if negated:
        return f"∀x ({agent}(x) → ¬{action}(x))"
    return f"∀x ({agent}(x) → {action}(x))"


def _build_grounded_fol_fallback(
    *,
    text: str,
    source_id: str,
    role_tuple: Optional[Dict[str, Any]],
    deontic_formula: Optional[str],
) -> Optional[str]:
    if _is_heading_like(source_id, text):
        return None

    agent_name = ""
    action_name = ""
    negated = False

    if role_tuple:
        agent_name = str(role_tuple.get("agent") or "").strip()
        action_name = str(role_tuple.get("action") or "").strip()
        negated = bool(role_tuple.get("negated"))

    if not agent_name:
        agent_name = _extract_deontic_tag(deontic_formula) or "Regulated Party"

    if not action_name:
        decoded = _decode_deontic_formula_to_text(deontic_formula)
        if decoded:
            action_name = decoded
    if not action_name:
        action_name = _extract_normative_focus_text(text)

    agent = _sanitize_symbol_token(agent_name, fallback="Agent")
    action = _sanitize_symbol_token(action_name, fallback="Action")
    if not agent or not action:
        return None
    if negated:
        return f"∀x ({agent}(x) → ¬{action}(x))"
    return f"∀x ({agent}(x) → {action}(x))"


def _repair_trivial_deontic_formula(
    *,
    formula: Optional[str],
    operator_name: Optional[str],
    fol_formula: Optional[str],
    text: str,
    source_id: str,
    role_tuple: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not _is_trivial_deontic_formula(formula):
        return formula
    if _is_heading_like(source_id, text):
        return formula

    inner = fol_formula if _is_informative_fol_formula(fol_formula) else None
    if not inner:
        fallback_fol = _build_grounded_fol_fallback(
            text=text,
            source_id=source_id,
            role_tuple=role_tuple,
            deontic_formula=formula,
        )
        if fallback_fol:
            inner = fallback_fol

    if not inner:
        return formula

    op_map = {
        "OBLIGATION": "O",
        "PERMISSION": "P",
        "FORBIDDEN": "F",
    }
    op = op_map.get(str(operator_name or "").upper(), "O")
    return f"{op}({inner})"


def _derive_kg_agent_and_proposition(rec: ConversionRecord) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if rec.theorem_candidate:
        agent_name = str(rec.theorem_candidate.get("agent_name") or "").strip() or None
        proposition = str(rec.theorem_candidate.get("proposition") or "").strip() or None
        operator = str(rec.theorem_candidate.get("operator") or "").strip() or None
        return agent_name, proposition, operator

    if rec.structured_role_tuple:
        agent_name = str(rec.structured_role_tuple.get("agent") or "").strip() or None
        proposition = str(rec.structured_role_tuple.get("action") or "").strip() or None
        modality = str(rec.structured_role_tuple.get("modality") or "shall")
        negated = bool(rec.structured_role_tuple.get("negated"))
        if negated:
            operator = "FORBIDDEN"
        elif modality.lower() == "may":
            operator = "PERMISSION"
        else:
            operator = "OBLIGATION"
        return agent_name, proposition, operator

    agent_name = _extract_deontic_tag(rec.deontic_formula)
    proposition: Optional[str] = None
    operator: Optional[str] = str(rec.deontic_operator or "").strip() or None

    if rec.fol_formula:
        proposition = _decode_fol_formula_to_text(rec.fol_formula)
    if not proposition and rec.deontic_formula:
        proposition = _decode_deontic_formula_to_text(rec.deontic_formula)
    if not proposition and rec.text:
        proposition = _extract_normative_focus_text(rec.text)

    proposition = (proposition or "").strip() or None
    if proposition and len(proposition) < 6:
        proposition = None
    if not agent_name and proposition:
        agent_name = "Unspecified Party"
    return agent_name, proposition, operator


def _build_roundtrip_candidates(
    original_text: str,
    formula: Optional[str],
    baseline_text: Optional[str],
    modality: str,
    allow_source_conditioning: bool,
) -> Dict[str, str]:
    candidates: Dict[str, str] = {}
    if baseline_text:
        candidates["baseline"] = baseline_text
    if not formula:
        return candidates

    symbolic = _logic_formula_to_text(formula)
    if symbolic:
        candidates["symbolic_expanded"] = symbolic
        candidates["symbolic_humanized"] = _humanize_logic_text(symbolic)

    if modality == "deontic":
        decoded_deontic = _decode_deontic_formula_to_text(formula)
        if decoded_deontic:
            candidates["deontic_structured_decode"] = decoded_deontic
        if allow_source_conditioning and _is_trivial_deontic_formula(formula):
            focus_text = _extract_normative_focus_text(original_text)
            if focus_text:
                candidates["deontic_normative_focus_fallback"] = focus_text
        extracted = _extract_deontic_inner(formula)
        if extracted:
            op, inner_raw = extracted
            inner = _decode_fol_formula_to_text(inner_raw) or _humanize_logic_text(
                _logic_formula_to_text(inner_raw) or inner_raw
            )
            if op == "O":
                candidates["deontic_obligation_gloss"] = f"it is obligatory that {inner}".strip()
            elif op == "P":
                candidates["deontic_permission_gloss"] = f"it is permitted that {inner}".strip()
            elif op == "F":
                candidates["deontic_prohibition_gloss"] = f"it is forbidden that {inner}".strip()
        if allow_source_conditioning:
            overlap = _best_source_overlap_sentence(original_text, formula)
            if overlap:
                candidates["deontic_source_overlap_sentence"] = overlap

    if modality == "fol":
        decoded_fol = _decode_fol_formula_to_text(formula)
        if decoded_fol:
            candidates["fol_structured_decode"] = decoded_fol
        if allow_source_conditioning and _is_weak_fol_formula(formula):
            focus_text = _extract_normative_focus_text(original_text)
            if focus_text:
                candidates["fol_normative_focus_fallback"] = focus_text
        if allow_source_conditioning:
            overlap = _best_source_overlap_sentence(original_text, formula)
            if overlap:
                candidates["fol_source_overlap_sentence"] = overlap

    # Predicate-style gloss: f(a,b) -> "f a b"
    pred = formula.replace("(", " ").replace(")", " ").replace(",", " ")
    pred = _humanize_logic_text(_logic_formula_to_text(pred) or pred)
    if pred:
        candidates["predicate_gloss"] = pred

    # Deduplicate values while preserving first-in ordering by key insertion.
    seen_values: Dict[str, str] = {}
    deduped: Dict[str, str] = {}
    for key, value in candidates.items():
        v = value.strip()
        if not v:
            continue
        if v in seen_values:
            continue
        seen_values[v] = key
        deduped[key] = v
    return deduped


def _optimizer_strategy_from_name(name: str) -> PromptOptimizationStrategy:
    # Use a stable default that supports online exploration/exploitation.
    _ = name
    return PromptOptimizationStrategy.MULTI_ARMED_BANDIT


def _choose_better_deontic_result(
    original: Any,
    original_formula_str: Optional[str],
    retry: Any,
    retry_formula_str: Optional[str],
) -> Tuple[Any, Optional[str]]:
    orig_trivial = _is_trivial_deontic_formula(original_formula_str)
    retry_trivial = _is_trivial_deontic_formula(retry_formula_str)
    if orig_trivial and not retry_trivial:
        return retry, retry_formula_str
    if not orig_trivial and retry_trivial:
        return original, original_formula_str
    if float(getattr(retry, "confidence", 0.0)) > float(getattr(original, "confidence", 0.0)):
        return retry, retry_formula_str
    return original, original_formula_str


def _choose_better_fol_result(
    original: Any,
    original_formula_str: Optional[str],
    retry: Any,
    retry_formula_str: Optional[str],
) -> Tuple[Any, Optional[str]]:
    orig_weak = _is_weak_fol_formula(original_formula_str)
    retry_weak = _is_weak_fol_formula(retry_formula_str)
    if orig_weak and not retry_weak:
        return retry, retry_formula_str
    if not orig_weak and retry_weak:
        return original, original_formula_str
    if float(getattr(retry, "confidence", 0.0)) > float(getattr(original, "confidence", 0.0)):
        return retry, retry_formula_str
    return original, original_formula_str


def _select_roundtrip_text_with_optimizer(
    *,
    original_text: str,
    formula: Optional[str],
    baseline_text: Optional[str],
    modality: str,
    prompt_optimizer: PromptOptimizer,
    optimizer_min_uses: int,
    dims: int,
    backend: str,
    model_name: str,
    st_state: Dict[str, Any],
    allow_source_conditioning: bool,
) -> Tuple[Optional[str], Optional[float], Optional[float], str, Optional[str], str]:
    candidates = _build_roundtrip_candidates(
        original_text,
        formula,
        baseline_text,
        modality,
        allow_source_conditioning=allow_source_conditioning,
    )
    if not candidates:
        return baseline_text, None, None, backend, None, "none"

    candidate_scores: Dict[str, float] = {}
    effective_backend = backend
    warning: Optional[str] = None

    for candidate_id, candidate_text in candidates.items():
        pid = f"{modality}:{candidate_id}"
        if pid not in prompt_optimizer.prompt_library:
            prompt_optimizer.add_prompt(
                "{text}",
                prompt_id=pid,
                metadata={"modality": modality, "strategy": candidate_id},
            )
        score, beff, warn = _roundtrip_similarity_with_backend(
            original_text,
            candidate_text,
            dims=dims,
            backend=effective_backend,
            model_name=model_name,
            st_state=st_state,
        )
        effective_backend = beff
        if warn and warning is None:
            warning = warn
        if score is None:
            continue
        candidate_scores[pid] = float(score)
        prompt_optimizer.record_usage(
            prompt_id=pid,
            success=score > 0.0,
            confidence=float(score),
            critic_score=float(score),
            extraction_time=0.0,
            domain="legal",
            formalism=modality,
        )

    if not candidate_scores:
        return baseline_text, None, None, effective_backend, warning, "none"

    baseline_key = f"{modality}:baseline"
    baseline_score = candidate_scores.get(baseline_key)
    best_pid, best_score = max(candidate_scores.items(), key=lambda x: x[1])

    # Also query global best learned prompt for observability/recommendations.
    best_global = prompt_optimizer.get_best_prompt(
        domain="legal",
        formalism=modality,
        min_uses=max(1, int(optimizer_min_uses)),
    )
    selected_pid = best_pid
    if best_global is not None and best_global.template_id in candidate_scores:
        selected_pid = best_global.template_id
    if selected_pid not in candidate_scores:
        selected_pid = best_pid

    selected_candidate_id = selected_pid.split(":", 1)[1] if ":" in selected_pid else selected_pid
    selected_text = candidates.get(selected_candidate_id, baseline_text)
    selected_score = candidate_scores[selected_pid]
    return selected_text, selected_score, baseline_score, effective_backend, warning, selected_candidate_id


def _roundtrip_similarity(original_text: str, roundtrip_text: Optional[str], dims: int) -> Optional[float]:
    if not roundtrip_text:
        return None
    v1 = _sparse_hash_embed(original_text, dims=dims)
    v2 = _sparse_hash_embed(roundtrip_text, dims=dims)
    return _cosine_sparse(v1, v2)


def _dot(a: List[float], b: List[float]) -> float:
    return float(sum(x * y for x, y in zip(a, b)))


def _norm(a: List[float]) -> float:
    return math.sqrt(sum(x * x for x in a))


def _cosine_dense(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    na = _norm(a)
    nb = _norm(b)
    if na == 0.0 or nb == 0.0:
        return 0.0
    return _dot(a, b) / (na * nb)


def _safe_ratio(numer: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    return float(numer / denom)


def _shannon_entropy_from_counts(counts: Dict[str, int]) -> Optional[float]:
    total = sum(int(v) for v in counts.values() if v and v > 0)
    if total <= 0:
        return None
    ent = 0.0
    for v in counts.values():
        if not v or v <= 0:
            continue
        p = float(v) / float(total)
        ent -= p * math.log2(p)
    return float(ent)


def _normalized_entropy_from_counts(counts: Dict[str, int]) -> Optional[float]:
    nonzero = [k for k, v in counts.items() if v and v > 0]
    if len(nonzero) <= 1:
        return 0.0
    ent = _shannon_entropy_from_counts(counts)
    if ent is None:
        return None
    return float(ent / math.log2(len(nonzero)))


def _roundtrip_similarity_with_backend(
    original_text: str,
    roundtrip_text: Optional[str],
    *,
    dims: int,
    backend: str,
    model_name: str,
    st_state: Dict[str, Any],
) -> Tuple[Optional[float], str, Optional[str]]:
    if not roundtrip_text:
        return None, backend, None

    if backend == "sentence-transformers":
        try:
            model = st_state.get("model")
            if model is None:
                from sentence_transformers import SentenceTransformer

                model = SentenceTransformer(model_name)
                st_state["model"] = model
            emb = model.encode([original_text, roundtrip_text], convert_to_numpy=False)
            v1 = [float(x) for x in emb[0]]
            v2 = [float(x) for x in emb[1]]
            return _cosine_dense(v1, v2), "sentence-transformers", None
        except Exception as exc:
            # Fall back to hash embeddings when transformer backend is unavailable.
            sim = _roundtrip_similarity(original_text, roundtrip_text, dims=dims)
            return sim, "hash", f"sentence-transformers backend unavailable: {exc}"

    sim = _roundtrip_similarity(original_text, roundtrip_text, dims=dims)
    return sim, "hash", None


def apply_semantic_thresholds(
    *,
    theorem_candidate: Optional[Dict[str, Any]],
    reasons: List[str],
    similarities: Dict[str, Optional[float]],
    thresholds: Dict[str, float],
    semantic_enabled: bool,
    allowed_missing_modalities: Optional[set] = None,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    if theorem_candidate is None:
        return theorem_candidate, reasons
    if not semantic_enabled:
        return theorem_candidate, reasons

    for modality, threshold in thresholds.items():
        if threshold < 0:
            continue
        value = similarities.get(modality)
        if value is None:
            if allowed_missing_modalities and modality in allowed_missing_modalities:
                continue
            reasons.append(f"semantic_{modality}_missing")
            continue
        if value < threshold:
            reasons.append(f"semantic_{modality}_below_threshold")

    if any(r.startswith("semantic_") for r in reasons):
        return None, reasons
    return theorem_candidate, reasons


def setup_tdfol_cec(enable_tdfol: bool, enable_cec: bool) -> Dict[str, Any]:
    tools: Dict[str, Any] = {
        "grammar_bridge": None,
        "cec_bridge": None,
        "nl_compiler": None,
        "tdfol_enabled": False,
        "cec_enabled": False,
        "setup_errors": [],
    }

    if not enable_tdfol and not enable_cec:
        return tools

    try:
        from ipfs_datasets_py.logic.integration.bridges.tdfol_grammar_bridge import TDFOLGrammarBridge

        tools["grammar_bridge"] = TDFOLGrammarBridge()
        tools["tdfol_enabled"] = bool(tools["grammar_bridge"] and tools["grammar_bridge"].is_available())
    except Exception as exc:
        tools["setup_errors"].append(f"tdfol_setup_failed: {exc}")

    if enable_cec:
        try:
            from ipfs_datasets_py.logic.integration.bridges.tdfol_cec_bridge import TDFOLCECBridge

            tools["cec_bridge"] = TDFOLCECBridge()
            tools["cec_enabled"] = bool(tools["cec_bridge"] and tools["cec_bridge"].is_available())
        except Exception as exc:
            tools["setup_errors"].append(f"cec_bridge_setup_failed: {exc}")

        try:
            from ipfs_datasets_py.logic.CEC.nl.nl_to_policy_compiler import NLToDCECCompiler

            tools["nl_compiler"] = NLToDCECCompiler(policy_id="corpus-logic", strict=False)
            tools["cec_enabled"] = tools["cec_enabled"] or True
        except Exception as exc:
            tools["setup_errors"].append(f"cec_compiler_setup_failed: {exc}")

    return tools


def _extract_temporal_markers(text: str) -> List[str]:
    t = text.lower()
    markers: List[str] = []
    if "every second year" in t:
        markers.append("EVERY_SECOND_YEAR")
    if "within three years" in t:
        markers.append("WITHIN_THREE_YEARS")
    if "after" in t:
        markers.append("AFTER")
    if "before" in t:
        markers.append("BEFORE")
    if "when " in t:
        markers.append("WHEN")
    if "shall" in t:
        markers.append("DEONTIC_SHALL")
    out: List[str] = []
    seen = set()
    for m in markers:
        if m in seen:
            continue
        seen.add(m)
        out.append(m)
    return out


def _focus_text_for_markers(text: str, markers: List[str]) -> str:
    sentences = _split_sentences(text)
    if not sentences:
        return text
    marker_map = {
        "EVERY_SECOND_YEAR": "every second year",
        "WITHIN_THREE_YEARS": "within three years",
        "AFTER": "after",
        "BEFORE": "before",
        "WHEN": "when",
        "DEONTIC_SHALL": "shall",
    }
    needles = [marker_map[m] for m in markers if m in marker_map]
    best = sentences[0]
    best_score = -1
    for s in sentences:
        ls = s.lower()
        score = sum(1 for n in needles if n in ls)
        if score > best_score:
            best_score = score
            best = s
    return best


def _deontic_polarity(text: str, deontic_formula: Optional[str], focus_text: Optional[str] = None) -> str:
    scopes = [focus_text, text]
    for scope in scopes:
        if not scope:
            continue
        t = scope.lower()
        neg = ("shall not" in t) or ("must not" in t) or ("may not" in t) or ("forbidden" in t)
        perm = (" may " in f" {t} ") and ("may not" not in t)
        oblig = (" shall " in f" {t} ") or (" must " in f" {t} ")
        if neg and not oblig:
            return "FORBIDDEN"
        if oblig and not neg:
            return "OBLIGATORY"
        if perm and not neg and not oblig:
            return "PERMITTED"
    if deontic_formula and deontic_formula.strip().startswith("F"):
        return "FORBIDDEN"
    if deontic_formula and deontic_formula.strip().startswith("P"):
        return "PERMITTED"
    return "OBLIGATORY"


def _temporal_atom_from_logic(
    *,
    text: str,
    deontic_formula: Optional[str],
    fol_formula: Optional[str],
) -> str:
    base = _decode_deontic_formula_to_text(deontic_formula)
    if not base:
        base = _decode_fol_formula_to_text(fol_formula)
    if not base:
        base = _extract_normative_focus_text(text)
    atom = re.sub(r"[^A-Za-z0-9_ ]+", " ", base or "norm")
    atom = re.sub(r"\s+", "_", atom.strip().lower())
    atom = re.sub(r"^(it_is_obligatory_that_)+", "", atom)
    atom = re.sub(r"^(it_is_permitted_that_)+", "", atom)
    atom = re.sub(r"^(it_is_forbidden_that_)+", "", atom)
    if not atom:
        atom = "norm"
    if len(atom) > 64:
        atom = atom[:64].rstrip("_")
    return atom


def _derive_tdfol_fallback_formula(
    *,
    text: str,
    deontic_formula: Optional[str],
    fol_formula: Optional[str],
) -> Optional[str]:
    markers = _extract_temporal_markers(text)
    if not markers:
        return None
    atom = _temporal_atom_from_logic(text=text, deontic_formula=deontic_formula, fol_formula=fol_formula)
    focus = _focus_text_for_markers(text, markers)
    polarity = _deontic_polarity(text, deontic_formula, focus_text=focus)
    typed_terms: List[str] = []
    if "EVERY_SECOND_YEAR" in markers:
        typed_terms.append(f"PERIODIC(EVERY_SECOND_YEAR,{atom})")
    if "WITHIN_THREE_YEARS" in markers:
        typed_terms.append(f"DEADLINE(WITHIN_THREE_YEARS,{atom})")
    if "WHEN" in markers:
        typed_terms.append(f"CONDITIONAL(WHEN,{atom})")
    if "AFTER" in markers:
        typed_terms.append(f"SEQUENCE(AFTER,{atom})")
    if "BEFORE" in markers:
        typed_terms.append(f"SEQUENCE(BEFORE,{atom})")
    if not typed_terms:
        typed_terms.append(f"TEMPORAL({atom})")
    joined = " & ".join(typed_terms[:3])
    return f"TDFOL_{polarity}({joined})"


def _derive_cec_bridge_fallback_formula(
    *,
    text: str,
    deontic_formula: Optional[str],
    fol_formula: Optional[str],
    tdfol_formula: Optional[str],
) -> Optional[str]:
    atom = _temporal_atom_from_logic(text=text, deontic_formula=deontic_formula, fol_formula=fol_formula)
    tag = "TEMP"
    if tdfol_formula:
        if "PERIODIC(" in tdfol_formula:
            tag = "PERIODIC"
        elif "DEADLINE(" in tdfol_formula:
            tag = "DEADLINE"
        elif "CONDITIONAL(" in tdfol_formula:
            tag = "CONDITIONAL"
    focus = _focus_text_for_markers(text, _extract_temporal_markers(text))
    polarity = _deontic_polarity(text, deontic_formula, focus_text=focus)
    return (
        f"TemporalContext({tag}, t) & HoldsAt({atom}, t) -> "
        f"NormativeForce({polarity}, {atom}, t)"
    )


def run_tdfol_cec_conversions(
    *,
    text: str,
    source_id: str,
    tools: Dict[str, Any],
    deontic_formula: Optional[str],
    fol_formula: Optional[str],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "tdfol_success": False,
        "tdfol_formula": None,
        "tdfol_decoded_text": None,
        "tdfol_formula_origin": "none",
        "tdfol_errors": [],
        "cec_bridge_success": False,
        "cec_bridge_formula": None,
        "cec_bridge_decoded_text": None,
        "cec_bridge_formula_origin": "none",
        "cec_compile_success": False,
        "cec_formula_count": 0,
        "cec_compile_text": None,
        "cec_errors": [],
    }

    grammar_bridge = tools.get("grammar_bridge")
    cec_bridge = tools.get("cec_bridge")
    nl_compiler = tools.get("nl_compiler")

    formula = None
    if tools.get("tdfol_enabled") and grammar_bridge is not None:
        # Skip parser on heading-like/very short text to avoid noisy low-value parser failures.
        if _is_heading_like(source_id, text) or len(text.strip()) < 24:
            formula = None
        else:
            try:
                formula = grammar_bridge.parse_natural_language(text)
                if formula is not None:
                    formula_text = str(formula)
                    if _is_weak_tdfol_formula(formula_text):
                        # Retry with a focused clause before dropping TDFOL.
                        focused = _extract_normative_focus_text(text)
                        if focused and focused != text:
                            retry = grammar_bridge.parse_natural_language(focused)
                            if retry is not None and not _is_weak_tdfol_formula(str(retry)):
                                formula = retry
                                formula_text = str(formula)
                            else:
                                formula = None
                        else:
                            # Treat weak lexical fallbacks as non-parses, not errors.
                            formula = None
                    else:
                        out["tdfol_success"] = True
                        out["tdfol_formula"] = formula_text
                        out["tdfol_formula_origin"] = "grammar"
                        try:
                            out["tdfol_decoded_text"] = grammar_bridge.formula_to_natural_language(formula)
                        except Exception:
                            out["tdfol_decoded_text"] = formula_text

                    if formula is not None and out["tdfol_formula"] is None:
                        out["tdfol_success"] = True
                        out["tdfol_formula"] = str(formula)
                        out["tdfol_formula_origin"] = "grammar"
                        try:
                            out["tdfol_decoded_text"] = grammar_bridge.formula_to_natural_language(formula)
                        except Exception:
                            out["tdfol_decoded_text"] = str(formula)
            except Exception as exc:
                out["tdfol_errors"].append(str(exc))

    # Deterministic fallback: derive temporalized representation from deontic/FOL + source cues.
    if out["tdfol_formula"] is None and not _is_heading_like(source_id, text):
        tdfol_fallback = _derive_tdfol_fallback_formula(
            text=text,
            deontic_formula=deontic_formula,
            fol_formula=fol_formula,
        )
        if tdfol_fallback and not _is_weak_tdfol_formula(tdfol_fallback):
            out["tdfol_success"] = True
            out["tdfol_formula"] = tdfol_fallback
            out["tdfol_formula_origin"] = "fallback"
            out["tdfol_decoded_text"] = _humanize_logic_text(_logic_formula_to_text(tdfol_fallback) or tdfol_fallback)

    if tools.get("cec_enabled"):
        if out["tdfol_formula"] is not None and cec_bridge is not None and formula is not None:
            try:
                out["cec_bridge_formula"] = cec_bridge.tdfol_to_dcec_string(formula)
                out["cec_bridge_success"] = True
                out["cec_bridge_formula_origin"] = "grammar_bridge"
                try:
                    back = cec_bridge.dcec_string_to_tdfol(out["cec_bridge_formula"])
                    if grammar_bridge is not None:
                        out["cec_bridge_decoded_text"] = grammar_bridge.formula_to_natural_language(back)
                    else:
                        out["cec_bridge_decoded_text"] = str(back)
                except Exception:
                    out["cec_bridge_decoded_text"] = _logic_formula_to_text(out["cec_bridge_formula"])
            except Exception as exc:
                out["cec_errors"].append(f"cec_bridge: {exc}")

        if out["cec_bridge_formula"] is None and out["tdfol_formula"] is not None:
            cec_fallback = _derive_cec_bridge_fallback_formula(
                text=text,
                deontic_formula=deontic_formula,
                fol_formula=fol_formula,
                tdfol_formula=out["tdfol_formula"],
            )
            if cec_fallback:
                out["cec_bridge_formula"] = cec_fallback
                out["cec_bridge_success"] = True
                out["cec_bridge_formula_origin"] = "fallback"
                out["cec_bridge_decoded_text"] = _humanize_logic_text(
                    _logic_formula_to_text(cec_fallback) or cec_fallback
                )

        if nl_compiler is not None:
            try:
                samples = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
                cue_samples = [s for s in samples if _has_normative_cue(s)]
                if cue_samples:
                    sample_text = cue_samples[:2]
                elif samples:
                    sample_text = samples[:2]
                else:
                    sample_text = [text]
                comp = nl_compiler.compile(sample_text)
                formula_count = len(comp.dcec_formulas or [])
                out["cec_compile_success"] = bool(comp.success or formula_count > 0)
                out["cec_formula_count"] = len(comp.dcec_formulas or [])
                if comp.dcec_formulas:
                    out["cec_compile_text"] = " ; ".join(str(x) for x in comp.dcec_formulas)
            except Exception as exc:
                out["cec_errors"].append(f"cec_compile: {exc}")

    if out["tdfol_formula"] is None and out["tdfol_success"]:
        out["tdfol_success"] = False

    return out


def build_logic_jsonld(records: List[ConversionRecord], summary: Dict[str, Any]) -> Dict[str, Any]:
    parts: List[Dict[str, Any]] = []
    graph_nodes: List[Dict[str, Any]] = []
    seen_agents: Dict[str, str] = {}
    seen_props: Dict[str, str] = {}

    def _id_for(prefix: str, raw: str) -> str:
        token = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
        return f"urn:logic:{prefix}:{token}"

    for idx, rec in enumerate(records, start=1):
        part: Dict[str, Any] = {
            "@type": "CreativeWork",
            "@id": f"urn:logic:assertion:{idx}",
            "identifier": rec.source_id,
            "isBasedOn": rec.source_path,
            "text": rec.text,
            "deonticSuccess": rec.deontic_success,
            "folSuccess": rec.fol_success,
            "tdfolSuccess": rec.tdfol_success,
            "cecCompileSuccess": rec.cec_compile_success,
            "theoremFilterPassed": rec.theorem_filter_passed,
            "theoremFilterReasons": rec.theorem_filter_reasons,
            "normativeCueDetected": _has_normative_cue(rec.text),
        }
        if rec.deontic_formula:
            part["deonticFormula"] = rec.deontic_formula
        if rec.fol_formula:
            part["folFormula"] = rec.fol_formula
        if rec.structured_role_tuple:
            part["structuredRoleTuple"] = rec.structured_role_tuple
        if rec.tdfol_formula:
            part["tdfolFormula"] = rec.tdfol_formula
        if rec.tdfol_formula_origin:
            part["tdfolFormulaOrigin"] = rec.tdfol_formula_origin
        if rec.cec_bridge_formula:
            part["cecBridgeFormula"] = rec.cec_bridge_formula
        if rec.cec_bridge_formula_origin:
            part["cecBridgeFormulaOrigin"] = rec.cec_bridge_formula_origin
        if rec.cec_formula_count:
            part["cecFormulaCount"] = rec.cec_formula_count
        if rec.semantic_similarity_deontic is not None:
            part["semanticSimilarityDeontic"] = rec.semantic_similarity_deontic
        if rec.semantic_similarity_fol is not None:
            part["semanticSimilarityFOL"] = rec.semantic_similarity_fol
        if rec.semantic_similarity_tdfol is not None:
            part["semanticSimilarityTDFOL"] = rec.semantic_similarity_tdfol
        if rec.semantic_similarity_cec_bridge is not None:
            part["semanticSimilarityCECBridge"] = rec.semantic_similarity_cec_bridge
        if rec.semantic_similarity_cec_compile is not None:
            part["semanticSimilarityCECCompile"] = rec.semantic_similarity_cec_compile
        if rec.theorem_candidate:
            part["theoremCandidate"] = rec.theorem_candidate

        agent_name, proposition, operator = _derive_kg_agent_and_proposition(rec)
        if agent_name:
            if agent_name not in seen_agents:
                agent_id = _id_for("agent", agent_name)
                seen_agents[agent_name] = agent_id
                graph_nodes.append({"@id": agent_id, "@type": "Person", "name": agent_name})
            else:
                agent_id = seen_agents[agent_name]
            part["mentionsAgent"] = {"@id": agent_id}

        if proposition:
            theorem_canonical = ""
            if rec.theorem_candidate:
                theorem_canonical = str(rec.theorem_candidate.get("proposition_canonical") or "")
            prop_key = theorem_canonical or _canonicalize_proposition_text(proposition) or proposition
            if prop_key not in seen_props:
                prop_id = _id_for("proposition", prop_key)
                seen_props[prop_key] = prop_id
                graph_nodes.append(
                    {
                        "@id": prop_id,
                        "@type": "DefinedTerm",
                        "name": proposition,
                        "alternateName": prop_key,
                    }
                )
            else:
                prop_id = seen_props[prop_key]
            part["aboutProposition"] = {"@id": prop_id}

        if operator:
            part["deonticOperator"] = operator
        if rec.theorem_ingest:
            part["theoremIngest"] = rec.theorem_ingest
        parts.append(part)

    return {
        "@context": {
            "@vocab": "https://schema.org/",
            "deonticFormula": "https://example.org/logic/deonticFormula",
            "folFormula": "https://example.org/logic/folFormula",
            "structuredRoleTuple": "https://example.org/logic/structuredRoleTuple",
            "tdfolFormula": "https://example.org/logic/tdfolFormula",
            "tdfolFormulaOrigin": "https://example.org/logic/tdfolFormulaOrigin",
            "cecBridgeFormula": "https://example.org/logic/cecBridgeFormula",
            "cecBridgeFormulaOrigin": "https://example.org/logic/cecBridgeFormulaOrigin",
            "cecFormulaCount": "https://example.org/logic/cecFormulaCount",
            "theoremCandidate": "https://example.org/logic/theoremCandidate",
            "theoremIngest": "https://example.org/logic/theoremIngest",
            "mentionsAgent": {
                "@id": "https://example.org/logic/mentionsAgent",
                "@type": "@id",
            },
            "aboutProposition": {
                "@id": "https://example.org/logic/aboutProposition",
                "@type": "@id",
            },
            "deonticOperator": "https://example.org/logic/deonticOperator",
            "normativeCueDetected": "https://example.org/logic/normativeCueDetected",
        },
        "@type": "Dataset",
        "name": "Legal Corpus Formal Logic Conversion",
        "summary": summary,
        "hasPart": parts,
        "@graph": graph_nodes + parts,
    }


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
    tdfol_cec_tools = setup_tdfol_cec(
        enable_tdfol=bool(args.enable_tdfol),
        enable_cec=bool(args.enable_cec),
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

    segment_count_pre_clause_decomposition = len(segments)
    clause_segments_created = 0
    if args.enable_clause_decomposition:
        segments, clause_segments_created = _expand_segments_by_clause(
            segments,
            min_chars=int(args.clause_min_chars),
            max_chars=int(args.clause_max_chars),
        )

    if args.limit_segments > 0:
        segments = segments[: args.limit_segments]

    records: List[ConversionRecord] = []
    theorem_candidates = 0
    ingested_theorems = 0
    rejected_theorem_candidates = 0
    rejection_reason_counts: Dict[str, int] = {}
    tdfol_success_count = 0
    tdfol_fallback_used_count = 0
    cec_bridge_success_count = 0
    cec_compile_success_count = 0
    cec_formula_total = 0
    semantic_pairs = 0
    semantic_similarity_sum = 0.0
    semantic_by_modality: Dict[str, Dict[str, float]] = {
        "deontic": {"sum": 0.0, "count": 0.0},
        "fol": {"sum": 0.0, "count": 0.0},
        "tdfol": {"sum": 0.0, "count": 0.0},
        "cec_bridge": {"sum": 0.0, "count": 0.0},
        "cec_compile": {"sum": 0.0, "count": 0.0},
    }
    theorem_ingest_enabled, theorem_ingest_blocker = theorem_ingestion_preflight(
        args.add_to_theorem_store
    )
    embedding_backend_requested = str(args.embedding_backend)
    embedding_backend_effective = embedding_backend_requested
    embedding_backend_warnings: List[str] = []
    st_state: Dict[str, Any] = {}
    roundtrip_optimizer_warnings: List[str] = []
    strict_embedding_backend = bool(args.strict_embedding_backend)
    roundtrip_optimizer_requested = bool(args.enable_roundtrip_optimizer)
    roundtrip_optimizer_enabled = False
    roundtrip_optimizer: Optional[PromptOptimizer] = None
    roundtrip_gain_by_modality: Dict[str, Dict[str, float]] = {
        "deontic": {"gain_sum": 0.0, "count": 0.0},
        "fol": {"gain_sum": 0.0, "count": 0.0},
        "tdfol": {"gain_sum": 0.0, "count": 0.0},
        "cec_bridge": {"gain_sum": 0.0, "count": 0.0},
        "cec_compile": {"gain_sum": 0.0, "count": 0.0},
    }
    roundtrip_gain_sum = 0.0
    roundtrip_gain_count = 0
    focused_retry_attempts = 0
    focused_retry_deontic_improved = 0
    focused_retry_fol_improved = 0
    repaired_trivial_deontic_count = 0
    repaired_weak_fol_count = 0
    encoder_quality_retry_attempts = 0
    encoder_quality_retry_deontic_improved = 0
    encoder_quality_retry_fol_improved = 0
    fragment_merge_attempts = 0
    fragment_merge_applied = 0
    fragment_prior_context: Dict[str, List[str]] = {}
    encoder_stream_context: Dict[str, List[str]] = {}
    allowed_missing_modalities = {
        x.strip()
        for x in str(args.allow_missing_semantic_modalities).split(",")
        if x.strip()
    }

    if roundtrip_optimizer_requested:
        if not args.enable_semantic_roundtrip:
            roundtrip_optimizer_warnings.append(
                "roundtrip optimizer requested but semantic roundtrip is disabled; optimizer disabled"
            )
        else:
            try:
                roundtrip_optimizer = PromptOptimizer(
                    strategy=_optimizer_strategy_from_name("multi_armed_bandit"),
                    enable_versioning=False,
                    track_metrics=True,
                    exploration_rate=float(args.roundtrip_optimizer_exploration_rate),
                )
                roundtrip_optimizer_enabled = True
            except Exception as exc:
                roundtrip_optimizer_warnings.append(f"roundtrip optimizer initialization failed: {exc}")

    for seg in segments:
        stream_key_for_context = _segment_stream_key(seg.source_path, seg.source_id)
        prior_texts = encoder_stream_context.get(stream_key_for_context, [])

        d_res = deontic.convert(seg.text)
        f_res = fol.convert(seg.text)

        d_formula_initial = d_res.output if d_res.success and d_res.output is not None else None
        f_formula_initial = f_res.output if f_res.success and f_res.output is not None else None
        d_formula_initial_str = d_formula_initial.to_fol_string() if d_formula_initial is not None else None
        f_formula_initial_str = f_formula_initial.formula_string if f_formula_initial is not None else None

        if args.enable_focused_retry_optimizer:
            should_retry = _is_trivial_deontic_formula(d_formula_initial_str) or _is_weak_fol_formula(
                f_formula_initial_str
            )
            if should_retry:
                focused_text = _extract_normative_focus_text(seg.text)
                if focused_text and focused_text != seg.text:
                    focused_retry_attempts += 1
                    d_retry = deontic.convert(focused_text)
                    f_retry = fol.convert(focused_text)

                    d_retry_formula = d_retry.output if d_retry.success and d_retry.output is not None else None
                    f_retry_formula = f_retry.output if f_retry.success and f_retry.output is not None else None
                    d_retry_formula_str = d_retry_formula.to_fol_string() if d_retry_formula is not None else None
                    f_retry_formula_str = f_retry_formula.formula_string if f_retry_formula is not None else None

                    d_before = d_formula_initial_str
                    f_before = f_formula_initial_str
                    d_res, d_formula_initial_str = _choose_better_deontic_result(
                        d_res,
                        d_formula_initial_str,
                        d_retry,
                        d_retry_formula_str,
                    )
                    f_res, f_formula_initial_str = _choose_better_fol_result(
                        f_res,
                        f_formula_initial_str,
                        f_retry,
                        f_retry_formula_str,
                    )
                    if d_formula_initial_str != d_before:
                        focused_retry_deontic_improved += 1
                    if f_formula_initial_str != f_before:
                        focused_retry_fol_improved += 1

        if args.enable_encoder_quality_retry:
            weak_deontic = not _is_informative_deontic_formula(d_formula_initial_str)
            weak_fol = not _is_informative_fol_formula(f_formula_initial_str)
            if weak_deontic or weak_fol:
                retry_texts = _build_encoder_retry_texts(
                    seg.text,
                    prior_texts=prior_texts[-max(0, int(args.encoder_context_window_prior)):],
                    max_attempts=int(args.encoder_retry_max_attempts),
                )
                if retry_texts:
                    encoder_quality_retry_attempts += 1
                d_best = d_res
                f_best = f_res
                d_best_score = _deontic_quality_score(d_res)
                f_best_score = _fol_quality_score(f_res)
                d_before_formula = d_formula_initial_str
                f_before_formula = f_formula_initial_str
                for text_retry in retry_texts:
                    d_try = deontic.convert(text_retry)
                    f_try = fol.convert(text_retry)
                    d_try_score = _deontic_quality_score(d_try)
                    f_try_score = _fol_quality_score(f_try)
                    if d_try_score > d_best_score:
                        d_best = d_try
                        d_best_score = d_try_score
                    if f_try_score > f_best_score:
                        f_best = f_try
                        f_best_score = f_try_score
                d_res = d_best
                f_res = f_best
                d_formula_after = d_res.output.to_fol_string() if d_res.output is not None else None
                f_formula_after = f_res.output.formula_string if f_res.output is not None else None
                if d_formula_after != d_before_formula:
                    encoder_quality_retry_deontic_improved += 1
                if f_formula_after != f_before_formula:
                    encoder_quality_retry_fol_improved += 1

        d_formula = d_res.output if d_res.success and d_res.output is not None else None
        f_formula = f_res.output if f_res.success and f_res.output is not None else None
        deontic_formula_string = d_formula.to_fol_string() if d_formula is not None else None
        fol_formula_string = f_formula.formula_string if f_formula is not None else None
        structured_role_tuple = _extract_structured_role_tuple(seg.text)
        structured_fol_fallback = _build_structured_fol_formula(structured_role_tuple)
        force_structured_fol = bool(
            structured_role_tuple
            and bool(structured_role_tuple.get("negated"))
            and _is_misaligned_negation_fol_formula(fol_formula_string)
        )
        if structured_fol_fallback and (_is_weak_fol_formula(fol_formula_string) or force_structured_fol):
            fol_formula_string = structured_fol_fallback

        if _is_weak_fol_formula(fol_formula_string):
            repaired_fol = _build_grounded_fol_fallback(
                text=seg.text,
                source_id=seg.source_id,
                role_tuple=structured_role_tuple,
                deontic_formula=deontic_formula_string,
            )
            if repaired_fol and repaired_fol != fol_formula_string:
                fol_formula_string = repaired_fol
                repaired_weak_fol_count += 1

        if d_formula is not None:
            operator_name_candidate = getattr(getattr(d_formula, "operator", None), "name", None)
            repaired_deontic = _repair_trivial_deontic_formula(
                formula=deontic_formula_string,
                operator_name=operator_name_candidate,
                fol_formula=fol_formula_string,
                text=seg.text,
                source_id=seg.source_id,
                role_tuple=structured_role_tuple,
            )
            if repaired_deontic and repaired_deontic != deontic_formula_string:
                deontic_formula_string = repaired_deontic
                repaired_trivial_deontic_count += 1

        tdfol_cec = run_tdfol_cec_conversions(
            text=seg.text,
            source_id=seg.source_id,
            tools=tdfol_cec_tools,
            deontic_formula=deontic_formula_string,
            fol_formula=fol_formula_string,
        )

        operator_name = None
        theorem_candidate = None
        theorem_filter_reasons: List[str] = []
        if d_formula is not None:
            operator_name = getattr(getattr(d_formula, "operator", None), "name", None)
            proposition = getattr(d_formula, "proposition", "") or ""
            # Bridge weak deontic outputs (e.g., O()) with FOL-derived proposition text.
            if not proposition and fol_formula_string is not None:
                proposition = _decode_fol_formula_to_text(fol_formula_string) or ""
            if not proposition and structured_role_tuple is not None:
                proposition = str(structured_role_tuple.get("action") or "")
            proposition_canonical = _canonicalize_proposition_text(proposition)

            merged_fragment = False
            if args.enable_fragment_merging:
                fragment_merge_attempts += 1
                stream_key = _segment_stream_key(seg.source_path, seg.source_id)
                prior_props = fragment_prior_context.get(stream_key, [])
                proposition, merged_fragment = _merge_fragment_proposition(
                    proposition=proposition,
                    fol_formula=fol_formula_string,
                    prior_props=prior_props,
                    min_prop_chars=int(args.theorem_min_proposition_chars),
                    enabled=True,
                )
                if merged_fragment:
                    fragment_merge_applied += 1
                if proposition:
                    max_prior = max(1, int(args.fragment_merge_max_prior))
                    prior_props = (prior_props + [proposition])[-max_prior:]
                    fragment_prior_context[stream_key] = prior_props

            theorem_candidate, theorem_filter_reasons = build_theorem_candidate(
                source_id=seg.source_id,
                text=seg.text,
                deontic_operator_name=operator_name,
                deontic_proposition=proposition,
                deontic_proposition_canonical=proposition_canonical,
                agent_name=(
                    d_formula.agent.name if getattr(d_formula, "agent", None) else "Unspecified Party"
                ),
                deontic_confidence=float(d_res.confidence),
                min_text_chars=int(args.theorem_min_text_chars),
                min_prop_chars=int(args.theorem_min_proposition_chars),
                min_confidence=float(args.theorem_min_deontic_confidence),
                require_normative_cue=not bool(args.allow_non_normative_theorems),
                is_merged_fragment=merged_fragment,
            )

        theorem_ingest = None

        if tdfol_cec["tdfol_success"]:
            tdfol_success_count += 1
        if tdfol_cec.get("tdfol_formula_origin") == "fallback":
            tdfol_fallback_used_count += 1
        if tdfol_cec["cec_bridge_success"]:
            cec_bridge_success_count += 1
        if tdfol_cec["cec_compile_success"]:
            cec_compile_success_count += 1
        cec_formula_total += int(tdfol_cec["cec_formula_count"])

        deontic_roundtrip_text = _decode_deontic_formula_to_text(deontic_formula_string)
        fol_roundtrip_text = _decode_fol_formula_to_text(
            fol_formula_string
        )
        if (
            deontic_roundtrip_text is None
            and _is_trivial_deontic_formula(deontic_formula_string)
            and fol_roundtrip_text
        ):
            deontic_roundtrip_text = f"it is obligatory that {fol_roundtrip_text}"
        tdfol_roundtrip_text = tdfol_cec.get("tdfol_decoded_text")
        cec_bridge_roundtrip_text = tdfol_cec.get("cec_bridge_decoded_text")
        cec_compile_roundtrip_text = _logic_formula_to_text(tdfol_cec.get("cec_compile_text"))

        semantic_similarity_deontic = None
        semantic_similarity_fol = None
        semantic_similarity_tdfol = None
        semantic_similarity_cec_bridge = None
        semantic_similarity_cec_compile = None

        if args.enable_semantic_roundtrip:
            dims = int(args.embedding_dim)
            if roundtrip_optimizer_enabled and roundtrip_optimizer is not None:
                optimized_modalities = [
                    ("deontic", deontic_formula_string, deontic_roundtrip_text),
                    ("fol", fol_formula_string, fol_roundtrip_text),
                    ("tdfol", tdfol_cec.get("tdfol_formula"), tdfol_roundtrip_text),
                    ("cec_bridge", tdfol_cec.get("cec_bridge_formula"), cec_bridge_roundtrip_text),
                    ("cec_compile", tdfol_cec.get("cec_compile_text"), cec_compile_roundtrip_text),
                ]
                similarity_map: Dict[str, Optional[float]] = {
                    "deontic": None,
                    "fol": None,
                    "tdfol": None,
                    "cec_bridge": None,
                    "cec_compile": None,
                }
                for modality, formula_text, baseline_text in optimized_modalities:
                    selected_text, selected_score, baseline_score, beff, warn, _ = _select_roundtrip_text_with_optimizer(
                        original_text=seg.text,
                        formula=formula_text,
                        baseline_text=baseline_text,
                        modality=modality,
                        prompt_optimizer=roundtrip_optimizer,
                        optimizer_min_uses=int(args.roundtrip_optimizer_min_uses),
                        dims=dims,
                        backend=embedding_backend_effective,
                        model_name=str(args.embedding_model),
                        st_state=st_state,
                        allow_source_conditioning=bool(args.allow_source_conditioned_roundtrip),
                    )
                    embedding_backend_effective = beff
                    if warn and warn not in embedding_backend_warnings:
                        embedding_backend_warnings.append(warn)
                    if (
                        strict_embedding_backend
                        and embedding_backend_requested == "sentence-transformers"
                        and embedding_backend_effective != embedding_backend_requested
                    ):
                        raise RuntimeError(
                            "Requested embedding backend sentence-transformers is unavailable "
                            "and strict backend mode is enabled."
                        )
                    similarity_map[modality] = selected_score

                    if modality == "deontic":
                        deontic_roundtrip_text = selected_text
                    elif modality == "fol":
                        fol_roundtrip_text = selected_text
                    elif modality == "tdfol":
                        tdfol_roundtrip_text = selected_text
                    elif modality == "cec_bridge":
                        cec_bridge_roundtrip_text = selected_text
                    elif modality == "cec_compile":
                        cec_compile_roundtrip_text = selected_text

                    if baseline_score is not None and selected_score is not None:
                        gain = float(selected_score - baseline_score)
                        roundtrip_gain_by_modality[modality]["gain_sum"] += gain
                        roundtrip_gain_by_modality[modality]["count"] += 1.0
                        roundtrip_gain_sum += gain
                        roundtrip_gain_count += 1

                semantic_similarity_deontic = similarity_map["deontic"]
                semantic_similarity_fol = similarity_map["fol"]
                semantic_similarity_tdfol = similarity_map["tdfol"]
                semantic_similarity_cec_bridge = similarity_map["cec_bridge"]
                semantic_similarity_cec_compile = similarity_map["cec_compile"]
            else:
                semantic_similarity_deontic, beff, warn = _roundtrip_similarity_with_backend(
                    seg.text,
                    deontic_roundtrip_text,
                    dims=dims,
                    backend=embedding_backend_effective,
                    model_name=str(args.embedding_model),
                    st_state=st_state,
                )
                embedding_backend_effective = beff
                if warn and warn not in embedding_backend_warnings:
                    embedding_backend_warnings.append(warn)
                if (
                    strict_embedding_backend
                    and embedding_backend_requested == "sentence-transformers"
                    and embedding_backend_effective != embedding_backend_requested
                ):
                    raise RuntimeError(
                        "Requested embedding backend sentence-transformers is unavailable "
                        "and strict backend mode is enabled."
                    )

                semantic_similarity_fol, beff, warn = _roundtrip_similarity_with_backend(
                    seg.text,
                    fol_roundtrip_text,
                    dims=dims,
                    backend=embedding_backend_effective,
                    model_name=str(args.embedding_model),
                    st_state=st_state,
                )
                embedding_backend_effective = beff
                if warn and warn not in embedding_backend_warnings:
                    embedding_backend_warnings.append(warn)

                semantic_similarity_tdfol, beff, warn = _roundtrip_similarity_with_backend(
                    seg.text,
                    tdfol_roundtrip_text,
                    dims=dims,
                    backend=embedding_backend_effective,
                    model_name=str(args.embedding_model),
                    st_state=st_state,
                )
                embedding_backend_effective = beff
                if warn and warn not in embedding_backend_warnings:
                    embedding_backend_warnings.append(warn)

                semantic_similarity_cec_bridge, beff, warn = _roundtrip_similarity_with_backend(
                    seg.text,
                    cec_bridge_roundtrip_text,
                    dims=dims,
                    backend=embedding_backend_effective,
                    model_name=str(args.embedding_model),
                    st_state=st_state,
                )
                embedding_backend_effective = beff
                if warn and warn not in embedding_backend_warnings:
                    embedding_backend_warnings.append(warn)

                semantic_similarity_cec_compile, beff, warn = _roundtrip_similarity_with_backend(
                    seg.text,
                    cec_compile_roundtrip_text,
                    dims=dims,
                    backend=embedding_backend_effective,
                    model_name=str(args.embedding_model),
                    st_state=st_state,
                )
                embedding_backend_effective = beff
                if warn and warn not in embedding_backend_warnings:
                    embedding_backend_warnings.append(warn)

            modality_values = {
                "deontic": semantic_similarity_deontic,
                "fol": semantic_similarity_fol,
                "tdfol": semantic_similarity_tdfol,
                "cec_bridge": semantic_similarity_cec_bridge,
                "cec_compile": semantic_similarity_cec_compile,
            }
            for mod, val in modality_values.items():
                if val is not None:
                    semantic_by_modality[mod]["sum"] += float(val)
                    semantic_by_modality[mod]["count"] += 1.0
                    semantic_similarity_sum += float(val)
                    semantic_pairs += 1

        theorem_candidate, theorem_filter_reasons = apply_semantic_thresholds(
            theorem_candidate=theorem_candidate,
            reasons=theorem_filter_reasons,
            similarities={
                "deontic": semantic_similarity_deontic,
                "fol": semantic_similarity_fol,
                "tdfol": semantic_similarity_tdfol,
                "cec_bridge": semantic_similarity_cec_bridge,
                "cec_compile": semantic_similarity_cec_compile,
            },
            thresholds={
                "deontic": float(args.semantic_threshold_deontic),
                "fol": float(args.semantic_threshold_fol),
                "tdfol": float(args.semantic_threshold_tdfol),
                "cec_bridge": float(args.semantic_threshold_cec_bridge),
                "cec_compile": float(args.semantic_threshold_cec_compile),
            },
            semantic_enabled=bool(args.enable_semantic_roundtrip),
            allowed_missing_modalities=allowed_missing_modalities,
        )

        if theorem_candidate is not None and structured_role_tuple is not None:
            expected_neg = bool(structured_role_tuple.get("negated"))
            actual_neg = _formula_has_negation(fol_formula_string)
            if expected_neg != actual_neg:
                theorem_filter_reasons.append("structural_negation_mismatch")
                theorem_candidate = None

        if theorem_candidate is not None:
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
        elif theorem_filter_reasons:
            rejected_theorem_candidates += 1
            for reason in theorem_filter_reasons:
                rejection_reason_counts[reason] = rejection_reason_counts.get(reason, 0) + 1

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
            fol_formula=fol_formula_string,
            fol_confidence=float(f_res.confidence),
            fol_errors=list(f_res.errors),
            structured_role_tuple=structured_role_tuple,
            tdfol_success=bool(tdfol_cec["tdfol_success"]),
            tdfol_formula=tdfol_cec["tdfol_formula"],
            tdfol_formula_origin=tdfol_cec.get("tdfol_formula_origin"),
            tdfol_errors=list(tdfol_cec["tdfol_errors"]),
            cec_bridge_success=bool(tdfol_cec["cec_bridge_success"]),
            cec_bridge_formula=tdfol_cec["cec_bridge_formula"],
            cec_bridge_formula_origin=tdfol_cec.get("cec_bridge_formula_origin"),
            cec_compile_success=bool(tdfol_cec["cec_compile_success"]),
            cec_formula_count=int(tdfol_cec["cec_formula_count"]),
            cec_errors=list(tdfol_cec["cec_errors"]),
            deontic_roundtrip_text=deontic_roundtrip_text,
            fol_roundtrip_text=fol_roundtrip_text,
            tdfol_roundtrip_text=tdfol_roundtrip_text,
            cec_bridge_roundtrip_text=cec_bridge_roundtrip_text,
            cec_compile_roundtrip_text=cec_compile_roundtrip_text,
            semantic_similarity_deontic=semantic_similarity_deontic,
            semantic_similarity_fol=semantic_similarity_fol,
            semantic_similarity_tdfol=semantic_similarity_tdfol,
            semantic_similarity_cec_bridge=semantic_similarity_cec_bridge,
            semantic_similarity_cec_compile=semantic_similarity_cec_compile,
            theorem_filter_passed=theorem_candidate is not None,
            theorem_filter_reasons=theorem_filter_reasons,
            theorem_candidate=theorem_candidate,
            theorem_ingest=theorem_ingest,
        )
        records.append(rec)

        # Maintain local stream context for encoder retry windows.
        max_ctx = max(0, int(args.encoder_context_window_prior))
        if max_ctx > 0:
            prev = encoder_stream_context.get(stream_key_for_context, [])
            encoder_stream_context[stream_key_for_context] = (prev + [seg.text])[-max_ctx:]

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl = Path(args.output_jsonl)
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_logic_jsonld = Path(args.output_logic_jsonld)
    out_logic_jsonld.parent.mkdir(parents=True, exist_ok=True)

    with out_jsonl.open("w", encoding="utf-8") as fp:
        for r in records:
            fp.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")

    deontic_success_count = sum(1 for r in records if r.deontic_success)
    fol_success_count = sum(1 for r in records if r.fol_success)
    theorem_candidates_with_canonical = [
        r.theorem_candidate
        for r in records
        if r.theorem_candidate and str(r.theorem_candidate.get("proposition_canonical") or "").strip()
    ]
    theorem_unique_canonical_props = {
        str(tc.get("proposition_canonical") or "").strip()
        for tc in theorem_candidates_with_canonical
        if str(tc.get("proposition_canonical") or "").strip()
    }
    deontic_trivial_formula_count = sum(
        1 for r in records if _is_trivial_deontic_formula(r.deontic_formula)
    )
    fol_weak_formula_count = sum(1 for r in records if _is_weak_fol_formula(r.fol_formula))
    normative_cue_segment_count = sum(1 for r in records if _has_normative_cue(r.text))
    deontic_operator_counts: Dict[str, int] = {}
    for r in records:
        key = str(r.deontic_operator or "UNKNOWN")
        deontic_operator_counts[key] = deontic_operator_counts.get(key, 0) + 1
    modality_means = {
        mod: (float(vals["sum"] / vals["count"]) if vals["count"] > 0 else None)
        for mod, vals in semantic_by_modality.items()
    }
    modality_floors = {
        "deontic": float(args.semantic_floor_deontic),
        "fol": float(args.semantic_floor_fol),
        "cec_compile": float(args.semantic_floor_cec_compile),
    }
    modality_floor_results: Dict[str, Optional[bool]] = {}
    for mod, floor in modality_floors.items():
        if floor < 0:
            modality_floor_results[mod] = None
            continue
        value = modality_means.get(mod)
        modality_floor_results[mod] = bool(value is not None and value >= floor)

    summary = {
        "inputs": [str(p) for p in input_files],
        "input_file_count": len(input_files),
        "segment_count": len(segments),
        "segment_count_pre_clause_decomposition": segment_count_pre_clause_decomposition,
        "clause_decomposition_enabled": bool(args.enable_clause_decomposition),
        "clause_segments_created": clause_segments_created,
        "deontic_success_count": deontic_success_count,
        "fol_success_count": fol_success_count,
        "theorem_candidate_count": theorem_candidates,
        "theorem_candidate_canonical_prop_count": len(theorem_candidates_with_canonical),
        "theorem_unique_canonical_prop_count": len(theorem_unique_canonical_props),
        "theorem_candidates_rejected": rejected_theorem_candidates,
        "theorem_rejection_reason_counts": rejection_reason_counts,
        "conversion_entropy_diagnostics": {
            "deontic_trivial_formula_count": deontic_trivial_formula_count,
            "deontic_trivial_formula_rate": _safe_ratio(deontic_trivial_formula_count, len(records)),
            "repaired_trivial_deontic_count": repaired_trivial_deontic_count,
            "fol_weak_formula_count": fol_weak_formula_count,
            "fol_weak_formula_rate": _safe_ratio(fol_weak_formula_count, len(records)),
            "repaired_weak_fol_count": repaired_weak_fol_count,
            "normative_cue_segment_count": normative_cue_segment_count,
            "normative_cue_segment_rate": _safe_ratio(normative_cue_segment_count, len(records)),
            "deontic_operator_counts": deontic_operator_counts,
            "deontic_operator_entropy_bits": _shannon_entropy_from_counts(deontic_operator_counts),
            "deontic_operator_entropy_normalized": _normalized_entropy_from_counts(
                deontic_operator_counts
            ),
            "theorem_rejection_entropy_bits": _shannon_entropy_from_counts(rejection_reason_counts),
            "theorem_rejection_entropy_normalized": _normalized_entropy_from_counts(
                rejection_reason_counts
            ),
        },
        "tdfol_enabled": bool(tdfol_cec_tools.get("tdfol_enabled")),
        "cec_enabled": bool(tdfol_cec_tools.get("cec_enabled")),
        "tdfol_cec_setup_errors": tdfol_cec_tools.get("setup_errors", []),
        "tdfol_success_count": tdfol_success_count,
        "tdfol_fallback_used_count": tdfol_fallback_used_count,
        "cec_bridge_success_count": cec_bridge_success_count,
        "cec_compile_success_count": cec_compile_success_count,
        "cec_formula_total": cec_formula_total,
        "semantic_roundtrip_enabled": bool(args.enable_semantic_roundtrip),
        "semantic_embedding_backend_requested": embedding_backend_requested,
        "semantic_embedding_backend_effective": embedding_backend_effective,
        "semantic_embedding_backend_warnings": embedding_backend_warnings,
        "strict_embedding_backend": strict_embedding_backend,
        "semantic_embedding_model": str(args.embedding_model),
        "semantic_embedding_dim": int(args.embedding_dim),
        "roundtrip_optimizer_requested": roundtrip_optimizer_requested,
        "roundtrip_optimizer_enabled": roundtrip_optimizer_enabled,
        "roundtrip_source_conditioning_enabled": bool(args.allow_source_conditioned_roundtrip),
        "roundtrip_optimizer_warnings": roundtrip_optimizer_warnings,
        "focused_retry_optimizer_enabled": bool(args.enable_focused_retry_optimizer),
        "focused_retry_attempts": focused_retry_attempts,
        "focused_retry_deontic_improved": focused_retry_deontic_improved,
        "focused_retry_fol_improved": focused_retry_fol_improved,
        "encoder_quality_retry_enabled": bool(args.enable_encoder_quality_retry),
        "encoder_quality_retry_attempts": encoder_quality_retry_attempts,
        "encoder_quality_retry_deontic_improved": encoder_quality_retry_deontic_improved,
        "encoder_quality_retry_fol_improved": encoder_quality_retry_fol_improved,
        "fragment_merging_enabled": bool(args.enable_fragment_merging),
        "fragment_merge_attempts": fragment_merge_attempts,
        "fragment_merge_applied": fragment_merge_applied,
        "allowed_missing_semantic_modalities": sorted(allowed_missing_modalities),
        "roundtrip_optimizer_avg_similarity_gain": (
            float(roundtrip_gain_sum / roundtrip_gain_count) if roundtrip_gain_count > 0 else None
        ),
        "roundtrip_optimizer_gain_by_modality": {
            mod: (
                float(vals["gain_sum"] / vals["count"]) if vals["count"] > 0 else None
            )
            for mod, vals in roundtrip_gain_by_modality.items()
        },
        "semantic_similarity_thresholds": {
            "deontic": float(args.semantic_threshold_deontic),
            "fol": float(args.semantic_threshold_fol),
            "tdfol": float(args.semantic_threshold_tdfol),
            "cec_bridge": float(args.semantic_threshold_cec_bridge),
            "cec_compile": float(args.semantic_threshold_cec_compile),
        },
        "semantic_similarity_pairs": semantic_pairs,
        "semantic_similarity_mean": (
            float(semantic_similarity_sum / semantic_pairs) if semantic_pairs > 0 else None
        ),
        "semantic_similarity_by_modality": modality_means,
        "semantic_similarity_floors": modality_floors,
        "semantic_similarity_floor_pass": modality_floor_results,
        "theorems_ingested_count": ingested_theorems,
        "add_to_theorem_store": bool(args.add_to_theorem_store),
        "theorem_ingestion_enabled": theorem_ingest_enabled,
        "theorem_ingestion_blocker": theorem_ingest_blocker,
        "output_json": str(out_json),
        "output_jsonl": str(out_jsonl),
        "output_logic_jsonld": str(out_logic_jsonld),
    }
    report = {
        "summary": summary,
        "records": [asdict(r) for r in records],
    }

    if roundtrip_optimizer_enabled and roundtrip_optimizer is not None and args.roundtrip_optimizer_export:
        try:
            roundtrip_optimizer.export_library(str(args.roundtrip_optimizer_export))
        except Exception as exc:
            summary["roundtrip_optimizer_warnings"].append(
                f"roundtrip optimizer export failed: {exc}"
            )

    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    logic_jsonld = build_logic_jsonld(records=records, summary=summary)
    out_logic_jsonld.write_text(json.dumps(logic_jsonld, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return summary


def main() -> None:
    args = parse_args()
    summary = asyncio.run(run(args))
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
