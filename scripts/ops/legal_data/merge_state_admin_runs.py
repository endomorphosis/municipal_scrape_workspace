#!/usr/bin/env python3
"""Merge state-admin run artifacts into canonical per-state JSONLD and global JSONL.

This script scans prior run directories under artifacts/state_admin_rules, merges all
STATE-XX.jsonld files per state, deduplicates rows, and writes canonical outputs.
It also scans one-state summary JSON files (XX.json) and keeps a best summary per state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

STATE_RE = re.compile(r"STATE-([A-Z]{2})\.jsonld$")
SUMMARY_RE = re.compile(r"([A-Z]{2})\.json$")
_RAW_RTF_PREFIX_RE = re.compile(r"^\s*\{\\rtf", re.IGNORECASE)
_RTF_NOISE_RE = re.compile(
    r"Times New Roman;|Arial;|Calibri;|Aptos;|Cambria Math;|Default Paragraph Font|Normal Table|\\fonttbl|\\stylesheet|\\panose",
    re.IGNORECASE,
)
_LEGAL_TEXT_RE = re.compile(
    r"\b(?:chapter|article|title|section|rule|authority|historical\s+note)\b|R\d{1,2}-\d{1,2}-\d{2,4}",
    re.IGNORECASE,
)


def _parse_selected_states(values: Optional[Sequence[str]]) -> Optional[List[str]]:
    if not values:
        return None
    selected: List[str] = []
    for value in values:
        for item in str(value or "").split(","):
            code = item.strip().upper()
            if re.fullmatch(r"[A-Z]{2}", code) and code not in selected:
                selected.append(code)
    return selected or None


@dataclass
class CandidateRow:
    state: str
    source_file: Path
    row: Dict[str, Any]


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _stable_json_sha1(obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _row_url(row: Dict[str, Any]) -> str:
    return _normalize_text(row.get("url") or row.get("sameAs") or row.get("source_url") or row.get("sourceUrl"))


def _row_identifier(row: Dict[str, Any]) -> str:
    return _normalize_text(row.get("identifier") or row.get("legislationIdentifier"))


def _row_name(row: Dict[str, Any]) -> str:
    return _normalize_text(row.get("name") or row.get("title") or row.get("description"))


def _row_text(row: Dict[str, Any]) -> str:
    return _normalize_text(row.get("text") or row.get("full_text"))


def _text_quality_tuple(text: str) -> Tuple[int, int, int]:
    normalized = str(text or "").strip()
    if not normalized:
        return (0, -999, 0)

    prefix = normalized[:4000]
    non_raw_rtf = 0 if _RAW_RTF_PREFIX_RE.match(prefix) else 1
    legal_hits = len(_LEGAL_TEXT_RE.findall(prefix))
    noise_hits = len(_RTF_NOISE_RE.findall(prefix))
    semantic_score = legal_hits * 5 - noise_hits * 4
    return (non_raw_rtf, semantic_score, len(normalized))


def _dedupe_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    url = _row_url(row).lower()
    ident = _row_identifier(row).lower()
    name = _row_name(row).lower()
    if url:
        return ("url", url, "")
    if ident:
        return ("id", ident, name)
    return ("name", name, _stable_json_sha1(row))


def _quality_tuple(row: Dict[str, Any]) -> Tuple[int, int, int]:
    # Prefer substantive extracted text over raw RTF noise, then richer rows.
    text_quality = _text_quality_tuple(_row_text(row))
    has_url = 1 if _row_url(row) else 0
    name_len = len(_row_name(row))
    return (*text_quality, has_url, name_len)


def _prefer_row(existing: Dict[str, Any], new_row: Dict[str, Any]) -> Dict[str, Any]:
    if _quality_tuple(new_row) > _quality_tuple(existing):
        return new_row
    return existing


def _extract_state_from_row(row: Dict[str, Any]) -> Optional[str]:
    j = _normalize_text(row.get("legislationJurisdiction"))
    if j.startswith("US-") and len(j) == 5:
        code = j[-2:].upper()
        if re.fullmatch(r"[A-Z]{2}", code):
            return code
    ident = _row_identifier(row).upper()
    m = re.match(r"^([A-Z]{2})[-_]", ident)
    if m:
        return m.group(1)
    return None


def _status_rank(status: str) -> int:
    s = (status or "").lower()
    return {
        "success": 4,
        "partial_success": 3,
        "completed": 2,
        "timeout": 1,
        "error": 0,
    }.get(s, 0)


def _discover_jsonld_files(root: Path) -> Dict[str, List[Path]]:
    per_state: Dict[str, List[Path]] = {}
    for path in root.rglob("STATE-*.jsonld"):
        m = STATE_RE.search(path.name)
        if not m:
            continue
        st = m.group(1)
        per_state.setdefault(st, []).append(path)
    return per_state


def _discover_summary_files(root: Path) -> Dict[str, List[Path]]:
    per_state: Dict[str, List[Path]] = {}
    for path in root.rglob("*.json"):
        if path.name in {"summary.json", "coverage_report.json", "manifest.json"}:
            continue
        m = SUMMARY_RE.fullmatch(path.name)
        if not m:
            continue
        st = m.group(1)
        per_state.setdefault(st, []).append(path)
    return per_state


def _is_state_admin_corpus_jsonl(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".jsonl") and (
        "state_admin_rule_kg_corpus" in name
        or "state_admin_rules" in name and "corpus" in name
    )


def _discover_corpus_jsonl_files(root: Path) -> List[Path]:
    out: List[Path] = []
    for path in root.rglob("*.jsonl"):
        if _is_state_admin_corpus_jsonl(path):
            out.append(path)
    return out


def _extract_state_from_any_row(row: Dict[str, Any]) -> Optional[str]:
    direct = str(row.get("state_code") or row.get("state") or "").strip().upper()
    if re.fullmatch(r"[A-Z]{2}", direct):
        return direct
    return _extract_state_from_row(row)


def _load_corpus_rows_by_state(paths: Sequence[Path]) -> Dict[str, List[CandidateRow]]:
    by_state: Dict[str, List[CandidateRow]] = {}
    for path in sorted(paths):
        rows = _read_jsonl(path)
        for row in rows:
            st = _extract_state_from_any_row(row)
            if not st:
                continue
            by_state.setdefault(st, []).append(CandidateRow(state=st, source_file=path, row=row))
    return by_state


def _merge_map_list(dst: Dict[str, List[Path]], src: Dict[str, List[Path]]) -> Dict[str, List[Path]]:
    out = dict(dst)
    for st, files in src.items():
        out.setdefault(st, []).extend(files)
    return out


def _merge_state_rows(state: str, files: Iterable[Path]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    by_key: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    input_rows = 0
    file_count = 0

    for path in sorted(files):
        file_count += 1
        rows = _read_jsonl(path)
        for row in rows:
            input_rows += 1
            row_state = _extract_state_from_row(row)
            if row_state and row_state != state:
                continue
            key = _dedupe_key(row)
            if key in by_key:
                by_key[key] = _prefer_row(by_key[key], row)
            else:
                by_key[key] = row

    merged_rows = list(by_key.values())
    merged_rows.sort(key=lambda r: (_quality_tuple(r), _row_url(r)), reverse=True)

    stats = {
        "state": state,
        "source_jsonld_files": file_count,
        "source_rows_total": input_rows,
        "merged_rows_total": len(merged_rows),
    }
    return merged_rows, stats


def _choose_best_summary(files: Iterable[Path]) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_rank: Optional[Tuple[int, int, int]] = None
    for path in sorted(files):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        rules_count = int(obj.get("rules_count") or 0)
        status = str(obj.get("status") or "")
        rank = (_status_rank(status), rules_count, int(path.stat().st_mtime))
        if best is None or rank > (best_rank or (-1, -1, -1)):
            best = dict(obj)
            best["_source_file"] = str(path)
            best_rank = rank
    return best


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge state admin run outputs into canonical datasets")
    parser.add_argument(
        "--input-root",
        action="append",
        default=None,
        help="Input root directory containing prior run artifacts (repeatable)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: artifacts/state_admin_rules/canonical_merged_<UTC timestamp>)",
    )
    parser.add_argument(
        "--include-corpus-jsonl",
        action="store_true",
        help="Also ingest state-admin corpus JSONL rows found in input roots",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=None,
        help="Restrict the merge to one or more state codes. Repeatable or comma-separated.",
    )
    args = parser.parse_args()

    input_roots = [Path(p).resolve() for p in (args.input_root or ["artifacts/state_admin_rules"])]
    missing = [str(p) for p in input_roots if not p.exists()]
    if missing:
        raise SystemExit(f"Input root(s) do not exist: {', '.join(missing)}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (Path("artifacts/state_admin_rules").resolve() / f"canonical_merged_{ts}")
    state_jsonld_dir = output_dir / "state_admin_rules_jsonld"
    state_summary_dir = output_dir / "state_summaries"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_jsonld_dir.mkdir(parents=True, exist_ok=True)
    state_summary_dir.mkdir(parents=True, exist_ok=True)

    jsonld_files: Dict[str, List[Path]] = {}
    summary_files: Dict[str, List[Path]] = {}
    corpus_jsonl_files: List[Path] = []
    for root in input_roots:
        jsonld_files = _merge_map_list(jsonld_files, _discover_jsonld_files(root))
        summary_files = _merge_map_list(summary_files, _discover_summary_files(root))
        if args.include_corpus_jsonl:
            corpus_jsonl_files.extend(_discover_corpus_jsonl_files(root))

    corpus_rows_by_state: Dict[str, List[CandidateRow]] = {}
    if args.include_corpus_jsonl:
        corpus_rows_by_state = _load_corpus_rows_by_state(corpus_jsonl_files)

    selected_states = _parse_selected_states(args.state)
    if selected_states is not None:
        allowed = set(selected_states)
        jsonld_files = {state: paths for state, paths in jsonld_files.items() if state in allowed}
        summary_files = {state: paths for state, paths in summary_files.items() if state in allowed}
        corpus_rows_by_state = {state: rows for state, rows in corpus_rows_by_state.items() if state in allowed}

    all_states = sorted(set(jsonld_files.keys()) | set(summary_files.keys()) | set(corpus_rows_by_state.keys()))

    merged_manifest: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_roots": [str(p) for p in input_roots],
        "output_dir": str(output_dir),
        "include_corpus_jsonl": bool(args.include_corpus_jsonl),
        "selected_states": list(selected_states or []),
        "states": {},
        "totals": {
            "states_seen": len(all_states),
            "states_with_jsonld": 0,
            "rows_combined_jsonl": 0,
            "source_corpus_jsonl_files": len(corpus_jsonl_files),
        },
    }

    combined_jsonl = output_dir / "state_admin_rules_canonical.jsonl"
    with combined_jsonl.open("w", encoding="utf-8") as combined_handle:
        for state in all_states:
            state_entry: Dict[str, Any] = {
                "source_jsonld_files": len(jsonld_files.get(state, [])),
                "source_summary_files": len(summary_files.get(state, [])),
                "source_corpus_rows": len(corpus_rows_by_state.get(state, [])),
            }

            merged_rows: List[Dict[str, Any]] = []
            state_jsonld_inputs = list(jsonld_files.get(state, []))
            tmp_jsonl_for_state = corpus_rows_by_state.get(state, [])
            temp_state_jsonl_path: Optional[Path] = None
            if tmp_jsonl_for_state:
                temp_state_jsonl_path = output_dir / f".tmp_corpus_{state}.jsonl"
                with temp_state_jsonl_path.open("w", encoding="utf-8") as handle:
                    for c in tmp_jsonl_for_state:
                        handle.write(json.dumps(c.row, ensure_ascii=False) + "\n")
                state_jsonld_inputs.append(temp_state_jsonl_path)

            if state_jsonld_inputs:
                merged_rows, stats = _merge_state_rows(state, state_jsonld_inputs)
                state_entry.update(stats)
                out_state_jsonld = state_jsonld_dir / f"STATE-{state}.jsonld"
                with out_state_jsonld.open("w", encoding="utf-8") as handle:
                    for row in merged_rows:
                        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                        combined_handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                merged_manifest["totals"]["states_with_jsonld"] += 1
                merged_manifest["totals"]["rows_combined_jsonl"] += len(merged_rows)
                state_entry["canonical_jsonld"] = str(out_state_jsonld)

            if temp_state_jsonl_path and temp_state_jsonl_path.exists():
                temp_state_jsonl_path.unlink(missing_ok=True)

            best_summary = _choose_best_summary(summary_files.get(state, []))
            if best_summary is not None:
                out_summary = state_summary_dir / f"{state}.json"
                out_summary.write_text(json.dumps(best_summary, indent=2, sort_keys=True), encoding="utf-8")
                state_entry["canonical_summary_json"] = str(out_summary)
                state_entry["canonical_summary_status"] = best_summary.get("status")
                state_entry["canonical_summary_rules_count"] = int(best_summary.get("rules_count") or 0)

            merged_manifest["states"][state] = state_entry

    (output_dir / "manifest.json").write_text(
        json.dumps(merged_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(json.dumps({
        "ok": True,
        "output_dir": str(output_dir),
        "states_seen": merged_manifest["totals"]["states_seen"],
        "states_with_jsonld": merged_manifest["totals"]["states_with_jsonld"],
        "rows_combined_jsonl": merged_manifest["totals"]["rows_combined_jsonl"],
        "combined_jsonl": str(combined_jsonl),
        "manifest": str(output_dir / "manifest.json"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
