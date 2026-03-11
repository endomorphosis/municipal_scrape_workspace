#!/usr/bin/env python3
"""Merge prior state court-rules runs into canonical JSONL and per-state JSONLD.

The historical court-rules data in this workspace is split across:
- supplemental procedural-rules JSONL files written to /tmp
- Oregon local-court-rules indexed parquet artifacts

This script discovers those prior run artifacts, normalizes them into a shared
record shape, filters obvious navigation noise, deduplicates overlapping rows,
and writes:
- one combined canonical JSONL file
- one per-state JSONLD file per state
- a manifest with source discovery and per-state counts
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import pyarrow.parquet as pq  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    pq = None


ASSET_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".woff",
    ".woff2",
    ".ttf",
)

GOOD_SIGNAL_RE = re.compile(
    r"""
    \blocal\s+court\s+rules?\b|
    \bcourt\s+rules?\b|
    \brules?\s+of\s+civil\s+procedure\b|
    \brules?\s+of\s+criminal\s+procedure\b|
    \bcivil\s+procedure\b|
    \bcriminal\s+procedure\b|
    \bcivil\s+practice\b|
    \bcriminal\s+practice\b|
    \bspecial\s+civil\b|
    \borcp\b|
    \borcrp\b|
    \bndrcivp\b|
    \bndrcrimp\b|
    revisor\.mn\.gov/court_rules/|
    /legal-resources/rules|
    /pages/rules|
    /court_rules/
    """,
    re.IGNORECASE | re.VERBOSE,
)

INDEXED_PARQUET_RULE_RE = re.compile(
    r"""
    \blocal\s+court\s+rules?\b|
    \bcourt\s+rules?\b|
    \blocal\s+rule\b|
    \bslr\b|
    \butcr\b|
    \brules?,\s+etiquette\b|
    /pages/rules|/pages/courtrules|/rules/pages/slr-
    """,
    re.IGNORECASE | re.VERBOSE,
)

BAD_NAME_RE = re.compile(
    r"^(?:image\b|more\.\.\.$|read more\.\.\.$|how you know|home$|contact us$|careers$|your visit$|"
    r"forms/rules/fees arrow_drop_down$|application$|eligibility$|compensable costs$|"
    r"today's superior court cases$|superior court case search$|back to top$|law library$)",
    re.IGNORECASE,
)

BAD_URL_RE = re.compile(
    r"justia\.com/(?:lawyers|individuals)(?:/|$)|"
    r"justia\.com/(?:injury|criminal|family|bankruptcy|business|employment|estate-planning)(?:/|$)|"
    r"dccourts\.gov/.*/(?:case-calendars|case-search|crime-victims-compensation-program)(?:/|$)",
    re.IGNORECASE,
)

FAMILY_RE = re.compile(
    r"rules?\s+of\s+civil\s+procedure|civil\s+procedure|civil\s+practice|special\s+civil|orcp|ndrcivp",
    re.IGNORECASE,
)

CRIMINAL_RE = re.compile(
    r"rules?\s+of\s+criminal\s+procedure|criminal\s+procedure|criminal\s+practice|orcrp|ndrcrimp",
    re.IGNORECASE,
)


def _parse_selected_states(values: Optional[List[str]]) -> Optional[List[str]]:
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
class SourceRecord:
    source_file: Path
    source_kind: str
    raw: Dict[str, Any]


def _norm(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.split()).strip()
    return str(value).strip()


def _pick(mapping: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _default_output_dir() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Path("artifacts") / "state_court_rules" / f"canonical_merged_{stamp}"


def _discover_jsonl_files(root: Path) -> List[Path]:
    return sorted(path for path in root.rglob("us_state_procedural_rules*.jsonl") if path.is_file())


def _discover_parquet_files(root: Path) -> List[Path]:
    return sorted(path for path in root.rglob("oregon_local_court_rules_indexed.parquet") if path.is_file())


def _load_jsonl_sources(path: Path) -> Iterator[SourceRecord]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield SourceRecord(source_file=path, source_kind="supplemental_jsonl", raw=row)


def _load_parquet_sources(path: Path) -> Iterator[SourceRecord]:
    if pq is None:
        raise RuntimeError("pyarrow is required to read parquet source files")
    table = pq.read_table(path)
    for row in table.to_pylist():
        if isinstance(row, dict):
            yield SourceRecord(source_file=path, source_kind="indexed_parquet", raw=row)


def _record_code_name(record: Dict[str, Any], wrapper: Dict[str, Any]) -> str:
    is_part_of = record.get("isPartOf")
    if isinstance(is_part_of, dict):
        name = is_part_of.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return _norm(_pick(record, "code_name", "codeName") or _pick(wrapper, "code_name", "codeName"))


def _infer_family(record: Dict[str, Any], wrapper: Dict[str, Any], source_kind: str) -> str:
    if source_kind == "indexed_parquet":
        dataset_family = _norm(record.get("dataset_family"))
        if dataset_family:
            return dataset_family
    signal = "\n".join(
        [
            _norm(_pick(record, "name")),
            _norm(_pick(record, "titleName", "title_name")),
            _norm(_pick(record, "chapterName", "chapter_name")),
            _norm(_pick(record, "sectionName", "section_name")),
            _norm(_pick(record, "sourceUrl", "source_url")),
            _norm(_pick(record, "text")),
        ]
    )
    lower_signal = signal.lower()
    if "local court rules" in lower_signal:
        return "local_court_rules"
    if "special civil" in lower_signal or "special-civil" in lower_signal:
        return "civil_procedure"
    if "civil practice" in lower_signal:
        return "civil_procedure"
    if "criminal practice" in lower_signal:
        return "criminal_procedure"
    has_civil = FAMILY_RE.search(signal) is not None
    has_criminal = CRIMINAL_RE.search(signal) is not None
    if has_civil and has_criminal:
        return "civil_and_criminal_procedure"
    if has_civil:
        return "civil_procedure"
    if has_criminal:
        return "criminal_procedure"
    family = _norm(_pick(wrapper, "procedure_family", "procedureFamily"))
    if family:
        return family
    return "court_rules"


def _default_context(state_code: str) -> Dict[str, str]:
    lower = state_code.lower() if state_code else "us"
    return {
        "@vocab": "https://schema.org/",
        "state": f"https://www.usa.gov/states/{lower}",
        "stateCode": "state:code",
        "sectionNumber": "state:sectionNumber",
        "sourceUrl": "state:sourceUrl",
    }


def _make_identifier(state_code: str, name: str, url: str, family: str) -> str:
    base = "|".join(part for part in [state_code.upper(), family, name, url] if part)
    digest = abs(hash(base))
    return f"{state_code.upper()}-{family}-{digest}"


def _make_id(state_code: str, identifier: str) -> str:
    if not identifier:
        return ""
    return f"urn:state:{state_code.lower()}:court-rule:{identifier}"


def _normalize(source: SourceRecord) -> Optional[Dict[str, Any]]:
    wrapper = source.raw
    record = wrapper.get("record") if isinstance(wrapper.get("record"), dict) else wrapper

    state_code = _norm(
        _pick(wrapper, "jurisdiction_code", "stateCode")
        or _pick(record, "stateCode", "state_code")
    ).upper()
    if not state_code:
        return None

    state_name = _norm(
        _pick(wrapper, "jurisdiction_name", "stateName")
        or _pick(record, "stateName", "state_name")
        or state_code
    )
    family = _infer_family(record, wrapper, source.source_kind)
    name = _norm(_pick(record, "name") or _pick(wrapper, "name"))
    url = _norm(_pick(record, "sourceUrl", "source_url") or _pick(wrapper, "sourceUrl", "source_url"))
    direct_signal = f"{name}\n{url}".lower()
    if "special civil" in direct_signal or "special-civil" in direct_signal:
        family = "civil_procedure"
    title_name = _pick(record, "titleName", "title_name") or _pick(wrapper, "titleName", "title_name")
    chapter_name = _pick(record, "chapterName", "chapter_name") or _pick(wrapper, "chapterName", "chapter_name")
    section_name = _pick(record, "sectionName", "section_name") or _pick(wrapper, "sectionName", "section_name")
    text = _pick(record, "text") or _pick(wrapper, "text")
    code_name = _record_code_name(record, wrapper)
    identifier = _norm(
        _pick(record, "identifier", "statute_id")
        or _pick(wrapper, "identifier", "statute_id")
        or _pick(record, "sectionNumber", "section_number")
    )
    if not identifier:
        identifier = _make_identifier(state_code=state_code, name=name, url=url, family=family)

    normalized = {
        "@context": _pick(record, "@context") or _default_context(state_code),
        "@type": _pick(record, "@type") or "Legislation",
        "@id": _pick(record, "@id") or _make_id(state_code, identifier),
        "identifier": identifier,
        "name": name,
        "isPartOf": _pick(record, "isPartOf")
        or {"@type": "CreativeWork", "name": code_name or "State Court Rules", "identifier": f"{state_code}-court-rules"},
        "legislationType": _pick(record, "legislationType", "legislation_type") or "court_rule",
        "stateCode": state_code,
        "stateName": state_name,
        "titleNumber": _pick(record, "titleNumber", "title_number"),
        "titleName": title_name,
        "chapterNumber": _pick(record, "chapterNumber", "chapter_number"),
        "chapterName": chapter_name,
        "sectionNumber": _pick(record, "sectionNumber", "section_number") or identifier,
        "sectionName": section_name or name,
        "dateModified": _pick(record, "dateModified", "date_modified"),
        "sourceUrl": url,
        "chapter": _pick(record, "chapter"),
        "preamble": _pick(record, "preamble"),
        "citations": _pick(record, "citations"),
        "legislativeHistory": _pick(record, "legislativeHistory", "legislative_history"),
        "text": text,
        "subsections": _pick(record, "subsections") or [],
        "parser_warnings": _pick(record, "parser_warnings") or [],
        "procedureFamily": family,
        "datasetFamily": _pick(record, "dataset_family") or family,
        "code_name": code_name,
        "ipfs_cid": _pick(record, "ipfs_cid") or _pick(wrapper, "ipfs_cid"),
        "source_file": str(source.source_file),
        "source_kind": source.source_kind,
    }
    return normalized


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _signal_text(row: Dict[str, Any]) -> str:
    part = row.get("isPartOf")
    part_name = part.get("name") if isinstance(part, dict) else ""
    return "\n".join(
        [
            _norm(row.get("name")),
            _norm(row.get("titleName")),
            _norm(row.get("chapterName")),
            _norm(row.get("sectionName")),
            _norm(row.get("text")),
            _norm(row.get("sourceUrl")),
            _norm(row.get("code_name")),
            _norm(part_name),
            _norm(row.get("procedureFamily")),
            _norm(row.get("datasetFamily")),
        ]
    )


def _reject_reason(row: Dict[str, Any]) -> Optional[str]:
    name = _norm(row.get("name"))
    url = _norm(row.get("sourceUrl"))
    signal = _signal_text(row)
    lower_url = url.lower()
    host = _host(url)

    if not url:
        return "missing_url"
    if lower_url.endswith(ASSET_EXTENSIONS):
        return "asset_url"
    if BAD_NAME_RE.search(name):
        return "bad_name"
    if BAD_URL_RE.search(url):
        return "bad_url"
    if host == "www.courts.nh.gov" and "rules" not in lower_url and "procedure" not in signal.lower():
        return "nh_navigation"
    if host == "www.dccourts.gov" and "rules" not in lower_url and "procedure" not in signal.lower():
        return "dc_navigation"
    if row.get("source_kind") == "indexed_parquet":
        indexed_signal = "\n".join(
            [
                name,
                _norm(row.get("sectionName")),
                url,
                _norm(row.get("preamble")),
            ]
        )
        if not INDEXED_PARQUET_RULE_RE.search(indexed_signal):
            return "indexed_parquet_non_rule"
    if not GOOD_SIGNAL_RE.search(signal):
        return "weak_signal"
    return None


def _quality(row: Dict[str, Any]) -> int:
    signal = _signal_text(row).lower()
    score = 0
    if "local court rules" in signal:
        score += 120
    if "rules of civil procedure" in signal or "civil procedure" in signal:
        score += 90
    if "rules of criminal procedure" in signal or "criminal procedure" in signal:
        score += 90
    if "court rules" in signal:
        score += 70
    if "civil practice" in signal or "criminal practice" in signal:
        score += 60
    if "special civil" in signal:
        score += 50
    if "/rules" in signal or "/court_rules/" in signal or "legal-resources/rules" in signal:
        score += 30
    name = _norm(row.get("name")).lower()
    if name in {"rules", "court rules", "local court rules"}:
        score += 20
    if "arrow_drop_down" in name or name.startswith("how you know") or name.startswith("read more"):
        score -= 100
    text_len = len(_norm(row.get("text")))
    score += min(text_len // 20, 25)
    return score


def _dedupe_key(row: Dict[str, Any]) -> str:
    cid = _norm(row.get("ipfs_cid"))
    if cid:
        return f"cid:{cid}"
    url = _norm(row.get("sourceUrl")).lower()
    if url:
        return f"url:{row.get('stateCode')}:{url}"
    identifier = _norm(row.get("identifier"))
    return f"id:{row.get('stateCode')}:{identifier}:{_norm(row.get('name')).lower()}"


def _sort_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _norm(row.get("stateCode")),
        _norm(row.get("sourceUrl")),
        _norm(row.get("name")),
    )


def run(input_roots: List[Path], output_dir: Path, selected_states: Optional[List[str]] = None) -> Dict[str, Any]:
    allowed_states = set(selected_states or [])
    discovered_jsonl: List[Path] = []
    discovered_parquet: List[Path] = []
    for root in input_roots:
        if not root.exists():
            continue
        if root.is_file():
            if root.name.startswith("us_state_procedural_rules") and root.suffix == ".jsonl":
                discovered_jsonl.append(root)
            elif root.name == "oregon_local_court_rules_indexed.parquet":
                discovered_parquet.append(root)
            continue
        discovered_jsonl.extend(_discover_jsonl_files(root))
        discovered_parquet.extend(_discover_parquet_files(root))

    discovered_jsonl = sorted(set(discovered_jsonl))
    discovered_parquet = sorted(set(discovered_parquet))

    manifest: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_roots": [str(path) for path in input_roots],
        "output_dir": str(output_dir),
        "selected_states": list(selected_states or []),
        "source_files": {
            "supplemental_jsonl": [str(path) for path in discovered_jsonl],
            "indexed_parquet": [str(path) for path in discovered_parquet],
        },
        "totals": {
            "raw_records": 0,
            "accepted_records": 0,
            "deduped_records": 0,
            "states": 0,
        },
        "rejections": {},
        "states": {},
    }

    candidates: List[Dict[str, Any]] = []
    rejection_counts: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()

    for path in discovered_jsonl:
        for source in _load_jsonl_sources(path):
            manifest["totals"]["raw_records"] += 1
            normalized = _normalize(source)
            if not normalized:
                rejection_counts["normalize_failed"] += 1
                continue
            if allowed_states and _norm(normalized.get("stateCode")) not in allowed_states:
                rejection_counts["filtered_state"] += 1
                continue
            reason = _reject_reason(normalized)
            if reason:
                rejection_counts[reason] += 1
                continue
            candidates.append(normalized)
            source_counter[str(path)] += 1

    for path in discovered_parquet:
        for source in _load_parquet_sources(path):
            manifest["totals"]["raw_records"] += 1
            normalized = _normalize(source)
            if not normalized:
                rejection_counts["normalize_failed"] += 1
                continue
            if allowed_states and _norm(normalized.get("stateCode")) not in allowed_states:
                rejection_counts["filtered_state"] += 1
                continue
            reason = _reject_reason(normalized)
            if reason:
                rejection_counts[reason] += 1
                continue
            candidates.append(normalized)
            source_counter[str(path)] += 1

    manifest["totals"]["accepted_records"] = len(candidates)

    best_by_key: Dict[str, Dict[str, Any]] = {}
    for row in candidates:
        key = _dedupe_key(row)
        existing = best_by_key.get(key)
        if existing is None or _quality(row) > _quality(existing):
            best_by_key[key] = row

    merged_rows = sorted(best_by_key.values(), key=_sort_key)
    manifest["totals"]["deduped_records"] = len(merged_rows)

    rows_by_state: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in merged_rows:
        rows_by_state[_norm(row.get("stateCode"))].append(row)

    manifest["totals"]["states"] = len(rows_by_state)
    manifest["rejections"] = dict(rejection_counts)
    manifest["source_counts"] = dict(source_counter)

    output_dir.mkdir(parents=True, exist_ok=True)
    state_dir = output_dir / "state_court_rules_jsonld"
    state_dir.mkdir(parents=True, exist_ok=True)
    combined_jsonl = output_dir / "state_court_rules_canonical.jsonl"

    with combined_jsonl.open("w", encoding="utf-8") as handle:
        for row in merged_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    for state_code, rows in sorted(rows_by_state.items()):
        path = state_dir / f"STATE-{state_code}.jsonld"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        families = Counter(_norm(row.get("procedureFamily")) for row in rows)
        manifest["states"][state_code] = {
            "canonical_jsonld": str(path),
            "rows_total": len(rows),
            "counts_by_family": dict(sorted(families.items())),
        }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    manifest["combined_jsonl"] = str(combined_jsonl)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge prior state court-rules runs into canonical JSONL/JSONLD")
    parser.add_argument(
        "--input-root",
        action="append",
        default=[],
        help="Input root to search. Repeatable. Defaults to workspace root and /tmp.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(_default_output_dir()),
        help="Output directory for canonical court-rules bundle",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=None,
        help="Restrict the merge to one or more state codes. Repeatable or comma-separated.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_roots = [Path(path).expanduser().resolve() for path in (args.input_root or [".", "/tmp"])]
    if not args.input_root:
        input_roots = [Path(".").resolve(), Path("/tmp").resolve()]
    manifest = run(
        input_roots=input_roots,
        output_dir=Path(args.output_dir).expanduser().resolve(),
        selected_states=_parse_selected_states(args.state),
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())