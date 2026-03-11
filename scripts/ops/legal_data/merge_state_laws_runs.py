#!/usr/bin/env python3
"""Merge state-law artifacts into canonical per-state JSONLD and global JSONL.

This script scans prior run directories (workspace and /tmp), ingests state-law JSONLD
from both JSONL-style and full JSON files, deduplicates per state, and writes:
- per-state canonical JSONLD files (STATE-XX.jsonld)
- combined canonical JSONL file
- merge manifest with source and row counts
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

STATE_FILE_RE = re.compile(r"(?:^|[_-])([A-Z]{2})(?:\.[^.]+)?$")
JURIS_RE = re.compile(r"^US-([A-Z]{2})$")


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _stable_hash(obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _path_state_hint(path: Path) -> Optional[str]:
    # Common layouts: .../state_XX.jsonld, .../STATE-XX.jsonld, .../<XX>/parsed/jsonld/*.jsonld
    m = re.search(r"(?:state[_-]|STATE-)([A-Z]{2})\.jsonld$", path.name)
    if m:
        return m.group(1)
    parts = path.parts
    for i, part in enumerate(parts):
        if re.fullmatch(r"[A-Z]{2}", part):
            if i + 2 < len(parts) and parts[i + 1] == "parsed" and parts[i + 2] == "jsonld":
                return part
    return None


def _row_state(row: Dict[str, Any], path_hint: Optional[str] = None) -> Optional[str]:
    direct = _normalize(row.get("stateCode") or row.get("state_code") or row.get("state")).upper()
    if re.fullmatch(r"[A-Z]{2}", direct):
        return direct

    for key in ("jurisdiction", "legislationJurisdiction"):
        j = _normalize(row.get(key)).upper()
        m = JURIS_RE.fullmatch(j)
        if m:
            return m.group(1)

    ident = _normalize(row.get("identifier") or row.get("legislationIdentifier") or row.get("@id")).upper()
    m = re.match(r"^([A-Z]{2})[-_ ]", ident)
    if m:
        return m.group(1)

    return path_hint


def _row_url(row: Dict[str, Any]) -> str:
    return _normalize(
        row.get("sourceUrl")
        or row.get("source_url")
        or row.get("url")
        or row.get("sameAs")
        or row.get("@id")
    )


def _row_identifier(row: Dict[str, Any]) -> str:
    return _normalize(row.get("identifier") or row.get("legislationIdentifier") or row.get("@id"))


def _row_name(row: Dict[str, Any]) -> str:
    return _normalize(row.get("name") or row.get("title") or row.get("sectionName"))


def _row_text(row: Dict[str, Any]) -> str:
    return _normalize(row.get("text") or row.get("full_text") or row.get("description"))


def _dedupe_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    ident = _row_identifier(row).lower()
    url = _row_url(row).lower()
    name = _row_name(row).lower()
    if ident:
        return ("id", ident, "")
    if url:
        return ("url", url, "")
    return ("name", name, _stable_hash(row))


def _quality(row: Dict[str, Any]) -> Tuple[int, int, int]:
    return (
        len(_row_text(row)),
        1 if _row_url(row) else 0,
        len(_row_name(row)),
    )


def _parse_jsonl(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _expand_json_obj(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, dict):
        has_part = obj.get("hasPart")
        if isinstance(has_part, list):
            parts = [p for p in has_part if isinstance(p, dict)]
            if parts:
                return parts
        return [obj]
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    return []


def _read_records(path: Path) -> List[Dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []

    text = text.strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        rows = _expand_json_obj(parsed)
        if rows:
            return rows
    except Exception:
        pass

    return _parse_jsonl(text)


def _is_candidate_jsonld(path: Path) -> bool:
    lower = str(path).lower()
    if "state_laws" in lower:
        return True
    if "hf_ipfs_state_laws_upload" in lower and "/parsed/jsonld/" in lower:
        return True
    return False


def _discover_jsonld_files(root: Path) -> Dict[str, List[Path]]:
    per_state: Dict[str, List[Path]] = {}

    direct_patterns = ("state_??.jsonld", "STATE-??.jsonld")
    for pattern in direct_patterns:
        for path in root.rglob(pattern):
            if not _is_candidate_jsonld(path):
                continue
            st = _path_state_hint(path)
            if st:
                per_state.setdefault(st, []).append(path)

    # Include per-chapter JSONLD trees in prior uploads/runs (e.g., OR in /tmp).
    for path in root.rglob("*.jsonld"):
        if not _is_candidate_jsonld(path):
            continue
        if not any(seg == "parsed" for seg in path.parts):
            continue
        if not any(seg == "jsonld" for seg in path.parts):
            continue
        st = _path_state_hint(path)
        if st:
            per_state.setdefault(st, []).append(path)

    # Deduplicate paths while preserving sorted order.
    for st in list(per_state.keys()):
        uniq = sorted({p.resolve() for p in per_state[st]})
        per_state[st] = uniq
    return per_state


def _discover_summary_files(root: Path) -> Dict[str, List[Path]]:
    per_state: Dict[str, List[Path]] = {}
    for path in root.rglob("*.json"):
        if "state_laws" not in str(path).lower():
            continue
        if not re.fullmatch(r"[A-Z]{2}\.json", path.name):
            continue
        st = path.stem
        per_state.setdefault(st, []).append(path)
    return per_state


def _merge_map_list(dst: Dict[str, List[Path]], src: Dict[str, List[Path]]) -> Dict[str, List[Path]]:
    out = dict(dst)
    for st, paths in src.items():
        out.setdefault(st, []).extend(paths)
    return out


def _merge_state_rows(state: str, files: Sequence[Path]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    by_key: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    source_rows_total = 0

    for path in sorted(files):
        hint = _path_state_hint(path)
        for row in _read_records(path):
            source_rows_total += 1
            row_st = _row_state(row, path_hint=hint)
            if row_st and row_st != state:
                continue

            key = _dedupe_key(row)
            existing = by_key.get(key)
            if existing is None or _quality(row) > _quality(existing):
                by_key[key] = row

    merged = list(by_key.values())
    merged.sort(key=lambda r: (_quality(r), _row_url(r)), reverse=True)

    stats = {
        "state": state,
        "source_jsonld_files": len(files),
        "source_rows_total": source_rows_total,
        "merged_rows_total": len(merged),
    }
    return merged, stats


def _status_rank(status: str) -> int:
    s = (status or "").lower()
    return {
        "success": 4,
        "partial_success": 3,
        "completed": 2,
        "timeout": 1,
        "error": 0,
    }.get(s, 0)


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
        rules_count = int(obj.get("rules_count") or obj.get("final_real") or 0)
        rank = (_status_rank(str(obj.get("status") or "")), rules_count, int(path.stat().st_mtime))
        if best is None or rank > (best_rank or (-1, -1, -1)):
            best = dict(obj)
            best["_source_file"] = str(path)
            best_rank = rank
    return best


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge state-law run outputs into canonical JSONLD/JSONL datasets")
    parser.add_argument(
        "--input-root",
        action="append",
        default=None,
        help="Input root directory containing prior run artifacts (repeatable)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: artifacts/state_laws/canonical_merged_<UTC timestamp>)",
    )
    args = parser.parse_args()

    input_roots = [Path(p).resolve() for p in (args.input_root or ["data/state_laws", "/tmp"])]
    missing = [str(p) for p in input_roots if not p.exists()]
    if missing:
        raise SystemExit(f"Input root(s) do not exist: {', '.join(missing)}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (Path("artifacts/state_laws").resolve() / f"canonical_merged_{ts}")
    state_jsonld_dir = output_dir / "state_laws_jsonld"
    state_summary_dir = output_dir / "state_summaries"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_jsonld_dir.mkdir(parents=True, exist_ok=True)
    state_summary_dir.mkdir(parents=True, exist_ok=True)

    jsonld_files: Dict[str, List[Path]] = {}
    summary_files: Dict[str, List[Path]] = {}
    for root in input_roots:
        jsonld_files = _merge_map_list(jsonld_files, _discover_jsonld_files(root))
        summary_files = _merge_map_list(summary_files, _discover_summary_files(root))

    all_states = sorted(set(jsonld_files.keys()) | set(summary_files.keys()))

    manifest: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_roots": [str(p) for p in input_roots],
        "output_dir": str(output_dir),
        "states": {},
        "totals": {
            "states_seen": len(all_states),
            "states_with_jsonld": 0,
            "rows_combined_jsonl": 0,
        },
    }

    combined_jsonl = output_dir / "state_laws_canonical.jsonl"
    with combined_jsonl.open("w", encoding="utf-8") as combined_handle:
        for state in all_states:
            entry: Dict[str, Any] = {
                "source_jsonld_files": len(jsonld_files.get(state, [])),
                "source_summary_files": len(summary_files.get(state, [])),
            }

            files = sorted({p.resolve() for p in jsonld_files.get(state, [])})
            if files:
                merged_rows, stats = _merge_state_rows(state, files)
                entry.update(stats)
                out_state_jsonld = state_jsonld_dir / f"STATE-{state}.jsonld"
                with out_state_jsonld.open("w", encoding="utf-8") as handle:
                    for row in merged_rows:
                        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                        combined_handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                manifest["totals"]["states_with_jsonld"] += 1
                manifest["totals"]["rows_combined_jsonl"] += len(merged_rows)
                entry["canonical_jsonld"] = str(out_state_jsonld)

            best_summary = _choose_best_summary(summary_files.get(state, []))
            if best_summary is not None:
                out_summary = state_summary_dir / f"{state}.json"
                out_summary.write_text(json.dumps(best_summary, indent=2, sort_keys=True), encoding="utf-8")
                entry["canonical_summary_json"] = str(out_summary)
                entry["canonical_summary_status"] = best_summary.get("status")

            manifest["states"][state] = entry

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "output_dir": str(output_dir),
                "states_seen": manifest["totals"]["states_seen"],
                "states_with_jsonld": manifest["totals"]["states_with_jsonld"],
                "rows_combined_jsonl": manifest["totals"]["rows_combined_jsonl"],
                "combined_jsonl": str(combined_jsonl),
                "manifest": str(output_dir / "manifest.json"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
