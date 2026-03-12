#!/usr/bin/env python3
"""Build a cleaned canonical state-admin dataset from a merged canonical bundle.

Input: canonical_merged_<ts>/state_admin_rules_jsonld/STATE-XX.jsonld files
Output: cleaned bundle with per-state JSONLD, combined JSONL, and manifest.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

STATE_FILE_RE = re.compile(r"^STATE-([A-Z]{2})\.jsonld$")

# Known off-target domains repeatedly observed in noisy runs.
BLOCKED_DOMAINS = {
    "www.city-data.com",
    "city-data.com",
    "www.governmentjobs.com",
    "governmentjobs.com",
    "www.nationalgeographic.com",
    "nationalgeographic.com",
    "www.indeed.com",
    "indeed.com",
    "www.linkedin.com",
    "linkedin.com",
}

# URL fragments that are usually not administrative-rule content.
BLOCKED_URL_HINTS = (
    "/careers/",
    "employeehome",
    "/jobs",
    "/job/",
    "/news/",
    "/events/",
    "/contact-us",
    "login",
    "forgot-password",
    "codes_displaytext.xhtml",
    "codedisplayexpand.xhtml",
    "codes_displayexpandedbranch.xhtml",
    "/faces/codes.xhtml",
    "arsdetail",
)

# Positive URL cues for likely administrative rules/regulatory content.
POSITIVE_URL_HINTS = (
    "admin",
    "administrative",
    "rule",
    "rules",
    "regulation",
    "register",
    "code",
    "nmac",
    "iac",
    "nycrr",
    "tac",
    "chapter",
    "part/",
)

# Positive content cues in title/text.
POSITIVE_TEXT_HINTS = (
    "administrative code",
    "administrative rules",
    "state register",
    "chapter",
    "section",
    "rule",
    "regulation",
    "authority",
)

# Negative content cues for legislature statute/code pages that should not survive
# admin-rule cleaning even when they contain incidental words like "chapter".
NEGATIVE_TEXT_HINTS = (
    "revised statutes",
    "codes display text",
    "codes: code search",
    "civil code",
    "corporations code",
    "education code",
    "penal code",
    "bill information",
    "legislative council",
    "session laws",
    "arizona legislature",
    "agentic source",
    "source url:",
    "portal reference",
    "california legislative information",
    "quick code search",
    "quick bill search",
)


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                out.append(row)
    return out


def _row_url(row: Dict[str, Any]) -> str:
    return str(row.get("url") or row.get("sameAs") or row.get("source_url") or row.get("sourceUrl") or "").strip()


def _row_name(row: Dict[str, Any]) -> str:
    return str(row.get("name") or row.get("title") or row.get("description") or "").strip()


def _row_text(row: Dict[str, Any]) -> str:
    return str(row.get("text") or row.get("full_text") or "").strip()


def _domain_from_url(url: str) -> str:
    m = re.match(r"https?://([^/]+)", url.lower())
    return m.group(1) if m else ""


def _row_score(row: Dict[str, Any]) -> int:
    url = _row_url(row).lower()
    domain = _domain_from_url(url)
    name = _row_name(row).lower()
    text = _row_text(row).lower()

    score = 0

    if domain and domain.endswith(".gov"):
        score += 2
    if domain and domain in BLOCKED_DOMAINS:
        score -= 8

    for hint in BLOCKED_URL_HINTS:
        if hint in url:
            score -= 4

    for hint in POSITIVE_URL_HINTS:
        if hint in url:
            score += 2

    hay = f"{name}\n{text}"
    for hint in POSITIVE_TEXT_HINTS:
        if hint in hay:
            score += 1

    for hint in NEGATIVE_TEXT_HINTS:
        if hint in hay:
            score -= 3

    if "agentic source" in hay:
        score -= 4
    if "source url:" in hay or "portal reference" in hay:
        score -= 4

    if "legislature" in domain and ("code" in url or "statute" in hay or "revised statutes" in hay):
        score -= 4

    if "you need to enable javascript to run this app" in text:
        score -= 2

    # Longer extracted text tends to be more substantive.
    tlen = len(text)
    if tlen >= 1200:
        score += 2
    elif tlen >= 300:
        score += 1

    return score


def _dedupe_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    url = _row_url(row).lower()
    ident = str(row.get("identifier") or row.get("legislationIdentifier") or "").strip().lower()
    name = _row_name(row).lower()
    if url:
        return ("url", url, "")
    if ident:
        return ("id", ident, name)
    return ("name", name, "")


def _clean_state_rows(rows: List[Dict[str, Any]], threshold: int) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    # Deduplicate first while keeping highest score variant.
    best: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in rows:
        key = _dedupe_key(row)
        if key not in best:
            best[key] = row
            continue
        if _row_score(row) > _row_score(best[key]):
            best[key] = row

    deduped = list(best.values())
    scored = sorted(((_row_score(r), r) for r in deduped), key=lambda x: x[0], reverse=True)

    kept = [r for s, r in scored if s >= threshold]
    fallback_used = 0

    # Preserve coverage: if all rows are filtered, keep best one.
    if not kept and scored and scored[0][0] >= threshold:
        kept = [scored[0][1]]
        fallback_used = 1

    stats = {
        "input_rows": len(rows),
        "deduped_rows": len(deduped),
        "kept_rows": len(kept),
        "dropped_rows": max(0, len(deduped) - len(kept)),
        "fallback_used": fallback_used,
    }
    return kept, stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean merged canonical state-admin dataset")
    parser.add_argument("--input-dir", required=True, help="Path to canonical_merged_<ts> directory")
    parser.add_argument("--output-dir", default=None, help="Output dir for cleaned bundle")
    parser.add_argument("--score-threshold", type=int, default=2, help="Minimum score for keeping rows")
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    src_state_dir = input_dir / "state_admin_rules_jsonld"
    if not src_state_dir.exists():
        raise SystemExit(f"Missing source state dir: {src_state_dir}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (input_dir.parent / f"{input_dir.name}_cleaned_{ts}")
    out_state_dir = output_dir / "state_admin_rules_jsonld"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_state_dir.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_dir": str(input_dir),
        "score_threshold": int(args.score_threshold),
        "states": {},
        "totals": {
            "states": 0,
            "input_rows": 0,
            "kept_rows": 0,
            "dropped_rows": 0,
            "fallback_states": 0,
        },
    }

    combined_path = output_dir / "state_admin_rules_canonical_cleaned.jsonl"
    with combined_path.open("w", encoding="utf-8") as combined:
        for path in sorted(src_state_dir.glob("STATE-*.jsonld")):
            m = STATE_FILE_RE.match(path.name)
            if not m:
                continue
            state = m.group(1)
            rows = _read_jsonl(path)
            cleaned, stats = _clean_state_rows(rows, threshold=int(args.score_threshold))

            out_state = out_state_dir / path.name
            with out_state.open("w", encoding="utf-8") as handle:
                for row in cleaned:
                    s = json.dumps(row, ensure_ascii=False)
                    handle.write(s + "\n")
                    combined.write(s + "\n")

            manifest["states"][state] = {
                **stats,
                "source_file": str(path),
                "output_file": str(out_state),
            }
            manifest["totals"]["states"] += 1
            manifest["totals"]["input_rows"] += stats["input_rows"]
            manifest["totals"]["kept_rows"] += stats["kept_rows"]
            manifest["totals"]["dropped_rows"] += stats["dropped_rows"]
            manifest["totals"]["fallback_states"] += stats["fallback_used"]

    manifest_path = output_dir / "manifest.cleaned.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "source_dir": str(input_dir),
        "output_dir": str(output_dir),
        "combined_jsonl": str(combined_path),
        "manifest": str(manifest_path),
        **manifest["totals"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
