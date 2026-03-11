#!/usr/bin/env python3
"""Convert merged state-admin JSONL into standardized per-state JSONLD files.

The output schema mirrors the existing legal corpus style:
- @context / @type Legislation
- stable @id
- legislationType / legislationJurisdiction
- sourceUrl / text / identifier
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

STATE_RE = re.compile(r"^[A-Z]{2}$")


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


def _extract_state(row: Dict[str, Any]) -> Optional[str]:
    j = str(row.get("legislationJurisdiction") or "").strip().upper()
    if j.startswith("US-") and len(j) == 5:
        code = j[-2:]
        if STATE_RE.match(code):
            return code

    for key in ("state_code", "state"):
        s = str(row.get(key) or "").strip().upper()
        if STATE_RE.match(s):
            return s

    ident = str(row.get("identifier") or row.get("legislationIdentifier") or "").strip().upper()
    m = re.match(r"^([A-Z]{2})[-_:]", ident)
    if m:
        return m.group(1)

    return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(state: str, source_url: str, identifier: str, text: str) -> str:
    base = "|".join([state, source_url, identifier, text[:160]])
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    ident = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", identifier).strip("-")
    if ident:
        return f"urn:state-admin:{state}:{ident}:{digest}"
    return f"urn:state-admin:{state}:{digest}"


def _to_jsonld_row(row: Dict[str, Any], state: str) -> Dict[str, Any]:
    source_url = _normalize_text(row.get("sourceUrl") or row.get("source_url") or row.get("url") or row.get("sameAs"))
    text = _normalize_text(row.get("text") or row.get("full_text"))
    name = _normalize_text(row.get("name") or row.get("title") or row.get("description"))
    identifier = _normalize_text(row.get("identifier") or row.get("legislationIdentifier"))

    if not identifier:
        identifier = f"{state}-ADMIN-{hashlib.sha1((source_url + '|' + name).encode('utf-8')).hexdigest()[:12]}"

    if not name:
        name = f"{state} Administrative Rule"

    rid = _stable_id(state, source_url, identifier, text)

    out: Dict[str, Any] = {
        "@context": {
            "@vocab": "https://schema.org/",
            "state": "https://example.org/state-admin-rules/",
            "sourceUrl": "state:sourceUrl",
            "stateCode": "state:stateCode",
            "ruleIdentifier": "state:ruleIdentifier",
        },
        "@type": "Legislation",
        "@id": rid,
        "name": name,
        "isPartOf": {
            "@type": "CreativeWork",
            "name": f"State Administrative Rules {state}",
            "identifier": f"STATE-ADMIN-{state}",
        },
        "legislationType": "StateAdministrativeRule",
        "legislationJurisdiction": f"US-{state}",
        "identifier": identifier,
        "ruleIdentifier": identifier,
        "stateCode": state,
        "sourceUrl": source_url,
        "url": source_url,
        "sameAs": source_url,
        "text": text,
        "dateModified": datetime.now(timezone.utc).date().isoformat(),
    }

    # Keep a few passthrough fields if present for traceability.
    for k in ("description", "chapterName", "sectionName", "provenance"):
        if k in row and row[k] not in (None, ""):
            out[k] = row[k]

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert state-admin canonical JSONL to standardized per-state JSONLD")
    parser.add_argument("--input-jsonl", required=True, help="Path to combined canonical JSONL")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()

    input_jsonl = Path(args.input_jsonl).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_jsonl(input_jsonl)

    by_state: Dict[str, List[Dict[str, Any]]] = {}
    skipped = 0
    for row in rows:
        st = _extract_state(row)
        if not st:
            skipped += 1
            continue
        by_state.setdefault(st, []).append(_to_jsonld_row(row, st))

    # Deduplicate per state by @id
    totals = 0
    for st, srows in sorted(by_state.items()):
        seen = set()
        deduped: List[Dict[str, Any]] = []
        for r in srows:
            rid = str(r.get("@id") or "")
            if rid in seen:
                continue
            seen.add(rid)
            deduped.append(r)

        out_path = output_dir / f"STATE-{st}.jsonld"
        with out_path.open("w", encoding="utf-8") as handle:
            for r in deduped:
                handle.write(json.dumps(r, ensure_ascii=False) + "\n")
        totals += len(deduped)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_jsonl": str(input_jsonl),
        "output_dir": str(output_dir),
        "states": sorted(by_state.keys()),
        "state_count": len(by_state),
        "rows_written": totals,
        "rows_skipped_no_state": skipped,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
