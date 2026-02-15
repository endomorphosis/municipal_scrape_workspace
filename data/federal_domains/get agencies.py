#!/usr/bin/env python3
"""
Fetch FederalRegister.gov agencies and emit JSONL suitable for building an "executive branch agencies" index.

Primary source:
  https://www.federalregister.gov/api/v1/agencies  (no API key required)
Docs:
  https://www.federalregister.gov/developers/documentation/api/v1

Optional enrichment source (USA.gov Federal Agency Directory API docs):
  https://github.com/usagov/Federal-Agency-Directory-API-Documentation
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


FR_AGENCIES_URL = "https://www.federalregister.gov/api/v1/agencies"


# ---------- small HTTP helper ----------
def http_get_json(url: str, timeout: int = 60) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "agency-jsonl-builder/1.0 (+https://example.invalid)",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8", errors="replace")
    return json.loads(data)


def get_host(url: str) -> str:
    if not url:
        return ""
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


# ---------- branch classification heuristics ----------
JUDICIAL_HOST_HINTS = (
    "uscourts.gov",
    "supremecourt.gov",
    "cafc.uscourts.gov",
    "cadc.uscourts.gov",
)
LEGISLATIVE_HOST_HINTS = (
    "congress.gov",
    "house.gov",
    "senate.gov",
    "loc.gov",
    "gao.gov",   # legislative branch support agency
    "crs.gov",   # (often not public, but keep as hint)
)

# Name hints are purposely broad; you can refine these as you observe false positives.
JUDICIAL_NAME_HINTS = (
    "court",
    "judicial conference",
    "bankruptcy court",
    "district court",
    "court of appeals",
    "supreme court",
)
LEGISLATIVE_NAME_HINTS = (
    "house of representatives",
    "u.s. house",
    "senate",
    "congress",
    "congressional",
    "library of congress",
    "government accountability office",
)

# Executive Office / White House often appears as EOP components.
EXEC_HOST_HINTS = (
    "whitehouse.gov",
    ".gov",  # broad; used as default if not matched by other branches
)

# Some FR agency records are historical/defunct (e.g., ACTION); keep them but tag.
DEFUNCT_NAME_HINTS = (
    "abolished",
    "terminated",
    "no longer",
    "transferred",
)


@dataclass
class BranchGuess:
    branch: str              # "executive" | "judicial" | "legislative" | "other"
    confidence: float        # 0..1
    reasons: List[str]


def guess_branch(name: str, agency_url: str) -> BranchGuess:
    n = (name or "").strip().lower()
    host = get_host(agency_url)

    reasons: List[str] = []

    # Domain-based strong signals
    if any(h in host for h in JUDICIAL_HOST_HINTS):
        return BranchGuess("judicial", 0.98, [f"host:{host} matches judicial hint"])
    if any(h in host for h in LEGISLATIVE_HOST_HINTS):
        return BranchGuess("legislative", 0.98, [f"host:{host} matches legislative hint"])

    # Name-based signals
    if any(k in n for k in JUDICIAL_NAME_HINTS):
        reasons.append("name matches judicial keywords")
        return BranchGuess("judicial", 0.90, reasons)

    if any(k in n for k in LEGISLATIVE_NAME_HINTS):
        reasons.append("name matches legislative keywords")
        return BranchGuess("legislative", 0.90, reasons)

    # Otherwise: most FR publishers are executive/independent agencies.
    # Treat as executive by default, with moderate confidence.
    if host.endswith(".gov") or any(h in host for h in EXEC_HOST_HINTS):
        reasons.append("default-to-executive for FR publisher with .gov/whitehouse host")
        return BranchGuess("executive", 0.75, reasons)

    # Fallback: unknown/other (e.g., .mil, .us, legacy domains, or blanks)
    if host.endswith(".mil"):
        reasons.append("host endswith .mil (executive/DoD ecosystem)")
        return BranchGuess("executive", 0.80, reasons)

    reasons.append("no strong signals; classify as other")
    return BranchGuess("other", 0.40, reasons)


def looks_defunct(description: str) -> bool:
    d = (description or "").lower()
    return any(h in d for h in DEFUNCT_NAME_HINTS)


# ---------- schema mapping ----------
def to_jsonl_record(fr_obj: Dict[str, Any]) -> Dict[str, Any]:
    bg = guess_branch(fr_obj.get("name", ""), fr_obj.get("agency_url", ""))

    rec: Dict[str, Any] = {
        "source": "federalregister.gov/api/v1/agencies",
        "fr_agency_id": fr_obj.get("id"),
        "name": fr_obj.get("name"),
        "short_name": fr_obj.get("short_name"),
        "slug": fr_obj.get("slug"),
        "parent_fr_agency_id": fr_obj.get("parent_id"),
        "child_fr_agency_ids": fr_obj.get("child_ids") or [],
        "agency_url": fr_obj.get("agency_url") or "",
        "agency_host": get_host(fr_obj.get("agency_url") or ""),
        "fr_page_url": fr_obj.get("url"),
        "fr_json_url": fr_obj.get("json_url"),
        "fr_recent_articles_url": fr_obj.get("recent_articles_url"),
        "description": fr_obj.get("description") or "",
        "logo": fr_obj.get("logo"),
        "branch_guess": bg.branch,
        "branch_confidence": bg.confidence,
        "branch_reasons": bg.reasons,
        "flags": {
            "defunct_or_historical_suspected": looks_defunct(fr_obj.get("description") or ""),
            "missing_agency_url": not bool(fr_obj.get("agency_url")),
        },
    }
    return rec


# ---------- optional USA.gov enrichment ----------
def enrich_with_usagov(records: List[Dict[str, Any]], enable: bool) -> None:
    """
    USA.gov agency directory API can help find canonical agency URLs/names,
    but it's not strictly executive-only and coverage varies.
    Docs: https://github.com/usagov/Federal-Agency-Directory-API-Documentation
    """
    if not enable:
        return

    # The USA.gov API base can evolve; the docs repo is the stable reference.
    # Keep this optional and non-fatal.
    #
    # If you decide to use it, update these endpoints per the docs repo.
    USAGOV_SEARCH = "https://api.gsa.gov/technology/agency-directory/v1/agencies?query={q}"  # may change

    for rec in records:
        # Only try to enrich executive-ish items where agency_url is missing or non-.gov.
        if rec.get("branch_guess") != "executive":
            continue
        if rec["agency_url"] and rec["agency_host"].endswith(".gov"):
            continue

        q = urllib.parse.quote(rec.get("name") or "")
        url = USAGOV_SEARCH.format(q=q)

        try:
            data = http_get_json(url)
        except Exception:
            continue

        # Very light-touch: attach raw candidates; you can resolve later.
        rec["usagov_candidates"] = data


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only-executive", action="store_true", help="Output only records guessed to be executive")
    ap.add_argument("--min-confidence", type=float, default=0.0, help="Filter by branch_confidence >= value")
    ap.add_argument("--enrich-usagov", action="store_true", help="Optional: attempt USA.gov directory enrichment (best-effort)")
    ap.add_argument("--out", default="-", help="Output file path, or '-' for stdout")
    args = ap.parse_args()

    fr_data = http_get_json(FR_AGENCIES_URL)
    if not isinstance(fr_data, list):
        raise SystemExit("Unexpected response shape from FederalRegister agencies endpoint")

    records = [to_jsonl_record(obj) for obj in fr_data]

    # Optional enrichment
    enrich_with_usagov(records, enable=args.enrich_usagov)

    # Filtering
    if args.only_executive:
        records = [r for r in records if r.get("branch_guess") == "executive"]
    if args.min_confidence > 0:
        records = [r for r in records if float(r.get("branch_confidence", 0.0)) >= args.min_confidence]

    # Write JSONL
    out_f = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        for r in records:
            out_f.write(json.dumps(r, ensure_ascii=False) + "\n")
    finally:
        if out_f is not sys.stdout:
            out_f.close()


if __name__ == "__main__":
    main()
