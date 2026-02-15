#!/usr/bin/env python3
"""
Build a cross-branch inventory of U.S. federal entities (executive + legislative + judicial).

Primary source (best coverage across all 3 branches):
  - U.S. Government Manual (GOVMAN) bulk XML on govinfo.gov

Secondary sources:
  - FederalRegister.gov agencies API (best for executive + subagencies that publish rules)
  - uscourts.gov "Court Website Links" (best for enumerating court websites)

Output:
  - JSONL records, one per entity, with provenance and branch tags.

Notes:
  - GOVMAN is the closest thing to "complete across all branches".
  - FR agencies are a strong executive proxy but not exhaustive for legislative/judicial.
  - Courts: GOVMAN includes courts, but USCourts list is a great completeness check.

Refs:
  GOVMAN bulk + description: https://www.govinfo.gov/app/collection/govman and bulkdata/GOVMAN
  Govinfo API feature page: https://www.govinfo.gov/features/api  (api.data.gov key)
  FR Agencies API: https://www.federalregister.gov/api/v1/agencies
  USCourts court links: https://www.uscourts.gov/about-federal-courts/court-role-and-structure/court-website-links
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ---------------- HTTP helpers ----------------

def http_get(url: str, accept: str = "*/*", timeout: int = 90) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": "federal-entity-inventory/1.1",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_json(url: str, timeout: int = 90) -> Any:
    return json.loads(http_get(url, accept="application/json", timeout=timeout).decode("utf-8", "replace"))


def get_host(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


# ---------------- Source 1: GOVMAN via govinfo bulkdata ----------------
#
# There are two ways to get GOVMAN XML:
#   A) Use govinfo API (requires api.data.gov key) to find latest GOVMAN package.
#   B) Use bulkdata directory listing (no key) and choose latest folder heuristically.
#
# This script supports both. If you pass --govinfo-key, it uses (A).
# Otherwise it tries (B). If (B) fails, pass --govman-edition manually.

GOVINFO_API_BASE = "https://api.govinfo.gov"  # requires api.data.gov key
GOVMAN_BULK_BASE = "https://www.govinfo.gov/bulkdata/GOVMAN"

# This regex matches package IDs like GOVMAN-YYYY-MM-DD
RE_GOVMAN_EDITION = re.compile(r"\bGOVMAN-(\d{4})-(\d{2})-(\d{2})\b")


def govinfo_api_latest_govman_package(api_key: str) -> str:
    """
    Uses govinfo API to get the most recent GOVMAN package.
    """
    # The govinfo API supports listing packages within a collection.
    # Endpoint family is documented on the govinfo features/api page.
    # We keep this conservative: request a small window of latest packages.
    #
    # If the endpoint changes, you can still use --govman-edition with bulkdata.
    url = f"{GOVINFO_API_BASE}/collections/GOVMAN/2020-01-01T00:00:00Z/9999-12-31T00:00:00Z?offset=0&pageSize=1&api_key={urllib.parse.quote(api_key)}"
    data = http_get_json(url)
    # Common structure: {"packages":[{"packageId":"GOVMAN-YYYY-MM-DD", ...}], ...}
    pkgs = data.get("packages") or []
    if not pkgs:
        raise RuntimeError("govinfo API returned no GOVMAN packages. Check API key or endpoint.")
    return pkgs[0].get("packageId") or pkgs[0].get("package_id")  # be tolerant


def bulk_list_govman_editions() -> List[str]:
    """
    Attempts to list GOVMAN editions from bulkdata.
    govinfo bulkdata endpoints commonly return XML-ish directory listings.
    """
    raw = http_get(GOVMAN_BULK_BASE, accept="application/xml,text/xml,*/*")
    text = raw.decode("utf-8", "replace")
    # Heuristic: find all GOVMAN-YYYY-MM-DD strings in the listing.
    editions = sorted(set(m.group(0) for m in RE_GOVMAN_EDITION.finditer(text)))
    return editions


def choose_latest_edition(editions: List[str]) -> str:
    def to_date(e: str) -> dt.date:
        m = RE_GOVMAN_EDITION.search(e)
        if not m:
            return dt.date(1900, 1, 1)
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return dt.date(y, mo, d)
    return max(editions, key=to_date)


def bulk_find_govman_zip_url(edition: str) -> Optional[str]:
    """
    Many govinfo bulk collections provide a ZIP for the edition.
    We don't assume exact filenames; we search the edition directory listing for a .zip.
    """
    edition_url = f"{GOVMAN_BULK_BASE}/{edition}"
    raw = http_get(edition_url, accept="application/xml,text/xml,*/*")
    text = raw.decode("utf-8", "replace")
    # Find a zip filename that contains the edition
    m = re.search(rf'({re.escape(edition)}[^"<\s]*\.zip)\b', text, flags=re.IGNORECASE)
    if m:
        return f"{edition_url}/{m.group(1)}"
    # Fallback: sometimes it's just {edition}.zip
    guess = f"{edition_url}/{edition}.zip"
    try:
        http_get(guess, accept="application/zip,*/*", timeout=30)
        return guess
    except Exception:
        return None


def download_and_extract_govman_xml(zip_url: str) -> List[Tuple[str, bytes]]:
    """
    Download GOVMAN ZIP and return list of (filename, xml_bytes) for XML files inside.
    """
    import zipfile
    from io import BytesIO

    zbytes = http_get(zip_url, accept="application/zip,*/*", timeout=180)
    zf = zipfile.ZipFile(BytesIO(zbytes))
    out: List[Tuple[str, bytes]] = []
    for name in zf.namelist():
        if name.lower().endswith(".xml"):
            out.append((name, zf.read(name)))
    if not out:
        raise RuntimeError("No XML files found inside GOVMAN zip.")
    return out


# ---------------- GOVMAN XML parsing ----------------
#
# GOVMAN XML schemas have varied across years.
# We implement a resilient parser that:
#  - pulls obvious "agency/entity" nodes by searching for tags containing 'Agency'/'Entity'
#  - extracts best-effort fields: name, website, description, and hierarchical context if available
#
# For your downstream processing, you'll likely re-normalize anyway.

def text_or_none(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t or None


def find_first_text(elem: ET.Element, paths: List[str]) -> Optional[str]:
    for p in paths:
        x = elem.find(p)
        if x is not None and (x.text or "").strip():
            return (x.text or "").strip()
    return None


def parse_govman_xml_docs(xml_docs: List[Tuple[str, bytes]]) -> List[Dict[str, Any]]:
    entities: List[Dict[str, Any]] = []

    # Common field tag guesses (vary by edition)
    NAME_PATHS = [
        ".//AgencyName", ".//agencyName", ".//Name", ".//name", ".//Heading", ".//heading",
        ".//Title", ".//title"
    ]
    URL_PATHS = [
        ".//URL", ".//Url", ".//url", ".//Website", ".//website", ".//WebSite", ".//webSite",
        ".//HomePage", ".//homepage"
    ]
    DESC_PATHS = [
        ".//Description", ".//description", ".//Mission", ".//mission", ".//Summary", ".//summary"
    ]
    BRANCH_HINT_PATHS = [
        ".//Branch", ".//branch", ".//GovernmentBranch", ".//governmentBranch"
    ]

    for filename, xb in xml_docs:
        try:
            root = ET.fromstring(xb)
        except Exception:
            continue

        # Strategy: scan elements that look like "Agency" or "Entity" blocks.
        # GOVMAN is big; but this is still manageable for one edition.
        for node in root.iter():
            tag = (node.tag or "")
            if not isinstance(tag, str):
                continue
            low = tag.lower()

            if ("agency" in low or "entity" in low) and not low.endswith(("agencies", "entities")):
                name = find_first_text(node, NAME_PATHS)
                if not name:
                    continue

                url = find_first_text(node, URL_PATHS) or ""
                desc = find_first_text(node, DESC_PATHS) or ""
                branch_hint = find_first_text(node, BRANCH_HINT_PATHS) or ""

                ent = {
                    "source": "govinfo.gov bulkdata GOVMAN (U.S. Government Manual XML)",
                    "source_file": filename,
                    "name": name,
                    "website": url,
                    "host": get_host(url),
                    "description": desc,
                    "branch_hint": branch_hint,
                }
                entities.append(ent)

    # Deduplicate by (name, host) aggressively
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for e in entities:
        key = (e.get("name", "").strip().lower(), e.get("host", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    return uniq


# ---------------- Source 2: FederalRegister.gov agencies (executive-heavy) ----------------

FR_AGENCIES_URL = "https://www.federalregister.gov/api/v1/agencies"

def fetch_fr_agencies() -> List[Dict[str, Any]]:
    data = http_get_json(FR_AGENCIES_URL)
    if not isinstance(data, list):
        raise RuntimeError("Unexpected FR agencies response shape.")
    out = []
    for a in data:
        out.append({
            "source": "federalregister.gov/api/v1/agencies",
            "fr_agency_id": a.get("id"),
            "name": a.get("name"),
            "short_name": a.get("short_name"),
            "slug": a.get("slug"),
            "parent_fr_agency_id": a.get("parent_id"),
            "child_fr_agency_ids": a.get("child_ids") or [],
            "website": a.get("agency_url") or "",
            "host": get_host(a.get("agency_url") or ""),
            "fr_page_url": a.get("url"),
            "fr_json_url": a.get("json_url"),
            "description": a.get("description") or "",
        })
    return out


# ---------------- Source 3: USCourts court website links (judicial courts list) ----------------

USCOURTS_LINKS_URL = "https://www.uscourts.gov/about-federal-courts/court-role-and-structure/court-website-links"

def fetch_uscourts_court_links() -> List[Dict[str, Any]]:
    # HTML page; we extract https://*.uscourts.gov and supremecourt.gov links.
    html = http_get(USCOURTS_LINKS_URL, accept="text/html,*/*").decode("utf-8", "replace")
    urls = set(re.findall(r'https?://[a-z0-9\.\-]+/(?:[^\s"<>]*)', html, flags=re.IGNORECASE))
    keep = []
    for u in urls:
        host = get_host(u)
        if host.endswith("uscourts.gov") or host.endswith("supremecourt.gov"):
            # Normalize to base origin
            origin = urllib.parse.urlunparse((urllib.parse.urlparse(u).scheme, host, "/", "", "", ""))
            keep.append(origin)
    keep = sorted(set(keep))

    out = []
    for u in keep:
        out.append({
            "source": "uscourts.gov court-website-links",
            "name": None,
            "website": u,
            "host": get_host(u),
            "branch": "judicial",
            "entity_type": "court_website",
        })
    return out


# ---------------- Branch classification & normalization ----------------

LEGISLATIVE_HOST_HINTS = (
    "congress.gov",
    "house.gov",
    "senate.gov",
    "loc.gov",
    "gao.gov",
    "cbo.gov",
    "gpo.gov",
    "crs.gov",   # often not public, but keep as hint
    "capitol.gov",
    "aoc.gov",
)

JUDICIAL_HOST_HINTS = (
    "uscourts.gov",
    "supremecourt.gov",
    "fjc.gov",
    "ussc.gov",
    "pacer.uscourts.gov",
)

EXEC_HOST_HINTS = (
    "whitehouse.gov",
    ".gov",   # default for many executive agencies
    ".mil",
)

def guess_branch(name: Optional[str], website: str, branch_hint: str = "") -> Tuple[str, float, List[str]]:
    n = (name or "").strip().lower()
    host = get_host(website)
    bh = (branch_hint or "").strip().lower()

    reasons: List[str] = []
    # Branch hint from GOVMAN (if present)
    if bh:
        if "legis" in bh:
            return "legislative", 0.95, ["govman branch_hint"]
        if "judic" in bh:
            return "judicial", 0.95, ["govman branch_hint"]
        if "exec" in bh:
            return "executive", 0.95, ["govman branch_hint"]

    if any(h in host for h in JUDICIAL_HOST_HINTS):
        return "judicial", 0.98, [f"host:{host} matches judicial"]
    if any(h in host for h in LEGISLATIVE_HOST_HINTS):
        return "legislative", 0.98, [f"host:{host} matches legislative"]

    # Name heuristics
    if "court" in n or "judicial" in n:
        return "judicial", 0.85, ["name heuristic"]
    if "congress" in n or "senate" in n or "house of representatives" in n:
        return "legislative", 0.85, ["name heuristic"]

    # Default: executive (most .gov not matched above)
    if host.endswith(".gov") or host.endswith(".mil"):
        reasons.append("default-to-executive (.gov/.mil and not matched elsewhere)")
        return "executive", 0.70, reasons

    return "other", 0.40, ["no strong signals"]


def stable_id(branch: str, host: str, name: Optional[str]) -> str:
    # Deterministic-ish, human-readable ID
    slug = re.sub(r"[^a-z0-9]+", "-", (name or host or "unknown").strip().lower()).strip("-")
    slug = slug[:60] if slug else "unknown"
    host_part = re.sub(r"[^a-z0-9]+", "-", host.lower()).strip("-")[:60]
    return f"{branch}:{host_part or 'nohost'}:{slug}"


def merge_records(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge by host+name where possible; keep provenance.
    """
    index: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def key(r: Dict[str, Any]) -> Tuple[str, str]:
        return ((r.get("host") or "").lower(), (r.get("name") or "").strip().lower())

    def upsert(r: Dict[str, Any]) -> None:
        k = key(r)
        if k not in index:
            index[k] = {
                "sources": [],
                "name": r.get("name"),
                "website": r.get("website") or "",
                "host": r.get("host") or get_host(r.get("website") or ""),
                "description": r.get("description") or "",
                "raw": [],
            }
        index[k]["sources"].append(r.get("source"))
        index[k]["raw"].append(r)

        # Prefer a non-empty website and description
        if not index[k]["website"] and r.get("website"):
            index[k]["website"] = r["website"]
            index[k]["host"] = get_host(r["website"])
        if (not index[k]["description"]) and r.get("description"):
            index[k]["description"] = r["description"]

    for r in primary:
        upsert(r)
    for r in secondary:
        upsert(r)

    return list(index.values())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="-", help="Output JSONL path or '-' for stdout")
    ap.add_argument("--govinfo-key", default="", help="Optional api.data.gov key for govinfo API (to find latest GOVMAN edition)")
    ap.add_argument("--govman-edition", default="", help="Optional explicit edition like GOVMAN-2024-12-01")
    ap.add_argument("--skip-fr", action="store_true", help="Skip FederalRegister agency crosswalk")
    ap.add_argument("--skip-uscourts", action="store_true", help="Skip USCourts court website links")
    args = ap.parse_args()

    # ---- GOVMAN edition selection ----
    edition = args.govman_edition.strip()

    if not edition:
        if args.govinfo_key:
            try:
                edition = govinfo_api_latest_govman_package(args.govinfo_key)
            except Exception as e:
                print(f"[warn] govinfo API path failed: {e}", file=sys.stderr)

    if not edition:
        try:
            editions = bulk_list_govman_editions()
            if not editions:
                raise RuntimeError("No editions found in bulk listing.")
            edition = choose_latest_edition(editions)
        except Exception as e:
            raise SystemExit(
                "Could not auto-discover GOVMAN edition from bulkdata.\n"
                f"Error: {e}\n"
                "Fix: pass --govman-edition GOVMAN-YYYY-MM-DD (and optionally update logic).\n"
            )

    # ---- Download GOVMAN XML ----
    zip_url = bulk_find_govman_zip_url(edition)
    if not zip_url:
        raise SystemExit(
            f"Could not find GOVMAN zip for {edition} via bulkdata listing.\n"
            "Fix: inspect https://www.govinfo.gov/bulkdata/GOVMAN manually and adjust bulk_find_govman_zip_url()."
        )

    xml_docs = download_and_extract_govman_xml(zip_url)
    govman_entities = parse_govman_xml_docs(xml_docs)

    # ---- Optional: FR agencies ----
    fr_entities: List[Dict[str, Any]] = []
    if not args.skip_fr:
        try:
            fr_entities = fetch_fr_agencies()
        except Exception as e:
            print(f"[warn] FederalRegister fetch failed: {e}", file=sys.stderr)

    # ---- Optional: USCourts list ----
    court_entities: List[Dict[str, Any]] = []
    if not args.skip_uscourts:
        try:
            court_entities = fetch_uscourts_court_links()
        except Exception as e:
            print(f"[warn] USCourts court link fetch failed: {e}", file=sys.stderr)

    # ---- Merge + normalize ----
    merged = merge_records(govman_entities, fr_entities + court_entities)

    out_f = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        for r in merged:
            branch, conf, reasons = guess_branch(r.get("name"), r.get("website", ""), branch_hint="")
            rid = stable_id(branch, r.get("host") or "", r.get("name"))
            rec = {
                "id": rid,
                "branch": branch,
                "branch_confidence": conf,
                "branch_reasons": reasons,
                "name": r.get("name"),
                "website": r.get("website"),
                "host": r.get("host"),
                "description": r.get("description"),
                "sources": sorted(set(r.get("sources") or [])),
                "provenance": r.get("raw"),  # keep raw source objects for later reconciliation
            }
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    finally:
        if out_f is not sys.stdout:
            out_f.close()


if __name__ == "__main__":
    main()
