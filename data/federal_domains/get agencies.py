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
import datetime as dt
import glob
import json
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


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


def canonical_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    return h


def normalize_origin_url(url: str) -> str:
    """Normalize to scheme://host/ (empty if url is empty/unparseable)."""
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urllib.parse.urlparse(u)
        scheme = p.scheme or "https"
        host = canonical_host(p.netloc)
        if not host:
            return ""
        return urllib.parse.urlunparse((scheme, host, "/", "", "", ""))
    except Exception:
        return ""


def normalize_seed_url(url: str) -> str:
    """Normalize to scheme://host/path (no query/fragment), preserving path.

    This is useful for crawl/index seed lists where a non-root path is meaningful.
    Trailing slashes are removed except for the root path.
    """
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urllib.parse.urlparse(u)
        scheme = p.scheme or "https"
        host = canonical_host(p.netloc)
        if not host:
            return ""
        path = p.path or "/"
        path = re.sub(r"/{2,}", "/", path)
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return urllib.parse.urlunparse((scheme, host, path, "", "", ""))
    except Exception:
        return ""


def norm_name(name: Optional[str]) -> str:
    n = (name or "").strip().lower()
    n = re.sub(r"\s+", " ", n)
    return n


_NAME_STOPWORDS = {
    "u", "s", "us", "u.s", "united", "states",
    "department", "dept", "of", "the",
    "office", "agency", "administration", "commission", "committee",
    "bureau", "service", "council", "board", "authority",
    "and", "for", "on", "in", "to",
}


def name_signature(name: Optional[str]) -> Set[str]:
    n = (name or "").strip().lower()
    n = re.sub(r"[^a-z0-9\s]+", " ", n)
    toks = [t for t in n.split() if t and t not in _NAME_STOPWORDS]
    return set(toks)


def names_similar(a: Optional[str], b: Optional[str]) -> bool:
    sa = name_signature(a)
    sb = name_signature(b)
    if not sa or not sb:
        return False
    if sa.issubset(sb) or sb.issubset(sa):
        return True
    inter = len(sa & sb)
    union = len(sa | sb)
    if union == 0:
        return False
    return (inter / union) >= 0.60


def try_read_jsonlish(path: str) -> List[Dict[str, Any]]:
    """Reads JSONL-ish files (one JSON object per line, with blank lines allowed).

    Also supports:
      - a single JSON array
      - a single JSON object
    """
    raw = open(path, "r", encoding="utf-8").read()
    txt = raw.strip()
    if not txt:
        return []

    # Fast path: whole-file JSON
    if txt.startswith("[") or txt.startswith("{"):
        try:
            data = json.loads(txt)
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
            if isinstance(data, dict):
                return [data]
        except Exception:
            pass

    out: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


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
    """List GOVMAN editions from bulkdata without requiring an API key.

    The GOVMAN bulkdata root lists YEAR folders (e.g. /GOVMAN/2025).
    Each year folder contains one or more ZIPs named GOVMAN-YYYY-MM-DD.zip.
    """
    raw = http_get(GOVMAN_BULK_BASE, accept="application/xml,text/xml,text/html,*/*")
    text = raw.decode("utf-8", "replace")

    # Year folders: <name>2025</name> and/or /GOVMAN/2025
    years = sorted(set(int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", text) if y.isdigit()))
    # Be conservative: govinfo lists lots of years; we only care about those that have GOVMAN zips.

    editions: Set[str] = set()
    for y in sorted(years, reverse=True):
        try:
            year_url = f"{GOVMAN_BULK_BASE}/{y}"
            raw_y = http_get(year_url, accept="application/xml,text/xml,text/html,*/*")
            text_y = raw_y.decode("utf-8", "replace")
        except Exception:
            continue
        for m in RE_GOVMAN_EDITION.finditer(text_y):
            editions.add(m.group(0))
        # Small optimization: if we found at least one edition in the newest years, stop early.
        if editions and y >= max(years) - 1:
            # keep scanning the next year only; otherwise stop
            continue

    return sorted(editions)


def choose_latest_edition(editions: List[str]) -> str:
    def to_date(e: str) -> dt.date:
        m = RE_GOVMAN_EDITION.search(e)
        if not m:
            return dt.date(1900, 1, 1)
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return dt.date(y, mo, d)
    return max(editions, key=to_date)


def bulk_find_govman_zip_url(edition: str) -> Optional[str]:
    """Resolve an edition like GOVMAN-YYYY-MM-DD to a bulkdata ZIP URL.

    Current bulkdata layout is /GOVMAN/YYYY/GOVMAN-YYYY-MM-DD.zip.
    """
    ed = (edition or "").strip()
    if not ed:
        return None
    if ed.lower().endswith(".zip"):
        ed = ed[:-4]

    m = RE_GOVMAN_EDITION.search(ed)
    if not m:
        return None
    year = m.group(1)

    guess = f"{GOVMAN_BULK_BASE}/{year}/{ed}.zip"
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

                seed_url = normalize_seed_url(url)

                ent = {
                    "source": "govinfo.gov bulkdata GOVMAN (U.S. Government Manual XML)",
                    "source_file": filename,
                    "name": name,
                    "website": normalize_origin_url(url) or url,
                    "seed_url": seed_url,
                    "host": canonical_host(get_host(url)),
                    "description": desc,
                    "branch_hint": branch_hint,
                    "kind": "entity",
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
        agency_url = a.get("agency_url") or ""
        out.append({
            "source": "federalregister.gov/api/v1/agencies",
            "fr_agency_id": a.get("id"),
            "name": a.get("name"),
            "short_name": a.get("short_name"),
            "slug": a.get("slug"),
            "parent_fr_agency_id": a.get("parent_id"),
            "child_fr_agency_ids": a.get("child_ids") or [],
            "website": normalize_origin_url(agency_url) or agency_url,
            "seed_url": normalize_seed_url(agency_url),
            "host": canonical_host(get_host(agency_url)),
            "fr_page_url": a.get("url"),
            "fr_json_url": a.get("json_url"),
            "description": a.get("description") or "",
            "branch": "executive",
            "kind": "agency",
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
            "website": normalize_origin_url(u) or u,
            "seed_url": normalize_seed_url(u),
            "host": canonical_host(get_host(u)),
            "branch": "judicial",
            "kind": "court",
            "entity_type": "court_website",
        })
    return out


def load_curated_records(glob_pattern: str) -> List[Dict[str, Any]]:
    paths = sorted(glob.glob(glob_pattern))
    out: List[Dict[str, Any]] = []
    for p in paths:
        for obj in try_read_jsonlish(p):
            if not isinstance(obj, dict):
                continue
            seed_in = obj.get("website") or obj.get("url") or obj.get("base_url") or ""
            host = obj.get("host") or get_host(seed_in)
            rec = dict(obj)
            rec.setdefault("source", f"curated:{p}")
            rec.setdefault("name", obj.get("name") or obj.get("site_name"))
            rec["website"] = normalize_origin_url(seed_in) or (seed_in or "")
            rec["seed_url"] = normalize_seed_url(seed_in)
            rec["host"] = canonical_host(host)
            # Prefer your curated kind/branch if present
            if "kind" not in rec:
                if rec.get("court_type") or (rec.get("branch") == "judicial"):
                    rec["kind"] = "court"
                else:
                    rec["kind"] = "entity"
            if "branch" not in rec:
                # If file is e.g. legislative/judicial lists, they already include it.
                rec["branch"] = None
            out.append(rec)
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


def stable_id(branch: str, kind: str, host: str, name: Optional[str]) -> str:
    # Deterministic-ish, human-readable ID
    slug = re.sub(r"[^a-z0-9]+", "-", (name or host or "unknown").strip().lower()).strip("-")
    slug = slug[:60] if slug else "unknown"
    host_part = re.sub(r"[^a-z0-9]+", "-", host.lower()).strip("-")[:60]
    kind_part = re.sub(r"[^a-z0-9]+", "-", (kind or "entity").strip().lower()).strip("-")[:40] or "entity"
    return f"{branch}:{kind_part}:{host_part or 'nohost'}:{slug}"


def merge_records(all_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge records into one canonical row per entity.

    Strategy:
      - Prefer merging by canonical host when present.
      - Otherwise fall back to normalized name.
      - Preserve aliases + sources; optionally retain raw provenance.
    """

    index: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    host_to_keys: Dict[str, List[Tuple[Any, ...]]] = {}

    def record_key(r: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
        fr_id = r.get("fr_agency_id")
        if fr_id is not None:
            try:
                return ("fr", int(fr_id))
            except Exception:
                return ("fr", str(fr_id))
        host = canonical_host(r.get("host") or get_host(r.get("website") or ""))
        name = norm_name(r.get("name"))
        if host and name:
            return ("host_name", host, name)
        if host:
            return ("host_only", host)
        if name:
            return ("name_only", name)
        return None

    def score_website(u: str, source: str) -> int:
        url = (u or "").strip().lower()
        s = (source or "")
        score = 0
        if url.startswith("https://"):
            score += 3
        if url.startswith("http"):
            score += 1
        # Curated should win ties.
        if s.startswith("curated:"):
            score += 5
        if "govinfo.gov bulkdata GOVMAN" in s:
            score += 2
        if "federalregister.gov" in s:
            score += 1
        if "uscourts.gov" in s:
            score += 1
        return score

    def upsert(r: Dict[str, Any]) -> None:
        k = record_key(r)
        if not k:
            return

        source = r.get("source")
        website = normalize_origin_url(r.get("website") or "") or (r.get("website") or "")
        seed_url = normalize_seed_url(r.get("seed_url") or r.get("website") or "")
        host = canonical_host(r.get("host") or get_host(website))
        name = (r.get("name") or "").strip() or None
        kind = (r.get("kind") or r.get("entity_type") or "entity")
        branch = r.get("branch")
        desc = (r.get("description") or "").strip()
        branch_hint = (r.get("branch_hint") or "").strip()

        # If key would create a new row but host already exists, try a conservative
        # same-host merge when names are very similar (prevents host-only overmerge).
        if k not in index and host and name:
            for existing_key in host_to_keys.get(host, []):
                existing = index.get(existing_key)
                if not existing:
                    continue
                if names_similar(existing.get("name"), name):
                    k = existing_key
                    break

        if k not in index:
            index[k] = {
                "sources": [],
                "name": name,
                "aliases": set(),
                "website": website,
                "seed_urls": set(),
                "host": host,
                "description": desc,
                "kind": kind,
                "branch": branch,
                "branch_hint": branch_hint,
                "raw": [],
                "_website_score": score_website(website, source or ""),
            }
            if host:
                host_to_keys.setdefault(host, []).append(k)

        row = index[k]
        row["sources"].append(source)
        row["raw"].append(r)

        if seed_url:
            row["seed_urls"].add(seed_url)

        if name and name != row.get("name"):
            row["aliases"].add(name)
        if row.get("name") is None and name:
            row["name"] = name
        if not row.get("host") and host:
            row["host"] = host

        ws = score_website(website, source or "")
        if website and (not row.get("website") or ws > (row.get("_website_score") or 0)):
            row["website"] = website
            row["host"] = canonical_host(get_host(website)) or row.get("host")
            row["_website_score"] = ws

        if desc and (not row.get("description")):
            row["description"] = desc

        # Prefer explicit branch/kind if any source provides it
        if (not row.get("branch")) and branch:
            row["branch"] = branch
        if (row.get("kind") in (None, "entity")) and kind:
            row["kind"] = kind
        if (not row.get("branch_hint")) and branch_hint:
            row["branch_hint"] = branch_hint

        # Preserve FR identifiers if present
        for f in (
            "fr_agency_id",
            "short_name",
            "slug",
            "parent_fr_agency_id",
            "child_fr_agency_ids",
            "fr_page_url",
            "fr_json_url",
        ):
            if f in r and r.get(f) is not None and f not in row:
                row[f] = r.get(f)

    for r in all_records:
        upsert(r)

    merged: List[Dict[str, Any]] = []
    for row in index.values():
        aliases = sorted(set(a for a in (row.get("aliases") or set()) if a))
        row["aliases"] = aliases
        row["sources"] = sorted(set(s for s in (row.get("sources") or []) if s))
        row["seed_urls"] = sorted(set(u for u in (row.get("seed_urls") or set()) if u))
        row.pop("_website_score", None)
        merged.append(row)
    return merged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="-", help="Output JSONL path or '-' for stdout")
    ap.add_argument(
        "--curated-glob",
        default="data/federal_domains/[0-9].json",
        help="Glob for curated JSON/JSONL seed records (defaults to data/federal_domains/{1,2,3}.json)",
    )
    ap.add_argument("--no-curated", action="store_true", help="Do not load curated seed records")
    ap.add_argument("--govinfo-key", default="", help="Optional api.data.gov key for govinfo API (to find latest GOVMAN edition)")
    ap.add_argument("--govman-edition", default="", help="Optional explicit edition like GOVMAN-2024-12-01")
    ap.add_argument("--skip-govman", action="store_true", help="Skip GOVMAN download/parse (faster, less comprehensive)")
    ap.add_argument("--skip-fr", action="store_true", help="Skip FederalRegister agency crosswalk")
    ap.add_argument("--skip-uscourts", action="store_true", help="Skip USCourts court website links")
    ap.add_argument(
        "--branch",
        default="all",
        choices=["all", "executive", "legislative", "judicial", "other"],
        help="Filter output to a single branch (default: all)",
    )
    ap.add_argument(
        "--kinds",
        default="all",
        help="Comma-separated kind filter (e.g. agency,court,entity). Default: all",
    )
    ap.add_argument(
        "--include-sources",
        action="store_true",
        help="Include curated meta records with kind=source in output",
    )
    ap.add_argument("--include-provenance", action="store_true", help="Include raw source records in output")
    args = ap.parse_args()

    wanted_kinds: Optional[Set[str]]
    if args.kinds.strip().lower() == "all":
        wanted_kinds = None
    else:
        wanted_kinds = set(k.strip().lower() for k in args.kinds.split(",") if k.strip())

    curated: List[Dict[str, Any]] = []
    if not args.no_curated:
        try:
            curated = load_curated_records(args.curated_glob)
        except Exception as e:
            print(f"[warn] Curated load failed: {e}", file=sys.stderr)

    govman_entities: List[Dict[str, Any]] = []
    if not args.skip_govman:
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
                    "Fix: pass --govman-edition GOVMAN-YYYY-MM-DD (and optionally update logic), or run with --skip-govman.\n"
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
    merged = merge_records(curated + govman_entities + fr_entities + court_entities)

    out_f = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        for r in merged:
            # If any source had an explicit branch, trust it.
            explicit_branch = (r.get("branch") or "").strip().lower() if isinstance(r.get("branch"), str) else None
            if explicit_branch in ("executive", "legislative", "judicial"):
                branch, conf, reasons = explicit_branch, 1.0, ["explicit branch"]
            else:
                branch, conf, reasons = guess_branch(r.get("name"), r.get("website", ""), branch_hint=r.get("branch_hint") or "")

            kind = (r.get("kind") or "entity").strip().lower()
            if kind == "court_website":
                kind = "court"

            if (not args.include_sources) and kind == "source":
                continue

            # Filters
            if args.branch != "all" and branch != args.branch:
                continue
            if wanted_kinds is not None and kind not in wanted_kinds:
                continue

            host = canonical_host(r.get("host") or get_host(r.get("website") or ""))
            website = normalize_origin_url(r.get("website") or "") or (r.get("website") or "")

            rid = stable_id(branch, kind, host or "", r.get("name"))
            rec: Dict[str, Any] = {
                "id": rid,
                "kind": kind,
                "branch": branch,
                "branch_confidence": conf,
                "branch_reasons": reasons,
                "name": r.get("name"),
                "aliases": r.get("aliases") or [],
                "website": website,
                "host": host,
                "description": r.get("description") or "",
                "sources": r.get("sources") or [],
            }

            seed_urls = r.get("seed_urls") or []
            if seed_urls:
                rec["seed_urls"] = seed_urls

            # Attach FR metadata if present
            for f in (
                "fr_agency_id",
                "short_name",
                "slug",
                "parent_fr_agency_id",
                "child_fr_agency_ids",
                "fr_page_url",
                "fr_json_url",
            ):
                if f in r:
                    rec[f] = r.get(f)

            if args.include_provenance:
                rec["provenance"] = r.get("raw")

            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    finally:
        if out_f is not sys.stdout:
            out_f.close()


if __name__ == "__main__":
    main()
