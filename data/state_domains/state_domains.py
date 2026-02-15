#!/usr/bin/env python3
"""
State/Territory government domain seeder + discovery + Wikipedia enrichment.

Spine seeds:
  - USA.gov state governments directory (executive portals): https://www.usa.gov/state-governments
  - Congress.gov state legislature websites: https://www.congress.gov/state-legislature-websites

Optional enrichment:
  - Wikipedia ".gov" page table of state/territory portal domains: https://en.wikipedia.org/wiki/.gov
  - Wikipedia Action API extlinks for targeted pages per jurisdiction:
      https://en.wikipedia.org/w/api.php?action=query&prop=extlinks&titles=...

Output JSONL:
  one record per (jurisdiction, branch_guess, host/domain), with provenance.

Important:
  - This is for building a URL/domain seed list. Respect robots.txt / terms when scraping later.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import deque
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


# ---------------- Constants ----------------

USAGOV_STATES_URL = "https://www.usa.gov/state-governments"
CONGRESS_LEG_URL = "https://www.congress.gov/state-legislature-websites"

WIKI_PAGE_GOV_TLD = ".gov"
WIKI_API = "https://en.wikipedia.org/w/api.php"

DEFAULT_MAX_PAGES_PER_SEED = 200
DEFAULT_MAX_DEPTH = 2
DEFAULT_SLEEP_S = 0.25
DEFAULT_WIKI_SLEEP_S = 0.2


# ---------------- Helpers ----------------

class LinkExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if href:
            self.links.append(href)


def http_get(url: str, timeout: int = 45, accept: str = "text/html,*/*") -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "state-domain-seeder/2.0", "Accept": accept},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_json(url: str, timeout: int = 60) -> Any:
    raw = http_get(url, timeout=timeout, accept="application/json,*/*")
    return json.loads(raw.decode("utf-8", errors="replace"))


def parse_links(base_url: str, html_bytes: bytes) -> List[str]:
    text = html_bytes.decode("utf-8", errors="replace")
    p = LinkExtractor()
    p.feed(text)
    out = []
    for href in p.links:
        out.append(urllib.parse.urljoin(base_url, href))
    return out


def host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


def normalize_origin(url: str) -> str:
    """Normalize URL to scheme://host/"""
    try:
        p = urllib.parse.urlparse(url)
        if not p.scheme or not p.netloc:
            return ""
        return urllib.parse.urlunparse((p.scheme, p.netloc.lower(), "/", "", "", ""))
    except Exception:
        return ""


def looks_government_host(host: str) -> bool:
    host = host.lower()
    if host.endswith(".gov") or ".gov." in host:
        return True
    if host.endswith(".mil"):
        return True
    # state patterns like *.state.xx.us
    if host.endswith(".us") and (".state." in host or host.count(".") >= 2):
        return True
    return False


def registrableish_domain(host: str) -> str:
    """
    Minimal registrable-ish extraction. Good enough for .gov and common *.us patterns.
    """
    host = host.lower().strip(".")
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    # keep last 3 for *.state.xx.us patterns
    if host.endswith(".state.") or False:
        return ".".join(parts[-4:])  # defensive
    if parts[-1] == "us" and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def branch_guess(host: str, context: str = "") -> Tuple[str, float]:
    """
    Pragmatic branch classifier using host tokens and context (seed / wiki page title).
    """
    h = host.lower()
    c = (context or "").lower()

    # Judicial
    if any(k in h for k in ["courts", "judiciary", "supremecourt", "court"]):
        return "judicial", 0.75
    if any(k in c for k in ["court", "judiciary", "supreme court"]):
        return "judicial", 0.70

    # Legislative
    if any(k in h for k in ["leg", "legislature", "senate", "house", "capitol", "assembly"]):
        return "legislative", 0.75
    if any(k in c for k in ["legislature", "legislative", "senate", "house", "general assembly"]):
        return "legislative", 0.70

    # Executive default for gov hosts
    if looks_government_host(h):
        return "executive", 0.55

    return "unknown", 0.30


# ---------------- Seeds: USA.gov and Congress.gov ----------------

def seeds_from_congress_legislatures() -> List[Dict[str, Any]]:
    """
    Pull legislature site links from Congress.gov directory page.
    (We don't rely on any fixed HTML structure beyond external hrefs.)
    """
    html = http_get(CONGRESS_LEG_URL)
    links = parse_links(CONGRESS_LEG_URL, html)

    seeds: List[Dict[str, Any]] = []
    seen_hosts: Set[str] = set()

    for u in links:
        h = host_of(u)
        if not h or h.endswith("congress.gov"):
            continue
        origin = normalize_origin(u)
        if not origin:
            continue
        if h in seen_hosts:
            continue
        seen_hosts.add(h)
        seeds.append({
            "seed_branch": "legislative",
            "seed_url": origin,
            "seed_source": CONGRESS_LEG_URL,
            "seed_source_ref": "congress.gov state legislature websites",
        })

    return seeds


def seeds_from_usagov_portals() -> List[Dict[str, Any]]:
    """
    USA.gov index page points to state/territory pages and/or directly to portals.
    We crawl the index page and then, for any USA.gov state page discovered, crawl one level
    to find an external government portal.
    """
    index_html = http_get(USAGOV_STATES_URL)
    index_links = parse_links(USAGOV_STATES_URL, index_html)

    # Candidate per-jurisdiction pages (structure can change; keep broad):
    per_pages = [
        u for u in index_links
        if host_of(u).endswith("usa.gov") and (
            "/state-" in u or "/states/" in u or "/state/" in u
        )
    ]
    per_pages = sorted(set(per_pages))

    seeds: List[Dict[str, Any]] = []
    seen_hosts: Set[str] = set()

    # Also, sometimes the index includes direct external portal links; keep them too.
    for u in index_links:
        h = host_of(u)
        if not h or h.endswith("usa.gov"):
            continue
        if looks_government_host(h):
            origin = normalize_origin(u)
            if origin and h not in seen_hosts:
                seen_hosts.add(h)
                seeds.append({
                    "seed_branch": "executive",
                    "seed_url": origin,
                    "seed_source": USAGOV_STATES_URL,
                    "seed_source_ref": "usa.gov state governments",
                    "seed_notes": "direct portal link on index page",
                })

    # Follow per-state pages to discover portals
    for page in per_pages:
        try:
            html = http_get(page)
            links = parse_links(page, html)
        except Exception:
            continue

        # pick first external government-ish link as portal
        portal = None
        for u in links:
            h = host_of(u)
            if not h or h.endswith("usa.gov"):
                continue
            if looks_government_host(h):
                portal = normalize_origin(u)
                break

        if portal:
            ph = host_of(portal)
            if ph and ph not in seen_hosts:
                seen_hosts.add(ph)
                seeds.append({
                    "seed_branch": "executive",
                    "seed_url": portal,
                    "seed_source": USAGOV_STATES_URL,
                    "seed_source_ref": "usa.gov state governments",
                    "seed_notes": f"discovered via {page}",
                })

        time.sleep(DEFAULT_SLEEP_S)

    return seeds


# ---------------- Discovery crawl from seeds ----------------

def crawl_seed_same_host(seed_url: str, max_pages: int, max_depth: int, sleep_s: float) -> Set[str]:
    """
    Crawl within the seed host only, collect outbound hosts from links.
    """
    seed_host = host_of(seed_url)
    if not seed_host:
        return set()

    q = deque([(seed_url, 0)])
    visited: Set[str] = set()
    outbound_hosts: Set[str] = set()

    while q and len(visited) < max_pages:
        url, depth = q.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            html = http_get(url)
            links = parse_links(url, html)
        except Exception:
            continue

        for u in links:
            h = host_of(u)
            if not h:
                continue
            outbound_hosts.add(h)

            # crawl only within same host
            if depth < max_depth and h == seed_host:
                if re.search(r"\.(pdf|zip|jpg|jpeg|png|gif|mp4|docx?|xlsx?|pptx?)($|\?)", u.lower()):
                    continue
                q.append((u, depth + 1))

        time.sleep(sleep_s)

    outbound_hosts.add(seed_host)
    return outbound_hosts


# ---------------- Wikipedia enrichment ----------------

def wiki_api_url(params: Dict[str, str]) -> str:
    p = params.copy()
    p["format"] = "json"
    p["formatversion"] = "2"
    return WIKI_API + "?" + urllib.parse.urlencode(p)


def wiki_get_extlinks(title: str, limit_per_page: int = 500) -> List[str]:
    """
    Use MediaWiki Action API prop=extlinks to fetch external links on a page.
    """
    extlinks: List[str] = []
    cont: Optional[str] = None

    while True:
        params = {
            "action": "query",
            "prop": "extlinks",
            "titles": title,
            "ellimit": str(limit_per_page),
        }
        if cont:
            params["eloffset"] = cont

        data = http_get_json(wiki_api_url(params))
        pages = data.get("query", {}).get("pages", [])
        if pages:
            els = pages[0].get("extlinks", []) or []
            for item in els:
                # extlinks are objects like {"*":"https://example.com"}
                u = item.get("*")
                if u:
                    extlinks.append(u)

        cont_obj = data.get("continue", {})
        cont = cont_obj.get("eloffset")
        if not cont:
            break

        time.sleep(DEFAULT_WIKI_SLEEP_S)

    return extlinks


def wiki_search_top_title(query: str) -> Optional[str]:
    """
    Find the top Wikipedia page title for a query using action=query&list=search.
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": "1",
    }
    data = http_get_json(wiki_api_url(params))
    hits = data.get("query", {}).get("search", []) or []
    if not hits:
        return None
    return hits[0].get("title")


def wiki_get_gov_portal_domains_from_dotgov_page() -> List[Tuple[str, str]]:
    """
    Parse the Wikipedia '.gov' page extlinks and return (host, url) pairs that look like state/territory portals.
    This is intentionally heuristic: we keep only .gov hosts and prefer roots like xx.gov / statename.gov.
    """
    links = wiki_get_extlinks(WIKI_PAGE_GOV_TLD)
    out: List[Tuple[str, str]] = []
    for u in links:
        h = host_of(u)
        if not h:
            continue
        if h.endswith(".gov"):
            out.append((h, u))
    # de-dupe by host, keep first
    seen: Set[str] = set()
    uniq: List[Tuple[str, str]] = []
    for h, u in out:
        if h in seen:
            continue
        seen.add(h)
        uniq.append((h, u))
    return uniq


def wikipedia_enrich_for_state(state_name: str) -> List[Dict[str, Any]]:
    """
    For a given state/territory name, search for a few canonical pages and harvest extlinks.
    """
    queries = [
        f"Government of {state_name}",
        f"{state_name} Legislature",
        f"{state_name} Supreme Court",
        f"Judiciary of {state_name}",
    ]

    records: List[Dict[str, Any]] = []

    for q in queries:
        title = wiki_search_top_title(q)
        if not title:
            continue

        try:
            ext = wiki_get_extlinks(title)
        except Exception:
            continue

        for u in ext:
            h = host_of(u)
            if not h:
                continue
            if not looks_government_host(h):
                continue
            b, conf = branch_guess(h, context=title)
            records.append({
                "source": "wikipedia",
                "wiki_title": title,
                "wiki_query": q,
                "url": u,
                "host": h,
                "branch_guess": b,
                "confidence": conf,
            })

        time.sleep(DEFAULT_WIKI_SLEEP_S)

    # De-dupe by host+branch_guess
    seen: Set[Tuple[str, str]] = set()
    uniq: List[Dict[str, Any]] = []
    for r in records:
        k = (r["host"], r["branch_guess"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)

    return uniq


# ---------------- Jurisdiction mapping ----------------
#
# For now, we keep a mapping table (can be expanded to territories as needed).
# If you want guaranteed correctness, we can add a stricter USA.gov parsing step
# to map portal -> state name/abbr.

STATE_ABBR: Dict[str, str] = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA","Colorado":"CO","Connecticut":"CT",
    "Delaware":"DE","Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
    "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI",
    "Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV","New Hampshire":"NH",
    "New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK",
    "Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD","Tennessee":"TN",
    "Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
    "District of Columbia":"DC",
    "American Samoa":"AS","Guam":"GU","Northern Mariana Islands":"MP","Puerto Rico":"PR","United States Virgin Islands":"VI",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="-", help="Output JSONL path or '-' for stdout")
    ap.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES_PER_SEED)
    ap.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S)
    ap.add_argument("--keep-non-gov", action="store_true", help="Also keep non-gov hosts (default: drop)")
    ap.add_argument("--wikipedia", action="store_true", help="Enable Wikipedia enrichment (extlinks + .gov page)")
    ap.add_argument("--wikipedia-per-state", action="store_true", help="Also query per-state Wikipedia pages (slower)")
    ap.add_argument("--jurisdiction", default="", help="Limit to a single jurisdiction by name (e.g., 'Oregon')")
    args = ap.parse_args()

    # Build spine seeds
    seeds = []
    seeds += seeds_from_usagov_portals()
    seeds += seeds_from_congress_legislatures()

    # Optional: Wikipedia portal domains from '.gov' page (not state-mapped here; still useful seeds)
    wiki_dotgov_hosts: List[Tuple[str, str]] = []
    if args.wikipedia:
        try:
            wiki_dotgov_hosts = wiki_get_gov_portal_domains_from_dotgov_page()
        except Exception:
            wiki_dotgov_hosts = []

    # Output
    out_f = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        emitted: Set[Tuple[Optional[str], str, str]] = set()  # (jurisdiction, branch, host)

        # Emit Wikipedia '.gov' portal seeds first (unscoped)
        if args.wikipedia and wiki_dotgov_hosts:
            for h, u in wiki_dotgov_hosts:
                b, conf = branch_guess(h, context="Wikipedia:.gov")
                rec = {
                    "jurisdiction": None,
                    "name": None,
                    "branch": b,
                    "domain": registrableish_domain(h),
                    "host": h,
                    "seed_url": normalize_origin(u) or ("https://" + h + "/"),
                    "seed_branch": "executive",
                    "seed_source": "https://en.wikipedia.org/wiki/.gov",
                    "discovered_from": "wikipedia_dotgov_page",
                    "confidence": max(conf, 0.60),
                    "provenance": {"wiki_title": ".gov", "wiki_method": "extlinks"},
                }
                k = (rec["jurisdiction"], rec["branch"], rec["host"])
                if k not in emitted:
                    emitted.add(k)
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # Crawl each spine seed
        for seed in seeds:
            seed_url = seed["seed_url"]
            seed_branch = seed["seed_branch"]

            # If user limited to a jurisdiction, we can only apply that filter after mapping.
            # This script currently doesn’t guarantee mapping of every seed to a state.
            # We'll still run, but you can post-filter downstream.
            hosts = crawl_seed_same_host(seed_url, args.max_pages, args.max_depth, args.sleep)

            for h in sorted(hosts):
                if not h:
                    continue
                if (not args.keep_non_gov) and (not looks_government_host(h)):
                    continue

                # classify with seed prior
                b, conf = branch_guess(h, context=seed_branch)
                if b == "unknown":
                    b = seed_branch
                    conf = max(conf, 0.60)

                rec = {
                    "jurisdiction": None,  # fill later via your own mapping pass
                    "name": None,
                    "branch": b,
                    "domain": registrableish_domain(h),
                    "host": h,
                    "seed_url": seed_url,
                    "seed_branch": seed_branch,
                    "seed_source": seed["seed_source"],
                    "discovered_from": "seed_crawl",
                    "confidence": conf,
                }
                k = (rec["jurisdiction"], rec["branch"], rec["host"])
                if k not in emitted:
                    emitted.add(k)
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # Optional: per-state Wikipedia enrichment (slow but can fill judiciary/court domains)
        if args.wikipedia and args.wikipedia_per_state:
            targets = list(STATE_ABBR.keys())
            if args.jurisdiction:
                targets = [args.jurisdiction]

            for name in targets:
                # Skip if unknown jurisdiction name
                if name not in STATE_ABBR and not args.jurisdiction:
                    continue

                try:
                    enrich = wikipedia_enrich_for_state(name)
                except Exception:
                    enrich = []

                for r in enrich:
                    h = r["host"]
                    if (not args.keep_non_gov) and (not looks_government_host(h)):
                        continue

                    rec = {
                        "jurisdiction": STATE_ABBR.get(name),
                        "name": name,
                        "branch": r["branch_guess"],
                        "domain": registrableish_domain(h),
                        "host": h,
                        "seed_url": normalize_origin(r["url"]) or ("https://" + h + "/"),
                        "seed_branch": r["branch_guess"],
                        "seed_source": "wikipedia",
                        "discovered_from": "wikipedia_extlinks",
                        "confidence": r["confidence"],
                        "provenance": {
                            "wiki_title": r["wiki_title"],
                            "wiki_query": r["wiki_query"],
                            "wiki_method": "query+extlinks",
                        },
                    }
                    k = (rec["jurisdiction"], rec["branch"], rec["host"])
                    if k not in emitted:
                        emitted.add(k)
                        out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

                time.sleep(DEFAULT_WIKI_SLEEP_S)

    finally:
        if out_f is not sys.stdout:
            out_f.close()


if __name__ == "__main__":
    main()
