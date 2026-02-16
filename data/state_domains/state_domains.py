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
DEFAULT_SEED_TIMEOUT_S = 30

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

DEFAULT_MODE = "agencies"  # agencies | hosts
DEFAULT_AGENCY_MAX_PAGES = 120
DEFAULT_AGENCY_MAX_DEPTH = 3


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


class AnchorExtractor(HTMLParser):
    """Collect (href, anchor_text) pairs."""

    def __init__(self):
        super().__init__()
        self.anchors: List[Tuple[str, str]] = []
        self._in_a = False
        self._href: Optional[str] = None
        self._text_parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if href:
            self._in_a = True
            self._href = href
            self._text_parts = []

    def handle_data(self, data):
        if self._in_a and data:
            self._text_parts.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a":
            return
        if self._in_a and self._href:
            text = " ".join(self._text_parts).strip()
            self.anchors.append((self._href, text))
        self._in_a = False
        self._href = None
        self._text_parts = []


def http_get(
    url: str,
    timeout: int = 45,
    accept: str = "text/html,*/*",
    user_agent: str = DEFAULT_USER_AGENT,
) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": user_agent, "Accept": accept},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_json(url: str, timeout: int = 60, user_agent: str = DEFAULT_USER_AGENT) -> Any:
    raw = http_get(url, timeout=timeout, accept="application/json,*/*", user_agent=user_agent)
    return json.loads(raw.decode("utf-8", errors="replace"))


def parse_links(base_url: str, html_bytes: bytes) -> List[str]:
    text = html_bytes.decode("utf-8", errors="replace")
    p = LinkExtractor()
    p.feed(text)
    out = []
    for href in p.links:
        out.append(urllib.parse.urljoin(base_url, href))
    return out


def parse_anchors(base_url: str, html_bytes: bytes) -> List[Tuple[str, str]]:
    text = html_bytes.decode("utf-8", errors="replace")
    p = AnchorExtractor()
    p.feed(text)
    out: List[Tuple[str, str]] = []
    for href, atext in p.anchors:
        out.append((urllib.parse.urljoin(base_url, href), (atext or "").strip()))
    return out


def strip_fragment_and_query(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
        return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return url


def looks_html_url(url: str) -> bool:
    u = url.lower()
    if any(u.startswith(x) for x in ["mailto:", "tel:", "javascript:"]):
        return False
    if re.search(r"\.(pdf|zip|jpg|jpeg|png|gif|mp4|docx?|xlsx?|pptx?|csv|json|xml)($|\?)", u):
        return False
    return True


def text_normalize(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def html_title(html_bytes: bytes) -> str:
    try:
        text = html_bytes.decode("utf-8", errors="replace")
    except Exception:
        return ""
    m = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    t = re.sub(r"\s+", " ", m.group(1)).strip()
    t = re.sub(r"\s+-\s+.*$", "", t).strip()  # common pattern: "X - Official Site"
    return t


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


def is_same_host(url: str, host: str) -> bool:
    try:
        return urllib.parse.urlparse(url).netloc.lower() == host.lower()
    except Exception:
        return False


def is_social_or_noise_host(host: str) -> bool:
    h = (host or "").lower()
    return any(
        h.endswith(x)
        for x in [
            "facebook.com",
            "twitter.com",
            "x.com",
            "instagram.com",
            "linkedin.com",
            "youtube.com",
            "youtu.be",
            "tiktok.com",
        ]
    )


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


AGENCY_NAME_HINTS = [
    "department",
    "dept",
    "agency",
    "commission",
    "board",
    "office",
    "bureau",
    "authority",
    "administration",
    "secretary",
    "treasurer",
    "attorney general",
    "governor",
]

AGENCY_PATH_HINTS = [
    "agency",
    "agencies",
    "departments",
    "department",
    "boards",
    "commissions",
    "offices",
    "cabinet",
    "secretary",
    "treasurer",
    "attorney",
]

CRAWL_PATH_HINTS = sorted(set(AGENCY_PATH_HINTS + [
    "directory",
    "government",
    "about",
    "state-government",
    "state_agencies",
    "state-agencies",
]))


def looks_agency_anchor(text: str, url: str) -> bool:
    t = (text or "").lower()
    u = (url or "").lower()
    if not text_normalize(text):
        return False
    if len(text_normalize(text)) < 4:
        return False
    # avoid obvious nav / utility links
    if t.strip().startswith("skip to"):
        return False
    if any(x in t for x in [
        "privacy",
        "accessibility",
        "site map",
        "sitemap",
        "contact",
        "login",
        "search",
        "live chat",
        "chat",
        "subscribe",
        "sign up",
        "newsletter",
    ]):
        return False
    # avoid directory / section headers
    if t.strip() in [
        "state government",
        "government",
        "agencies",
        "state agencies",
        "state agency directory",
        "agency directory",
    ]:
        return False
    if any(h in t for h in AGENCY_NAME_HINTS):
        return True
    # URL hints
    try:
        p = urllib.parse.urlparse(url)
        path = p.path.lower()
    except Exception:
        path = u
    if any(h in path for h in AGENCY_PATH_HINTS):
        return True
    return False


def target_path_looks_agency_like(url: str) -> bool:
    try:
        path = urllib.parse.urlparse(url).path.lower()
    except Exception:
        return False
    # Intentionally narrower than crawl hints.
    tokens = ["agency", "agencies", "department", "departments", "board", "boards", "commission", "commissions", "office", "offices", "bureau", "authority", "administration"]
    return any(t in path for t in tokens)


def is_directory_like_page(url: str) -> bool:
    try:
        path = urllib.parse.urlparse(url).path.lower()
    except Exception:
        return False
    return any(x in path for x in ["agency", "agencies", "departments", "department", "directory", "cabinet", "boards", "commissions"])


def looks_reasonable_agency_name(text: str) -> bool:
    t = text_normalize(text)
    if not t:
        return False
    if len(t) < 5 or len(t) > 120:
        return False
    low = t.lower()
    if low.strip().startswith("skip to"):
        return False
    if any(x in low for x in ["privacy", "accessibility", "site map", "sitemap", "contact", "login", "search"]):
        return False
    if any(x in low for x in ["live chat", "chat", "send us a message", "file a complaint", "complaint"]):
        return False
    if low.strip() in [
        "home",
        "homepage",
        "government",
        "state government",
        "agencies",
        "state agencies",
        "state agency directory",
        "agency directory",
    ]:
        return False
    return True


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

def seeds_from_congress_legislatures(user_agent: str = DEFAULT_USER_AGENT) -> List[Dict[str, Any]]:
    """
    Pull legislature site links from Congress.gov directory page.
    (We don't rely on any fixed HTML structure beyond external hrefs.)
    """
    try:
        html = http_get(CONGRESS_LEG_URL, user_agent=user_agent, timeout=DEFAULT_SEED_TIMEOUT_S)
    except Exception as e:
        sys.stderr.write(f"[state_domains] WARN: failed to fetch {CONGRESS_LEG_URL}: {e}\n")
        return []
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


def seeds_from_usagov_portals(
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_s: int = DEFAULT_SEED_TIMEOUT_S,
) -> List[Dict[str, Any]]:
    """
    USA.gov index page points to state/territory pages and/or directly to portals.
    We crawl the index page and then, for any USA.gov state page discovered, crawl one level
    to find an external government portal.
    """
    try:
        index_html = http_get(USAGOV_STATES_URL, user_agent=user_agent, timeout=timeout_s)
    except Exception as e:
        sys.stderr.write(f"[state_domains] WARN: failed to fetch {USAGOV_STATES_URL}: {e}\n")
        return []
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

    # Follow per-state pages to discover portals
    for page in per_pages:
        html = None
        try:
            html = http_get(page, user_agent=user_agent, timeout=timeout_s)
        except Exception as e1:
            # Retry once with a more forgiving timeout. Some USA.gov pages are occasionally slow.
            try:
                html = http_get(page, user_agent=user_agent, timeout=max(timeout_s * 3, 45))
            except Exception as e2:
                sys.stderr.write(f"[state_domains] WARN: failed to fetch per-state seed page {page}: {e2} (first error: {e1})\n")
                continue

        try:
            anchors = parse_anchors(page, html)
        except Exception as e:
            sys.stderr.write(f"[state_domains] WARN: failed to parse anchors for {page}: {e}\n")
            continue

        # Try to infer jurisdiction name from the page itself.
        page_title = html_title(html)
        juris_name = infer_jurisdiction_name(page, page_title)
        juris_abbr = STATE_ABBR.get(juris_name) if juris_name else None

        # pick best-scoring external government-ish link as portal
        portal = None
        best_score = -1e9
        if juris_name:
            for u, atext in anchors:
                h = host_of(u)
                if not h or h.endswith("usa.gov"):
                    continue
                if not looks_government_host(h):
                    continue
                s = portal_candidate_score(juris_name, juris_abbr, u, atext)
                if s > best_score:
                    best_score = s
                    portal = normalize_origin(u)

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
                    "jurisdiction": juris_abbr,
                    "name": juris_name,
                })

        time.sleep(DEFAULT_SLEEP_S)

    return seeds


def infer_jurisdiction_name(page_url: str, page_title: str) -> Optional[str]:
    """Best-effort mapping of USA.gov per-state page -> state/territory name."""
    hay = " ".join([page_url or "", page_title or ""]).lower()
    # Prefer longest names first (e.g., "Northern Mariana Islands")
    for name in sorted(STATE_ABBR.keys(), key=len, reverse=True):
        if name.lower() in hay:
            return name
        # Common phrasing
        if ("state of " + name.lower()) in hay:
            return name
    return None


def state_slug(name: str) -> str:
    return re.sub(r"[^a-z]", "", (name or "").lower())


def portal_candidate_score(juris_name: str, abbr: Optional[str], url: str, anchor_text: str) -> float:
    """Heuristic score for selecting an official state portal from a USA.gov per-state page."""
    host = host_of(url)
    if not host or host.endswith("usa.gov"):
        return -1e9
    if not looks_government_host(host):
        return -1e9
    if is_social_or_noise_host(host):
        return -1e9

    t = (anchor_text or "").strip().lower()
    slug = state_slug(juris_name)
    ab = (abbr or "").lower()

    score = 0.0
    if host.endswith(".gov"):
        score += 1.0

    # Prefer the obvious state portal domains.
    if slug and host in {f"{slug}.gov", f"www.{slug}.gov"}:
        score += 8.0
    if slug and slug in host:
        score += 3.0
    if ab and ab in host.split("."):
        score += 2.0

    # Anchor text hints.
    if "official" in t:
        score += 2.5
    if "website" in t:
        score += 1.5
    if "government" in t or "gov" in t:
        score += 1.0
    if "state" in t:
        score += 0.5

    # Penalize likely branch sites when we're trying to find the executive portal.
    bad_tokens = [
        "attorney general",
        "courts",
        "judiciary",
        "supreme court",
        "legislature",
        "senate",
        "house",
    ]
    if any(bt in t for bt in bad_tokens):
        score -= 4.0
    if any(x in host for x in ["court", "courts", "judiciary", "leg", "legislature", "senate", "house", "doj"]):
        score -= 2.5
    if "ag" in host.split("."):
        score -= 2.0

    # Prefer shorter, higher-level hosts.
    score -= 0.15 * len(host)
    score -= 0.5 * max(0, len(host.split(".")) - 3)
    return score


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


def crawl_agencies_from_portal(
    seed: Dict[str, Any],
    max_pages: int,
    max_depth: int,
    sleep_s: float,
) -> List[Dict[str, Any]]:
    """Crawl a state portal to find links that look like state agencies and return agency records."""
    seed_url = seed.get("seed_url") or ""
    seed_host = host_of(seed_url)
    if not seed_host:
        return []

    jurisdiction = seed.get("jurisdiction")
    name = seed.get("name")

    # Seed queue with portal root plus a few common directory paths.
    base_origin = normalize_origin(seed_url) or ("https://" + seed_host + "/")
    common_paths = [
        "agencies",
        "agency",
        "government/agencies",
        "government",
        "about",
        "about/government",
        "directory",
        "departments",
        "departments-and-agencies",
        "state-agencies",
        "state-government",
        "state-government/agencies",
    ]
    start_urls = [base_origin]
    for p in common_paths:
        start_urls.append(urllib.parse.urljoin(base_origin, p))

    q = deque([(u, 0) for u in sorted(set(start_urls))])
    visited: Set[str] = set()

    emitted_keys: Set[Tuple[Optional[str], str, str]] = set()  # (jurisdiction, agency_name_norm, host)
    agencies: List[Dict[str, Any]] = []

    def maybe_emit(agency_name: str, agency_url: str, found_on: str, anchor_text: str) -> None:
        a_name = text_normalize(agency_name)
        if not a_name:
            return
        # Avoid emitting the portal itself as an "agency".
        if a_name.lower() in ["home", "homepage"]:
            return

        a_url = strip_fragment_and_query(agency_url)
        a_host = host_of(a_url)
        if not a_host:
            return
        if is_social_or_noise_host(a_host):
            return
        if (not looks_government_host(a_host)) and (a_host != seed_host):
            # Many portals link to non-gov partners; default to dropping those.
            return
        # Avoid emitting generic directory links as "agencies" (including other-language variants).
        try:
            pth = urllib.parse.urlparse(a_url).path.lower().rstrip("/")
        except Exception:
            pth = ""
        if a_host == seed_host and pth in {"/agency", "/agencies", "/departments", "/departments-and-agencies", "/government/agencies"}:
            low = a_name.lower()
            if any(x in low for x in ["agency", "agenc", "depart", "government", "gubernamental", "gouvernement", "minister", "ministerio", "instituciones", "institutions"]):
                return

        key = (jurisdiction, a_name.lower(), a_host)
        if key in emitted_keys:
            return
        emitted_keys.add(key)
        agencies.append(
            {
                "jurisdiction": jurisdiction,
                "name": name,
                "branch": "executive",
                "agency_name": a_name,
                "agency_url": a_url,
                "host": a_host,
                "domain": registrableish_domain(a_host),
                "seed_url": base_origin,
                "seed_source": seed.get("seed_source"),
                "discovered_from": "portal_agency_crawl",
                "provenance": {
                    "found_on": found_on,
                    "anchor_text": text_normalize(anchor_text)[:500],
                },
            }
        )

    pages_fetched = 0
    while q and pages_fetched < max_pages:
        url, depth = q.popleft()
        url = strip_fragment_and_query(url)
        if url in visited:
            continue
        visited.add(url)

        # Keep crawl constrained to the portal host for page fetching.
        if not is_same_host(url, seed_host):
            continue
        if not looks_html_url(url):
            continue

        try:
            html = http_get(url)
        except Exception:
            continue
        pages_fetched += 1

        # Extract anchors for agency candidates and next pages.
        try:
            anchors = parse_anchors(url, html)
        except Exception:
            anchors = []

        directory_context = is_directory_like_page(url)

        for a_url, a_text in anchors:
            if not a_url or not looks_html_url(a_url):
                continue
            a_text_n = text_normalize(a_text)

            # Emit agency records.
            if looks_agency_anchor(a_text_n, a_url):
                maybe_emit(a_text_n, a_url, found_on=url, anchor_text=a_text)
            elif directory_context and looks_reasonable_agency_name(a_text_n):
                # On directory-like pages, many agencies are listed without keywords like "Department".
                a_host = host_of(a_url)
                if a_host and (a_host != seed_host or target_path_looks_agency_like(a_url)):
                    maybe_emit(a_text_n, a_url, found_on=url, anchor_text=a_text)

            # Queue more pages, but keep it conservative.
            if depth < max_depth and is_same_host(a_url, seed_host):
                # Only chase likely directory / government pages beyond depth 0.
                try:
                    path = urllib.parse.urlparse(a_url).path.lower()
                except Exception:
                    path = ""
                # Restrict crawling to directory-like paths even from the homepage to avoid drifting
                # into generic content (which often links to federal/county resources).
                if any(h in path for h in CRAWL_PATH_HINTS):
                    q.append((a_url, depth + 1))

        time.sleep(sleep_s)

    return agencies


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
    ap.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent header for HTTP requests")
    ap.add_argument("--seed-timeout", type=int, default=DEFAULT_SEED_TIMEOUT_S, help="Timeout (seconds) for seed page fetches")
    ap.add_argument(
        "--mode",
        default=DEFAULT_MODE,
        choices=["agencies", "hosts"],
        help="Output mode: 'agencies' emits agency records; 'hosts' emits discovered hosts (legacy)",
    )
    ap.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES_PER_SEED)
    ap.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_S)
    ap.add_argument("--agency-max-pages", type=int, default=DEFAULT_AGENCY_MAX_PAGES)
    ap.add_argument("--agency-max-depth", type=int, default=DEFAULT_AGENCY_MAX_DEPTH)
    ap.add_argument("--keep-non-gov", action="store_true", help="Also keep non-gov hosts (default: drop)")
    ap.add_argument("--wikipedia", action="store_true", help="Enable Wikipedia enrichment (extlinks + .gov page)")
    ap.add_argument("--wikipedia-per-state", action="store_true", help="Also query per-state Wikipedia pages (slower)")
    ap.add_argument("--jurisdiction", default="", help="Limit to a single jurisdiction by name (e.g., 'Oregon')")
    args = ap.parse_args()

    # Build spine seeds
    seeds = []
    seeds += seeds_from_usagov_portals(user_agent=args.user_agent, timeout_s=args.seed_timeout)
    if args.mode == "hosts":
        seeds += seeds_from_congress_legislatures(user_agent=args.user_agent)

    # Optional: Wikipedia portal domains from '.gov' page (not state-mapped here; still useful seeds)
    wiki_dotgov_hosts: List[Tuple[str, str]] = []
    if args.wikipedia:
        try:
            wiki_dotgov_hosts = wiki_get_gov_portal_domains_from_dotgov_page()
        except Exception:
            wiki_dotgov_hosts = []

    # Output
    out_f = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8", buffering=1)
    try:
        def emit_record(rec: Dict[str, Any]) -> None:
            try:
                out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                try:
                    out_f.flush()
                except Exception:
                    pass
            except BrokenPipeError:
                raise SystemExit(0)

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
                    emit_record(rec)

        # Optional filtering by jurisdiction name (not abbreviation)
        if args.jurisdiction:
            # Normalize input to canonical key casing if possible
            j = None
            for k in STATE_ABBR.keys():
                if k.lower() == args.jurisdiction.lower():
                    j = k
                    break
            if j:
                args.jurisdiction = j

        # Crawl each spine seed
        for seed in seeds:
            seed_url = seed["seed_url"]
            seed_branch = seed["seed_branch"]

            # Filter to a single jurisdiction if the seed is mapped.
            if args.jurisdiction:
                if seed.get("name") and seed.get("name") != args.jurisdiction:
                    continue
                # If unmapped, skip when jurisdiction is requested.
                if not seed.get("name"):
                    continue

            # Agency mode: only makes sense for executive portal seeds.
            if args.mode == "agencies":
                if seed_branch != "executive":
                    continue
                agencies = crawl_agencies_from_portal(
                    seed,
                    max_pages=args.agency_max_pages,
                    max_depth=args.agency_max_depth,
                    sleep_s=args.sleep,
                )
                for rec in agencies:
                    # De-dupe by (jurisdiction, agency_name, host)
                    k = (rec.get("jurisdiction"), rec.get("agency_name", "").lower(), rec.get("host", ""))
                    if k in emitted:
                        continue
                    emitted.add(k)
                    emit_record(rec)
                continue

            # Host mode (legacy): crawl and emit discovered hosts.
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
                    emit_record(rec)

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
                        emit_record(rec)

                time.sleep(DEFAULT_WIKI_SLEEP_S)

    finally:
        if out_f is not sys.stdout:
            out_f.close()


if __name__ == "__main__":
    main()
