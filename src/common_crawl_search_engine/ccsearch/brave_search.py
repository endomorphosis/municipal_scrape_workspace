"""Brave Search integration (HTTP client).

This module contains only the outbound Brave API call + response parsing.
Higher-level workflows (resolving Brave URLs to CCIndex pointers) live in
`common_crawl_search_engine.ccindex.api`.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional


def brave_web_search(
    query: str,
    *,
    api_key: Optional[str] = None,
    count: int = 10,
    offset: int = 0,
    country: str = "us",
    safesearch: str = "moderate",
) -> List[Dict[str, str]]:
    """Search the web using Brave Search API.

    Returns a list of dicts with keys: title, url, description.

    Requires env var `BRAVE_SEARCH_API_KEY` or explicit api_key.
    """

    token = (api_key or os.environ.get("BRAVE_SEARCH_API_KEY") or "").strip()
    if not token:
        raise RuntimeError("Missing BRAVE_SEARCH_API_KEY (set env var or pass api_key)")

    q = (query or "").strip()
    if not q:
        return []

    try:
        import requests  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "requests is required for Brave Search integration. Install with: pip install -e '.[ccindex]'"
        ) from e

    url = "https://api.search.brave.com/res/v1/web/search"
    params = {
        "q": q,
        "count": int(count),
        "offset": int(offset),
        "country": str(country),
        "safesearch": str(safesearch),
    }
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": token,
    }

    resp = requests.get(url, params=params, headers=headers, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Brave Search HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json() if resp.content else {}
    web = data.get("web") if isinstance(data, dict) else None
    items = web.get("results") if isinstance(web, dict) else None
    if not isinstance(items, list):
        return []

    out: List[Dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        out.append(
            {
                "title": str(it.get("title") or ""),
                "url": str(it.get("url") or ""),
                "description": str(it.get("description") or ""),
            }
        )

    return out
