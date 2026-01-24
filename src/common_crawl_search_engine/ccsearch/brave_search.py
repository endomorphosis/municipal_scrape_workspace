"""Brave Search integration (HTTP client).

This module contains only the outbound Brave API call + response parsing.
Higher-level workflows (resolving Brave URLs to CCIndex pointers) live in
`common_crawl_search_engine.ccindex.api`.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional


def brave_web_search_max_count() -> int:
    """Return the maximum per-request result count supported by Brave web search.

    Brave enforces a server-side limit (commonly 20 for web search). We keep this
    as an overridable setting so deployments can adjust if Brave changes limits.
    """

    try:
        v = int((os.environ.get("BRAVE_SEARCH_MAX_COUNT") or "20").strip() or "20")
    except Exception:
        v = 20
    return max(1, int(v))


def _clamp_brave_count(count: int) -> int:
    mx = brave_web_search_max_count()
    try:
        n = int(count)
    except Exception:
        n = mx
    if n < 1:
        return 1
    if n > mx:
        return mx
    return n


def _clamp_brave_offset(offset: int) -> int:
    try:
        n = int(offset)
    except Exception:
        n = 0
    return 0 if n < 0 else n


def _brave_cache_path() -> Path:
    # Prefer explicit override for testability and advanced use.
    p = (os.environ.get("BRAVE_SEARCH_CACHE_PATH") or "").strip()
    if p:
        return Path(p).expanduser()

    state_dir = Path((os.environ.get("CCINDEX_STATE_DIR") or "state").strip() or "state")
    return state_dir / "brave_search_cache.json"


def brave_search_cache_path() -> Path:
    """Return the on-disk Brave Search cache file path."""

    return _brave_cache_path()


def brave_search_cache_stats() -> Dict[str, object]:
    """Return best-effort stats about the Brave Search on-disk cache."""

    path = _brave_cache_path()
    exists = path.exists() and path.is_file()
    size_bytes = int(path.stat().st_size) if exists else 0

    entries = 0
    newest_ts = None
    oldest_ts = None
    if exists:
        try:
            raw = path.read_text(encoding="utf-8").strip()
            data = json.loads(raw) if raw else {}
            if isinstance(data, dict):
                entries = len(data)
                for v in data.values():
                    if not isinstance(v, dict):
                        continue
                    ts = v.get("ts")
                    if not isinstance(ts, (int, float)):
                        continue
                    newest_ts = float(ts) if newest_ts is None else max(float(ts), float(newest_ts))
                    oldest_ts = float(ts) if oldest_ts is None else min(float(ts), float(oldest_ts))
        except Exception:
            pass

    return {
        "path": str(path),
        "exists": bool(exists),
        "entries": int(entries),
        "bytes": int(size_bytes),
        "oldest_ts": oldest_ts,
        "newest_ts": newest_ts,
        "ttl_s": int((os.environ.get("BRAVE_SEARCH_CACHE_TTL_S") or "86400").strip() or "86400"),
        "disabled": (os.environ.get("BRAVE_SEARCH_CACHE_DISABLE") or "").strip().lower()
        in {"1", "true", "yes", "on"},
    }


def clear_brave_search_cache() -> Dict[str, object]:
    """Delete the Brave Search cache file if present."""

    path = _brave_cache_path()
    try:
        if path.exists() and path.is_file():
            try:
                freed = int(path.stat().st_size)
            except Exception:
                freed = 0
            try:
                path.unlink()
                return {"deleted": True, "freed_bytes": freed, "path": str(path)}
            except Exception:
                # Fallback: truncate.
                try:
                    path.write_text("{}\n", encoding="utf-8")
                    return {"deleted": False, "freed_bytes": freed, "path": str(path), "truncated": True}
                except Exception:
                    return {"deleted": False, "freed_bytes": 0, "path": str(path)}
        return {"deleted": False, "freed_bytes": 0, "path": str(path)}
    except Exception:
        return {"deleted": False, "freed_bytes": 0, "path": str(path)}


def _brave_cache_key(*, q: str, count: int, offset: int, country: str, safesearch: str) -> str:
    payload = {
        "q": q,
        "count": int(count),
        "offset": int(offset),
        "country": str(country),
        "safesearch": str(safesearch),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@contextmanager
def _locked_cache_file(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open("a+", encoding="utf-8")
    try:
        try:
            import fcntl  # type: ignore

            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except Exception:
            # Best-effort lock; if unavailable, proceed without locking.
            pass
        yield f
    finally:
        try:
            f.close()
        except Exception:
            pass


def _load_cache_dict(f) -> Dict[str, dict]:
    try:
        f.seek(0)
        raw = f.read().strip()
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_cache_dict(f, data: Dict[str, dict]) -> None:
    f.seek(0)
    f.truncate()
    json.dump(data, f, sort_keys=True, indent=2)
    f.write("\n")
    try:
        f.flush()
        os.fsync(f.fileno())
    except Exception:
        pass


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

    count = _clamp_brave_count(int(count))
    offset = _clamp_brave_offset(int(offset))

    cache_disable = (os.environ.get("BRAVE_SEARCH_CACHE_DISABLE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    ttl_s = int((os.environ.get("BRAVE_SEARCH_CACHE_TTL_S") or "86400").strip() or "86400")
    max_entries = int((os.environ.get("BRAVE_SEARCH_CACHE_MAX_ENTRIES") or "1000").strip() or "1000")
    cache_key = _brave_cache_key(q=q, count=int(count), offset=int(offset), country=str(country), safesearch=str(safesearch))

    if not cache_disable and ttl_s > 0:
        try:
            cache_path = _brave_cache_path()
            with _locked_cache_file(cache_path) as f:
                cache = _load_cache_dict(f)
                ent = cache.get(cache_key)
                if isinstance(ent, dict):
                    ts = ent.get("ts")
                    items = ent.get("items")
                    if isinstance(ts, (int, float)) and isinstance(items, list):
                        if (time.time() - float(ts)) <= float(ttl_s):
                            out_cached: List[Dict[str, str]] = []
                            for it in items:
                                if not isinstance(it, dict):
                                    continue
                                out_cached.append(
                                    {
                                        "title": str(it.get("title") or ""),
                                        "url": str(it.get("url") or ""),
                                        "description": str(it.get("description") or ""),
                                    }
                                )
                            return out_cached
        except Exception:
            # Cache is best-effort; fall back to live request.
            pass

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

    if not cache_disable and ttl_s > 0:
        try:
            cache_path = _brave_cache_path()
            with _locked_cache_file(cache_path) as f:
                cache = _load_cache_dict(f)
                cache[cache_key] = {"ts": time.time(), "items": out}

                if max_entries > 0 and len(cache) > max_entries:
                    # Evict oldest entries by timestamp.
                    def _ts(kv) -> float:
                        v = kv[1]
                        if isinstance(v, dict) and isinstance(v.get("ts"), (int, float)):
                            return float(v["ts"])
                        return 0.0

                    keep = dict(sorted(cache.items(), key=_ts, reverse=True)[: int(max_entries)])
                    cache = keep

                _save_cache_dict(f, cache)
        except Exception:
            pass

    return out


def brave_web_search_page(
    query: str,
    *,
    api_key: Optional[str] = None,
    count: int = 10,
    offset: int = 0,
    country: str = "us",
    safesearch: str = "moderate",
) -> Dict[str, object]:
    """Brave web search that also returns pagination metadata.

    Returns:
      {"items": [...], "meta": {"count": int, "offset": int, "total": int|None, "max_count": int}}

    Notes:
    - We clamp count/offset before sending to Brave to avoid HTTP 422.
    - Brave's `web.total` (or similar field) is not guaranteed; if missing we return None.
    """

    token = (api_key or os.environ.get("BRAVE_SEARCH_API_KEY") or "").strip()
    if not token:
        raise RuntimeError("Missing BRAVE_SEARCH_API_KEY (set env var or pass api_key)")

    q = (query or "").strip()
    if not q:
        return {"items": [], "meta": {"count": 0, "offset": 0, "total": 0, "max_count": brave_web_search_max_count()}}

    count = _clamp_brave_count(int(count))
    offset = _clamp_brave_offset(int(offset))

    cache_disable = (os.environ.get("BRAVE_SEARCH_CACHE_DISABLE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    ttl_s = int((os.environ.get("BRAVE_SEARCH_CACHE_TTL_S") or "86400").strip() or "86400")
    max_entries = int((os.environ.get("BRAVE_SEARCH_CACHE_MAX_ENTRIES") or "1000").strip() or "1000")
    cache_key = _brave_cache_key(q=q, count=int(count), offset=int(offset), country=str(country), safesearch=str(safesearch))

    if not cache_disable and ttl_s > 0:
        try:
            cache_path = _brave_cache_path()
            with _locked_cache_file(cache_path) as f:
                cache = _load_cache_dict(f)
                ent = cache.get(cache_key)
                if isinstance(ent, dict):
                    ts = ent.get("ts")
                    items = ent.get("items")
                    meta = ent.get("meta")
                    if isinstance(ts, (int, float)) and isinstance(items, list):
                        if (time.time() - float(ts)) <= float(ttl_s):
                            out_cached: List[Dict[str, str]] = []
                            for it in items:
                                if not isinstance(it, dict):
                                    continue
                                out_cached.append(
                                    {
                                        "title": str(it.get("title") or ""),
                                        "url": str(it.get("url") or ""),
                                        "description": str(it.get("description") or ""),
                                    }
                                )
                            out_meta = meta if isinstance(meta, dict) else {}
                            total = out_meta.get("total")
                            total_int = int(total) if isinstance(total, (int, float)) else None
                            return {
                                "items": out_cached,
                                "meta": {
                                    "count": int(count),
                                    "offset": int(offset),
                                    "total": total_int,
                                    "max_count": brave_web_search_max_count(),
                                },
                            }
        except Exception:
            pass

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

    total_int = None
    if isinstance(web, dict):
        # Brave's schema may expose totals under different keys.
        for k in ("total", "total_results", "totalResults"):
            v = web.get(k)
            if isinstance(v, (int, float)):
                total_int = int(v)
                break

    items = web.get("results") if isinstance(web, dict) else None
    if not isinstance(items, list):
        out_items: List[Dict[str, str]] = []
    else:
        out_items = []
        for it in items:
            if not isinstance(it, dict):
                continue
            out_items.append(
                {
                    "title": str(it.get("title") or ""),
                    "url": str(it.get("url") or ""),
                    "description": str(it.get("description") or ""),
                }
            )

    if not cache_disable and ttl_s > 0:
        try:
            cache_path = _brave_cache_path()
            with _locked_cache_file(cache_path) as f:
                cache = _load_cache_dict(f)
                cache[cache_key] = {"ts": time.time(), "items": out_items, "meta": {"total": total_int}}

                if max_entries > 0 and len(cache) > max_entries:
                    def _ts(kv) -> float:
                        v = kv[1]
                        if isinstance(v, dict) and isinstance(v.get("ts"), (int, float)):
                            return float(v["ts"])
                        return 0.0

                    keep = dict(sorted(cache.items(), key=_ts, reverse=True)[: int(max_entries)])
                    cache = keep

                _save_cache_dict(f, cache)
        except Exception:
            pass

    return {
        "items": out_items,
        "meta": {"count": int(count), "offset": int(offset), "total": total_int, "max_count": brave_web_search_max_count()},
    }
