"""Importable API for the Common Crawl index tooling.

The ccindex subpackage historically grew as a set of runnable scripts.
This module provides a stable, import-friendly surface so other code (CLI, MCP,
notebooks, pipelines) can call the functionality without relying on `sys.argv`.

Most users will want `search_domain_via_meta_indexes()`.

Install requirements with:
  pip install -e '.[ccindex]'

Optionally, for the MCP server:
  pip install -e '.[ccindex-mcp]'
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


def normalize_domain(domain_or_url: str) -> str:
    """Normalize a domain or URL into a bare hostname.

    Examples:
      - "https://www.18f.gov/foo" -> "18f.gov"
      - "WWW.Example.COM" -> "example.com"
    """

    dom = (domain_or_url or "").strip().lower()
    dom = re.sub(r"^https?://", "", dom)
    dom = dom.split("/", 1)[0]
    if dom.startswith("www."):
        dom = dom[4:]
    return dom


def host_to_rev(host: str) -> str:
    """Convert host like "a.b.c" to CC host_rev prefix "c,b,a"."""

    parts = [p for p in (host or "").lower().split(".") if p]
    return ",".join(reversed(parts))


def _canonicalize_url_for_match(url: str) -> str:
    """Normalize URL for equality-ish matching across http/https and minor variations."""

    u = (url or "").strip()
    if not u:
        return ""

    if not re.match(r"^https?://", u, flags=re.IGNORECASE):
        u = "https://" + u

    try:
        p = urllib.parse.urlsplit(u)
    except Exception:
        return u.lower().rstrip("/")

    netloc = (p.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = p.path or "/"
    query = ("?" + p.query) if p.query else ""
    return f"{netloc}{path}{query}".rstrip("/")


def collection_year(collection: str) -> Optional[str]:
    parts = (collection or "").split("-")
    if len(parts) >= 3 and parts[2].isdigit():
        return parts[2]
    return None


def get_collection_parquet_dir(parquet_root: Path, collection: str) -> Path:
    """Locate a collection's Parquet directory under a root.

    Mirrors the logic used by the pipeline/orchestrator.
    """

    year = collection_year(collection)
    if year:
        primary = parquet_root / "cc_pointers_by_collection" / year / collection
        if primary.exists():
            return primary
        secondary = parquet_root / year / collection
        if secondary.exists():
            return secondary
    return parquet_root / collection


@dataclass(frozen=True)
class CollectionRef:
    year: Optional[str]
    collection: str
    collection_db_path: Path


@dataclass(frozen=True)
class MetaIndexSearchResult:
    records: List[Dict[str, object]]
    emitted: int
    elapsed_s: float
    meta_source: str
    collections_considered: int


@dataclass(frozen=True)
class WarcFetchResult:
    ok: bool
    status: Optional[int]
    url: str
    bytes_requested: int
    bytes_returned: int
    sha256: Optional[str]
    raw_base64: Optional[str]
    decoded_text_preview: Optional[str]
    error: Optional[str]


@dataclass(frozen=True)
class WarcHttpExtractResult:
    ok: bool
    warc_headers: Dict[str, str]
    http_status: Optional[int]
    http_status_line: Optional[str]
    http_headers: Dict[str, str]
    body_base64: Optional[str]
    body_text_preview: Optional[str]
    body_is_html: bool
    body_mime: Optional[str]
    body_charset: Optional[str]
    error: Optional[str]


@dataclass(frozen=True)
class BraveWebResult:
    title: str
    url: str
    description: str


@dataclass(frozen=True)
class BraveSearchResolvedResult:
    query: str
    count: int
    offset: int
    total_results: Optional[int]
    brave_cached: bool
    resolved_cached: bool
    results: List[Dict[str, object]]
    elapsed_s: float
    brave_elapsed_s: float
    resolve_elapsed_s: float
    resolve_mode: str
    resolve_domains: int
    resolve_parquet_files: int


def _url_variants_for_lookup(url: str) -> List[str]:
    """Generate a small set of URL variants likely to exist in CCIndex.

    This helps bridge minor differences (http/https, www, trailing slash) while
    still allowing an exact join on the Parquet `url` column.
    """

    u = (url or "").strip()
    if not u:
        return []

    # Ensure scheme for parsing.
    parse_u = u
    if not re.match(r"^https?://", parse_u, flags=re.IGNORECASE):
        parse_u = "https://" + parse_u

    try:
        p = urllib.parse.urlsplit(parse_u)
    except Exception:
        return [u]

    scheme = (p.scheme or "").lower()
    netloc = (p.netloc or "").strip()
    path = p.path or ""
    query = ("?" + p.query) if p.query else ""

    if not netloc:
        return [u]

    schemes = [scheme] if scheme in {"http", "https"} else ["https"]
    if "http" in schemes and "https" not in schemes:
        schemes.append("https")
    if "https" in schemes and "http" not in schemes:
        schemes.append("http")

    netlocs = [netloc]
    low = netloc.lower()
    if low.startswith("www."):
        netlocs.append(netloc[4:])
    else:
        netlocs.append("www." + netloc)

    paths = [path]
    if path.endswith("/"):
        paths.append(path.rstrip("/"))
    else:
        paths.append(path + "/")

    out: List[str] = []
    seen = set()
    for sch in schemes:
        for nl in netlocs:
            for pa in paths:
                cand = f"{sch}://{nl}{pa}{query}"
                cand = cand.replace("///", "//")
                if cand not in seen:
                    seen.add(cand)
                    out.append(cand)

    # Preserve the original (as-is) first if it is already absolute.
    if re.match(r"^https?://", u, flags=re.IGNORECASE) and u not in seen:
        out.insert(0, u)
    return out


def _brave_resolve_cache_path() -> Path:
    p = (os.environ.get("BRAVE_RESOLVE_CACHE_PATH") or "").strip()
    if p:
        return Path(p).expanduser()
    state_dir = Path((os.environ.get("CCINDEX_STATE_DIR") or "state").strip() or "state")
    return state_dir / "brave_resolve_ccindex_cache.json"


def brave_resolve_cache_stats() -> Dict[str, object]:
    """Return best-effort stats about the Brave->CCIndex resolve cache."""

    path = _brave_resolve_cache_path()
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
        "ttl_s": int((os.environ.get("BRAVE_RESOLVE_CACHE_TTL_S") or "86400").strip() or "86400"),
        "disabled": (os.environ.get("BRAVE_RESOLVE_CACHE_DISABLE") or "").strip().lower() in {"1", "true", "yes", "on"},
    }


def clear_brave_resolve_cache() -> Dict[str, object]:
    """Delete the Brave->CCIndex resolve cache file if present."""

    path = _brave_resolve_cache_path()
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
                try:
                    path.write_text("{}\n", encoding="utf-8")
                    return {"deleted": False, "freed_bytes": freed, "path": str(path), "truncated": True}
                except Exception:
                    return {"deleted": False, "freed_bytes": 0, "path": str(path)}
        return {"deleted": False, "freed_bytes": 0, "path": str(path)}
    except Exception:
        return {"deleted": False, "freed_bytes": 0, "path": str(path)}


def _brave_resolve_cache_key(
    *,
    query: str,
    count: int,
    offset: int,
    year: Optional[str],
    parquet_root: Path,
    master_db: Optional[Path],
    per_url_limit: int,
) -> str:
    payload = {
        "v": 1,
        "query": str(query),
        "count": int(count),
        "offset": int(offset),
        "year": (str(year) if year else None),
        "parquet_root": str(parquet_root),
        "master_db": (str(master_db) if master_db else None),
        "per_url_limit": int(per_url_limit),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_cache_dict(path: Path) -> Dict[str, dict]:
    try:
        raw = path.read_text(encoding="utf-8").strip() if (path.exists() and path.is_file()) else ""
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_cache_dict(path: Path, data: Dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    try:
        tmp.replace(path)
    except Exception:
        # Best-effort fallback.
        path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _maybe_evict_oldest(cache: Dict[str, dict], *, max_entries: int) -> Dict[str, dict]:
    if max_entries <= 0 or len(cache) <= max_entries:
        return cache
    try:
        def _ts(kv) -> float:
            v = kv[1]
            if isinstance(v, dict) and isinstance(v.get("ts"), (int, float)):
                return float(v["ts"])
            return 0.0

        keep = dict(sorted(cache.items(), key=_ts, reverse=True)[: int(max_entries)])
        return keep
    except Exception:
        return cache


def _require_duckdb() -> "object":
    try:
        import duckdb  # type: ignore

        return duckdb
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "duckdb is required for ccindex operations. "
            "Install with: pip install -e '.[ccindex]'"
        ) from e


def _duckdb_has_table(con: "object", table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [str(table_name)],
    ).fetchone()
    return row is not None


def load_collections_from_master(master_db: Path, year: Optional[str] = None) -> List[CollectionRef]:
    duckdb = _require_duckdb()
    con = duckdb.connect(str(master_db), read_only=True)
    try:
        if not _duckdb_has_table(con, "collection_summary"):
            raise RuntimeError(f"master DB missing collection_summary: {master_db}")

        if year:
            rows = con.execute(
                """
                SELECT year, collection, collection_db_path
                FROM collection_summary
                WHERE year = ?
                ORDER BY collection
                """,
                [str(year)],
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT year, collection, collection_db_path
                FROM collection_summary
                ORDER BY year, collection
                """
            ).fetchall()

        out: List[CollectionRef] = []
        for y, coll, dbp in rows:
            out.append(
                CollectionRef(
                    year=str(y) if y is not None else None,
                    collection=str(coll),
                    collection_db_path=Path(str(dbp)),
                )
            )
        return out
    finally:
        con.close()


def list_collections(
    *,
    master_db: Optional[Path] = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
    year_db: Optional[Path] = None,
    year: Optional[str] = None,
) -> List[CollectionRef]:
    """List collections from either the master meta-index or a year DB."""

    if year_db is not None:
        return load_collections_from_year_db(Path(year_db).expanduser().resolve())
    if master_db is None:
        raise ValueError("master_db must be set when year_db is not provided")
    return load_collections_from_master(Path(master_db).expanduser().resolve(), year)


def load_collections_from_year_db(year_db: Path) -> List[CollectionRef]:
    duckdb = _require_duckdb()
    con = duckdb.connect(str(year_db), read_only=True)
    try:
        if not _duckdb_has_table(con, "collection_registry"):
            raise RuntimeError(f"year DB missing collection_registry: {year_db}")

        rows = con.execute(
            """
            SELECT collection, db_path
            FROM collection_registry
            ORDER BY collection
            """
        ).fetchall()

        out: List[CollectionRef] = []
        for coll, dbp in rows:
            out.append(
                CollectionRef(
                    year=collection_year(str(coll)),
                    collection=str(coll),
                    collection_db_path=Path(str(dbp)),
                )
            )
        return out
    finally:
        con.close()


def parquet_relpaths_for_domain(collection_db: Path, host_rev_prefix: str) -> List[str]:
    duckdb = _require_duckdb()
    like_pat = host_rev_prefix + ",%"

    con = duckdb.connect(str(collection_db), read_only=True)
    try:
        if not _duckdb_has_table(con, "cc_domain_shards"):
            return []
        rows = con.execute(
            """
            SELECT DISTINCT parquet_relpath
            FROM cc_domain_shards
            WHERE host_rev = ? OR host_rev LIKE ?
            ORDER BY parquet_relpath
            """,
            [host_rev_prefix, like_pat],
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    finally:
        con.close()


def iter_warc_candidates_from_parquet(
    parquet_path: Path,
    host_rev_prefix: str,
    *,
    limit: int,
) -> Iterator[Dict[str, object]]:
    duckdb = _require_duckdb()
    like_pat = host_rev_prefix + ",%"

    def _infer_collection(collection: object, warc_filename: object) -> object:
        if collection not in (None, ""):
            return collection
        s = str(warc_filename or "")
        # Typical WARC path: crawl-data/CC-MAIN-2024-10/segments/.../warc/...
        marker = "CC-MAIN-"
        idx = s.find(marker)
        if idx == -1:
            return None
        rest = s[idx:]
        seg = rest.split("/", 1)[0]
        return seg or None

    def _parquet_columns(con) -> set[str]:
        rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)]).fetchall()
        return {str(r[0]) for r in rows if r and r[0]}

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        cols = _parquet_columns(con)

        # Build a schema-tolerant SELECT list.
        def col_or_null(name: str) -> str:
            return name if name in cols else f"NULL AS {name}"

        select_list = ",\n                ".join(
            [
                col_or_null("collection"),
                col_or_null("shard_file"),
                col_or_null("url"),
                col_or_null("ts"),
                col_or_null("status"),
                col_or_null("mime"),
                col_or_null("digest"),
                col_or_null("warc_filename"),
                col_or_null("warc_offset"),
                col_or_null("warc_length"),
            ]
        )

        # Prefer host_rev filtering when available; fall back to host filtering.
        params: list[object]
        if "host_rev" in cols:
            where_sql = "(host_rev = ? OR host_rev LIKE ?)"
            params = [host_rev_prefix, like_pat]
        elif "host" in cols:
            # host_rev_prefix is reversed; reconstruct plain domain for host matches.
            dom = ".".join(reversed(host_rev_prefix.split(".")))
            where_sql = "(host = ? OR host LIKE ?)"
            params = [dom, f"%.{dom}"]
        else:
            where_sql = "TRUE"
            params = []

        sql = f"""
            SELECT
                {select_list}
            FROM read_parquet(?)
            WHERE {where_sql}
            LIMIT ?
        """

        rows = con.execute(sql, [str(parquet_path), *params, int(limit)]).fetchall()

        for (
            collection,
            shard_file,
            url,
            ts,
            status,
            mime,
            digest,
            warc_filename,
            warc_offset,
            warc_length,
        ) in rows:
            yield {
                "collection": _infer_collection(collection, warc_filename),
                "shard_file": shard_file,
                "url": url,
                "timestamp": ts,
                "status": int(status) if status is not None else None,
                "mime": mime,
                "digest": digest,
                "warc_filename": warc_filename,
                "warc_offset": int(warc_offset) if warc_offset is not None else None,
                "warc_length": int(warc_length) if warc_length is not None else None,
                "parquet_path": str(parquet_path),
            }
    finally:
        con.close()


def search_domain_via_meta_indexes(
    domain_or_url: str,
    *,
    parquet_root: Path = Path("/storage/ccindex_parquet"),
    master_db: Optional[Path] = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
    year_db: Optional[Path] = None,
    collection_db: Optional[Path] = None,
    year: Optional[str] = None,
    max_parquet_files: int = 200,
    max_matches: int = 200,
    per_parquet_limit: int = 2000,
) -> MetaIndexSearchResult:
    """Search using master/year meta-indexes to find candidate WARC pointers.

    Returns records similar to `search_cc_via_meta_indexes.py` JSONL output.
    """

    dom = normalize_domain(domain_or_url)
    if not dom:
        raise ValueError("Empty domain")

    host_rev_prefix = host_to_rev(dom)
    if not host_rev_prefix:
        raise ValueError("Could not compute host_rev")

    parquet_root = Path(parquet_root).expanduser().resolve()
    if not parquet_root.exists():
        raise FileNotFoundError(f"Parquet root does not exist: {parquet_root}")

    t0 = time.perf_counter()

    # 1) Discover collections via meta-index layer.
    if collection_db is not None:
        coll_db = Path(collection_db).expanduser().resolve()
        collection_name = coll_db.stem.replace("cc_pointers_", "")
        collections = [
            CollectionRef(
                year=collection_year(collection_name),
                collection=collection_name,
                collection_db_path=coll_db,
            )
        ]
        meta_source = f"collection-db:{coll_db}"
    elif year_db is not None:
        ydb = Path(year_db).expanduser().resolve()
        collections = load_collections_from_year_db(ydb)
        meta_source = f"year-db:{ydb}"
    else:
        if master_db is None:
            raise ValueError("master_db must be set when year_db and collection_db are not provided")
        mdb = Path(master_db).expanduser().resolve()
        collections = load_collections_from_master(mdb, year)
        meta_source = f"master-db:{mdb}"

    if not collections:
        return MetaIndexSearchResult(
            records=[],
            emitted=0,
            elapsed_s=time.perf_counter() - t0,
            meta_source=meta_source,
            collections_considered=0,
        )

    emitted = 0
    records: List[Dict[str, object]] = []

    # 2) For each collection DB: find parquet shards, then expand to WARC pointers.
    for cref in collections:
        if emitted >= int(max_matches):
            break

        collection_db_path = cref.collection_db_path
        if not collection_db_path.exists():
            continue

        parquet_relpaths = parquet_relpaths_for_domain(collection_db_path, host_rev_prefix)
        if not parquet_relpaths:
            continue

        parquet_relpaths = parquet_relpaths[: max(0, int(max_parquet_files))]

        parquet_dir = get_collection_parquet_dir(parquet_root, cref.collection)
        if not parquet_dir.exists():
            continue

        for rel in parquet_relpaths:
            if emitted >= int(max_matches):
                break

            parquet_path = (parquet_dir / rel).resolve()
            if not parquet_path.exists():
                continue

            remaining = int(max_matches) - emitted
            per_file_limit = min(int(per_parquet_limit), remaining)
            for rec in iter_warc_candidates_from_parquet(parquet_path, host_rev_prefix, limit=per_file_limit):
                records.append(rec)
                emitted += 1
                if emitted >= int(max_matches):
                    break

    # Prefer records that are more likely to render as a "Wayback" page.
    # This improves the dashboard UX (and makes automated UI flows deterministic).
    def _wayback_score(r: Dict[str, object]) -> tuple[int, int]:
        wf = str(r.get("warc_filename") or "")
        mime = str(r.get("mime") or "")
        status = r.get("status")
        try:
            status_i = int(status) if status is not None else 0
        except Exception:
            status_i = 0

        score = 0
        if "/warc/" in wf:
            score += 4
        if "crawldiagnostics" in wf:
            score -= 4
        if status_i == 200:
            score += 2
        if mime.startswith("text/html"):
            score += 1

        # Secondary key: newer timestamps first when present.
        ts = str(r.get("timestamp") or "")
        ts_key = int(ts) if ts.isdigit() else 0
        return (score, ts_key)

    records.sort(key=_wayback_score, reverse=True)

    dt = time.perf_counter() - t0
    return MetaIndexSearchResult(
        records=records,
        emitted=emitted,
        elapsed_s=dt,
        meta_source=meta_source,
        collections_considered=len(collections),
    )


def to_jsonl(records: Iterable[Dict[str, object]]) -> str:
    return "".join(json.dumps(rec, ensure_ascii=False) + "\n" for rec in records)


def brave_web_search(
    query: str,
    *,
    api_key: Optional[str] = None,
    count: int = 10,
    offset: int = 0,
    country: str = "us",
    safesearch: str = "moderate",
) -> List[BraveWebResult]:
    """Search the web using Brave Search API.

    The HTTP integration lives in `common_crawl_search_engine.ccsearch.brave_search`.
    This function adapts results into the stable `BraveWebResult` dataclass.
    """

    from common_crawl_search_engine.ccsearch.brave_search import brave_web_search as _brave_web_search

    items = _brave_web_search(
        query,
        api_key=api_key,
        count=int(count),
        offset=int(offset),
        country=str(country),
        safesearch=str(safesearch),
    )

    out: List[BraveWebResult] = []
    for it in items:
        out.append(
            BraveWebResult(
                title=str(it.get("title") or ""),
                url=str(it.get("url") or ""),
                description=str(it.get("description") or ""),
            )
        )
    return out


def resolve_urls_to_ccindex(
    urls: Sequence[str],
    *,
    parquet_root: Path = Path("/storage/ccindex_parquet"),
    master_db: Optional[Path] = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
    year: Optional[str] = None,
    max_matches_per_domain: int = 400,
    per_url_limit: int = 5,
) -> Dict[str, List[Dict[str, object]]]:
    """Resolve a list of URLs to candidate CCIndex WARC pointers.

    Fast path (default): batch URL lookups by joining a temp URL table against
    targeted Parquet shards (shards determined via collection-domain shard maps).
    Fallback: the previous domain-scan approach (more robust but can be slow for
    many distinct domains).
    """

    want = [u for u in urls if (u or "").strip()]
    if not want:
        return {}

    def _via_domain_scan(scan_urls: Sequence[str]) -> Dict[str, List[Dict[str, object]]]:
        domain_to_urls: Dict[str, List[str]] = {}
        for u in scan_urls:
            dom = normalize_domain(u)
            if not dom:
                continue
            domain_to_urls.setdefault(dom, []).append(u)

        out: Dict[str, List[Dict[str, object]]] = {u: [] for u in scan_urls if (u or "").strip()}
        for dom, dom_urls in domain_to_urls.items():
            res = search_domain_via_meta_indexes(
                dom,
                parquet_root=parquet_root,
                master_db=master_db,
                year=year,
                max_matches=int(max_matches_per_domain),
            )

            canon_to_requested: Dict[str, List[str]] = {}
            for u in dom_urls:
                canon_to_requested.setdefault(_canonicalize_url_for_match(u), []).append(u)

            for rec in res.records:
                rec_url = str(rec.get("url") or "")
                canon = _canonicalize_url_for_match(rec_url)
                reqs = canon_to_requested.get(canon)
                if not reqs:
                    continue
                for requested in reqs:
                    if len(out[requested]) >= int(per_url_limit):
                        continue
                    out[requested].append(rec)
        return out

    # If we can't use meta-index info, fall back.
    if master_db is None:
        return _via_domain_scan(want)

    # Batch join approach.
    try:
        duckdb = _require_duckdb()
    except Exception:
        return _via_domain_scan(want)

    parquet_root = Path(parquet_root).expanduser().resolve()
    master_db = Path(master_db).expanduser().resolve()

    # Group URLs by domain and precompute host_rev_prefix.
    domain_to_urls: Dict[str, List[str]] = {}
    domain_to_hostrev: Dict[str, str] = {}
    for u in want:
        dom = normalize_domain(u)
        if not dom:
            continue
        domain_to_urls.setdefault(dom, []).append(u)
        if dom not in domain_to_hostrev:
            hr = host_to_rev(dom)
            if hr:
                domain_to_hostrev[dom] = hr

    if not domain_to_urls:
        return {u: [] for u in want}

    # Load collections once.
    collections = load_collections_from_master(master_db, year)
    if not collections:
        return {u: [] for u in want}

    # Prefer newest collections first for URL lookups.
    collections = list(reversed(collections))

    # Build URL variant table.
    variant_rows: List[Tuple[str, str]] = []
    for requested in want:
        for v in _url_variants_for_lookup(requested):
            variant_rows.append((v, requested))

    out: Dict[str, List[Dict[str, object]]] = {u: [] for u in want}
    per_url_counts: Dict[str, int] = {u: 0 for u in want}

    parquet_files_scanned = 0

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        con.execute("CREATE TABLE search_urls (url VARCHAR, requested_url VARCHAR)")
        con.executemany("INSERT INTO search_urls VALUES (?, ?)", variant_rows)

        def _parquet_columns(pq_path: Path) -> set[str]:
            rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(pq_path)]).fetchall()
            return {str(r[0]) for r in rows if r and r[0]}

        def col_or_null(cols: set[str], name: str) -> str:
            return f"p.{name} AS {name}" if name in cols else f"NULL AS {name}"

        # Iterate collections newest-first and stop once all URLs are satisfied.
        for cref in collections:
            if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in want):
                break

            cdb = cref.collection_db_path
            if not cdb.exists():
                continue
            parquet_dir = get_collection_parquet_dir(parquet_root, cref.collection)
            if not parquet_dir.exists():
                continue

            # Get shard relpaths for all requested domains using one connection per collection.
            relpaths: List[str] = []
            con_c = duckdb.connect(str(cdb), read_only=True)
            try:
                if not _duckdb_has_table(con_c, "cc_domain_shards"):
                    continue
                for host_rev_prefix in domain_to_hostrev.values():
                    like_pat = host_rev_prefix + ",%"
                    rows = con_c.execute(
                        """
                        SELECT DISTINCT parquet_relpath
                        FROM cc_domain_shards
                        WHERE host_rev = ? OR host_rev LIKE ?
                        """,
                        [host_rev_prefix, like_pat],
                    ).fetchall()
                    for r in rows:
                        if r and r[0]:
                            relpaths.append(str(r[0]))
            finally:
                con_c.close()

            if not relpaths:
                continue

            # De-dupe within the collection.
            relpaths = sorted(set(relpaths))
            for rel in relpaths:
                if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in want):
                    break
                pq = (parquet_dir / rel).resolve()
                if not pq.exists():
                    continue

                cols = _parquet_columns(pq)
                select_list = ", ".join(
                    [
                        col_or_null(cols, "collection"),
                        col_or_null(cols, "shard_file"),
                        col_or_null(cols, "url"),
                        col_or_null(cols, "ts"),
                        col_or_null(cols, "status"),
                        col_or_null(cols, "mime"),
                        col_or_null(cols, "digest"),
                        col_or_null(cols, "warc_filename"),
                        col_or_null(cols, "warc_offset"),
                        col_or_null(cols, "warc_length"),
                    ]
                )

                parquet_files_scanned += 1
                rows = con.execute(
                    f"""
                    SELECT s.requested_url, {select_list}
                    FROM read_parquet(?) p
                    INNER JOIN search_urls s ON p.url = s.url
                    """,
                    [str(pq)],
                ).fetchall()

                for row in rows:
                    if not row:
                        continue
                    requested_url = str(row[0] or "")
                    if not requested_url or requested_url not in out:
                        continue
                    if per_url_counts.get(requested_url, 0) >= int(per_url_limit):
                        continue

                    (
                        _requested,
                        collection,
                        shard_file,
                        url,
                        ts,
                        status,
                        mime,
                        digest,
                        warc_filename,
                        warc_offset,
                        warc_length,
                    ) = row
                    rec = {
                        "collection": collection,
                        "shard_file": shard_file,
                        "url": url,
                        "timestamp": ts,
                        "status": int(status) if status is not None else None,
                        "mime": mime,
                        "digest": digest,
                        "warc_filename": warc_filename,
                        "warc_offset": int(warc_offset) if warc_offset is not None else None,
                        "warc_length": int(warc_length) if warc_length is not None else None,
                        "parquet_path": str(pq),
                    }
                    out[requested_url].append(rec)
                    per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1
    finally:
        con.close()

    # Fallback for any URLs still unresolved.
    missing = [u for u in want if not out.get(u)]
    if missing:
        fallback = _via_domain_scan(missing)
        for u, recs in fallback.items():
            if recs:
                out[u] = recs

    return out


def brave_search_ccindex(
    query: str,
    *,
    count: int = 8,
    offset: int = 0,
    parquet_root: Path = Path("/storage/ccindex_parquet"),
    master_db: Optional[Path] = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
    year: Optional[str] = None,
    per_url_limit: int = 3,
    api_key: Optional[str] = None,
) -> BraveSearchResolvedResult:
    """Brave web search + resolve result URLs to CCIndex pointers."""

    t0 = time.perf_counter()

    # Second-layer cache: caches the *resolved* result set (Brave results + CCIndex pointers).
    resolve_cache_disable = (os.environ.get("BRAVE_RESOLVE_CACHE_DISABLE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    resolve_ttl_s = int((os.environ.get("BRAVE_RESOLVE_CACHE_TTL_S") or "86400").strip() or "86400")
    resolve_max_entries = int((os.environ.get("BRAVE_RESOLVE_CACHE_MAX_ENTRIES") or "2000").strip() or "2000")
    cache_path = _brave_resolve_cache_path()

    if not resolve_cache_disable and resolve_ttl_s > 0:
        try:
            cache_key = _brave_resolve_cache_key(
                query=str(query),
                count=int(count),
                offset=int(offset),
                year=str(year) if year else None,
                parquet_root=Path(parquet_root),
                master_db=Path(master_db) if master_db is not None else None,
                per_url_limit=int(per_url_limit),
            )
            cache = _load_cache_dict(cache_path)
            ent = cache.get(cache_key)
            if isinstance(ent, dict):
                ts = ent.get("ts")
                payload = ent.get("result")
                if isinstance(ts, (int, float)) and isinstance(payload, dict):
                    if (time.time() - float(ts)) <= float(resolve_ttl_s):
                        return BraveSearchResolvedResult(
                            query=str(payload.get("query") or query),
                            count=int(payload.get("count") or int(count)),
                            offset=int(payload.get("offset") or int(offset)),
                            total_results=(
                                int(payload.get("total_results"))
                                if isinstance(payload.get("total_results"), (int, float))
                                else None
                            ),
                            brave_cached=bool(payload.get("brave_cached")),
                            resolved_cached=True,
                            results=list(payload.get("results") or []),
                            elapsed_s=(time.perf_counter() - t0),
                            brave_elapsed_s=float(payload.get("brave_elapsed_s") or 0.0),
                            resolve_elapsed_s=float(payload.get("resolve_elapsed_s") or 0.0),
                            resolve_mode=str(payload.get("resolve_mode") or "cache"),
                            resolve_domains=int(payload.get("resolve_domains") or 0),
                            resolve_parquet_files=int(payload.get("resolve_parquet_files") or 0),
                        )
        except Exception:
            pass
    # Prefer the meta-returning variant so callers can render real pagination.
    from common_crawl_search_engine.ccsearch.brave_search import brave_web_search_page

    t_brave0 = time.perf_counter()
    page = brave_web_search_page(query, api_key=api_key, count=int(count), offset=int(offset))
    brave_elapsed_s = time.perf_counter() - t_brave0
    meta = page.get("meta") if isinstance(page, dict) else None
    items = page.get("items") if isinstance(page, dict) else None

    total_results: Optional[int] = None
    brave_cached = False
    effective_count = int(count)
    effective_offset = int(offset)
    if isinstance(meta, dict):
        try:
            effective_count = int(meta.get("count")) if meta.get("count") is not None else int(count)
        except Exception:
            effective_count = int(count)
        try:
            effective_offset = int(meta.get("offset")) if meta.get("offset") is not None else int(offset)
        except Exception:
            effective_offset = int(offset)
        v = meta.get("total")
        if isinstance(v, (int, float)):
            total_results = int(v)
        brave_cached = bool(meta.get("cached"))

    results: List[BraveWebResult] = []
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            results.append(
                BraveWebResult(
                    title=str(it.get("title") or ""),
                    url=str(it.get("url") or ""),
                    description=str(it.get("description") or ""),
                )
            )

    url_list = [r.url for r in results if r.url]

    t_res0 = time.perf_counter()
    resolved = resolve_urls_to_ccindex(
        url_list,
        parquet_root=parquet_root,
        master_db=master_db,
        year=year,
        per_url_limit=int(per_url_limit),
    )
    resolve_elapsed_s = time.perf_counter() - t_res0

    # Best-effort resolve stats.
    resolve_domains = len({normalize_domain(u) for u in url_list if normalize_domain(u)})
    # We can't easily count parquet files scanned from here without deeper plumbing.
    resolve_parquet_files = 0
    resolve_mode = "auto"

    out: List[Dict[str, object]] = []
    for r in results:
        out.append(
            {
                "title": r.title,
                "url": r.url,
                "description": r.description,
                "cc_matches": resolved.get(r.url, []),
            }
        )

    res_obj = BraveSearchResolvedResult(
        query=query,
        count=int(effective_count),
        offset=int(effective_offset),
        total_results=total_results,
        brave_cached=bool(brave_cached),
        resolved_cached=False,
        results=out,
        elapsed_s=(time.perf_counter() - t0),
        brave_elapsed_s=float(brave_elapsed_s),
        resolve_elapsed_s=float(resolve_elapsed_s),
        resolve_mode=str(resolve_mode),
        resolve_domains=int(resolve_domains),
        resolve_parquet_files=int(resolve_parquet_files),
    )

    if not resolve_cache_disable and resolve_ttl_s > 0:
        try:
            cache_key = _brave_resolve_cache_key(
                query=str(query),
                count=int(count),
                offset=int(offset),
                year=str(year) if year else None,
                parquet_root=Path(parquet_root),
                master_db=Path(master_db) if master_db is not None else None,
                per_url_limit=int(per_url_limit),
            )
            cache = _load_cache_dict(cache_path)
            cache[cache_key] = {
                "ts": time.time(),
                "result": {
                    "query": res_obj.query,
                    "count": res_obj.count,
                    "offset": res_obj.offset,
                    "total_results": res_obj.total_results,
                    "brave_cached": res_obj.brave_cached,
                    "brave_elapsed_s": res_obj.brave_elapsed_s,
                    "resolve_elapsed_s": res_obj.resolve_elapsed_s,
                    "resolve_mode": res_obj.resolve_mode,
                    "resolve_domains": res_obj.resolve_domains,
                    "resolve_parquet_files": res_obj.resolve_parquet_files,
                    "results": res_obj.results,
                },
            }
            cache = _maybe_evict_oldest(cache, max_entries=int(resolve_max_entries))
            _save_cache_dict(cache_path, cache)
        except Exception:
            pass

    return res_obj


def warc_download_url(warc_filename_or_url: str, *, prefix: str = "https://data.commoncrawl.org/") -> str:
    warc = (warc_filename_or_url or "").strip()
    if warc.startswith("http://") or warc.startswith("https://"):
        return warc
    pref = prefix if prefix.endswith("/") else prefix + "/"
    return pref + warc.lstrip("/")


def _default_warc_cache_dir() -> Optional[Path]:
    """Return a default cache dir for fetched WARC byte ranges.

    Disable by setting env var CCINDEX_WARC_CACHE_DIR='' (empty).
    """

    env = os.environ.get("CCINDEX_WARC_CACHE_DIR")
    if env is not None and str(env).strip() == "":
        return None
    if env:
        return Path(env)
    return Path("state") / "warc_cache"


def _default_full_warc_cache_dir() -> Optional[Path]:
    """Return a default cache dir for full *.warc.gz downloads.

    Disable by setting env var CCINDEX_FULL_WARC_CACHE_DIR='' (empty).
    """

    env = os.environ.get("CCINDEX_FULL_WARC_CACHE_DIR")
    if env is not None and str(env).strip() == "":
        return None
    if env:
        return Path(env)
    return Path("state") / "warc_files"


def _safe_cache_name_for_warc(warc_filename_or_url: str) -> str:
    base = (warc_filename_or_url or "").strip().rstrip("/").rsplit("/", 1)[-1] or "warc.gz"
    h = _sha256_hex(warc_filename_or_url)[:16]
    # Keep the filename stable but also recognizable.
    return f"{h}__{base}"


def _full_warc_cache_path(cache_dir: Path, warc_filename_or_url: str) -> Path:
    return cache_dir / _safe_cache_name_for_warc(warc_filename_or_url)


def _http_head_content_length(url: str, *, timeout_s: float) -> Optional[int]:
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            clen = resp.headers.get("content-length")
            if clen and str(clen).isdigit():
                return int(clen)
    except Exception:
        return None
    return None


def ensure_full_warc_cached(
    *,
    warc_filename: str,
    prefix: str = "https://data.commoncrawl.org/",
    cache_dir: Optional[Path] = None,
    timeout_s: float = 60.0,
    max_full_bytes: int = 5_000_000_000,
    cache_max_total_bytes: int = 0,
    overwrite: bool = False,
) -> Path:
    """Download and cache the full *.warc.gz file (last-ditch / bulk scraping mode).

    This can reduce many small Range requests into a single large download, which
    is useful when scraping many pages that live in the same WARC file.

    Safety:
    - Uses a max_full_bytes guard (default 5GB). Set <=0 to disable the limit.
    - Writes to a .part file then renames.
    - Optionally prunes the full-WARC cache to cache_max_total_bytes (0 disables).
    """

    url = warc_download_url(warc_filename, prefix=prefix)

    if cache_dir is None:
        cache_dir = _default_full_warc_cache_dir()
    if cache_dir is None:
        raise RuntimeError("full WARC cache disabled (CCINDEX_FULL_WARC_CACHE_DIR='')")

    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    out_path = _full_warc_cache_path(cache_dir, warc_filename)
    if out_path.exists() and out_path.is_file() and not overwrite:
        return out_path

    # If size is known, guard before downloading.
    clen = _http_head_content_length(url, timeout_s=float(timeout_s))
    if clen is not None and int(max_full_bytes) > 0 and clen > int(max_full_bytes):
        raise RuntimeError(f"full WARC too large: {clen} bytes > max_full_bytes={int(max_full_bytes)}")

    req = urllib.request.Request(url, method="GET")
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass

    # Stream to disk.
    with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
        status = int(getattr(resp, "status", 200))
        if status != 200:
            raise RuntimeError(f"expected 200 for full GET, got {status}")

        written = 0
        chunk_bytes = 8 * 1024 * 1024
        with tmp.open("wb") as f:
            while True:
                chunk = resp.read(chunk_bytes)
                if not chunk:
                    break
                f.write(chunk)
                written += len(chunk)
                if int(max_full_bytes) > 0 and written > int(max_full_bytes):
                    raise RuntimeError(f"download exceeded max_full_bytes={int(max_full_bytes)}")

    # Optional sanity check against content-length if present.
    if clen is not None:
        try:
            got = tmp.stat().st_size
            if got != int(clen):
                raise RuntimeError(f"size mismatch expected={clen} got={got}")
        except Exception:
            # If this fails, let it surface.
            raise

    if out_path.exists() and overwrite:
        out_path.unlink()
    tmp.replace(out_path)

    _maybe_prune_cache_glob(cache_dir, glob="*", max_cache_bytes=int(cache_max_total_bytes))
    return out_path


def _read_local_range(path: Path, *, start: int, length: int, max_bytes: int) -> bytes:
    if int(max_bytes) > 0 and int(length) > int(max_bytes):
        raise RuntimeError(f"record too large for max_bytes={int(max_bytes)}: {int(length)}")
    with path.open("rb") as f:
        f.seek(int(start))
        data = f.read(int(length))
    if len(data) != int(length):
        raise RuntimeError(f"local range short read expected={int(length)} got={len(data)}")
    return data


def fetch_warc_record(
    *,
    warc_filename: str,
    warc_offset: int,
    warc_length: int,
    prefix: str = "https://data.commoncrawl.org/",
    timeout_s: float = 30.0,
    max_bytes: int = 2_000_000,
    decode_gzip_text: bool = True,
    max_preview_chars: int = 40_000,
    cache_mode: str = "range",
    range_cache_dir: Optional[Path] = None,
    range_cache_max_bytes: int = 2_000_000_000,
    range_cache_max_item_bytes: int = 25_000_000,
    full_warc_cache_dir: Optional[Path] = None,
    full_warc_max_bytes: int = 5_000_000_000,
    full_warc_cache_max_total_bytes: int = 0,
) -> Tuple[WarcFetchResult, str, Optional[str]]:
    """Fetch a WARC record by pointer using either Range GET or a cached full WARC.

    cache_mode:
    - "range": HTTP Range GET (default; uses range blob cache)
    - "auto": use cached full WARC if present; otherwise Range GET
    - "full": ensure full WARC cached (download if needed), then read locally

    Returns (result, source, local_path).
    """

    mode = (cache_mode or "range").strip().lower()
    url = warc_download_url(warc_filename, prefix=prefix)

    # Try local full WARC first for auto/full.
    if mode in {"auto", "full"}:
        if full_warc_cache_dir is None:
            full_warc_cache_dir = _default_full_warc_cache_dir()
        local_path: Optional[Path] = None
        if full_warc_cache_dir is not None:
            candidate = _full_warc_cache_path(Path(full_warc_cache_dir), warc_filename)
            if candidate.exists() and candidate.is_file():
                local_path = candidate
            elif mode == "full":
                try:
                    local_path = ensure_full_warc_cached(
                        warc_filename=str(warc_filename),
                        prefix=str(prefix),
                        cache_dir=Path(full_warc_cache_dir),
                        timeout_s=float(timeout_s),
                        max_full_bytes=int(full_warc_max_bytes),
                        cache_max_total_bytes=int(full_warc_cache_max_total_bytes),
                        overwrite=False,
                    )
                except Exception as e:
                    if mode == "full":
                        return (
                            WarcFetchResult(
                                ok=False,
                                status=None,
                                url=url,
                                bytes_requested=int(warc_length),
                                bytes_returned=0,
                                sha256=None,
                                raw_base64=None,
                                decoded_text_preview=None,
                                error=f"full_warc_cache_failed: {type(e).__name__}: {e}",
                            ),
                            "full",
                            None,
                        )

        if local_path is not None:
            try:
                data = _read_local_range(
                    Path(local_path),
                    start=int(warc_offset),
                    length=int(warc_length),
                    max_bytes=int(max_bytes),
                )
                h = hashlib.sha256(data).hexdigest() if data else None
                raw_b64 = base64.b64encode(data).decode("ascii") if data else None

                preview: Optional[str] = None
                if decode_gzip_text and data:
                    try:
                        decompressed = gzip.decompress(data)
                        preview = decompressed[: max(0, int(max_preview_chars))].decode("utf-8", errors="replace")
                    except Exception:
                        preview = None

                return (
                    WarcFetchResult(
                        ok=True,
                        status=200,
                        url=url,
                        bytes_requested=int(warc_length),
                        bytes_returned=len(data) if data else 0,
                        sha256=h,
                        raw_base64=raw_b64,
                        decoded_text_preview=preview,
                        error=None,
                    ),
                    "full_cache",
                    str(local_path),
                )
            except Exception as e:
                if mode == "full":
                    return (
                        WarcFetchResult(
                            ok=False,
                            status=None,
                            url=url,
                            bytes_requested=int(warc_length),
                            bytes_returned=0,
                            sha256=None,
                            raw_base64=None,
                            decoded_text_preview=None,
                            error=f"local_range_failed: {type(e).__name__}: {e}",
                        ),
                        "full_cache",
                        str(local_path),
                    )

    # Fall back to ranged retrieval.
    res = fetch_warc_record_range(
        warc_filename=str(warc_filename),
        warc_offset=int(warc_offset),
        warc_length=int(warc_length),
        prefix=str(prefix),
        timeout_s=float(timeout_s),
        max_bytes=int(max_bytes),
        decode_gzip_text=bool(decode_gzip_text),
        max_preview_chars=int(max_preview_chars),
        cache_dir=range_cache_dir,
        cache_max_bytes=int(range_cache_max_bytes),
        cache_max_item_bytes=int(range_cache_max_item_bytes),
    )
    return res, "range", None


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _cache_path_for_range(cache_dir: Path, *, url: str, start: int, end_inclusive: int) -> Path:
    key = _sha256_hex(f"range:{url}|{int(start)}|{int(end_inclusive)}")
    return cache_dir / f"{key}.bin"


def _maybe_prune_cache(cache_dir: Path, *, max_cache_bytes: int) -> None:
    _maybe_prune_cache_glob(cache_dir, glob="*.bin", max_cache_bytes=int(max_cache_bytes))


def _maybe_prune_cache_glob(cache_dir: Path, *, glob: str, max_cache_bytes: int) -> None:
    try:
        limit = int(max_cache_bytes)
    except Exception:
        return
    if limit <= 0:
        return

    try:
        paths = [p for p in cache_dir.glob(str(glob)) if p.is_file()]
        total = 0
        infos: List[Tuple[float, int, Path]] = []
        for p in paths:
            st = p.stat()
            total += int(st.st_size)
            infos.append((float(st.st_mtime), int(st.st_size), p))
        if total <= limit:
            return

        # Delete oldest files first.
        infos.sort(key=lambda t: t[0])
        for _, sz, p in infos:
            try:
                p.unlink()
                total -= int(sz)
            except Exception:
                continue
            if total <= limit:
                break
    except Exception:
        return


def _http_range_get_cached(
    *,
    url: str,
    start: int,
    end_inclusive: int,
    timeout_s: float,
    cache_dir: Optional[Path],
    cache_max_bytes: int,
    cache_max_item_bytes: int,
) -> Tuple[Optional[int], Optional[bytes], Optional[str]]:
    """Return (status, data, error). Uses a best-effort on-disk cache."""

    bytes_requested = int(end_inclusive) - int(start) + 1

    cache_path: Optional[Path] = None
    if cache_dir is not None:
        try:
            cache_dir = Path(cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
            if bytes_requested > 0 and bytes_requested <= int(cache_max_item_bytes):
                cache_path = _cache_path_for_range(cache_dir, url=url, start=int(start), end_inclusive=int(end_inclusive))
                if cache_path.exists() and cache_path.is_file():
                    try:
                        if cache_path.stat().st_size == bytes_requested:
                            return 206, cache_path.read_bytes(), None
                    except Exception:
                        pass
        except Exception:
            cache_path = None

    req = urllib.request.Request(url, method="GET")
    req.add_header("Range", f"bytes={int(start)}-{int(end_inclusive)}")

    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            status = int(getattr(resp, "status", 200))
            if status != 206:
                # Don't read the body here; if the server ignores Range it may be
                # a multi-GB response.
                return status, None, f"expected 206 for range GET, got {status}"
            data = resp.read()

        if cache_path is not None and data is not None:
            try:
                tmp = cache_path.with_suffix(cache_path.suffix + ".part")
                tmp.write_bytes(data)
                if tmp.stat().st_size == bytes_requested:
                    tmp.replace(cache_path)
                    _maybe_prune_cache(cache_path.parent, max_cache_bytes=int(cache_max_bytes))
                else:
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
            except Exception:
                pass

        return status, data, None
    except urllib.error.HTTPError as e:
        code = int(getattr(e, "code", 0)) if getattr(e, "code", None) is not None else None
        return code, None, f"HTTPError {getattr(e, 'code', '?')}"
    except urllib.error.URLError as e:
        return None, None, f"URLError {getattr(e, 'reason', e)}"
    except Exception as e:
        return None, None, f"{type(e).__name__}: {e}"


def _parse_headers_block(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    lines = [ln.strip("\r") for ln in text.splitlines() if ln.strip("\r")]
    if not lines:
        return out
    out["_first_line"] = lines[0]
    for ln in lines[1:]:
        if ":" not in ln:
            continue
        k, v = ln.split(":", 1)
        out[k.strip().lower()] = v.strip()
    return out


_CT_CHARSET_RE = re.compile(r"charset=([^;\s]+)", re.IGNORECASE)


def _decode_chunked(body: bytes, *, max_output_bytes: int) -> Tuple[bytes, Optional[str]]:
    """Best-effort HTTP/1.1 chunked transfer decoding."""

    out = bytearray()
    i = 0
    n = len(body)
    try:
        while i < n:
            j = body.find(b"\n", i)
            if j == -1:
                return bytes(out), "chunked: missing size line"
            line = body[i : j + 1]
            i = j + 1
            line = line.strip()
            if b";" in line:
                line = line.split(b";", 1)[0]
            if not line:
                continue
            size = int(line, 16)
            if size == 0:
                break
            if i + size > n:
                return bytes(out), "chunked: truncated"
            out.extend(body[i : i + size])
            if len(out) > int(max_output_bytes):
                out = out[: int(max_output_bytes)]
                return bytes(out), "chunked: output truncated"
            i += size
            # Skip CRLF after chunk
            if i < n and body[i : i + 2] == b"\r\n":
                i += 2
            elif i < n and body[i : i + 1] == b"\n":
                i += 1
        return bytes(out), None
    except Exception as e:
        return bytes(out), f"chunked decode failed: {type(e).__name__}: {e}"


def extract_http_from_warc_gzip_member(
    gz_member_bytes: bytes,
    *,
    max_decompressed_bytes: int = 10_000_000,
    max_body_bytes: int = 2_000_000,
    max_preview_chars: int = 80_000,
    include_body_base64: bool = False,
) -> WarcHttpExtractResult:
    """Extract HTTP response headers/body from a gzip-member WARC record."""

    if not gz_member_bytes:
        return WarcHttpExtractResult(
            ok=False,
            warc_headers={},
            http_status=None,
            http_status_line=None,
            http_headers={},
            body_base64=None,
            body_text_preview=None,
            body_is_html=False,
            body_mime=None,
            body_charset=None,
            error="empty input",
        )

    try:
        decompressed = gzip.decompress(gz_member_bytes)
        if int(max_decompressed_bytes) > 0 and len(decompressed) > int(max_decompressed_bytes):
            decompressed = decompressed[: int(max_decompressed_bytes)]
    except Exception as e:
        return WarcHttpExtractResult(
            ok=False,
            warc_headers={},
            http_status=None,
            http_status_line=None,
            http_headers={},
            body_base64=None,
            body_text_preview=None,
            body_is_html=False,
            body_mime=None,
            body_charset=None,
            error=f"gzip_decompress_failed: {type(e).__name__}: {e}",
        )

    # Split WARC headers from payload.
    sep = decompressed.find(b"\r\n\r\n")
    sep_len = 4
    if sep == -1:
        sep = decompressed.find(b"\n\n")
        sep_len = 2
    if sep == -1:
        return WarcHttpExtractResult(
            ok=False,
            warc_headers={},
            http_status=None,
            http_status_line=None,
            http_headers={},
            body_base64=None,
            body_text_preview=None,
            body_is_html=False,
            body_mime=None,
            body_charset=None,
            error="missing_warc_header_separator",
        )

    warc_hdr_text = decompressed[:sep].decode("utf-8", errors="replace")
    warc_headers = _parse_headers_block(warc_hdr_text)

    payload = decompressed[sep + sep_len :]
    http_idx = payload.find(b"HTTP/")
    if http_idx == -1:
        # Not an HTTP response record.
        raw = payload[: int(max_body_bytes)] if payload else b""
        prev = raw.decode("utf-8", errors="replace") if raw else ""
        if prev and len(prev) > int(max_preview_chars):
            prev = prev[: int(max_preview_chars)]
        return WarcHttpExtractResult(
            ok=True,
            warc_headers=warc_headers,
            http_status=None,
            http_status_line=None,
            http_headers={},
            body_base64=base64.b64encode(raw).decode("ascii") if (include_body_base64 and raw) else None,
            body_text_preview=prev or None,
            body_is_html=False,
            body_mime=None,
            body_charset=None,
            error="no_http_payload",
        )

    http_part = payload[http_idx:]
    http_sep = http_part.find(b"\r\n\r\n")
    http_sep_len = 4
    if http_sep == -1:
        http_sep = http_part.find(b"\n\n")
        http_sep_len = 2
    if http_sep == -1:
        return WarcHttpExtractResult(
            ok=False,
            warc_headers=warc_headers,
            http_status=None,
            http_status_line=None,
            http_headers={},
            body_base64=None,
            body_text_preview=None,
            body_is_html=False,
            body_mime=None,
            body_charset=None,
            error="missing_http_header_separator",
        )

    http_hdr_text = http_part[:http_sep].decode("iso-8859-1", errors="replace")
    http_headers_all = _parse_headers_block(http_hdr_text)

    status_line = http_headers_all.get("_first_line")
    status_code: Optional[int] = None
    if status_line:
        parts = status_line.split()
        if len(parts) >= 2:
            try:
                status_code = int(parts[1])
            except Exception:
                status_code = None

    body = http_part[http_sep + http_sep_len :]
    if int(max_body_bytes) > 0 and len(body) > int(max_body_bytes):
        body = body[: int(max_body_bytes)]

    te = http_headers_all.get("transfer-encoding", "")
    if te and "chunked" in te.lower():
        body2, _ = _decode_chunked(body, max_output_bytes=int(max_body_bytes))
        body = body2

    mime = None
    charset = None
    ct = http_headers_all.get("content-type")
    if ct:
        mime = ct.split(";", 1)[0].strip().lower() or None
        m = _CT_CHARSET_RE.search(ct)
        if m:
            charset = m.group(1).strip("\"'").lower()

    body_is_html = bool(mime == "text/html" or (mime and mime.endswith("+html")))
    if not body_is_html and body[:64].lstrip().lower().startswith((b"<!doctype html", b"<html")):
        body_is_html = True

    body_text_preview: Optional[str] = None
    if body:
        enc = charset or "utf-8"
        try:
            body_text_preview = body.decode(enc, errors="replace")
        except Exception:
            body_text_preview = body.decode("utf-8", errors="replace")
        if body_text_preview and len(body_text_preview) > int(max_preview_chars):
            body_text_preview = body_text_preview[: int(max_preview_chars)]

    body_b64 = base64.b64encode(body).decode("ascii") if (include_body_base64 and body) else None

    http_headers = {k: v for k, v in http_headers_all.items() if k != "_first_line"}

    return WarcHttpExtractResult(
        ok=True,
        warc_headers=warc_headers,
        http_status=status_code,
        http_status_line=status_line,
        http_headers=http_headers,
        body_base64=body_b64,
        body_text_preview=body_text_preview,
        body_is_html=body_is_html,
        body_mime=mime,
        body_charset=charset,
        error=None,
    )


def fetch_warc_record_range(
    *,
    warc_filename: str,
    warc_offset: int,
    warc_length: int,
    prefix: str = "https://data.commoncrawl.org/",
    timeout_s: float = 30.0,
    max_bytes: int = 2_000_000,
    decode_gzip_text: bool = True,
    max_preview_chars: int = 40_000,
    cache_dir: Optional[Path] = None,
    cache_max_bytes: int = 2_000_000_000,
    cache_max_item_bytes: int = 25_000_000,
) -> WarcFetchResult:
    """Fetch the exact byte range for a WARC record pointer.

    Notes:
    - Common Crawl WARC files are typically gzip-compressed; the pointer range
      usually corresponds to a gzip member containing a WARC record.
    - For safety, this caps downloads via max_bytes.
    """

    start = int(warc_offset)
    length = int(warc_length)
    if start < 0 or length <= 0:
        raise ValueError("Invalid warc_offset/warc_length")

    if int(max_bytes) > 0 and length > int(max_bytes):
        return WarcFetchResult(
            ok=False,
            status=None,
            url=warc_download_url(warc_filename, prefix=prefix),
            bytes_requested=length,
            bytes_returned=0,
            sha256=None,
            raw_base64=None,
            decoded_text_preview=None,
            error=f"record too large for max_bytes={max_bytes}: {length}",
        )

    end_inclusive = start + length - 1
    url = warc_download_url(warc_filename, prefix=prefix)

    if cache_dir is None:
        cache_dir = _default_warc_cache_dir()

    status, data, err = _http_range_get_cached(
        url=url,
        start=start,
        end_inclusive=end_inclusive,
        timeout_s=float(timeout_s),
        cache_dir=cache_dir,
        cache_max_bytes=int(cache_max_bytes),
        cache_max_item_bytes=int(cache_max_item_bytes),
    )
    if data is None or err is not None:
        return WarcFetchResult(
            ok=False,
            status=int(status) if status is not None else None,
            url=url,
            bytes_requested=length,
            bytes_returned=0,
            sha256=None,
            raw_base64=None,
            decoded_text_preview=None,
            error=err or "fetch_failed",
        )

    h = hashlib.sha256(data).hexdigest() if data else None
    raw_b64 = base64.b64encode(data).decode("ascii") if data else None

    preview: Optional[str] = None
    if decode_gzip_text and data:
        try:
            decompressed = gzip.decompress(data)
            preview = decompressed[: max(0, int(max_preview_chars))].decode("utf-8", errors="replace")
        except Exception:
            preview = None

    return WarcFetchResult(
        ok=True,
        status=int(status) if status is not None else None,
        url=url,
        bytes_requested=length,
        bytes_returned=len(data) if data else 0,
        sha256=h,
        raw_base64=raw_b64,
        decoded_text_preview=preview,
        error=None,
    )
