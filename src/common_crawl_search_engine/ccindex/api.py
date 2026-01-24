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
    results: List[Dict[str, object]]
    elapsed_s: float


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
    """Resolve a list of URLs to candidate CCIndex WARC pointers."""

    want = [u for u in urls if (u or "").strip()]
    if not want:
        return {}

    domain_to_urls: Dict[str, List[str]] = {}
    for u in want:
        dom = normalize_domain(u)
        if not dom:
            continue
        domain_to_urls.setdefault(dom, []).append(u)

    out: Dict[str, List[Dict[str, object]]] = {u: [] for u in want}

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


def brave_search_ccindex(
    query: str,
    *,
    count: int = 8,
    parquet_root: Path = Path("/storage/ccindex_parquet"),
    master_db: Optional[Path] = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
    year: Optional[str] = None,
    per_url_limit: int = 3,
) -> BraveSearchResolvedResult:
    """Brave web search + resolve result URLs to CCIndex pointers."""

    t0 = time.perf_counter()
    results = brave_web_search(query, count=int(count))
    url_list = [r.url for r in results if r.url]

    resolved = resolve_urls_to_ccindex(
        url_list,
        parquet_root=parquet_root,
        master_db=master_db,
        year=year,
        per_url_limit=int(per_url_limit),
    )

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

    return BraveSearchResolvedResult(query=query, results=out, elapsed_s=(time.perf_counter() - t0))


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
    overwrite: bool = False,
) -> Path:
    """Download and cache the full *.warc.gz file (last-ditch / bulk scraping mode).

    This can reduce many small Range requests into a single large download, which
    is useful when scraping many pages that live in the same WARC file.

    Safety:
    - Uses a max_full_bytes guard (default 5GB). Set <=0 to disable the limit.
    - Writes to a .part file then renames.
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
    full_warc_cache_dir: Optional[Path] = None,
    full_warc_max_bytes: int = 5_000_000_000,
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
    )
    return res, "range", None


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _cache_path_for_range(cache_dir: Path, *, url: str, start: int, end_inclusive: int) -> Path:
    key = _sha256_hex(f"range:{url}|{int(start)}|{int(end_inclusive)}")
    return cache_dir / f"{key}.bin"


def _maybe_prune_cache(cache_dir: Path, *, max_cache_bytes: int) -> None:
    try:
        limit = int(max_cache_bytes)
    except Exception:
        return
    if limit <= 0:
        return

    try:
        paths = [p for p in cache_dir.glob("*.bin") if p.is_file()]
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
