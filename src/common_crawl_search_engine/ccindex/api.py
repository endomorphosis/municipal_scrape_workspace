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

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=4")
        rows = con.execute(
            """
            SELECT
                collection,
                shard_file,
                url,
                ts,
                status,
                mime,
                digest,
                warc_filename,
                warc_offset,
                warc_length
            FROM read_parquet(?)
            WHERE host_rev = ? OR host_rev LIKE ?
            LIMIT ?
            """,
            [str(parquet_path), host_rev_prefix, like_pat, int(limit)],
        ).fetchall()

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

    req = urllib.request.Request(url, method="GET")
    req.add_header("Range", f"bytes={start}-{end_inclusive}")

    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            status = int(getattr(resp, "status", 200))
            if status != 206:
                return WarcFetchResult(
                    ok=False,
                    status=status,
                    url=url,
                    bytes_requested=length,
                    bytes_returned=0,
                    sha256=None,
                    raw_base64=None,
                    decoded_text_preview=None,
                    error=f"expected 206 for range GET, got {status}",
                )

            data = resp.read()

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
            status=status,
            url=url,
            bytes_requested=length,
            bytes_returned=len(data) if data else 0,
            sha256=h,
            raw_base64=raw_b64,
            decoded_text_preview=preview,
            error=None,
        )

    except urllib.error.HTTPError as e:
        return WarcFetchResult(
            ok=False,
            status=int(getattr(e, "code", 0)) if getattr(e, "code", None) is not None else None,
            url=url,
            bytes_requested=length,
            bytes_returned=0,
            sha256=None,
            raw_base64=None,
            decoded_text_preview=None,
            error=f"HTTPError {getattr(e, 'code', '?')}",
        )
    except urllib.error.URLError as e:
        return WarcFetchResult(
            ok=False,
            status=None,
            url=url,
            bytes_requested=length,
            bytes_returned=0,
            sha256=None,
            raw_base64=None,
            decoded_text_preview=None,
            error=f"URLError {getattr(e, 'reason', e)}",
        )
    except Exception as e:
        return WarcFetchResult(
            ok=False,
            status=None,
            url=url,
            bytes_requested=length,
            bytes_returned=0,
            sha256=None,
            raw_base64=None,
            decoded_text_preview=None,
            error=f"{type(e).__name__}: {e}",
        )
