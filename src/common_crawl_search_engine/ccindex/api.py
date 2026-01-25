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
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


# Process-level memo: whether per-collection DBs tend to include a URL-level `cc_pointers`
# table (as opposed to domain-only `cc_domain_shards`). Probing this can be expensive.
_CC_POINTERS_FASTPATH_AVAILABLE: Optional[bool] = None


# Process-level memo: whether the per-collection domain pointer indexes exist.
_DOMAIN_POINTER_INDEX_AVAILABLE: Optional[bool] = None


def _domain_pointer_index_dir() -> Path:
    return Path((os.environ.get("CC_DOMAIN_POINTER_INDEX_DIR") or "/storage/ccindex_duckdb/cc_pointers_by_collection")).expanduser().resolve()


def _domain_pointer_parquet_root() -> Path:
    return Path((os.environ.get("CC_DOMAIN_POINTER_PARQUET_ROOT") or "/storage/ccindex_parquet/cc_pointers_by_collection")).expanduser().resolve()


def _rowgroup_slice_index_dir() -> Path:
    """Directory containing per-collection rowgroup slice index DBs.

    Expected layout:
      <dir>/<collection>.domain_rowgroups.duckdb
    """

    raw = (
        os.environ.get("CC_DOMAIN_ROWGROUP_INDEX_DIR")
        or os.environ.get("BRAVE_RESOLVE_ROWGROUP_INDEX_DIR")
        or "/storage/ccindex_duckdb/cc_domain_rowgroups_by_collection"
    )
    return Path(str(raw)).expanduser().resolve()


@lru_cache(maxsize=512)
def _rowgroup_index_db_for_collection(collection: str) -> Optional[Path]:
    """Best-effort locate the rowgroup slice index DB for a collection."""

    env_db = (os.environ.get("BRAVE_RESOLVE_ROWGROUP_INDEX_DB") or os.environ.get("CC_DOMAIN_ROWGROUP_INDEX_DB") or "").strip()
    if env_db:
        try:
            p = Path(env_db).expanduser().resolve()
            return p if p.exists() and p.is_file() else None
        except Exception:
            return None

    d = _rowgroup_slice_index_dir()
    try:
        if not d.exists():
            return None
    except Exception:
        return None

    for cand in [
        d / f"{collection}.domain_rowgroups.duckdb",
        d / f"{collection}.rowgroups.duckdb",
        d / f"{collection}.duckdb",
    ]:
        try:
            if cand.exists() and cand.is_file():
                return cand.resolve()
        except Exception:
            continue
    return None


@lru_cache(maxsize=8)
def _cc_pointers_duckdb_paths() -> List[Path]:
    """Return best-effort DuckDB paths that contain a URL-level `cc_pointers` table.

    These DBs can be used as a fast pre-check before scanning Parquet shards.

    Sources:
    - CCINDEX_CC_POINTERS_DB / CCINDEX_CC_POINTERS_DBS: comma-separated file/dir paths
    - Auto (best-effort): /storage/ccindex_duckdb/cc_pointers_dev.duckdb if present
    """

    raw = (
        os.environ.get("CCINDEX_CC_POINTERS_DBS")
        or os.environ.get("CCINDEX_CC_POINTERS_DB")
        or os.environ.get("CC_POINTERS_DB")
        or ""
    ).strip()

    parts: List[str] = []
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
    else:
        # Best-effort auto: this repo sometimes builds a single-collection URL pointer DB
        # as a development artifact; using it as a pre-check is safe (fall back to Parquet).
        dev = Path("/storage/ccindex_duckdb/cc_pointers_dev.duckdb")
        if dev.exists() and dev.is_file():
            parts = [str(dev)]

    out: List[Path] = []
    seen = set()
    for p in parts:
        try:
            pp = Path(p).expanduser().resolve()
        except Exception:
            continue

        if pp.is_dir():
            try:
                for child in sorted(pp.glob("*.duckdb")):
                    cps = str(child)
                    if cps not in seen and child.is_file():
                        seen.add(cps)
                        out.append(child)
            except Exception:
                continue
        else:
            cps = str(pp)
            if cps not in seen and pp.exists() and pp.is_file():
                seen.add(cps)
                out.append(pp)

    # Cap to keep accidental directory globs bounded.
    return out[:20]


@lru_cache(maxsize=16384)
def _collection_has_table(collection_db: str, table_name: str) -> bool:
    """Cache information_schema table existence checks per collection DB."""

    duckdb = _require_duckdb()
    con = duckdb.connect(str(collection_db), read_only=True)
    try:
        return bool(_duckdb_has_table(con, str(table_name)))
    finally:
        try:
            con.close()
        except Exception:
            pass


def _duckdb_threads() -> int:
    v = (os.environ.get("CCINDEX_DUCKDB_THREADS") or os.environ.get("DUCKDB_THREADS") or "").strip()
    if v.isdigit():
        return max(1, int(v))
    try:
        return max(1, int(os.cpu_count() or 4))
    except Exception:
        return 4


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
    resolve_stats: Dict[str, object] = field(default_factory=dict)


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


def _duckdb_table_columns(con: "object", table_name: str) -> set[str]:
    """Return lowercase column names for a table in the main schema (best-effort)."""

    try:
        rows = con.execute(
            """
            SELECT lower(column_name)
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [str(table_name)],
        ).fetchall()
        return {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        return set()


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


def parquet_relpaths_for_domain(
    collection_db: Path,
    host_rev_prefix: str,
    *,
    include_subdomains: bool = True,
) -> List[str]:
    duckdb = _require_duckdb()

    # Avoid an information_schema query for every call.
    if not _collection_has_table(str(collection_db), "cc_domain_shards"):
        return []

    like_pat = host_rev_prefix + ",%" if include_subdomains else None

    con = duckdb.connect(str(collection_db), read_only=True)
    try:
        if include_subdomains:
            rows = con.execute(
                """
                SELECT parquet_relpath
                FROM cc_domain_shards
                WHERE host_rev = ? OR host_rev LIKE ?
                """,
                [host_rev_prefix, like_pat],
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT parquet_relpath
                FROM cc_domain_shards
                WHERE host_rev = ?
                """,
                [host_rev_prefix],
            ).fetchall()
        # De-dupe in Python (often cheaper than DISTINCT+ORDER BY in DuckDB).
        out: List[str] = []
        seen = set()
        for r in rows:
            if not r or not r[0]:
                continue
            v = str(r[0])
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out
    finally:
        try:
            con.close()
        except Exception:
            pass


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
    stats_out: Optional[Dict[str, object]] = None,
    trace_events: bool = False,
) -> Dict[str, List[Dict[str, object]]]:
    """Resolve a list of URLs to candidate CCIndex WARC pointers.

    Strategy: group URLs by domain, then run `search_domain_via_meta_indexes()` for
    each domain in parallel and filter results down to the specific URLs.
    """

    want = [u for u in urls if (u or "").strip()]
    if not want:
        return {}

    # If we can't use meta-index info, we can't resolve to CCIndex pointers.
    if master_db is None:
        if stats_out is not None:
            stats_out["resolve_mode"] = "disabled_no_master_db"
            stats_out["collections_scanned"] = 0
            stats_out["parquet_files_scanned"] = 0
        return {u: [] for u in want}

    def _emit(evt: Dict[str, object]) -> None:
        if not trace_events:
            return
        try:
            state_dir = Path((os.environ.get("CCINDEX_STATE_DIR") or "state").strip() or "state")
            p = Path((os.environ.get("CCINDEX_EVENT_LOG_PATH") or str(state_dir / "ccindex_events.jsonl")).strip())
            p.parent.mkdir(parents=True, exist_ok=True)
            evt = dict(evt)
            evt.setdefault("ts", time.time())
            p.open("a", encoding="utf-8").write(json.dumps(evt, ensure_ascii=False) + "\n")
        except Exception:
            pass

    parquet_root = Path(parquet_root).expanduser().resolve()
    master_db = Path(master_db).expanduser().resolve()

    # Group URLs by domain.
    domain_to_urls: Dict[str, List[str]] = {}
    for u in want:
        dom = normalize_domain(u)
        if not dom:
            continue
        domain_to_urls.setdefault(dom, []).append(u)

    if not domain_to_urls:
        return {u: [] for u in want}

    strategy = (os.environ.get("BRAVE_RESOLVE_STRATEGY") or "meta_parallel").strip().lower()
    if strategy in {"url_join", "domain_url_join", "domain_url_join_parallel"}:
        resolve_strategy = "domain_url_join_parallel"
    else:
        resolve_strategy = "meta_parallel"

    # Parallel per-domain resolve.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    # Keep output stable even if we skip domains due to normalization errors.
    out: Dict[str, List[Dict[str, object]]] = {u: [] for u in want}

    domains = sorted(domain_to_urls.keys())
    if not domains:
        return out

    try:
        env_workers = (os.environ.get("BRAVE_RESOLVE_WORKERS") or "").strip()
        max_workers = int(env_workers) if env_workers else 0
    except Exception:
        max_workers = 0
    if max_workers <= 0:
        cpu = os.cpu_count() or 4
        max_workers = min(len(domains), max(2, cpu))
    max_workers = max(1, int(max_workers))

    collections_scanned_total = 0
    parquet_files_scanned_total = 0
    collections_considered_total = 0
    emitted_total = 0
    domain_errors = 0

    # Per-domain diagnostics (written once per domain by workers).
    _dom_lock = threading.Lock()
    _dom_details: Dict[str, Dict[str, object]] = {}

    # Aggregate stage timing (best-effort) across domains.
    setup_s_total = 0.0
    schema_s_total = 0.0
    query_s_total = 0.0
    filter_s_total = 0.0
    relpaths_s_total = 0.0
    relpaths_wall_s_total = 0.0
    domain_pointers_s_total = 0.0
    domain_pointers_wall_s_total = 0.0

    _emit(
        {
            "event": "resolve_urls_to_ccindex_start",
            "domains": len(domains),
            "urls": len(want),
            "workers": int(max_workers),
            "year": (str(year) if year else None),
        }
    )

    def _resolve_one_domain_meta(
        dom: str,
    ) -> tuple[str, Dict[str, List[Dict[str, object]]], int, int, float, Optional[str]]:
        dom_urls = domain_to_urls.get(dom) or []
        canon_to_requested: Dict[str, List[str]] = {}
        for u in dom_urls:
            if (u or "").strip():
                canon_to_requested.setdefault(_canonicalize_url_for_match(u), []).append(u)

        t_dom0 = time.perf_counter()
        t_search0 = time.perf_counter()
        _emit({"event": "resolve_domain_start", "domain": dom, "urls": len(dom_urls)})
        try:
            res = search_domain_via_meta_indexes(
                dom,
                parquet_root=parquet_root,
                master_db=master_db,
                year=year,
                max_matches=int(max_matches_per_domain),
            )
        except Exception as e:
            dt = time.perf_counter() - t_dom0
            _emit({"event": "resolve_domain_error", "domain": dom, "elapsed_s": float(dt), "error": str(e)})
            with _dom_lock:
                _dom_details[dom] = {
                    "domain": dom,
                    "mode": "meta_parallel",
                    "urls": int(len(dom_urls)),
                    "elapsed_s": float(dt),
                    "error": str(e),
                }
            return (dom, {}, 0, 0, float(dt), str(e))

        search_s = time.perf_counter() - t_search0

        t_filter0 = time.perf_counter()
        matches: Dict[str, List[Dict[str, object]]] = {}
        matched = 0
        for rec in res.records:
            rec_url = str(rec.get("url") or "")
            canon = _canonicalize_url_for_match(rec_url)
            reqs = canon_to_requested.get(canon)
            if not reqs:
                continue
            for requested in reqs:
                bucket = matches.setdefault(requested, [])
                if len(bucket) >= int(per_url_limit):
                    continue
                bucket.append(rec)
                matched += 1

            filter_s = time.perf_counter() - t_filter0

        dt = time.perf_counter() - t_dom0
        _emit(
            {
                "event": "resolve_domain_done",
                "domain": dom,
                "elapsed_s": float(dt),
                "records": int(res.emitted),
                "collections_considered": int(res.collections_considered),
                "matched": int(matched),
                "search_s": float(search_s),
                "filter_s": float(filter_s),
            }
        )

        with _dom_lock:
            _dom_details[dom] = {
                "domain": dom,
                "mode": "meta_parallel",
                "urls": int(len(dom_urls)),
                "elapsed_s": float(dt),
                "collections_considered": int(res.collections_considered),
                "records_emitted": int(res.emitted),
                "matched_records": int(matched),
                "search_s": float(search_s),
                "filter_s": float(filter_s),
            }
        return (dom, matches, int(res.collections_considered), int(res.emitted), float(dt), None)

    def _resolve_one_domain_url_join(
        dom: str,
    ) -> tuple[str, Dict[str, List[Dict[str, object]]], int, int, float, Optional[str]]:
        # We need DuckDB for Parquet reads.
        try:
            duckdb = _require_duckdb()
        except Exception as e:
            return (dom, {}, 0, 0, 0.0, str(e))

        # Load collections once per call (newest first tends to satisfy Brave URLs faster).
        collections = load_collections_from_master(master_db, year)
        if not collections:
            return (dom, {}, 0, 0, 0.0, None)
        collections = list(reversed(collections))

        dom_urls = domain_to_urls.get(dom) or []
        dom_urls = list(dict.fromkeys([u for u in dom_urls if (u or "").strip()]))
        if not dom_urls:
            return (dom, {}, 0, 0, 0.0, None)

        host_rev_prefix = host_to_rev(dom)
        if not host_rev_prefix:
            return (dom, {}, 0, 0, 0.0, "could_not_compute_host_rev")

        variant_rows: List[Tuple[str, str]] = []
        for requested in dom_urls:
            for v in _url_variants_for_lookup(requested):
                variant_rows.append((v, requested))

        if not variant_rows:
            return (dom, {}, 0, 0, 0.0, "no_url_variants")

        matches: Dict[str, List[Dict[str, object]]] = {u: [] for u in dom_urls}
        per_url_counts: Dict[str, int] = {u: 0 for u in dom_urls}

        collections_scanned = 0
        parquet_files_scanned = 0

        t_dom0 = time.perf_counter()
        setup_s = 0.0
        schema_s = 0.0
        query_s = 0.0
        cc_pointers_s = 0.0
        cc_pointers_calls = 0
        cc_pointers_rows = 0
        cc_pointers_check_s = 0.0
        relpaths_s = 0.0
        relpaths_wall_s = 0.0
        relpaths_calls = 0
        domain_pointers_s = 0.0
        domain_pointers_wall_s = 0.0
        domain_pointers_calls = 0
        domain_pointers_rows = 0
        batches = 0
        rows_returned = 0
        _emit({"event": "resolve_domain_start", "domain": dom, "urls": len(dom_urls)})

        t_setup0 = time.perf_counter()
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA threads={int(_duckdb_threads())}")
            # Helps reuse Parquet metadata across repeated reads within the process.
            try:
                con.execute("PRAGMA enable_object_cache=true")
            except Exception:
                pass

            # Avoid Python-level executemany overhead by building the small URL table via VALUES.
            values_sql = ",".join(["(?, ?)"] * len(variant_rows))
            params: List[object] = []
            for u, req in variant_rows:
                params.append(u)
                params.append(req)
            con.execute(
                f"CREATE TABLE search_urls AS SELECT * FROM (VALUES {values_sql}) AS t(url, requested_url)",
                params,
            )

            setup_s = time.perf_counter() - t_setup0

            def _parquet_columns(pq_path: Path) -> set[str]:
                rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(pq_path)]).fetchall()
                return {str(r[0]) for r in rows if r and r[0]}

            def col_or_null(cols: set[str], name: str) -> str:
                return f"p.{name} AS {name}" if name in cols else f"NULL AS {name}"

            def col_or_null_multi(cols: set[str], names: Sequence[str], *, alias: str) -> str:
                for n in names:
                    nn = str(n).lower()
                    if nn in cols:
                        return f"p.{nn} AS {alias}"
                return f"NULL AS {alias}"

            parquet_cols: Optional[set[str]] = None
            parquet_select_list: Optional[str] = None

            # union_by_name is safer across schema drift, but can be slower.
            # If your pointer Parquet schema is consistent, disabling it may reduce query time.
            union_by_name_env = (os.environ.get("BRAVE_RESOLVE_UNION_BY_NAME") or "1").strip().lower()
            union_by_name = union_by_name_env not in {"0", "false", "no", "off"}
            union_by_name_sql = "true" if union_by_name else "false"

            # Optional fast path: use a domain->rowgroup slice index (cc_domain_rowgroups)
            # to read only relevant rowgroups via PyArrow.
            rg_mode = (os.environ.get("BRAVE_RESOLVE_ROWGROUP_SLICE_MODE") or "off").strip().lower()
            if rg_mode not in {"auto", "on", "off"}:
                rg_mode = "off"

            # If enabled, try to use the per-collection rowgroup slice index DB.
            # In prod, these are expected under /storage/ccindex_duckdb/cc_domain_rowgroups_by_collection.

            def _resolve_with_rowgroup_slice_for_collection(
                *,
                coll: str,
                host_revs_for_domain: Sequence[str],
                variant_to_requested: Dict[str, List[str]],
                want_cols: Sequence[str],
            ) -> tuple[int, int]:
                """Try to satisfy URL matches for this collection using cc_domain_rowgroups.

                Returns: (parquet_files_opened, rowgroups_read)
                """

                nonlocal rows_returned
                nonlocal parquet_files_scanned
                nonlocal collections_scanned

                if not host_revs_for_domain:
                    return (0, 0)
                if not variant_to_requested:
                    return (0, 0)

                dbp = _rowgroup_index_db_for_collection(str(coll))
                if dbp is None:
                    return (0, 0)

                # Import lazily so this path has zero overhead unless enabled.
                try:
                    import pyarrow as pa  # type: ignore
                    import pyarrow.compute as pc  # type: ignore
                    import pyarrow.parquet as pq  # type: ignore
                except Exception:
                    return (0, 0)

                con_rg = duckdb.connect(str(dbp), read_only=True)
                try:
                    if not _duckdb_has_table(con_rg, "cc_domain_rowgroups"):
                        return (0, 0)

                    cols_rg = _duckdb_table_columns(con_rg, "cc_domain_rowgroups")

                    where_parts: List[str] = []
                    params: List[object] = []

                    if "collection" in cols_rg:
                        where_parts.append("collection = ?")
                        params.append(str(coll))

                    if "host_rev" not in cols_rg:
                        return (0, 0)

                    hr_placeholders = ",".join(["?"] * len(host_revs_for_domain))
                    where_parts.append(f"host_rev IN ({hr_placeholders})")
                    params.extend(list(host_revs_for_domain))

                    if "row_group" not in cols_rg or "dom_rg_row_start" not in cols_rg or "dom_rg_row_end" not in cols_rg:
                        return (0, 0)

                    if "source_path" not in cols_rg and "parquet_relpath" not in cols_rg:
                        return (0, 0)

                    where_sql = " AND ".join(where_parts) if where_parts else "1=1"
                    # Fetch both paths (when available) so we can fall back if one doesn't resolve.
                    sel_source = "source_path" if "source_path" in cols_rg else "NULL"
                    sel_rel = "parquet_relpath" if "parquet_relpath" in cols_rg else "NULL"

                    seg_rows = con_rg.execute(
                        f"""
                        SELECT {sel_source} AS source_path, {sel_rel} AS parquet_relpath,
                               row_group, dom_rg_row_start, dom_rg_row_end
                        FROM cc_domain_rowgroups
                        WHERE {where_sql}
                        ORDER BY source_path, parquet_relpath, row_group, dom_rg_row_start
                        """,
                        params,
                    ).fetchall()

                    if not seg_rows:
                        return (0, 0)

                finally:
                    try:
                        con_rg.close()
                    except Exception:
                        pass

                parquet_dir = get_collection_parquet_dir(parquet_root, coll)
                variant_urls = list(variant_to_requested.keys())
                if not variant_urls:
                    return (0, 0)
                variant_arr = pa.array(variant_urls)

                opened_files = 0
                rowgroups_read = 0
                counted_collection = False
                pf_cache: Dict[str, object] = {}
                schema_cols_cache: Dict[str, set[str]] = {}

                for source_path_raw, parquet_rel_raw, row_group, dom_rg_start, dom_rg_end in seg_rows:
                    if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                        break

                    pq_path: Optional[Path] = None
                    sp = str(source_path_raw or "").strip()
                    rel = str(parquet_rel_raw or "").strip()

                    if sp:
                        try:
                            cand = Path(sp).expanduser().resolve()
                            if cand.exists():
                                pq_path = cand
                        except Exception:
                            pq_path = None

                    if pq_path is None and rel:
                        try:
                            cand = (parquet_dir / rel).resolve()
                            if cand.exists():
                                pq_path = cand
                        except Exception:
                            pq_path = None

                    if pq_path is None:
                        continue

                    ps = str(pq_path)
                    pf = pf_cache.get(ps)
                    if pf is None:
                        try:
                            pf = pq.ParquetFile(ps)
                        except Exception:
                            continue
                        pf_cache[ps] = pf
                        opened_files += 1
                        parquet_files_scanned += 1

                        # Count the collection once when we successfully open the first shard.
                        if not counted_collection:
                            collections_scanned += 1
                            counted_collection = True

                        try:
                            schema_cols_cache[ps] = {str(n) for n in pf.schema.names}
                        except Exception:
                            schema_cols_cache[ps] = set()

                    avail = schema_cols_cache.get(ps) or set()
                    cols_to_read = [c for c in want_cols if c in avail]
                    if "url" not in cols_to_read:
                        continue

                    try:
                        rg = int(row_group)
                        s0 = int(dom_rg_start)
                        s1 = int(dom_rg_end)
                    except Exception:
                        continue
                    if s1 <= s0:
                        continue

                    try:
                        t_rg = pf.read_row_group(rg, columns=cols_to_read)
                        rowgroups_read += 1
                    except Exception:
                        continue

                    try:
                        t_rg = t_rg.slice(s0, s1 - s0)
                    except Exception:
                        continue

                    try:
                        url_col = t_rg.column(t_rg.schema.get_field_index("url"))
                        mask = pc.is_in(url_col, value_set=variant_arr)
                        t_hit = t_rg.filter(mask)
                    except Exception:
                        continue

                    if t_hit.num_rows <= 0:
                        continue

                    # Materialize only the URL column to drive mapping; other columns are pulled by index.
                    try:
                        hit_urls = t_hit.column(t_hit.schema.get_field_index("url")).to_pylist()
                    except Exception:
                        continue

                    # Derive shard_file from parquet filename.
                    try:
                        name = pq_path.name
                        suf = ".sorted.parquet"
                        shard_file = name[: -len(suf)] if name.endswith(suf) else name
                        shard_file = shard_file or None
                    except Exception:
                        shard_file = None

                    # Pull remaining columns once; if missing, treat as NULL.
                    def _col_pylist(colname: str) -> Optional[List[object]]:
                        if colname not in avail or colname not in {f.name for f in t_hit.schema}:
                            return None
                        try:
                            return t_hit.column(t_hit.schema.get_field_index(colname)).to_pylist()
                        except Exception:
                            return None

                    ts_list = _col_pylist("ts")
                    status_list = _col_pylist("status")
                    mime_list = _col_pylist("mime")
                    digest_list = _col_pylist("digest")
                    warc_fn_list = _col_pylist("warc_filename")
                    warc_off_list = _col_pylist("warc_offset")
                    warc_len_list = _col_pylist("warc_length")

                    for j, hit_url in enumerate(hit_urls):
                        if not hit_url:
                            continue
                        requested_list = variant_to_requested.get(str(hit_url))
                        if not requested_list:
                            continue

                        rec = {
                            "collection": str(coll),
                            "shard_file": shard_file,
                            "url": str(hit_url),
                            "timestamp": (ts_list[j] if ts_list is not None and j < len(ts_list) else None),
                            "status": (
                                int(status_list[j])
                                if status_list is not None and j < len(status_list) and status_list[j] is not None
                                else None
                            ),
                            "mime": (mime_list[j] if mime_list is not None and j < len(mime_list) else None),
                            "digest": (digest_list[j] if digest_list is not None and j < len(digest_list) else None),
                            "warc_filename": (
                                warc_fn_list[j] if warc_fn_list is not None and j < len(warc_fn_list) else None
                            ),
                            "warc_offset": (
                                int(warc_off_list[j])
                                if warc_off_list is not None and j < len(warc_off_list) and warc_off_list[j] is not None
                                else None
                            ),
                            "warc_length": (
                                int(warc_len_list[j])
                                if warc_len_list is not None and j < len(warc_len_list) and warc_len_list[j] is not None
                                else None
                            ),
                            "parquet_path": str(pq_path),
                        }

                        for requested_url in requested_list:
                            if requested_url not in matches:
                                continue
                            if per_url_counts.get(requested_url, 0) >= int(per_url_limit):
                                continue
                            matches[requested_url].append(rec)
                            per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1
                            rows_returned += 1

                return (opened_files, rowgroups_read)

            try:
                batch_sz = int((os.environ.get("BRAVE_RESOLVE_PARQUET_BATCH") or "16").strip() or "16")
            except Exception:
                batch_sz = 16
            batch_sz = max(1, min(64, int(batch_sz)))

            def _collection_sort_key(coll: str) -> tuple[int, int, str]:
                """Newest-first sort key for CC collections."""

                try:
                    m = re.match(r"^CC-MAIN-(\d{4})-(\d{2})$", str(coll))
                    if m:
                        return (int(m.group(1)), int(m.group(2)), str(coll))
                except Exception:
                    pass
                return (0, 0, str(coll))

            # Fast path (optional): if per-collection DBs include a URL-level pointer table
            # (`cc_pointers`), query it directly and avoid Parquet entirely.
            #
            # Many deployments only have domain-only DBs (`cc_domain_shards`). Opening dozens of
            # DB files just to discover `cc_pointers` doesn't exist can cost 10s+, so default to
            # an auto-probed mode.
            cc_mode = (os.environ.get("BRAVE_RESOLVE_CC_POINTERS_MODE") or "auto").strip().lower()
            if cc_mode not in {"auto", "on", "off"}:
                cc_mode = "auto"

            cc_pointers_global_dbs = _cc_pointers_duckdb_paths()

            # Fast pre-check: query any global `cc_pointers` DBs (if present) before scanning Parquet.
            # This is especially useful when you have a single-collection or per-year URL pointer DB.
            if cc_mode != "off" and cc_pointers_global_dbs:
                for db_path in cc_pointers_global_dbs:
                    if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                        break
                    try:
                        con_g = duckdb.connect(str(db_path), read_only=True)
                    except Exception:
                        continue

                    t_cc0 = time.perf_counter()
                    try:
                        if not _duckdb_has_table(con_g, "cc_pointers"):
                            continue
                        cols_p = _duckdb_table_columns(con_g, "cc_pointers")
                        cc_pointers_calls += 1

                        values_sql = ",".join(["(?, ?)"] * len(variant_rows))
                        params: List[object] = []
                        for (u, req) in variant_rows:
                            params.append(u)
                            params.append(req)

                        select_list = ", ".join(
                            [
                                col_or_null(cols_p, "collection"),
                                col_or_null(cols_p, "shard_file"),
                                col_or_null(cols_p, "url"),
                                col_or_null_multi(cols_p, ["ts", "timestamp"], alias="ts"),
                                col_or_null(cols_p, "status"),
                                col_or_null(cols_p, "mime"),
                                col_or_null(cols_p, "digest"),
                                col_or_null(cols_p, "warc_filename"),
                                col_or_null(cols_p, "warc_offset"),
                                col_or_null(cols_p, "warc_length"),
                            ]
                        )

                        rows = con_g.execute(
                            f"""
                            WITH search_urls(url, requested_url) AS (VALUES {values_sql})
                            SELECT s.requested_url, {select_list}
                            FROM cc_pointers p
                            INNER JOIN search_urls s ON p.url = s.url
                            """,
                            params,
                        ).fetchall()

                        cc_pointers_rows += int(len(rows))
                        for row in rows:
                            if not row:
                                continue
                            requested_url = str(row[0] or "")
                            if not requested_url or requested_url not in matches:
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
                                "collection": (collection if collection is not None else None),
                                "shard_file": shard_file,
                                "url": url,
                                "timestamp": ts,
                                "status": int(status) if status is not None else None,
                                "mime": mime,
                                "digest": digest,
                                "warc_filename": warc_filename,
                                "warc_offset": int(warc_offset) if warc_offset is not None else None,
                                "warc_length": int(warc_length) if warc_length is not None else None,
                                "parquet_path": "",
                            }
                            matches[requested_url].append(rec)
                            per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1
                    finally:
                        cc_pointers_s += float(time.perf_counter() - t_cc0)
                        try:
                            con_g.close()
                        except Exception:
                            pass

            global _CC_POINTERS_FASTPATH_AVAILABLE

            if cc_mode == "off":
                cc_pointers_enabled = False
            elif cc_mode == "on":
                cc_pointers_enabled = True
            else:
                # Only probe per-collection DBs when we *don't* already have a global URL pointer DB.
                # Probing can be expensive (opening many DB files).
                if cc_pointers_global_dbs:
                    cc_pointers_enabled = False
                elif _CC_POINTERS_FASTPATH_AVAILABLE is None:
                    # Probe a couple newest collections.
                    probed = 0
                    found = False
                    for cref in collections[:2]:
                        cdb = cref.collection_db_path
                        if not cdb.exists():
                            continue
                        probed += 1
                        t_chk0 = time.perf_counter()
                        try:
                            con_probe = duckdb.connect(str(cdb), read_only=True)
                            try:
                                if _duckdb_has_table(con_probe, "cc_pointers"):
                                    found = True
                                    break
                            finally:
                                try:
                                    con_probe.close()
                                except Exception:
                                    pass
                        finally:
                            cc_pointers_check_s += float(time.perf_counter() - t_chk0)
                    _CC_POINTERS_FASTPATH_AVAILABLE = bool(found) if probed > 0 else False

                cc_pointers_enabled = bool(_CC_POINTERS_FASTPATH_AVAILABLE)

            if cc_pointers_enabled:
                for cref in collections:
                    if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                        break

                    cdb = cref.collection_db_path
                    if not cdb.exists():
                        continue

                    t_chk0 = time.perf_counter()
                    con_c = duckdb.connect(str(cdb), read_only=True)
                    try:
                        has_tbl = _duckdb_has_table(con_c, "cc_pointers")
                    finally:
                        cc_pointers_check_s += float(time.perf_counter() - t_chk0)

                    if not has_tbl:
                        try:
                            con_c.close()
                        except Exception:
                            pass
                        continue

                    t_cc0 = time.perf_counter()
                    try:
                        cols_p = _duckdb_table_columns(con_c, "cc_pointers")
                        cc_pointers_calls += 1
                        collections_scanned += 1

                        # Use a VALUES CTE so we can keep the connection read-only.
                        values_sql = ",".join(["(?, ?)"] * len(variant_rows))
                        params: List[object] = []
                        for (u, req) in variant_rows:
                            params.append(u)
                            params.append(req)

                        select_list = ", ".join(
                            [
                                col_or_null(cols_p, "collection"),
                                col_or_null(cols_p, "shard_file"),
                                col_or_null(cols_p, "url"),
                                col_or_null_multi(cols_p, ["ts", "timestamp"], alias="ts"),
                                col_or_null(cols_p, "status"),
                                col_or_null(cols_p, "mime"),
                                col_or_null(cols_p, "digest"),
                                col_or_null(cols_p, "warc_filename"),
                                col_or_null(cols_p, "warc_offset"),
                                col_or_null(cols_p, "warc_length"),
                            ]
                        )

                        rows = con_c.execute(
                            f"""
                            WITH search_urls(url, requested_url) AS (VALUES {values_sql})
                            SELECT s.requested_url, {select_list}
                            FROM cc_pointers p
                            INNER JOIN search_urls s ON p.url = s.url
                            """,
                            params,
                        ).fetchall()

                        cc_pointers_rows += int(len(rows))
                        for row in rows:
                            if not row:
                                continue
                            requested_url = str(row[0] or "")
                            if not requested_url or requested_url not in matches:
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
                                "collection": (collection if collection is not None else str(cref.collection)),
                                "shard_file": shard_file,
                                "url": url,
                                "timestamp": ts,
                                "status": int(status) if status is not None else None,
                                "mime": mime,
                                "digest": digest,
                                "warc_filename": warc_filename,
                                "warc_offset": int(warc_offset) if warc_offset is not None else None,
                                "warc_length": int(warc_length) if warc_length is not None else None,
                                "parquet_path": "",
                            }
                            matches[requested_url].append(rec)
                            per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1
                    finally:
                        cc_pointers_s += float(time.perf_counter() - t_cc0)
                        try:
                            con_c.close()
                        except Exception:
                            pass

            # If `cc_pointers` satisfied all requested URLs, skip Parquet scans entirely.
            if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                matches = {u: recs for (u, recs) in matches.items() if recs}
                dt = time.perf_counter() - t_dom0
                _emit(
                    {
                        "event": "resolve_domain_done",
                        "domain": dom,
                        "elapsed_s": float(dt),
                        "collections_scanned": int(collections_scanned),
                        "parquet_files_scanned": int(parquet_files_scanned),
                        "matched_urls": int(len(matches)),
                        "setup_s": float(setup_s),
                        "schema_s": float(schema_s),
                        "query_s": float(query_s),
                        "cc_pointers_s": float(cc_pointers_s),
                        "cc_pointers_calls": int(cc_pointers_calls),
                        "cc_pointers_rows": int(cc_pointers_rows),
                        "cc_pointers_check_s": float(cc_pointers_check_s),
                        "relpaths_s": float(relpaths_s),
                        "relpaths_wall_s": float(relpaths_wall_s),
                        "relpaths_calls": int(relpaths_calls),
                        "domain_pointers_s": float(domain_pointers_s),
                        "domain_pointers_wall_s": float(domain_pointers_wall_s),
                        "domain_pointers_calls": int(domain_pointers_calls),
                        "domain_pointers_rows": int(domain_pointers_rows),
                        "batches": int(batches),
                        "rows_returned": int(rows_returned),
                    }
                )

                with _dom_lock:
                    _dom_details[dom] = {
                        "domain": dom,
                        "mode": "domain_url_join_parallel",
                        "urls": int(len(dom_urls)),
                        "elapsed_s": float(dt),
                        "collections_scanned": int(collections_scanned),
                        "parquet_files_scanned": int(parquet_files_scanned),
                        "matched_urls": int(len(matches)),
                        "setup_s": float(setup_s),
                        "schema_s": float(schema_s),
                        "query_s": float(query_s),
                        "cc_pointers_s": float(cc_pointers_s),
                        "cc_pointers_calls": int(cc_pointers_calls),
                        "cc_pointers_rows": int(cc_pointers_rows),
                        "cc_pointers_check_s": float(cc_pointers_check_s),
                        "relpaths_s": float(relpaths_s),
                        "relpaths_wall_s": float(relpaths_wall_s),
                        "relpaths_calls": int(relpaths_calls),
                        "domain_pointers_s": float(domain_pointers_s),
                        "domain_pointers_wall_s": float(domain_pointers_wall_s),
                        "domain_pointers_calls": int(domain_pointers_calls),
                        "domain_pointers_rows": int(domain_pointers_rows),
                        "batches": int(batches),
                        "rows_returned": int(rows_returned),
                    }
                return (dom, matches, int(collections_scanned), int(parquet_files_scanned), float(dt), None)

            # Shared worker setting for per-collection lookups.
            try:
                env_relw = (os.environ.get("BRAVE_RESOLVE_RELPATH_WORKERS") or "").strip()
                rel_workers = int(env_relw) if env_relw else 4
            except Exception:
                rel_workers = 4
            rel_workers = max(1, min(8, int(rel_workers)))

            # Optional fast path: use the prebuilt domain pointer indexes (domain -> parquet_file + row range)
            # to avoid per-collection cc_domain_shards relpath lookups.
            dp_mode = (os.environ.get("BRAVE_RESOLVE_DOMAIN_POINTERS_MODE") or "auto").strip().lower()
            if dp_mode not in {"auto", "on", "off"}:
                dp_mode = "auto"

            global _DOMAIN_POINTER_INDEX_AVAILABLE
            dp_enabled: bool
            if dp_mode == "off":
                dp_enabled = False
            elif dp_mode == "on":
                dp_enabled = True
            else:
                if _DOMAIN_POINTER_INDEX_AVAILABLE is None:
                    try:
                        idir = _domain_pointer_index_dir()
                        _DOMAIN_POINTER_INDEX_AVAILABLE = bool(idir.exists() and (idir / "master_index.duckdb").exists())
                    except Exception:
                        _DOMAIN_POINTER_INDEX_AVAILABLE = False
                dp_enabled = bool(_DOMAIN_POINTER_INDEX_AVAILABLE)

            # Build a small set of host_rev keys derived from the actual URLs we are resolving.
            # This is both tighter (avoids scanning every subdomain shard) and more correct than
            # only using the normalized domain.
            host_revs: List[str] = []
            try:
                host_revs_set = {host_to_rev(dom), host_to_rev("www." + dom)}
                for u in dom_urls:
                    uu = (u or "").strip()
                    if not uu:
                        continue
                    if not re.match(r"^https?://", uu, flags=re.IGNORECASE):
                        uu = "https://" + uu
                    try:
                        p = urllib.parse.urlsplit(uu)
                        h = (p.hostname or "").strip().lower()
                    except Exception:
                        h = ""
                    if not h:
                        continue
                    host_revs_set.add(host_to_rev(h))
                    if h.startswith("www."):
                        host_revs_set.add(host_to_rev(h[4:]))
                    else:
                        host_revs_set.add(host_to_rev("www." + h))
                host_revs = [r for r in sorted(host_revs_set) if r]
            except Exception:
                host_revs = [host_rev_prefix] if host_rev_prefix else []

            # If enabled, try the rowgroup-slice approach before any Parquet full scans.
            # This avoids DuckDB read_parquet filtering when we can directly read the relevant rowgroups.
            rg_enabled = rg_mode == "on" or (rg_mode == "auto" and (_rowgroup_slice_index_dir().exists() or bool(os.environ.get("BRAVE_RESOLVE_ROWGROUP_INDEX_DB") or "")))
            if rg_enabled and host_revs and not all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                try:
                    variant_to_requested: Dict[str, List[str]] = {}
                    for v, req in variant_rows:
                        variant_to_requested.setdefault(str(v), []).append(str(req))

                    want_cols = [
                        "url",
                        "ts",
                        "status",
                        "mime",
                        "digest",
                        "warc_filename",
                        "warc_offset",
                        "warc_length",
                    ]

                    # Scan collections newest-first; stop as soon as all URLs are satisfied.
                    for cref in collections:
                        if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                            break
                        _resolve_with_rowgroup_slice_for_collection(
                            coll=str(cref.collection),
                            host_revs_for_domain=host_revs,
                            variant_to_requested=variant_to_requested,
                            want_cols=want_cols,
                        )
                except Exception:
                    # Fall back silently to existing Parquet scan strategies.
                    pass

            def _lookup_domain_pointers(
                cref: CollectionRef,
            ) -> tuple[str, List[Path], float, int]:
                """Return [parquet_path] candidates for this collection.

                Uses cc_domain_shards(host_rev -> source_path/parquet_relpath) when present.
                """

                t0 = time.perf_counter()
                idx_db = Path(cref.collection_db_path)
                if not idx_db.exists():
                    return (str(cref.collection), [], float(time.perf_counter() - t0), 0)

                con_i = duckdb.connect(str(idx_db), read_only=True)
                try:
                    if not host_revs:
                        return (str(cref.collection), [], float(time.perf_counter() - t0), 0)

                    if not _duckdb_has_table(con_i, "cc_domain_shards"):
                        return (str(cref.collection), [], float(time.perf_counter() - t0), 0)

                    placeholders = ",".join(["?"] * len(host_revs))
                    rows = con_i.execute(
                        f"""
                        SELECT source_path, parquet_relpath
                        FROM cc_domain_shards
                        WHERE host_rev IN ({placeholders})
                        """,
                        host_revs,
                    ).fetchall()

                    pq_paths: List[Path] = []
                    seen = set()
                    parquet_dir: Optional[Path] = None
                    for sp, rel in rows:
                        sp_s = str(sp or "").strip()
                        rel_s = str(rel or "").strip()
                        p: Optional[Path] = None
                        if sp_s:
                            p = Path(sp_s)
                        elif rel_s:
                            if parquet_dir is None:
                                parquet_dir = get_collection_parquet_dir(parquet_root, cref.collection)
                            p = (parquet_dir / rel_s).resolve()
                        if p is None:
                            continue
                        ps = str(p)
                        if not ps or ps in seen:
                            continue
                        seen.add(ps)
                        if p.exists():
                            pq_paths.append(p)

                    return (str(cref.collection), pq_paths, float(time.perf_counter() - t0), int(len(rows)))
                finally:
                    try:
                        con_i.close()
                    except Exception:
                        pass

            if dp_enabled:
                did_dp_scan = False
                try:
                    env_dpb = (os.environ.get("BRAVE_RESOLVE_DOMAIN_POINTERS_BATCH") or "").strip()
                    dp_batch = int(env_dpb) if env_dpb else (int(rel_workers) * 2)
                except Exception:
                    dp_batch = int(rel_workers) * 2
                dp_batch = max(1, min(64, int(dp_batch)))

                from concurrent.futures import ThreadPoolExecutor as _TPE2, as_completed as _as_completed2

                # Prefer global per-year domain indexes when available.
                # These avoid opening many per-collection DBs just to discover shard Parquet files.
                covered_years: set[str] = set()
                global_coll_to_pq: Dict[str, List[Path]] = {}
                if host_revs:
                    try:
                        by_year_dir: Optional[Path] = None
                        for cand in [
                            Path("/storage/ccindex_duckdb/cc_domain_by_year_sorted"),
                            Path("/storage/ccindex_duckdb/cc_domain_by_year"),
                        ]:
                            if cand.exists():
                                by_year_dir = cand
                                break

                        if by_year_dir is not None:
                            years = sorted({str(c.year) for c in collections if getattr(c, "year", None)}, reverse=True)
                            for y in years:
                                dbp = (by_year_dir / f"cc_pointers_{y}.duckdb").resolve()
                                if not dbp.exists():
                                    continue
                                covered_years.add(str(y))

                                t_wall0 = time.perf_counter()
                                t0 = time.perf_counter()
                                con_y = duckdb.connect(str(dbp), read_only=True)
                                try:
                                    if not _duckdb_has_table(con_y, "cc_domain_shards"):
                                        continue
                                    placeholders = ",".join(["?"] * len(host_revs))
                                    rows = con_y.execute(
                                        f"""
                                        SELECT source_path, collection, parquet_relpath
                                        FROM cc_domain_shards
                                        WHERE host_rev IN ({placeholders})
                                        """,
                                        host_revs,
                                    ).fetchall()
                                finally:
                                    try:
                                        con_y.close()
                                    except Exception:
                                        pass

                                dt = float(time.perf_counter() - t0)
                                domain_pointers_calls += 1
                                domain_pointers_s += dt
                                domain_pointers_rows += int(len(rows) if rows else 0)
                                domain_pointers_wall_s += float(time.perf_counter() - t_wall0)

                                if not rows:
                                    continue

                                for sp, coll, rel in rows:
                                    coll_s = str(coll or "").strip()
                                    if not coll_s:
                                        continue
                                    sp_s = str(sp or "").strip()
                                    rel_s = str(rel or "").strip()
                                    pth: Optional[Path] = None
                                    if sp_s:
                                        pth = Path(sp_s)
                                    elif rel_s:
                                        pdir = get_collection_parquet_dir(parquet_root, coll_s)
                                        pth = (pdir / rel_s).resolve()
                                    if pth is None or not pth.exists():
                                        continue
                                    global_coll_to_pq.setdefault(coll_s, []).append(pth)
                    except Exception:
                        pass

                # If we found any Parquet shards via global-by-year lookup, scan them now.
                if global_coll_to_pq:
                    did_dp_scan = True

                    # De-dupe within each collection.
                    for _c, _paths in list(global_coll_to_pq.items()):
                        seen = set()
                        uniq: List[Path] = []
                        for pth in _paths:
                            ps = str(pth)
                            if ps and ps not in seen:
                                seen.add(ps)
                                uniq.append(pth)
                        global_coll_to_pq[_c] = uniq

                    # Process newest collections first to maximize the chance we can stop early.
                    ordered_colls = sorted(global_coll_to_pq.keys(), key=_collection_sort_key, reverse=True)

                    batch_files: List[Path] = []
                    file_to_coll: Dict[str, str] = {}
                    for _c in ordered_colls:
                        _paths = global_coll_to_pq.get(_c) or []
                        for pth in _paths:
                            batch_files.append(pth)
                            ps = str(pth)
                            if ps and ps not in file_to_coll:
                                file_to_coll[ps] = str(_c)

                    if batch_files:
                        if parquet_cols is None:
                            t_schema0 = time.perf_counter()
                            parquet_cols = _parquet_columns(batch_files[0])
                            schema_s += time.perf_counter() - t_schema0
                            parquet_select_list = ", ".join(
                                [
                                    "p.filename AS parquet_path",
                                    "pf.collection AS collection",
                                    "NULL AS shard_file",
                                    col_or_null(parquet_cols, "url"),
                                    col_or_null(parquet_cols, "ts"),
                                    col_or_null(parquet_cols, "status"),
                                    col_or_null(parquet_cols, "mime"),
                                    col_or_null(parquet_cols, "digest"),
                                    col_or_null(parquet_cols, "warc_filename"),
                                    col_or_null(parquet_cols, "warc_offset"),
                                    col_or_null(parquet_cols, "warc_length"),
                                ]
                            )

                        select_list = parquet_select_list or "p.filename AS parquet_path"
                        for i in range(0, len(batch_files), batch_sz):
                            if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                                break

                            batch = batch_files[i : i + batch_sz]
                            parquet_files_scanned += len(batch)
                            collections_scanned += len(global_coll_to_pq)
                            batches += 1

                            map_rows = [(str(p), file_to_coll.get(str(p), "")) for p in batch]
                            values_sql = ",".join(["(?, ?)"] * len(map_rows))
                            map_params: List[object] = []
                            for fp, coll in map_rows:
                                map_params.append(fp)
                                map_params.append(coll)

                            placeholders = ",".join(["?"] * len(batch))
                            t_q0 = time.perf_counter()
                            try:
                                rows = con.execute(
                                    f"""
                                    WITH parquet_files(parquet_path, collection) AS (VALUES {values_sql})
                                    SELECT s.requested_url, {select_list}
                                    FROM read_parquet([{placeholders}], filename=true, union_by_name={union_by_name_sql}) p
                                    INNER JOIN parquet_files pf ON pf.parquet_path = p.filename
                                    INNER JOIN search_urls s ON p.url = s.url
                                    """,
                                    (map_params + [str(p) for p in batch]),
                                ).fetchall()
                            finally:
                                query_s += float(time.perf_counter() - t_q0)
                            rows_returned += int(len(rows))

                            if not rows:
                                continue

                            for row in rows:
                                if not row:
                                    continue
                                requested_url = str(row[0] or "")
                                if not requested_url or requested_url not in matches:
                                    continue
                                if per_url_counts.get(requested_url, 0) >= int(per_url_limit):
                                    continue

                                (
                                    _requested,
                                    parquet_path,
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

                                if shard_file in (None, ""):
                                    try:
                                        pp = str(parquet_path or "")
                                        name = Path(pp).name
                                        suf = ".sorted.parquet"
                                        shard_file = name[: -len(suf)] if name.endswith(suf) else name
                                        shard_file = shard_file or None
                                    except Exception:
                                        shard_file = None

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
                                    "parquet_path": str(parquet_path or ""),
                                }
                                matches[requested_url].append(rec)
                                per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1

                # Only use per-collection domain pointers for years without a global-by-year index.
                collections_remaining = [c for c in collections if str(getattr(c, "year", "")) not in covered_years]

                # Scan newest collections first, but interleave index lookup with scanning so we can stop early.
                for start in range(0, len(collections_remaining), int(dp_batch)):
                    if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                        break

                    batch_cols = collections_remaining[start : start + int(dp_batch)]
                    t_dpwall0 = time.perf_counter()
                    coll_to_pq: Dict[str, List[Path]] = {}

                    try:
                        with _TPE2(max_workers=min(int(rel_workers), max(1, len(batch_cols)))) as dex:
                            futs = [dex.submit(_lookup_domain_pointers, cref) for cref in batch_cols]
                            for fut in _as_completed2(futs):
                                try:
                                    coll, pq_paths, dt, rows_n = fut.result()
                                except Exception:
                                    continue
                                domain_pointers_calls += 1
                                domain_pointers_s += float(dt)
                                domain_pointers_rows += int(rows_n)
                                if pq_paths:
                                    coll_to_pq[coll] = pq_paths
                    finally:
                        domain_pointers_wall_s += float(time.perf_counter() - t_dpwall0)

                    if not coll_to_pq:
                        continue

                    did_dp_scan = True

                    # Preserve newest-first ordering based on the batch_cols sequence.
                    ordered_colls = [
                        str(c.collection) for c in batch_cols if str(getattr(c, "collection", "")) in coll_to_pq
                    ]
                    batch_files: List[Path] = []
                    for _coll in ordered_colls:
                        paths = coll_to_pq.get(_coll) or []
                        if paths:
                            batch_files.extend(paths)

                    file_to_coll: Dict[str, str] = {}
                    for _coll, paths in coll_to_pq.items():
                        for pth in paths:
                            ps = str(pth)
                            if ps and ps not in file_to_coll:
                                file_to_coll[ps] = str(_coll)

                    if not batch_files:
                        continue

                    if parquet_cols is None:
                        t_schema0 = time.perf_counter()
                        parquet_cols = _parquet_columns(batch_files[0])
                        schema_s += time.perf_counter() - t_schema0
                        parquet_select_list = ", ".join(
                            [
                                "p.filename AS parquet_path",
                                "pf.collection AS collection",
                                "NULL AS shard_file",
                                col_or_null(parquet_cols, "url"),
                                col_or_null(parquet_cols, "ts"),
                                col_or_null(parquet_cols, "status"),
                                col_or_null(parquet_cols, "mime"),
                                col_or_null(parquet_cols, "digest"),
                                col_or_null(parquet_cols, "warc_filename"),
                                col_or_null(parquet_cols, "warc_offset"),
                                col_or_null(parquet_cols, "warc_length"),
                            ]
                        )

                    select_list = parquet_select_list or "p.filename AS parquet_path"
                    for i in range(0, len(batch_files), batch_sz):
                        if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                            break

                        batch = batch_files[i : i + batch_sz]
                        parquet_files_scanned += len(batch)
                        collections_scanned += len(coll_to_pq)
                        batches += 1

                        # Attach collection from the known file->collection mapping.
                        map_rows = [(str(p), file_to_coll.get(str(p), "")) for p in batch]
                        values_sql = ",".join(["(?, ?)"] * len(map_rows))
                        map_params: List[object] = []
                        for fp, coll in map_rows:
                            map_params.append(fp)
                            map_params.append(coll)

                        placeholders = ",".join(["?"] * len(batch))
                        t_q0 = time.perf_counter()
                        try:
                            rows = con.execute(
                                f"""
                                WITH parquet_files(parquet_path, collection) AS (VALUES {values_sql})
                                SELECT s.requested_url, {select_list}
                                FROM read_parquet([{placeholders}], filename=true, union_by_name={union_by_name_sql}) p
                                INNER JOIN parquet_files pf ON pf.parquet_path = p.filename
                                INNER JOIN search_urls s ON p.url = s.url
                                """,
                                (map_params + [str(p) for p in batch]),
                            ).fetchall()
                        finally:
                            query_s += float(time.perf_counter() - t_q0)
                        rows_returned += int(len(rows))

                        if not rows:
                            continue

                        for row in rows:
                            if not row:
                                continue
                            requested_url = str(row[0] or "")
                            if not requested_url or requested_url not in matches:
                                continue
                            if per_url_counts.get(requested_url, 0) >= int(per_url_limit):
                                continue

                            (
                                _requested,
                                parquet_path,
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

                            if shard_file in (None, ""):
                                try:
                                    pp = str(parquet_path or "")
                                    name = Path(pp).name
                                    suf = ".sorted.parquet"
                                    shard_file = name[: -len(suf)] if name.endswith(suf) else name
                                    shard_file = shard_file or None
                                except Exception:
                                    shard_file = None

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
                                "parquet_path": str(parquet_path or ""),
                            }
                            matches[requested_url].append(rec)
                            per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1

                # If the domain pointer index path satisfied all requested URLs, skip relpaths+shards.
                if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                    # Continue to finalization path (emit stats) without doing relpath lookups.
                    pass
                else:
                    # If we successfully scanned Parquet shards discovered via cc_domain_shards,
                    # do not also do relpath discovery (which is an alternate path to the same shards).
                    # This avoids doing 40+ per-collection DB lookups for URLs that may not exist.
                    if not did_dp_scan:
                        dp_enabled = False

            # The per-collection cc_domain_shards lookup (parquet_relpaths_for_domain) is often the
            # dominant cost, so only do it if we still need more matches.
            candidates: List[tuple[CollectionRef, Path]] = []
            if (not dp_enabled) and (not all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls)):
                for cref in collections:
                    cdb = cref.collection_db_path
                    if not cdb.exists():
                        continue
                    parquet_dir = get_collection_parquet_dir(parquet_root, cref.collection)
                    if not parquet_dir.exists():
                        continue
                    candidates.append((cref, parquet_dir))

            def _lookup_one(cref: CollectionRef, parquet_dir: Path) -> tuple[str, List[Path], float]:
                t0 = time.perf_counter()
                relpaths = parquet_relpaths_for_domain(
                    cref.collection_db_path,
                    host_rev_prefix,
                    include_subdomains=False,
                )
                dt = time.perf_counter() - t0
                pq_paths: List[Path] = []
                if relpaths:
                    for rel in relpaths:
                        pq = (parquet_dir / rel).resolve()
                        if pq.exists():
                            pq_paths.append(pq)
                return (str(cref.collection), pq_paths, float(dt))

            from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _as_completed

            # NOTE: relpath lookup can dominate. We do it in parallel, but *in ordered batches*
            # interleaved with Parquet scanning so we can stop early once URLs are satisfied.
            try:
                env_relb = (os.environ.get("BRAVE_RESOLVE_RELPATH_BATCH") or "").strip()
                rel_batch = int(env_relb) if env_relb else (int(rel_workers) * 2)
            except Exception:
                rel_batch = int(rel_workers) * 2
            rel_batch = max(1, min(64, int(rel_batch)))

            for start in range(0, len(candidates), int(rel_batch)):
                if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                    break

                batch_cands = candidates[start : start + int(rel_batch)]
                coll_to_pq: Dict[str, List[Path]] = {}

                t_relwall0 = time.perf_counter()
                try:
                    with _TPE(max_workers=min(int(rel_workers), max(1, len(batch_cands)))) as rex:
                        futs = [rex.submit(_lookup_one, cref, pdir) for (cref, pdir) in batch_cands]
                        for fut in _as_completed(futs):
                            try:
                                coll, pq_paths, dt = fut.result()
                            except Exception:
                                continue
                            relpaths_calls += 1
                            relpaths_s += float(dt)
                            if pq_paths:
                                coll_to_pq[coll] = pq_paths
                finally:
                    relpaths_wall_s += float(time.perf_counter() - t_relwall0)

                # Flatten parquet paths for this batch and scan them in larger chunks.
                # This avoids running 30-50 separate DuckDB queries when each collection
                # only yields 1 shard file.
                ordered_colls = [str(cref.collection) for (cref, _pdir) in batch_cands]
                batch_files: List[Path] = []
                for coll in ordered_colls:
                    paths = coll_to_pq.get(str(coll)) or []
                    if paths:
                        collections_scanned += 1
                        batch_files.extend(paths)

                file_to_coll: Dict[str, str] = {}
                for coll, paths in coll_to_pq.items():
                    for pth in paths:
                        ps = str(pth)
                        if ps and ps not in file_to_coll:
                            file_to_coll[ps] = str(coll)

                if not batch_files:
                    continue

                if parquet_cols is None:
                    t_schema0 = time.perf_counter()
                    parquet_cols = _parquet_columns(batch_files[0])
                    schema_s += time.perf_counter() - t_schema0
                    parquet_select_list = ", ".join(
                        [
                            "p.filename AS parquet_path",
                            "pf.collection AS collection",
                            "NULL AS shard_file",
                            col_or_null(parquet_cols, "url"),
                            col_or_null(parquet_cols, "ts"),
                            col_or_null(parquet_cols, "status"),
                            col_or_null(parquet_cols, "mime"),
                            col_or_null(parquet_cols, "digest"),
                            col_or_null(parquet_cols, "warc_filename"),
                            col_or_null(parquet_cols, "warc_offset"),
                            col_or_null(parquet_cols, "warc_length"),
                        ]
                    )

                select_list = parquet_select_list or "p.filename AS parquet_path"
                for i in range(0, len(batch_files), batch_sz):
                    if all(per_url_counts.get(u, 0) >= int(per_url_limit) for u in dom_urls):
                        break

                    batch = batch_files[i : i + batch_sz]
                    parquet_files_scanned += len(batch)

                    t_q0 = time.perf_counter()
                    batches += 1

                    # Attach collection from the known file->collection mapping.
                    map_rows = [(str(p), file_to_coll.get(str(p), "")) for p in batch]
                    values_sql = ",".join(["(?, ?)"] * len(map_rows))
                    map_params: List[object] = []
                    for fp, coll in map_rows:
                        map_params.append(fp)
                        map_params.append(coll)

                    placeholders = ",".join(["?"] * len(batch))
                    rows = con.execute(
                        f"""
                        WITH parquet_files(parquet_path, collection) AS (VALUES {values_sql})
                        SELECT s.requested_url, {select_list}
                        FROM read_parquet([{placeholders}], filename=true, union_by_name={union_by_name_sql}) p
                        INNER JOIN parquet_files pf ON pf.parquet_path = p.filename
                        INNER JOIN search_urls s ON p.url = s.url
                        """,
                        (map_params + [str(p) for p in batch]),
                    ).fetchall()

                    q_dt = time.perf_counter() - t_q0
                    query_s += q_dt
                    rows_returned += len(rows)

                    for row in rows:
                        if not row:
                            continue
                        requested_url = str(row[0] or "")
                        if not requested_url or requested_url not in matches:
                            continue
                        if per_url_counts.get(requested_url, 0) >= int(per_url_limit):
                            continue

                        (
                            _requested,
                            parquet_path,
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
                        if shard_file in (None, ""):
                            try:
                                pp = str(parquet_path or "")
                                name = Path(pp).name
                                suf = ".sorted.parquet"
                                shard_file = name[: -len(suf)] if name.endswith(suf) else name
                                shard_file = shard_file or None
                            except Exception:
                                shard_file = None
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
                            "parquet_path": str(parquet_path or ""),
                        }
                        matches[requested_url].append(rec)
                        per_url_counts[requested_url] = per_url_counts.get(requested_url, 0) + 1
        except Exception as e:
            dt = time.perf_counter() - t_dom0
            _emit({"event": "resolve_domain_error", "domain": dom, "elapsed_s": float(dt), "error": str(e)})
            with _dom_lock:
                _dom_details[dom] = {
                    "domain": dom,
                    "mode": "domain_url_join_parallel",
                    "urls": int(len(dom_urls)),
                    "elapsed_s": float(dt),
                    "collections_scanned": int(collections_scanned),
                    "parquet_files_scanned": int(parquet_files_scanned),
                    "setup_s": float(setup_s),
                    "schema_s": float(schema_s),
                    "query_s": float(query_s),
                    "cc_pointers_s": float(cc_pointers_s),
                    "cc_pointers_calls": int(cc_pointers_calls),
                    "cc_pointers_rows": int(cc_pointers_rows),
                    "cc_pointers_check_s": float(cc_pointers_check_s),
                    "relpaths_s": float(relpaths_s),
                    "relpaths_wall_s": float(relpaths_wall_s),
                    "relpaths_calls": int(relpaths_calls),
                    "domain_pointers_s": float(domain_pointers_s),
                    "domain_pointers_wall_s": float(domain_pointers_wall_s),
                    "domain_pointers_calls": int(domain_pointers_calls),
                    "domain_pointers_rows": int(domain_pointers_rows),
                    "batches": int(batches),
                    "rows_returned": int(rows_returned),
                    "error": str(e),
                }
            return (dom, {}, int(collections_scanned), int(parquet_files_scanned), float(dt), str(e))
        finally:
            try:
                con.close()
            except Exception:
                pass

        matches = {u: recs for (u, recs) in matches.items() if recs}
        dt = time.perf_counter() - t_dom0
        _emit(
            {
                "event": "resolve_domain_done",
                "domain": dom,
                "elapsed_s": float(dt),
                "collections_scanned": int(collections_scanned),
                "parquet_files_scanned": int(parquet_files_scanned),
                "matched_urls": int(len(matches)),
                "setup_s": float(setup_s),
                "schema_s": float(schema_s),
                "query_s": float(query_s),
                "cc_pointers_s": float(cc_pointers_s),
                "cc_pointers_calls": int(cc_pointers_calls),
                "cc_pointers_rows": int(cc_pointers_rows),
                "cc_pointers_check_s": float(cc_pointers_check_s),
                "relpaths_s": float(relpaths_s),
                "relpaths_wall_s": float(relpaths_wall_s),
                "relpaths_calls": int(relpaths_calls),
                "domain_pointers_s": float(domain_pointers_s),
                "domain_pointers_wall_s": float(domain_pointers_wall_s),
                "domain_pointers_calls": int(domain_pointers_calls),
                "domain_pointers_rows": int(domain_pointers_rows),
                "batches": int(batches),
                "rows_returned": int(rows_returned),
            }
        )

        with _dom_lock:
            _dom_details[dom] = {
                "domain": dom,
                "mode": "domain_url_join_parallel",
                "urls": int(len(dom_urls)),
                "elapsed_s": float(dt),
                "collections_scanned": int(collections_scanned),
                "parquet_files_scanned": int(parquet_files_scanned),
                "matched_urls": int(len(matches)),
                "setup_s": float(setup_s),
                "schema_s": float(schema_s),
                "query_s": float(query_s),
                "cc_pointers_s": float(cc_pointers_s),
                "cc_pointers_calls": int(cc_pointers_calls),
                "cc_pointers_rows": int(cc_pointers_rows),
                "cc_pointers_check_s": float(cc_pointers_check_s),
                "relpaths_s": float(relpaths_s),
                "relpaths_wall_s": float(relpaths_wall_s),
                "relpaths_calls": int(relpaths_calls),
                "domain_pointers_s": float(domain_pointers_s),
                "domain_pointers_wall_s": float(domain_pointers_wall_s),
                "domain_pointers_calls": int(domain_pointers_calls),
                "domain_pointers_rows": int(domain_pointers_rows),
                "batches": int(batches),
                "rows_returned": int(rows_returned),
            }
        return (dom, matches, int(collections_scanned), int(parquet_files_scanned), float(dt), None)

    _resolve_one_domain = (
        _resolve_one_domain_url_join if resolve_strategy == "domain_url_join_parallel" else _resolve_one_domain_meta
    )

    with ThreadPoolExecutor(max_workers=int(max_workers)) as ex:
        futs = {ex.submit(_resolve_one_domain, dom): dom for dom in domains}
        for fut in as_completed(futs):
            dom = futs.get(fut) or ""
            try:
                _dom, matches, considered, emitted, _elapsed, err = fut.result()
            except Exception as e:
                domain_errors += 1
                _emit({"event": "resolve_domain_error", "domain": dom, "error": str(e)})
                continue

            if err is not None:
                domain_errors += 1

            if resolve_strategy == "domain_url_join_parallel":
                collections_scanned_total += int(considered)
                parquet_files_scanned_total += int(emitted)
            else:
                collections_considered_total += int(considered)
                emitted_total += int(emitted)
            for requested, recs in matches.items():
                if requested in out and recs:
                    out[requested] = recs

    # Compute aggregated timing and top slow domains.
    try:
        details = list(_dom_details.values())
        details_sorted = sorted(
            [d for d in details if isinstance(d, dict)],
            key=lambda d: float(d.get("elapsed_s") or 0.0),
            reverse=True,
        )

        for d in details_sorted:
            if str(d.get("mode") or "") == "domain_url_join_parallel":
                setup_s_total += float(d.get("setup_s") or 0.0)
                schema_s_total += float(d.get("schema_s") or 0.0)
                query_s_total += float(d.get("query_s") or 0.0)
                relpaths_s_total += float(d.get("relpaths_s") or 0.0)
                relpaths_wall_s_total += float(d.get("relpaths_wall_s") or 0.0)
                domain_pointers_s_total += float(d.get("domain_pointers_s") or 0.0)
                domain_pointers_wall_s_total += float(d.get("domain_pointers_wall_s") or 0.0)
            else:
                filter_s_total += float(d.get("filter_s") or 0.0)
    except Exception:
        details_sorted = []

    if stats_out is not None:
        stats_out["resolve_mode"] = str(resolve_strategy)
        if resolve_strategy == "domain_url_join_parallel":
            stats_out["collections_scanned"] = int(collections_scanned_total)
            stats_out["parquet_files_scanned"] = int(parquet_files_scanned_total)
        else:
            stats_out["collections_scanned"] = 0
            stats_out["parquet_files_scanned"] = 0
            stats_out["collections_considered_total"] = int(collections_considered_total)
            stats_out["records_emitted_total"] = int(emitted_total)
        stats_out["resolve_workers"] = int(max_workers)
        stats_out["domain_errors"] = int(domain_errors)
        # Diagnostics to help pinpoint what dominates resolve time.
        stats_out["domains_top"] = details_sorted[:10]
        stats_out["timing_sums"] = {
            "setup_s_total": float(setup_s_total),
            "schema_s_total": float(schema_s_total),
            "query_s_total": float(query_s_total),
            "cc_pointers_s_total": float(
                sum(float(d.get("cc_pointers_s") or 0.0) for d in details_sorted if isinstance(d, dict))
            ),
            "cc_pointers_check_s_total": float(
                sum(float(d.get("cc_pointers_check_s") or 0.0) for d in details_sorted if isinstance(d, dict))
            ),
            "filter_s_total": float(filter_s_total),
            "relpaths_s_total": float(relpaths_s_total),
            "relpaths_wall_s_total": float(relpaths_wall_s_total),
            "domain_pointers_s_total": float(domain_pointers_s_total),
            "domain_pointers_wall_s_total": float(domain_pointers_wall_s_total),
        }

    _emit(
        {
            "event": "resolve_urls_to_ccindex_done",
            "domains": len(domains),
            "urls": len(want),
            "workers": int(max_workers),
            "domain_errors": int(domain_errors),
        }
    )
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
                            resolve_stats=(
                                payload.get("resolve_stats")
                                if isinstance(payload.get("resolve_stats"), dict)
                                else {}
                            ),
                        )
        except Exception:
            pass
    # Prefer the meta-returning variant so callers can render real pagination.
    from common_crawl_search_engine.ccsearch.brave_search import brave_web_search_page

    trace = (os.environ.get("CCINDEX_BRAVE_TRACE") or "").strip().lower() in {"1", "true", "yes", "on"}
    trace = trace or bool((os.environ.get("CCINDEX_EVENT_LOG_PATH") or "").strip())

    if trace:
        # Uses the same event log as the resolve stage.
        try:
            state_dir = Path((os.environ.get("CCINDEX_STATE_DIR") or "state").strip() or "state")
            p = Path((os.environ.get("CCINDEX_EVENT_LOG_PATH") or str(state_dir / "ccindex_events.jsonl")).strip())
            p.parent.mkdir(parents=True, exist_ok=True)
            p.open("a", encoding="utf-8").write(
                json.dumps(
                    {
                        "event": "brave_search_ccindex_start",
                        "ts": time.time(),
                        "query": str(query),
                        "count": int(count),
                        "offset": int(offset),
                        "year": (str(year) if year else None),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        except Exception:
            pass

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
    resolve_stats: Dict[str, object] = {}
    resolved = resolve_urls_to_ccindex(
        url_list,
        parquet_root=parquet_root,
        master_db=master_db,
        year=year,
        per_url_limit=int(per_url_limit),
        stats_out=resolve_stats,
        trace_events=bool(trace),
    )
    resolve_elapsed_s = time.perf_counter() - t_res0

    # Best-effort resolve stats.
    resolve_domains = len({normalize_domain(u) for u in url_list if normalize_domain(u)})
    resolve_parquet_files = int(resolve_stats.get("parquet_files_scanned") or 0)
    resolve_mode = str(resolve_stats.get("resolve_mode") or "auto")

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
        resolve_stats=dict(resolve_stats),
    )

    if trace:
        try:
            state_dir = Path((os.environ.get("CCINDEX_STATE_DIR") or "state").strip() or "state")
            p = Path((os.environ.get("CCINDEX_EVENT_LOG_PATH") or str(state_dir / "ccindex_events.jsonl")).strip())
            p.parent.mkdir(parents=True, exist_ok=True)
            p.open("a", encoding="utf-8").write(
                json.dumps(
                    {
                        "event": "brave_search_ccindex_done",
                        "ts": time.time(),
                        "elapsed_s": float(res_obj.elapsed_s),
                        "brave_elapsed_s": float(res_obj.brave_elapsed_s),
                        "resolve_elapsed_s": float(res_obj.resolve_elapsed_s),
                        "brave_cached": bool(res_obj.brave_cached),
                        "resolved_cached": bool(res_obj.resolved_cached),
                        "resolve_mode": str(res_obj.resolve_mode),
                        "resolve_domains": int(res_obj.resolve_domains),
                        "resolve_parquet_files": int(res_obj.resolve_parquet_files),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        except Exception:
            pass

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
                    "resolve_stats": res_obj.resolve_stats,
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
