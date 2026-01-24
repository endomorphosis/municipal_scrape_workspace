"""MCP stdio server for ccindex (application layer)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from common_crawl_search_engine.ccindex import api


def _maybe_path(p: Optional[str]) -> Optional[Path]:
    if p is None:
        return None
    p = str(p).strip()
    if not p:
        return None
    return Path(p).expanduser().resolve()


def main() -> int:
    try:
        from mcp.server.fastmcp import FastMCP  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "Missing MCP dependency. Install with: pip install -e '.[ccindex-mcp]'\n" f"Import error: {e}"
        )

    mcp = FastMCP("ccindex")

    @mcp.tool()
    def list_collections(
        master_db: str = "/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb",
        year_db: Optional[str] = None,
        year: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List collections registered in the meta-index layer."""

        cols = api.list_collections(master_db=_maybe_path(master_db), year_db=_maybe_path(year_db), year=year)
        return [
            {
                "year": c.year,
                "collection": c.collection,
                "collection_db_path": str(c.collection_db_path),
            }
            for c in cols
        ]

    @mcp.tool()
    def search_domain_meta(
        domain: str,
        parquet_root: str = "/storage/ccindex_parquet",
        master_db: str = "/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb",
        year_db: Optional[str] = None,
        collection_db: Optional[str] = None,
        year: Optional[str] = None,
        max_matches: int = 200,
    ) -> Dict[str, Any]:
        """Search candidate WARC pointers via the meta-index chain."""

        res = api.search_domain_via_meta_indexes(
            domain,
            parquet_root=Path(parquet_root).expanduser().resolve(),
            master_db=_maybe_path(master_db),
            year_db=_maybe_path(year_db),
            collection_db=_maybe_path(collection_db),
            year=year,
            max_matches=int(max_matches),
        )
        return {
            "meta_source": res.meta_source,
            "collections_considered": res.collections_considered,
            "emitted": res.emitted,
            "elapsed_s": res.elapsed_s,
            "records": res.records,
        }

    @mcp.tool()
    def parquet_shards_for_domain(collection_db: str, domain: str) -> Dict[str, Any]:
        """Return parquet shard relpaths for a domain for a single collection DB."""

        dom = api.normalize_domain(domain)
        host_rev = api.host_to_rev(dom)
        relpaths = api.parquet_relpaths_for_domain(Path(collection_db).expanduser().resolve(), host_rev)
        return {"domain": dom, "host_rev_prefix": host_rev, "parquet_relpaths": relpaths}

    @mcp.tool()
    def fetch_warc_record(
        warc_filename: str,
        warc_offset: int,
        warc_length: int,
        prefix: str = "https://data.commoncrawl.org/",
        max_bytes: int = 2_000_000,
        decode_gzip_text: bool = True,
        max_preview_chars: int = 40_000,
        cache_mode: str = "range",
        full_warc_cache_dir: Optional[str] = None,
        full_warc_max_bytes: int = 5_000_000_000,
    ) -> Dict[str, Any]:
        """Fetch a WARC record by pointer using range or cached full WARC."""

        res, source, local_path = api.fetch_warc_record(
            warc_filename=str(warc_filename),
            warc_offset=int(warc_offset),
            warc_length=int(warc_length),
            prefix=str(prefix),
            max_bytes=int(max_bytes),
            decode_gzip_text=bool(decode_gzip_text),
            max_preview_chars=int(max_preview_chars),
            cache_mode=str(cache_mode),
            full_warc_cache_dir=Path(full_warc_cache_dir).expanduser().resolve() if full_warc_cache_dir else None,
            full_warc_max_bytes=int(full_warc_max_bytes),
        )
        out: Dict[str, Any] = {
            "ok": res.ok,
            "status": res.status,
            "url": res.url,
            "source": source,
            "local_warc_path": local_path,
            "bytes_requested": res.bytes_requested,
            "bytes_returned": res.bytes_returned,
            "sha256": res.sha256,
            "raw_base64": res.raw_base64,
            "decoded_text_preview": res.decoded_text_preview,
            "error": res.error,
        }

        # Add a structured HTTP extraction when possible (useful for replay/render).
        if res.ok and res.raw_base64:
            try:
                import base64 as _b64

                raw = _b64.b64decode(res.raw_base64)
                parsed = api.extract_http_from_warc_gzip_member(
                    raw,
                    max_body_bytes=int(max_bytes),
                    max_preview_chars=int(max_preview_chars),
                    include_body_base64=False,
                )
                out["http"] = {
                    "ok": parsed.ok,
                    "warc_headers": parsed.warc_headers,
                    "status": parsed.http_status,
                    "status_line": parsed.http_status_line,
                    "headers": parsed.http_headers,
                    "body_text_preview": parsed.body_text_preview,
                    "body_is_html": parsed.body_is_html,
                    "body_mime": parsed.body_mime,
                    "body_charset": parsed.body_charset,
                    "error": parsed.error,
                }
            except Exception as e:
                out["http"] = {"ok": False, "error": f"parse_failed: {type(e).__name__}: {e}"}

        return out

    @mcp.tool()
    def brave_search_ccindex(
        query: str,
        count: int = 8,
        parquet_root: str = "/storage/ccindex_parquet",
        master_db: str = "/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb",
        year: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Brave web search + resolve result URLs to CCIndex pointers."""

        res = api.brave_search_ccindex(
            str(query),
            count=int(count),
            parquet_root=Path(parquet_root).expanduser().resolve(),
            master_db=_maybe_path(master_db),
            year=str(year) if year else None,
            api_key=str(api_key) if api_key else None,
        )
        return {"query": res.query, "elapsed_s": res.elapsed_s, "results": res.results}

    @mcp.tool()
    def brave_cache_stats() -> Dict[str, Any]:
        """Return stats for the on-disk Brave Search cache."""

        from common_crawl_search_engine.ccsearch.brave_search import brave_search_cache_stats

        return brave_search_cache_stats()

    @mcp.tool()
    def brave_cache_clear() -> Dict[str, Any]:
        """Clear the on-disk Brave Search cache."""

        from common_crawl_search_engine.ccsearch.brave_search import clear_brave_search_cache

        return clear_brave_search_cache()

    @mcp.tool()
    def normalize_domain(domain_or_url: str) -> str:
        return api.normalize_domain(domain_or_url)

    @mcp.tool()
    def host_to_rev(host: str) -> str:
        return api.host_to_rev(host)

    mcp.run()
    return 0
