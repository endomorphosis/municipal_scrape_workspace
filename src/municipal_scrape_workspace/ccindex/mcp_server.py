"""MCP server for ccindex.

This exposes a small set of high-value ccindex operations as MCP tools.

Install with:
  pip install -e '.[ccindex-mcp]'

Run:
  ccindex-mcp

By default the server uses stdio transport (the MCP SDK default).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from . import api


def _maybe_path(p: Optional[str]) -> Optional[Path]:
    if p is None:
        return None
    p = str(p).strip()
    if not p:
        return None
    return Path(p).expanduser().resolve()


def main() -> int:
    try:
        # MCP Python SDK (FastMCP API)
        from mcp.server.fastmcp import FastMCP  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "Missing MCP dependency. Install with: pip install -e '.[ccindex-mcp]'\n"
            f"Import error: {e}"
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
    def parquet_shards_for_domain(
        collection_db: str,
        domain: str,
    ) -> Dict[str, Any]:
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
    ) -> Dict[str, Any]:
        """Fetch a WARC record by exact byte range (offset/length) and return a decoded preview."""

        res = api.fetch_warc_record_range(
            warc_filename=str(warc_filename),
            warc_offset=int(warc_offset),
            warc_length=int(warc_length),
            prefix=str(prefix),
            max_bytes=int(max_bytes),
            decode_gzip_text=bool(decode_gzip_text),
            max_preview_chars=int(max_preview_chars),
        )
        return {
            "ok": res.ok,
            "status": res.status,
            "url": res.url,
            "bytes_requested": res.bytes_requested,
            "bytes_returned": res.bytes_returned,
            "sha256": res.sha256,
            "raw_base64": res.raw_base64,
            "decoded_text_preview": res.decoded_text_preview,
            "error": res.error,
        }

    @mcp.tool()
    def normalize_domain(domain_or_url: str) -> str:
        """Normalize a domain or URL into a hostname."""

        return api.normalize_domain(domain_or_url)

    @mcp.tool()
    def host_to_rev(host: str) -> str:
        """Convert host like 'a.b.c' to 'c,b,a'."""

        return api.host_to_rev(host)

    mcp.run()
    return 0
