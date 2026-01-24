"""Backward-compatible wrapper.

The ccindex MCP server implementation lives in `common_crawl_search_engine.mcp_server`.
"""

from common_crawl_search_engine.mcp_server import main

__all__ = ["main"]
