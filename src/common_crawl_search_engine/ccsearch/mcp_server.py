"""MCP stdio server entrypoint (ccsearch).

This module re-exports the app-layer MCP server implementation currently located at
`common_crawl_search_engine.mcp_server`.
"""

from common_crawl_search_engine.mcp_server import main

__all__ = ["main"]
