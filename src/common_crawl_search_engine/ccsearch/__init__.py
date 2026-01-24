"""Application-layer entrypoints for ccindex.

This package is the “app shell” around the ccindex library:
- CLI
- MCP stdio server
- Local web dashboard + MCP-over-HTTP endpoint

The underlying index/search/build logic remains in `common_crawl_search_engine.ccindex`.
"""
