"""Dashboard entrypoint (ccsearch).

This module re-exports the app-layer dashboard implementation currently located at
`common_crawl_search_engine.dashboard`.
"""

from common_crawl_search_engine.dashboard import create_app, main

__all__ = ["create_app", "main"]
