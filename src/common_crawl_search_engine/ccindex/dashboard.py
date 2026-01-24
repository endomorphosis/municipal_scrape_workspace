"""Backward-compatible wrapper.

The ccindex dashboard implementation lives in `common_crawl_search_engine.dashboard`.
This wrapper keeps older import paths working while we reorganize the package.
"""

from common_crawl_search_engine.dashboard import create_app, main

__all__ = ["create_app", "main"]
