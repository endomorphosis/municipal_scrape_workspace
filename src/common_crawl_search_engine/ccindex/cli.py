"""Backward-compatible wrapper.

The ccindex app entrypoints have moved to `common_crawl_search_engine.cli`.
"""

from common_crawl_search_engine.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
