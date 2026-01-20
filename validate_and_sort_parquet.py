#!/usr/bin/env python3
"""Backwards-compatible wrapper for validating/sorting parquet shards.

Moved to:
  municipal_scrape_workspace.ccindex.validate_and_sort_parquet
"""

from municipal_scrape_workspace.ccindex.validate_and_sort_parquet import main


if __name__ == "__main__":
    raise SystemExit(main())
