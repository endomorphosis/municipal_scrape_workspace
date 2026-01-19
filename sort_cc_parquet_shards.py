#!/usr/bin/env python3
"""Backwards-compatible wrapper for sorting CC parquet shards.

Moved to:
  municipal_scrape_workspace.ccindex.sort_cc_parquet_shards
"""

from municipal_scrape_workspace.ccindex.sort_cc_parquet_shards import main


if __name__ == "__main__":
    raise SystemExit(main())
