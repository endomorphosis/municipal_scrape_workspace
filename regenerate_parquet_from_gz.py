#!/usr/bin/env python3
"""Backwards-compatible wrapper for parquet regenerator.

Moved to:
  municipal_scrape_workspace.ccindex.regenerate_parquet_from_gz
"""

from municipal_scrape_workspace.ccindex.regenerate_parquet_from_gz import main

if __name__ == "__main__":
    raise SystemExit(main())
