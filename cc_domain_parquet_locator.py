#!/usr/bin/env python3
"""Backwards-compatible wrapper for locating CC parquet shards for domains.

Moved to:
  municipal_scrape_workspace.ccindex.cc_domain_parquet_locator
"""

from municipal_scrape_workspace.ccindex.cc_domain_parquet_locator import main


if __name__ == "__main__":
    raise SystemExit(main())
