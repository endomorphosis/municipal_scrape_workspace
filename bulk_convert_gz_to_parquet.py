#!/usr/bin/env python3
"""Backwards-compatible wrapper for bulk GZ to Parquet converter.

Moved to:
  municipal_scrape_workspace.ccindex.bulk_convert_gz_to_parquet
"""

from municipal_scrape_workspace.ccindex.bulk_convert_gz_to_parquet import main

if __name__ == "__main__":
    raise SystemExit(main())
