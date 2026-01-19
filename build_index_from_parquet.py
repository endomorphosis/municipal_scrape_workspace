#!/usr/bin/env python3
"""Backwards-compatible wrapper for building pointer indexes from Parquet.

Moved to:
  municipal_scrape_workspace.ccindex.build_index_from_parquet
"""

from municipal_scrape_workspace.ccindex.build_index_from_parquet import main


if __name__ == "__main__":
    raise SystemExit(main())
