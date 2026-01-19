#!/usr/bin/env python3
"""Backwards-compatible wrapper for parallel parquet validation.

Moved to:
  municipal_scrape_workspace.ccindex.parallel_validate_parquet
"""

from municipal_scrape_workspace.ccindex.parallel_validate_parquet import main


if __name__ == "__main__":
    raise SystemExit(main())
