#!/usr/bin/env python3
"""Backwards-compatible wrapper for DuckDB pointer index builder.

Moved to:
  municipal_scrape_workspace.ccindex.build_duckdb_pointer_from_parquet
"""

from municipal_scrape_workspace.ccindex.build_duckdb_pointer_from_parquet import main

if __name__ == "__main__":
    raise SystemExit(main())
