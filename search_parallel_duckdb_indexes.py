#!/usr/bin/env python3
"""Backwards-compatible wrapper for searching per-collection DuckDB indexes.

Moved to:
  municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes
"""

from municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes import main


if __name__ == "__main__":
    raise SystemExit(main())
