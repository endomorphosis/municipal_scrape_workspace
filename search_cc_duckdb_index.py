#!/usr/bin/env python3
"""Backwards-compatible wrapper for searching a CC DuckDB index.

Moved to:
  municipal_scrape_workspace.ccindex.search_cc_duckdb_index
"""

from municipal_scrape_workspace.ccindex.search_cc_duckdb_index import main


if __name__ == "__main__":
    raise SystemExit(main())
