#!/usr/bin/env python3
"""Backwards-compatible wrapper for building the CC DuckDB pointer index.

Moved to:
  municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb
"""

from municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb import main


if __name__ == "__main__":
    raise SystemExit(main())
