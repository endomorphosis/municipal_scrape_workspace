#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC Parquet rowgroup index builder.

Moved to:
  municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index
"""

from municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index import main

if __name__ == "__main__":
    raise SystemExit(main())
