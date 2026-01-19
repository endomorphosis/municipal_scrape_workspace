#!/usr/bin/env python3
"""Backwards-compatible wrapper for memory-aware parquet sorter.

Moved to:
  municipal_scrape_workspace.ccindex.sort_unsorted_memory_aware
"""

from municipal_scrape_workspace.ccindex.sort_unsorted_memory_aware import main

if __name__ == "__main__":
    raise SystemExit(main())
