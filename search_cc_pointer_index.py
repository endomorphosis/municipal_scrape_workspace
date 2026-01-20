#!/usr/bin/env python3
"""Backwards-compatible wrapper for searching the CC pointer index.

Moved to:
  municipal_scrape_workspace.ccindex.search_cc_pointer_index
"""

from municipal_scrape_workspace.ccindex.search_cc_pointer_index import main


if __name__ == "__main__":
    raise SystemExit(main())
