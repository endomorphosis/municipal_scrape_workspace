#!/usr/bin/env python3
"""Backwards-compatible wrapper for search completeness validator.

Moved to:
  municipal_scrape_workspace.ccindex.validate_search_completeness
"""

from municipal_scrape_workspace.ccindex.validate_search_completeness import main

if __name__ == "__main__":
    raise SystemExit(main())
