#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC domain search.

Moved to:
  municipal_scrape_workspace.ccindex.search_cc_domain
"""

from municipal_scrape_workspace.ccindex.search_cc_domain import main


if __name__ == "__main__":
    raise SystemExit(main())
