#!/usr/bin/env python3
"""Backwards-compatible wrapper for the meta-index search tool.

This script was moved into the package at:
  municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes

Keep this wrapper so existing operational commands continue to work.
"""

from municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes import main


if __name__ == "__main__":
    raise SystemExit(main())
