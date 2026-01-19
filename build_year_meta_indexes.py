#!/usr/bin/env python3
"""Backwards-compatible wrapper for building year meta-indexes.

This script was moved into the package at:
  municipal_scrape_workspace.ccindex.build_year_meta_indexes

Keep this wrapper so existing operational commands continue to work.
"""

from municipal_scrape_workspace.ccindex.build_year_meta_indexes import main


if __name__ == "__main__":
    raise SystemExit(main())
