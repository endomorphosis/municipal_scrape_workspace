#!/usr/bin/env python3
"""Backwards-compatible wrapper for building the master meta-index.

This script was moved into the package at:
  municipal_scrape_workspace.ccindex.build_master_index

Keep this wrapper so existing operational commands continue to work.
"""

from municipal_scrape_workspace.ccindex.build_master_index import main


if __name__ == "__main__":
    raise SystemExit(main())
