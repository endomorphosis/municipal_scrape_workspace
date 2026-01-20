#!/usr/bin/env python3
"""Backwards-compatible wrapper for the CollectionValidator CLI.

This script was moved into the package at:
  municipal_scrape_workspace.ccindex.validate_collection_completeness

Keep this wrapper so existing operational commands continue to work.
"""

from municipal_scrape_workspace.ccindex.validate_collection_completeness import main


if __name__ == "__main__":
    raise SystemExit(main())
