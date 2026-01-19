#!/usr/bin/env python3
"""Backwards-compatible wrapper for WARC retrieval verification.

Moved to:
  municipal_scrape_workspace.ccindex.verify_warc_retrieval
"""

from municipal_scrape_workspace.ccindex.verify_warc_retrieval import main


if __name__ == "__main__":
    raise SystemExit(main())
