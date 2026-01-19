#!/usr/bin/env python3
"""Backwards-compatible wrapper for downloading exact WARC record byte ranges.

Moved to:
  municipal_scrape_workspace.ccindex.download_warc_records
"""

from municipal_scrape_workspace.ccindex.download_warc_records import main


if __name__ == "__main__":
    raise SystemExit(main())
