#!/usr/bin/env python3
"""Backwards-compatible wrapper for validating downloaded WARC record blobs.

Moved to:
  municipal_scrape_workspace.ccindex.validate_warc_record_blobs
"""

from municipal_scrape_workspace.ccindex.validate_warc_record_blobs import main


if __name__ == "__main__":
    raise SystemExit(main())
