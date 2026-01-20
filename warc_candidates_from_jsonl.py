#!/usr/bin/env python3
"""Backwards-compatible wrapper for WARC candidate extraction.

Moved to:
  municipal_scrape_workspace.ccindex.warc_candidates_from_jsonl
"""

from municipal_scrape_workspace.ccindex.warc_candidates_from_jsonl import main


if __name__ == "__main__":
    raise SystemExit(main())
