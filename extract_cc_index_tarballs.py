#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC index tarball extractor.

Moved to:
  municipal_scrape_workspace.ccindex.extract_cc_index_tarballs
"""

from municipal_scrape_workspace.ccindex.extract_cc_index_tarballs import main

if __name__ == "__main__":
    raise SystemExit(main())
