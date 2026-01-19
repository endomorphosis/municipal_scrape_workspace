#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC index sampler.

Moved to:
  municipal_scrape_workspace.ccindex.sample_ccindex_to_parquet
"""

from municipal_scrape_workspace.ccindex.sample_ccindex_to_parquet import main

if __name__ == "__main__":
    raise SystemExit(main())
