#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC Pipeline Watcher.

Moved to:
  municipal_scrape_workspace.ccindex.cc_pipeline_watch
"""

from municipal_scrape_workspace.ccindex.cc_pipeline_watch import main

if __name__ == "__main__":
    raise SystemExit(main())
