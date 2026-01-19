#!/usr/bin/env python3
"""Backwards-compatible wrapper for progress monitor.

Moved to:
  municipal_scrape_workspace.ccindex.monitor_progress
"""

from municipal_scrape_workspace.ccindex.monitor_progress import main

if __name__ == "__main__":
    raise SystemExit(main())
