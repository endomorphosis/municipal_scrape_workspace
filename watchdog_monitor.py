#!/usr/bin/env python3
"""Backwards-compatible wrapper for watchdog monitor.

Moved to:
  municipal_scrape_workspace.ccindex.watchdog_monitor
"""

from municipal_scrape_workspace.ccindex.watchdog_monitor import main

if __name__ == "__main__":
    raise SystemExit(main())
