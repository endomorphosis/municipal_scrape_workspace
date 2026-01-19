#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC pointer build watchdog.

Moved to:
  municipal_scrape_workspace.ccindex.watchdog_cc_pointer_build
"""

from municipal_scrape_workspace.ccindex.watchdog_cc_pointer_build import main

if __name__ == "__main__":
    raise SystemExit(main())
