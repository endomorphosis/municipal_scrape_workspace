#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC pointer status tool.

Moved to:
  municipal_scrape_workspace.ccindex.cc_pointer_status
"""

from municipal_scrape_workspace.ccindex.cc_pointer_status import main

if __name__ == "__main__":
    raise SystemExit(main())
