#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC pointer build launcher.

Moved to:
  municipal_scrape_workspace.ccindex.launch_cc_pointer_build
"""

from municipal_scrape_workspace.ccindex.launch_cc_pointer_build import main

if __name__ == "__main__":
    raise SystemExit(main())
