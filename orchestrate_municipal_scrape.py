#!/usr/bin/env python3
"""Backwards-compatible wrapper for municipal scrape orchestrator.

Moved to:
  municipal_scrape_workspace.orchestrate_municipal_scrape
"""

from municipal_scrape_workspace.orchestrate_municipal_scrape import main

if __name__ == "__main__":
    raise SystemExit(main())
