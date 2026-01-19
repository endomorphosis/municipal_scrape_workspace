#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC Pipeline Orchestrator.

Moved to:
  municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator
"""

from municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator import main

if __name__ == "__main__":
    raise SystemExit(main())
