"""Legacy wrapper script tests.

Historically this repo shipped root-level wrapper scripts (e.g. search_cc_domain.py).
Those wrappers have been removed in favor of module entrypoints and the unified
`ccindex` CLI.
"""

import subprocess
import sys
from pathlib import Path


def test_legacy_root_wrapper_removed() -> None:
    root = Path(__file__).resolve().parents[3]
    assert not (root / "search_cc_domain.py").exists()


def test_search_domain_module_imports() -> None:
    # The modern equivalent for search_cc_domain.py is a module under src.
    import common_crawl_search_engine.ccindex.search_cc_domain as mod

    assert hasattr(mod, "__file__")


def test_ccindex_cli_help_includes_search() -> None:
    # Exercise the unified CLI help path (this also validates console script layout).
    out = subprocess.check_output(
        [sys.executable, "-m", "common_crawl_search_engine.cli", "--help"],
        text=True,
        timeout=15,
    )
    assert "search" in out.lower()
