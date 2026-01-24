"""Pytest configuration and shared fixtures."""

import pytest
import sys
from pathlib import Path


# Deprecated: repo-root wrapper scripts (e.g., ./search_cc_domain.py).
# We no longer ship or support these; prefer the packaged CLI entrypoints.
collect_ignore = ["common_crawl_search_engine/ccindex/test_wrappers.py"]


@pytest.fixture
def repo_root():
    """Return the repository root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def src_path(repo_root):
    """Return the src directory path."""
    return repo_root / "src"


@pytest.fixture
def ccindex_path(src_path):
    """Return the ccindex package path."""
    return src_path / "common_crawl_search_engine" / "ccindex"
