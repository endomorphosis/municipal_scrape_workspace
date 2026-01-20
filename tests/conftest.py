"""Pytest configuration and shared fixtures."""

import pytest
import sys
from pathlib import Path


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
    return src_path / "municipal_scrape_workspace" / "ccindex"
