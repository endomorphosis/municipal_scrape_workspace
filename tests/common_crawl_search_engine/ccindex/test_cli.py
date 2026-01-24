"""Test that CLI help commands work correctly."""

import subprocess
import sys
import json
import os
from pathlib import Path


def test_search_domain_help():
    """Test that search_cc_domain displays help."""
    result = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.ccindex.search_cc_domain", "--help"],
        capture_output=True,
        text=True,
        timeout=10
    )
    assert result.returncode == 0
    assert "domain" in result.stdout.lower()


def test_build_pointer_help():
    """Test that build_cc_pointer_duckdb displays help."""
    result = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.ccindex.build_cc_pointer_duckdb", "--help"],
        capture_output=True,
        text=True,
        timeout=10
    )
    assert result.returncode == 0
    assert "duckdb" in result.stdout.lower()


def test_orchestrator_help():
    """Test that cc_pipeline_orchestrator displays help."""
    result = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator", "--help"],
        capture_output=True,
        text=True,
        timeout=10
    )
    assert result.returncode == 0
    assert "orchestrat" in result.stdout.lower()


def test_validate_collection_help():
    """Test that validate_collection_completeness displays help."""
    result = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.ccindex.validate_collection_completeness", "--help"],
        capture_output=True,
        text=True,
        timeout=10
    )
    assert result.returncode == 0
    assert "validate" in result.stdout.lower() or "collection" in result.stdout.lower()


def test_ccindex_unified_help():
    """Test that the unified ccindex CLI displays help."""
    result = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.cli", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "ccindex" in result.stdout.lower()


def test_ccindex_mcp_help():
    """Test that `ccindex mcp --help` works."""
    result = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.cli", "mcp", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "mcp" in result.stdout.lower()


def test_ccindex_brave_cache_stats_and_clear(tmp_path: Path):
    env = dict(**os.environ)
    env["BRAVE_SEARCH_CACHE_PATH"] = str(tmp_path / "brave_cache.json")

    r1 = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.cli", "brave-cache", "stats"],
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )
    assert r1.returncode == 0
    s = json.loads(r1.stdout)
    assert "path" in s

    r2 = subprocess.run(
        [sys.executable, "-m", "common_crawl_search_engine.cli", "brave-cache", "clear"],
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )
    assert r2.returncode == 0
    c = json.loads(r2.stdout)
    assert "path" in c
