"""Test that all ccindex tools can be imported correctly."""

import pytest


def test_search_domain_import():
    """Test search_cc_domain imports correctly."""
    from municipal_scrape_workspace.ccindex.search_cc_domain import main
    assert callable(main)


def test_search_duckdb_import():
    """Test search_cc_duckdb_index imports correctly."""
    from municipal_scrape_workspace.ccindex.search_cc_duckdb_index import main
    assert callable(main)


def test_search_pointer_import():
    """Test search_cc_pointer_index imports correctly."""
    from municipal_scrape_workspace.ccindex.search_cc_pointer_index import main
    assert callable(main)


def test_search_meta_indexes_import():
    """Test search_cc_via_meta_indexes imports correctly."""
    from municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes import main
    assert callable(main)


def test_search_parallel_import():
    """Test search_parallel_duckdb_indexes imports correctly."""
    from municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes import main
    assert callable(main)


def test_build_pointer_import():
    """Test build_cc_pointer_duckdb imports correctly."""
    from municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb import main
    assert callable(main)


def test_build_parallel_import():
    """Test build_parallel_duckdb_indexes imports correctly."""
    from municipal_scrape_workspace.ccindex.build_parallel_duckdb_indexes import main
    assert callable(main)


def test_build_year_meta_import():
    """Test build_year_meta_indexes imports correctly."""
    from municipal_scrape_workspace.ccindex.build_year_meta_indexes import main
    assert callable(main)


def test_build_master_import():
    """Test build_master_index imports correctly."""
    from municipal_scrape_workspace.ccindex.build_master_index import main
    assert callable(main)


def test_orchestrator_import():
    """Test cc_pipeline_orchestrator imports correctly."""
    from municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator import main
    assert callable(main)


def test_validate_collection_import():
    """Test validate_collection_completeness imports correctly."""
    from municipal_scrape_workspace.ccindex.validate_collection_completeness import main
    assert callable(main)


def test_validate_and_sort_import():
    """Test validate_and_sort_parquet imports correctly."""
    from municipal_scrape_workspace.ccindex.validate_and_sort_parquet import main
    assert callable(main)
