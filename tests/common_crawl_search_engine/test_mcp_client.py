from __future__ import annotations

import pytest


def test_normalize_mcp_endpoint_adds_mcp_path():
    from common_crawl_search_engine.mcp_client import normalize_mcp_endpoint

    assert normalize_mcp_endpoint("http://example.com") == "http://example.com/mcp"
    assert normalize_mcp_endpoint("http://example.com/") == "http://example.com/mcp"
    assert normalize_mcp_endpoint("http://example.com/foo") == "http://example.com/foo/mcp"
    assert normalize_mcp_endpoint("http://example.com/foo/") == "http://example.com/foo/mcp"
    assert normalize_mcp_endpoint("http://example.com/mcp") == "http://example.com/mcp"


def test_normalize_mcp_endpoint_allows_host_port_shorthand():
    from common_crawl_search_engine.mcp_client import normalize_mcp_endpoint

    assert normalize_mcp_endpoint("localhost:8787") == "http://localhost:8787/mcp"


def test_normalize_mcp_endpoint_rejects_empty():
    from common_crawl_search_engine.mcp_client import normalize_mcp_endpoint

    with pytest.raises(ValueError):
        normalize_mcp_endpoint("")
