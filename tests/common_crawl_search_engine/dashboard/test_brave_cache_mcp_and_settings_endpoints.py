from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.mark.parametrize("endpoint", ["/settings/brave_cache_stats", "/settings/clear_brave_cache"])
def test_dashboard_brave_cache_endpoints_exist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, endpoint: str):
    # Isolate cache to a temp file.
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_PATH", str(tmp_path / "brave_cache.json"))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    if endpoint.endswith("_stats"):
        r = c.get(endpoint)
    else:
        r = c.post(endpoint)

    assert r.status_code == 200
    data = r.json()
    assert data.get("ok") is True


def test_dashboard_mcp_exposes_brave_cache_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_PATH", str(tmp_path / "brave_cache.json"))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    # tools/list
    r = c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert r.status_code == 200
    tools = r.json()["result"]["tools"]
    names = {t.get("name") for t in tools}
    assert "brave_cache_stats" in names
    assert "brave_cache_clear" in names

    # tools/call brave_cache_stats
    r2 = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "brave_cache_stats", "arguments": {}},
        },
    )
    assert r2.status_code == 200
    out = r2.json()["result"]
    assert "path" in out

    # tools/call brave_cache_clear
    r3 = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "brave_cache_clear", "arguments": {}},
        },
    )
    assert r3.status_code == 200
    out2 = r3.json()["result"]
    assert "path" in out2
