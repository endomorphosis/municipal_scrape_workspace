from __future__ import annotations

from pathlib import Path

import pytest


def test_dashboard_html_respects_forwarded_prefix(monkeypatch: pytest.MonkeyPatch):
    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    r = c.get("/", headers={"X-Forwarded-Prefix": "/ccsearch"})
    assert r.status_code == 200
    html = r.text

    assert "<meta name='ccindex-base-path' content='/ccsearch'>" in html
    assert "href='/ccsearch/settings'" in html
    assert "href='/ccsearch/discover'" in html


def test_forwarded_prefix_strips_path_for_mcp(monkeypatch: pytest.MonkeyPatch):
    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    # Simulate a proxy that forwards a prefix *and* leaves it in the path.
    r = c.post(
        "/ccsearch/mcp",
        headers={"X-Forwarded-Prefix": "/ccsearch"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
    )

    assert r.status_code == 200
    data = r.json()
    assert data.get("result", {}).get("tools") is not None
