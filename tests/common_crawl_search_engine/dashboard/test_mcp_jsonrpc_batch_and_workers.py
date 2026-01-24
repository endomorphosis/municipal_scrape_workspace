import os
from pathlib import Path

import pytest


def test_mcp_accepts_jsonrpc_batch_tools_list_and_call(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Avoid touching the repo's real state/ cache files.
    cache_path = tmp_path / "brave_cache.json"
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_PATH", str(cache_path))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=tmp_path / "master.duckdb")

    from fastapi.testclient import TestClient

    c = TestClient(app)

    batch = [
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "brave_cache_stats", "arguments": {}},
        },
    ]

    r = c.post("/mcp", json=batch)
    assert r.status_code == 200

    data = r.json()
    assert isinstance(data, list)
    assert {item.get("id") for item in data} == {1, 2}

    tools_resp = next(item for item in data if item.get("id") == 1)
    assert "result" in tools_resp
    assert "tools" in tools_resp["result"]

    stats_resp = next(item for item in data if item.get("id") == 2)
    assert "result" in stats_resp
    assert stats_resp["result"].get("path")


def test_dashboard_main_workers_uses_import_string(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls = {}

    def fake_run(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return None

    monkeypatch.setenv("CCINDEX_MASTER_DB", str(tmp_path / "master.duckdb"))

    # Prevent importing/running the real uvicorn implementation.
    import sys
    import types

    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = fake_run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)

    import common_crawl_search_engine.dashboard as dashboard

    # Workers>1 should use import string for multi-process startup.
    rc = dashboard.main(["--host", "127.0.0.1", "--port", "8787", "--workers", "2"])
    assert rc == 0

    assert calls["args"][0] == "common_crawl_search_engine.dashboard:app"
    assert calls["kwargs"].get("workers") == 2
    assert calls["kwargs"].get("proxy_headers") is True
