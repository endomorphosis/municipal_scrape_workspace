from __future__ import annotations

import os
from pathlib import Path

import pytest


def test_mcp_tools_include_orchestrator():
    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)
    r = c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert r.status_code == 200
    tools = r.json()["result"]["tools"]
    names = {t.get("name") for t in tools}

    assert "orchestrator_settings_get" in names
    assert "orchestrator_settings_set" in names
    assert "orchestrator_collection_status" in names
    assert "orchestrator_delete_collection_index" in names
    assert "orchestrator_job_plan" in names


def test_orchestrator_settings_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Isolate orchestrator settings to a temp file.
    monkeypatch.setenv("CCINDEX_ORCHESTRATOR_SETTINGS_PATH", str(tmp_path / "orch_settings.json"))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    # Set
    r = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "orchestrator_settings_set",
                "arguments": {"settings": {"max_workers": 3, "collections_filter": "2024-10"}},
            },
        },
    )
    assert r.status_code == 200
    out = r.json()["result"]
    assert int(out.get("max_workers") or 0) == 3
    assert str(out.get("collections_filter")) == "2024-10"

    # Get
    r2 = c.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "orchestrator_settings_get", "arguments": {}}},
    )
    assert r2.status_code == 200
    out2 = r2.json()["result"]
    assert int(out2.get("max_workers") or 0) == 3
    assert str(out2.get("collections_filter")) == "2024-10"

    # And the file exists.
    assert Path(os.environ["CCINDEX_ORCHESTRATOR_SETTINGS_PATH"]).exists()


def test_orchestrator_job_plan_returns_cmd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CCINDEX_ORCHESTRATOR_SETTINGS_PATH", str(tmp_path / "orch_settings.json"))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    r = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "orchestrator_job_plan", "arguments": {"mode": "download_only", "filter": "2024-10", "workers": 2}},
        },
    )
    assert r.status_code == 200
    planned = r.json()["result"]
    assert isinstance(planned.get("cmd"), list)
    assert "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator" in " ".join(planned["cmd"])


def test_dashboard_index_page_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CCINDEX_ORCHESTRATOR_SETTINGS_PATH", str(tmp_path / "orch_settings.json"))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)
    r = c.get("/index")
    assert r.status_code == 200
    assert "ccindex-base-path" in r.text
    assert "CCIndex Orchestrator" in r.text


def test_mcp_batch_tools_list_and_orchestrator_get(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CCINDEX_ORCHESTRATOR_SETTINGS_PATH", str(tmp_path / "orch_settings.json"))

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)
    r = c.post(
        "/mcp",
        json=[
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "orchestrator_settings_get", "arguments": {}},
            },
        ],
    )
    assert r.status_code == 200
    out = r.json()
    assert isinstance(out, list)
    by_id = {item.get("id"): item for item in out}
    assert by_id[1].get("result", {}).get("tools")
    assert isinstance(by_id[2].get("result"), dict)
