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
    assert "cc_collinfo_list" in names
    assert "cc_collinfo_update" in names
    assert "orchestrator_collections_status" in names
    assert "orchestrator_delete_collection_indexes" in names
    assert "orchestrator_jobs_list" in names
    assert "orchestrator_job_status" in names


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

    # Set rowgroup-index knobs
    r_rg = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "orchestrator_settings_set",
                "arguments": {
                    "settings": {
                        "build_domain_rowgroup_index": False,
                        "domain_rowgroup_index_root": str(tmp_path / "rg_root"),
                        "domain_rowgroup_index_batch_size": 7,
                    }
                },
            },
        },
    )
    assert r_rg.status_code == 200
    out_rg = r_rg.json()["result"]
    assert out_rg.get("build_domain_rowgroup_index") is False
    assert str(out_rg.get("domain_rowgroup_index_root")) == str(tmp_path / "rg_root")
    assert int(out_rg.get("domain_rowgroup_index_batch_size") or 0) == 7

    # Get
    r2 = c.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "orchestrator_settings_get", "arguments": {}}},
    )
    assert r2.status_code == 200
    out2 = r2.json()["result"]
    assert int(out2.get("max_workers") or 0) == 3
    assert str(out2.get("collections_filter")) == "2024-10"
    assert out2.get("build_domain_rowgroup_index") is False
    assert str(out2.get("domain_rowgroup_index_root")) == str(tmp_path / "rg_root")
    assert int(out2.get("domain_rowgroup_index_batch_size") or 0) == 7

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

    cmd_str = " ".join(planned["cmd"])
    assert "--build-domain-rowgroup-index" in cmd_str or "--no-build-domain-rowgroup-index" in cmd_str
    assert "--domain-rowgroup-index-batch-size" in cmd_str


def test_orchestrator_job_plan_allows_rowgroup_index_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
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
            "id": 40,
            "method": "tools/call",
            "params": {
                "name": "orchestrator_job_plan",
                "arguments": {
                    "mode": "pipeline",
                    "filter": "CC-MAIN-2024-10",
                    "workers": 2,
                    "build_domain_rowgroup_index": False,
                    "domain_rowgroup_index_root": "/tmp/cc_rowgroups_test",
                    "domain_rowgroup_index_batch_size": 12345,
                },
            },
        },
    )
    assert r.status_code == 200
    planned = r.json()["result"]

    cmd_str = " ".join(planned["cmd"])
    assert "--no-build-domain-rowgroup-index" in cmd_str
    assert "--domain-rowgroup-index-root" in cmd_str
    assert "/tmp/cc_rowgroups_test" in cmd_str
    assert "--domain-rowgroup-index-batch-size" in cmd_str
    assert "12345" in cmd_str


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
    assert "Orchestrator Console" in r.text


def test_collinfo_update_and_list_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Isolate collinfo cache to a temp file.
    cache_path = tmp_path / "collinfo.json"
    monkeypatch.setenv("CCINDEX_COLLINFO_CACHE_PATH", str(cache_path))

    # Provide a local collinfo payload.
    src = tmp_path / "src_collinfo.json"
    src.write_text(
        "[\n  {\"id\": \"CC-MAIN-2099-01\", \"name\": \"Test Crawl\"}\n]\n",
        encoding="utf-8",
    )

    from common_crawl_search_engine.dashboard import create_app

    app = create_app(master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"))

    try:
        from fastapi.testclient import TestClient
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"fastapi.testclient missing: {e}")

    c = TestClient(app)

    # Update from file:// URL.
    r = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {"name": "cc_collinfo_update", "arguments": {"url": src.as_uri()}},
        },
    )
    assert r.status_code == 200
    out = r.json()["result"]
    assert out.get("ok") is True
    assert cache_path.exists()

    # List should read from the cache.
    r2 = c.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "cc_collinfo_list", "arguments": {"prefer_cache": True}},
        },
    )
    assert r2.status_code == 200
    out2 = r2.json()["result"]
    assert out2.get("ok") is True
    assert isinstance(out2.get("collections"), list)
    assert any(c.get("id") == "CC-MAIN-2099-01" for c in out2["collections"])


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
