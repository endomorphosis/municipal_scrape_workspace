import os
from pathlib import Path

from common_crawl_search_engine.ccindex import api


def test_rowgroup_index_db_for_collection_prefers_explicit_db(monkeypatch, tmp_path: Path):
    db = tmp_path / "explicit.duckdb"
    db.write_bytes(b"")

    monkeypatch.setenv("BRAVE_RESOLVE_ROWGROUP_INDEX_DB", str(db))
    monkeypatch.delenv("BRAVE_RESOLVE_ROWGROUP_INDEX_DIR", raising=False)
    monkeypatch.delenv("CC_DOMAIN_ROWGROUP_INDEX_DIR", raising=False)

    api._rowgroup_index_db_for_collection.cache_clear()
    p = api._rowgroup_index_db_for_collection("CC-MAIN-2024-10")
    assert p == db.resolve()


def test_rowgroup_index_db_for_collection_uses_dir_layout(monkeypatch, tmp_path: Path):
    coll = "CC-MAIN-2024-10"
    db = tmp_path / f"{coll}.domain_rowgroups.duckdb"
    db.write_bytes(b"")

    monkeypatch.delenv("BRAVE_RESOLVE_ROWGROUP_INDEX_DB", raising=False)
    monkeypatch.setenv("BRAVE_RESOLVE_ROWGROUP_INDEX_DIR", str(tmp_path))

    api._rowgroup_index_db_for_collection.cache_clear()
    p = api._rowgroup_index_db_for_collection(coll)
    assert p == db.resolve()


def test_rowgroup_slice_index_dir_default(monkeypatch):
    monkeypatch.delenv("CC_DOMAIN_ROWGROUP_INDEX_DIR", raising=False)
    monkeypatch.delenv("BRAVE_RESOLVE_ROWGROUP_INDEX_DIR", raising=False)

    d = api._rowgroup_slice_index_dir()
    assert isinstance(d, Path)
    # no guarantee it exists in CI; just ensure it's the expected default string
    assert str(d).endswith("/storage/ccindex_duckdb/cc_domain_rowgroups_by_collection")
