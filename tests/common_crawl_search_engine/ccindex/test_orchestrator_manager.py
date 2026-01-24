from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_update_collinfo_writes_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from common_crawl_search_engine.ccindex import orchestrator_manager as om

    cache_path = tmp_path / "collinfo.json"
    monkeypatch.setenv("CCINDEX_COLLINFO_CACHE_PATH", str(cache_path))

    src = tmp_path / "src_collinfo.json"
    src.write_text("[{\"id\": \"CC-MAIN-2099-01\", \"name\": \"Test\"}]\n", encoding="utf-8")

    res = om.update_collinfo(url=src.as_uri(), timeout_s=5.0)
    assert res.get("ok") is True
    assert int(res.get("count") or 0) == 1
    assert cache_path.exists()

    cached = json.loads(cache_path.read_text(encoding="utf-8"))
    assert isinstance(cached, list)
    assert cached[0]["id"] == "CC-MAIN-2099-01"


def test_load_collinfo_prefers_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from common_crawl_search_engine.ccindex import orchestrator_manager as om

    cache_path = tmp_path / "collinfo.json"
    monkeypatch.setenv("CCINDEX_COLLINFO_CACHE_PATH", str(cache_path))

    cache_path.write_text("[{\"id\": \"CC-MAIN-2099-02\", \"name\": \"Cached\"}]\n", encoding="utf-8")

    res = om.load_collinfo(prefer_cache=True)
    assert res.get("ok") is True
    assert res.get("source_path") == str(cache_path)
    cols = res.get("collections")
    assert isinstance(cols, list)
    assert any(c.get("id") == "CC-MAIN-2099-02" for c in cols)


def test_list_jobs_newest_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from common_crawl_search_engine.ccindex import orchestrator_manager as om

    reg = tmp_path / "jobs.jsonl"
    monkeypatch.setenv("CCINDEX_JOBS_REGISTRY_PATH", str(reg))

    om._append_job_record({"pid": 1, "started_at": "t1"})
    om._append_job_record({"pid": 2, "started_at": "t2"})

    jobs = om.list_jobs(limit=10)
    assert [j.get("pid") for j in jobs[:2]] == [2, 1]


def test_parse_progress_from_tail_extracts_stage_and_collection() -> None:
    from common_crawl_search_engine.ccindex import orchestrator_manager as om

    tail = """
    [cleanup] sweeping CC-MAIN-2099-01
    something else
    [download] Downloading CC-MAIN-2099-02 now
    done
    """.strip()

    p = om._parse_progress_from_tail(tail)
    assert p.get("stage") == "download"
    assert p.get("collection") == "CC-MAIN-2099-02"
    assert isinstance(p.get("last_line"), str)


def test_job_status_reads_tail_and_progress(tmp_path: Path) -> None:
    from common_crawl_search_engine.ccindex import orchestrator_manager as om

    log = tmp_path / "job.log"
    log.write_text("[download] Downloading CC-MAIN-2099-01\nline2\n", encoding="utf-8")

    st = om.job_status(pid=None, log_path=str(log), lines=50)
    assert st.get("ok") is True
    assert "CC-MAIN-2099-01" in (st.get("tail") or "")
    prog = st.get("progress") or {}
    assert prog.get("collection") == "CC-MAIN-2099-01"
    assert prog.get("stage") == "download"
