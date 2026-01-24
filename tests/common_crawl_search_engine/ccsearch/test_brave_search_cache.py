from __future__ import annotations

import json
import os
import sys
import types
from pathlib import Path

import pytest


class _Resp:
    def __init__(self, payload: dict, status_code: int = 200):
        self.status_code = status_code
        self._payload = payload
        self.content = b"x"
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _RequestsModule:
    def __init__(self, payload: dict):
        self.calls = 0
        self._payload = payload

    def get(self, *_args, **_kwargs):
        self.calls += 1
        return _Resp(self._payload)


@pytest.mark.parametrize("ttl_s", [3600])
def test_brave_web_search_uses_disk_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ttl_s: int):
    # Point cache at temp file.
    cache_path = tmp_path / "brave_cache.json"
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_PATH", str(cache_path))
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_TTL_S", str(ttl_s))
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_MAX_ENTRIES", "100")
    monkeypatch.delenv("BRAVE_SEARCH_CACHE_DISABLE", raising=False)

    # Provide token.
    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "test-token")

    payload = {
        "web": {
            "results": [
                {"title": "T1", "url": "https://example.com", "description": "D1"},
                {"title": "T2", "url": "https://example.org", "description": "D2"},
            ]
        }
    }
    fake_requests = _RequestsModule(payload)

    # Inject fake 'requests' module.
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    from common_crawl_search_engine.ccsearch.brave_search import brave_web_search

    r1 = brave_web_search("cats", count=2, offset=0, country="us", safesearch="moderate")
    r2 = brave_web_search("cats", count=2, offset=0, country="us", safesearch="moderate")

    assert r1 == r2
    assert fake_requests.calls == 1
    assert cache_path.exists()


def test_brave_web_search_cache_can_be_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cache_path = tmp_path / "brave_cache.json"
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_PATH", str(cache_path))
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_TTL_S", "3600")
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_DISABLE", "1")

    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "test-token")

    payload = {"web": {"results": [{"title": "T", "url": "https://example.com", "description": "D"}]}}
    fake_requests = _RequestsModule(payload)
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    from common_crawl_search_engine.ccsearch.brave_search import brave_web_search

    brave_web_search("cats")
    brave_web_search("cats")

    assert fake_requests.calls == 2


def test_brave_cache_stats_and_clear(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cache_path = tmp_path / "brave_cache.json"
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_PATH", str(cache_path))
    monkeypatch.setenv("BRAVE_SEARCH_CACHE_TTL_S", "3600")
    monkeypatch.delenv("BRAVE_SEARCH_CACHE_DISABLE", raising=False)

    from common_crawl_search_engine.ccsearch.brave_search import (
        brave_search_cache_stats,
        clear_brave_search_cache,
    )

    s0 = brave_search_cache_stats()
    assert s0["path"] == str(cache_path)
    assert s0["entries"] == 0

    cache_path.write_text('{"k": {"ts": 123, "items": []}}\n', encoding="utf-8")
    s1 = brave_search_cache_stats()
    assert s1["exists"] is True
    assert s1["entries"] == 1

    cleared = clear_brave_search_cache()
    assert cleared["path"] == str(cache_path)
    # deleted may be False on filesystems where unlink fails; but should not error.
    assert "freed_bytes" in cleared
