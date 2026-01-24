"""Mocked Playwright E2E tests for the dashboard.

This file used to contain Playwright E2E tests for Brave + Settings panels.
It is restored and expanded to validate end-to-end UI behavior for all dashboard
sections (Wayback, Search, Index, Settings), without requiring real Common Crawl
datasets.

These tests:
- start the dashboard in a subprocess with isolated tmp state
- intercept `POST /mcp` to return deterministic JSON-RPC tool outputs
- validate key UI elements and flows (including record rendering)

They are opt-in via the `integration` marker.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest


pytestmark = pytest.mark.integration


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_http_ready(url: str, timeout_s: float = 15.0) -> None:
    deadline = time.time() + float(timeout_s)
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.5) as resp:
                if 200 <= int(resp.status) < 500:
                    return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(0.2)
    raise RuntimeError(f"Server not ready after {timeout_s}s: {url}. Last error: {last_err}")


@pytest.fixture()
def dashboard_mocked(tmp_path: Path) -> tuple[str, subprocess.Popen[str], Path]:
    """Start a dashboard subprocess with isolated temp state."""

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    run_dir = tmp_path / "dashboard_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Point the dashboard at a throwaway DB path. It doesn't need to exist for
    # UI to load when /mcp is mocked.
    master_db = run_dir / "cc_master_index.duckdb"

    env = os.environ.copy()
    env["CCINDEX_WARC_CACHE_DIR"] = str(run_dir / "warc_cache")
    env["CCINDEX_FULL_WARC_CACHE_DIR"] = str(run_dir / "warc_files")
    env["BRAVE_SEARCH_CACHE_PATH"] = str(run_dir / "brave_cache.json")
    # Orchestrator manager uses this; keep it aligned with the cwd state dir.
    env["CCINDEX_STATE_DIR"] = str(run_dir / "state")

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "common_crawl_search_engine.dashboard",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--master-db",
            str(master_db),
        ],
        cwd=str(run_dir),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_http_ready(base_url + "/")
        yield base_url, proc, run_dir
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=8)

        if proc.stdout is not None:
            try:
                out = proc.stdout.read() or ""
            except Exception:  # noqa: BLE001
                out = ""
            if out:
                (run_dir / "dashboard_stdout.log").write_text(out)


def _mock_mcp(route, request) -> None:
    """Intercept POST /mcp and return deterministic tool results."""

    if request.method != "POST":
        route.continue_()
        return

    try:
        payload = json.loads(request.post_data() or "{}")
    except Exception:  # noqa: BLE001
        payload = {}

    def _ok(req_id, result: dict) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}),
        )

    def _err(req_id, code: int, message: str) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}),
        )

    req_id = payload.get("id")
    method = payload.get("method")

    if method == "tools/list":
        _ok(
            req_id,
            {
                "tools": [
                    {"name": "search_domain_meta"},
                    {"name": "fetch_warc_record"},
                    {"name": "brave_search_ccindex"},
                    {"name": "orchestrator_settings_get"},
                    {"name": "cc_collinfo_list"},
                    {"name": "cc_collinfo_update"},
                    {"name": "orchestrator_collections_status"},
                    {"name": "orchestrator_jobs_list"},
                    {"name": "orchestrator_job_status"},
                    {"name": "orchestrator_job_tail"},
                    {"name": "orchestrator_job_stop"},
                    {"name": "orchestrator_job_plan"},
                    {"name": "orchestrator_job_start"},
                ]
            },
        )
        return

    if method != "tools/call":
        _err(req_id, -32601, f"unsupported method: {method}")
        return

    params = payload.get("params") or {}
    name = params.get("name")
    args = params.get("arguments") or {}

    if name == "orchestrator_settings_get":
        _ok(req_id, {"max_workers": 4})
        return

    if name == "cc_collinfo_update":
        _ok(req_id, {"ok": True, "url": "mock://collinfo", "path": "state/collinfo.json", "count": 2})
        return

    if name == "cc_collinfo_list":
        _ok(
            req_id,
            {
                "ok": True,
                "source_path": "state/collinfo.json",
                "fetched_at": None,
                "collections": [
                    {"id": "CC-MAIN-2099-01", "name": "Mock Crawl A", "time_range": "2099-01"},
                    {"id": "CC-MAIN-2099-02", "name": "Mock Crawl B", "time_range": "2099-02"},
                ],
            },
        )
        return

    if name == "orchestrator_collections_status":
        cols = args.get("collections") or []
        out = {}
        for c in cols:
            if str(c).endswith("01"):
                out[str(c)] = {"ok": True, "fully_complete": True}
            else:
                out[str(c)] = {"ok": True, "fully_complete": False}
        _ok(req_id, {"ok": True, "collections": out, "summary": {"total": len(out), "fully_complete": 1}})
        return

    if name == "orchestrator_jobs_list":
        _ok(
            req_id,
            {
                "ok": True,
                "jobs": [
                    {
                        "pid": 12345,
                        "log_path": "logs/cc_pipeline_mock.log",
                        "cmd": ["python", "-m", "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator"],
                        "label": "cc_pipeline_mock",
                        "started_at": "2099-01-01T00:00:00+00:00",
                    }
                ],
            },
        )
        return

    if name == "orchestrator_job_status":
        _ok(
            req_id,
            {
                "ok": True,
                "pid": args.get("pid"),
                "alive": False,
                "log_path": args.get("log_path"),
                "tail": "[download] Downloading CC-MAIN-2099-01\n[download] done",
                "progress": {"stage": "download", "collection": "CC-MAIN-2099-01", "last_line": "[download] done"},
            },
        )
        return

    if name == "orchestrator_job_tail":
        _ok(req_id, {"ok": True, "tail": "[download] Downloading CC-MAIN-2099-01\n[download] done"})
        return

    if name == "orchestrator_job_stop":
        _ok(req_id, {"ok": True, "pid": int(args.get("pid") or 0), "signal": str(args.get("sig") or "TERM")})
        return

    if name == "orchestrator_job_plan":
        _ok(
            req_id,
            {
                "cmd": [
                    sys.executable,
                    "-m",
                    "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator",
                    "--download-only",
                ]
            },
        )
        return

    if name == "orchestrator_job_start":
        _ok(
            req_id,
            {"pid": 99999, "log_path": "logs/cc_pipeline_started.log", "cmd": (args.get("planned") or {}).get("cmd") or []},
        )
        return

    if name == "search_domain_meta":
        _ok(
            req_id,
            {
                "ok": True,
                "meta_source": "mock",
                "collections_considered": 1,
                "emitted": 1,
                "elapsed_s": 0.01,
                "records": [
                    {
                        "url": "https://example.test/page",
                        "timestamp": "20990101000000",
                        "status": 200,
                        "mime": "text/html",
                        "collection": "CC-MAIN-2099-01",
                        "warc_filename": "crawl-data/CC-MAIN-2099-01/segments/0/warc/CC-MAIN-20990101000000-00000.warc.gz",
                        "warc_offset": 1,
                        "warc_length": 2,
                    }
                ],
            },
        )
        return

    if name == "brave_search_ccindex":
        _ok(
            req_id,
            {
                "query": str(args.get("query") or ""),
                "elapsed_s": 0.01,
                "results": [
                    {
                        "title": "Example result",
                        "url": "https://example.test/page",
                        "description": "stubbed brave result",
                        "cc_matches": [
                            {
                                "warc_filename": "crawl-data/CC-MAIN-2099-01/segments/0/warc/CC-MAIN-20990101000000-00000.warc.gz",
                                "warc_offset": 1,
                                "warc_length": 2,
                                "collection": "CC-MAIN-2099-01",
                                "url": "https://example.test/page",
                            }
                        ],
                    }
                ],
            },
        )
        return

    if name == "fetch_warc_record":
        html_doc = "<html><body><h1>hello from warc</h1><p>mocked</p></body></html>"
        _ok(
            req_id,
            {
                "ok": True,
                "status": 206,
                "url": "https://data.commoncrawl.org/example.warc.gz",
                "source": "range",
                "local_warc_path": None,
                "bytes_requested": 10,
                "bytes_returned": 10,
                "sha256": None,
                "decoded_text_preview": "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" + html_doc,
                "http": {
                    "ok": True,
                    "status": 200,
                    "headers": {"content-type": "text/html"},
                    "body_text_preview": html_doc,
                    "body_is_html": True,
                    "body_mime": "text/html",
                    "body_charset": "utf-8",
                    "error": None,
                },
                "error": None,
            },
        )
        return

    _err(req_id, -32601, f"unknown tool: {name}")


def _import_sync_playwright() -> "object":
    try:
        from playwright.sync_api import sync_playwright  # type: ignore

        return sync_playwright
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Playwright not installed: {e}")


def _launch_chromium(p) -> "object":
    try:
        return p.chromium.launch()
    except Exception as e:  # noqa: BLE001
        pytest.skip(
            "Chromium not available for Playwright. Run: `playwright install chromium`. "
            f"Original error: {e}"
        )


def test_wayback_to_record_flow_works_with_mocked_mcp(dashboard_mocked) -> None:
    base_url, _proc, _run_dir = dashboard_mocked

    sync_playwright = _import_sync_playwright()
    with sync_playwright() as p:
        browser = _launch_chromium(p)
        page = browser.new_page()

        console_lines: list[str] = []
        page_errors: list[str] = []
        page.on("console", lambda msg: console_lines.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        page.route("**/mcp", _mock_mcp)

        try:
            page.goto(f"{base_url}/", wait_until="domcontentloaded")

            page.fill("#q", "example.test")
            page.click("#searchForm button[type='submit']")

            page.wait_for_function(
                "() => document.querySelector('#status .badge.ok') || document.querySelector('#status .badge.err')",
                timeout=10_000,
            )
            status_text = (page.locator("#status").inner_text() or "").lower()
            assert "ok" in status_text

            page.wait_for_selector("a[href^='/record']", timeout=10_000)
            page.locator("a[href^='/record']").first.click()

            page.wait_for_url("**/record**", timeout=10_000)
            page.wait_for_selector("#recStatus", timeout=10_000)
            page.wait_for_function(
                "() => document.querySelector('#recStatus .badge.ok') || document.querySelector('#recStatus .badge.err')",
                timeout=10_000,
            )

            rec_status = (page.locator("#recStatus").inner_text() or "").lower()
            assert "ok" in rec_status
            page.wait_for_selector("iframe#recFrame", timeout=10_000, state="attached")
            srcdoc = page.evaluate(
                """() => {
                  const f = document.querySelector('iframe#recFrame');
                  return (f && (f.getAttribute('srcdoc') || '')) || '';
                }"""
            )
            assert "hello from warc" in str(srcdoc)
            assert page_errors == [], f"Page errors: {page_errors}"
        finally:
            browser.close()


def test_search_panel_to_record_flow_works_with_mocked_mcp(dashboard_mocked) -> None:
    base_url, _proc, _run_dir = dashboard_mocked

    sync_playwright = _import_sync_playwright()
    with sync_playwright() as p:
        browser = _launch_chromium(p)
        page = browser.new_page()
        page.route("**/mcp", _mock_mcp)

        try:
            page.goto(f"{base_url}/discover", wait_until="domcontentloaded")
            page.fill("#dq", "site:example.test hello")
            page.click("#discoverForm button[type='submit']")
            page.wait_for_function(
                "() => document.querySelector('#dstatus .badge.ok') || document.querySelector('#dstatus .badge.err')",
                timeout=10_000,
            )
            status_text = (page.locator("#dstatus").inner_text() or "").lower()
            assert "ok" in status_text

            page.wait_for_selector("a[href^='/record']", timeout=10_000)
            page.locator("a[href^='/record']").first.click()
            page.wait_for_url("**/record**", timeout=10_000)
            page.wait_for_function(
                "() => document.querySelector('#recStatus .badge.ok') || document.querySelector('#recStatus .badge.err')",
                timeout=10_000,
            )
            rec_status = (page.locator("#recStatus").inner_text() or "").lower()
            assert "ok" in rec_status
        finally:
            browser.close()


def test_settings_panel_save_and_cache_controls_work(dashboard_mocked) -> None:
    base_url, _proc, run_dir = dashboard_mocked

    # Seed some fake caches so stats + clear-cache are meaningful.
    (run_dir / "warc_cache").mkdir(parents=True, exist_ok=True)
    (run_dir / "warc_files").mkdir(parents=True, exist_ok=True)
    (run_dir / "warc_cache" / "a.bin").write_bytes(b"x" * 10)
    (run_dir / "warc_files" / "f.warc.gz").write_bytes(b"y" * 12)

    sync_playwright = _import_sync_playwright()
    with sync_playwright() as p:
        browser = _launch_chromium(p)
        page = browser.new_page()
        page.route("**/mcp", _mock_mcp)

        try:
            page.goto(f"{base_url}/settings", wait_until="domcontentloaded")

            page.click("#refreshCacheStatsBtn")
            page.wait_for_function(
                "() => (document.querySelector('#cacheStats')?.textContent || '').includes('range_cache:')",
                timeout=10_000,
            )

            page.select_option("#default_cache_mode", "full")
            page.fill("#default_max_bytes", "12345")
            page.fill("#default_max_preview_chars", "777")
            page.click("#saveBtn")
            page.wait_for_function(
                "() => (document.querySelector('#status')?.textContent || '').includes('saved')",
                timeout=10_000,
            )

            page.reload(wait_until="domcontentloaded")
            page.wait_for_timeout(200)
            assert page.evaluate("() => document.getElementById('default_cache_mode')?.value") == "full"
            assert page.evaluate("() => document.getElementById('default_max_bytes')?.value") == "12345"
            assert page.evaluate("() => document.getElementById('default_max_preview_chars')?.value") == "777"

            page.click("#clearRangeCacheBtn")
            page.wait_for_function(
                "() => (document.querySelector('#cacheStats')?.textContent || '').includes('cleared range')",
                timeout=10_000,
            )
            page.click("#clearFullCacheBtn")
            page.wait_for_function(
                "() => (document.querySelector('#cacheStats')?.textContent || '').includes('cleared full')",
                timeout=10_000,
            )
        finally:
            browser.close()


def test_index_panel_load_select_bulk_status_and_jobs_list(dashboard_mocked) -> None:
    base_url, _proc, _run_dir = dashboard_mocked

    sync_playwright = _import_sync_playwright()
    with sync_playwright() as p:
        browser = _launch_chromium(p)
        page = browser.new_page()
        page.route("**/mcp", _mock_mcp)

        try:
            page.goto(f"{base_url}/index", wait_until="domcontentloaded")

            # Auto-load happens on page load; confirm we rendered rows.
            page.wait_for_selector("#collectionsTbody tr", timeout=10_000)

            # Select the first collection.
            page.locator("#collectionsTbody input[type='checkbox'][data-coll]").first.check()

            # Trigger bulk status; should mark one as complete.
            page.click("#btnBulkStatus")
            page.wait_for_function(
                "() => (document.getElementById('status')?.textContent || '').includes('fully_complete')",
                timeout=10_000,
            )

            # Now load jobs and ensure status reports ok.
            page.click("#btnJobsList")
            page.wait_for_function(
                "() => (document.getElementById('status')?.textContent || '').includes('jobs_count')",
                timeout=10_000,
            )

            # Ensure clicking open fills the pid/log inputs.
            page.wait_for_selector("#jobsTbody tr button[data-open-job]", timeout=10_000)
            page.locator("#jobsTbody tr button[data-open-job]").first.click()
            page.wait_for_function(
                "() => (document.getElementById('jobPid')?.value || '').length > 0",
                timeout=10_000,
            )
        finally:
            browser.close()
