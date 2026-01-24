"""Real Playwright E2E tests for the dashboard.

These tests intentionally DO NOT mock MCP.

They start the dashboard subprocess and exercise the real `/mcp` JSON-RPC
endpoint end-to-end through the browser UI.

Because the dashboard is designed to operate on large Common Crawl-derived
datasets, these tests expect real assets to be present (by default under
`/storage`). If the datasets are missing, the tests skip with a clear message.

When enabled, the tests also write screenshots for each panel so you can
visually confirm the dashboard is working.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest


pytestmark = pytest.mark.integration


RUN_PLAYWRIGHT = os.getenv("RUN_PLAYWRIGHT") == "1"
RUN_DASHBOARD_E2E = os.getenv("RUN_DASHBOARD_E2E") == "1"


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


def _poll_text_contains(page, selector: str, needles: list[str], *, timeout_s: float = 20.0) -> str:
    deadline = time.time() + float(timeout_s)
    last = ""
    while time.time() < deadline:
        try:
            last = page.locator(selector).inner_text() or ""
        except Exception:  # noqa: BLE001
            last = ""
        if any(n in last for n in needles):
            return last
        time.sleep(0.2)
    raise AssertionError(f"Timed out waiting for {selector} to contain one of {needles}. Last text: {last!r}")


def _require_real_ccindex_assets() -> tuple[Path, Path]:
    master_db = Path(os.getenv("DASHBOARD_E2E_MASTER_DB") or "/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb")
    parquet_root = Path(os.getenv("DASHBOARD_E2E_PARQUET_ROOT") or "/storage/ccindex_parquet")
    if not master_db.exists():
        pytest.skip(f"Missing master db: {master_db} (set DASHBOARD_E2E_MASTER_DB)")
    if not parquet_root.exists():
        pytest.skip(f"Missing parquet root: {parquet_root} (set DASHBOARD_E2E_PARQUET_ROOT)")
    return master_db, parquet_root


@pytest.fixture()
def dashboard_real(tmp_path: Path) -> tuple[str, subprocess.Popen[str], Path, Path]:
    """Start a dashboard subprocess backed by real datasets.

    State/caches are isolated to tmp_path so repeated test runs don't pollute
    the repo.
    """

    master_db, parquet_root = _require_real_ccindex_assets()

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    run_dir = tmp_path / "dashboard_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["CCINDEX_WARC_CACHE_DIR"] = str(run_dir / "warc_cache")
    env["CCINDEX_FULL_WARC_CACHE_DIR"] = str(run_dir / "warc_files")
    env["BRAVE_SEARCH_CACHE_PATH"] = str(run_dir / "brave_cache.json")
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
        yield base_url, proc, run_dir, parquet_root
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


@pytest.mark.skipif(not RUN_PLAYWRIGHT, reason="Set RUN_PLAYWRIGHT=1 to enable")
@pytest.mark.skipif(not RUN_DASHBOARD_E2E, reason="Set RUN_DASHBOARD_E2E=1 to enable")
def test_dashboard_all_panels_screenshots_e2e(dashboard_real, tmp_path: Path) -> None:
    base_url, _proc, run_dir, parquet_root = dashboard_real

    artifacts_root = Path(os.getenv("DASHBOARD_E2E_ARTIFACTS_DIR") or tmp_path)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    shots_dir = artifacts_root / "screens"
    shots_dir.mkdir(parents=True, exist_ok=True)

    domain = os.getenv("DASHBOARD_E2E_DOMAIN") or "iana.org"
    brave_query = os.getenv("DASHBOARD_E2E_BRAVE_QUERY") or "site:iana.org iana"

    sync_playwright = _import_sync_playwright()
    with sync_playwright() as p:
        browser = _launch_chromium(p)
        page = browser.new_page()

        console_lines: list[str] = []
        page_errors: list[str] = []
        page.on("console", lambda msg: console_lines.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        try:
            # 1) Wayback
            page.goto(f"{base_url}/", wait_until="domcontentloaded")
            page.wait_for_timeout(250)
            page.screenshot(path=str(shots_dir / "01_home.png"), full_page=True)

            search_url = f"{base_url}/?q={domain}&max_matches=5&parquet_root={parquet_root}"
            page.goto(search_url, wait_until="domcontentloaded")
            page.wait_for_selector("#status", timeout=20_000)
            page.wait_for_function(
                """() => {
                  const el = document.querySelector('#status');
                  if (!el) return false;
                  const t = (el.textContent || '').toLowerCase();
                  return t.includes('ok') || t.includes('error');
                }""",
                timeout=120_000,
            )
            page.screenshot(path=str(shots_dir / "02_wayback_search.png"), full_page=True)
            status_text = (page.locator("#status").inner_text() or "").lower()
            assert "ok" in status_text, f"Wayback search failed: {status_text}"

            page.wait_for_selector("a[href^='/record']", timeout=60_000)
            page.locator("a[href^='/record']").first.click()
            page.wait_for_url("**/record**", timeout=30_000)
            page.wait_for_selector("#recStatus", timeout=30_000)
            page.wait_for_function(
                "() => document.querySelector('#recStatus .badge.ok') || document.querySelector('#recStatus .badge.err')",
                timeout=180_000,
            )
            page.screenshot(path=str(shots_dir / "03_record.png"), full_page=True)
            rec_status = (page.locator("#recStatus").inner_text() or "").lower()
            assert "ok" in rec_status, f"Record fetch failed: {rec_status}"

            # 2) Settings (cache ops are local filesystem)
            # Seed cache files so clear-cache buttons do real work.
            (run_dir / "warc_cache").mkdir(parents=True, exist_ok=True)
            (run_dir / "warc_files").mkdir(parents=True, exist_ok=True)
            (run_dir / "warc_cache" / "a.bin").write_bytes(b"x" * 10)
            (run_dir / "warc_files" / "f.warc.gz").write_bytes(b"y" * 12)

            page.goto(f"{base_url}/settings", wait_until="domcontentloaded")
            _poll_text_contains(page, "#cacheStats", ["range_cache:", "cache stats error"], timeout_s=30.0)
            page.screenshot(path=str(shots_dir / "04_settings.png"), full_page=True)

            page.select_option("#default_cache_mode", "range")
            page.fill("#default_max_bytes", "12345")
            page.click("#saveBtn")
            _poll_text_contains(page, "#status", ["saved", "error"], timeout_s=20.0)
            page.screenshot(path=str(shots_dir / "05_settings_saved.png"), full_page=True)

            page.click("#clearRangeCacheBtn")
            _poll_text_contains(page, "#cacheStats", ["range_cache: 0 items", "clear error"], timeout_s=30.0)
            page.click("#clearFullCacheBtn")
            _poll_text_contains(page, "#cacheStats", ["full_warc_cache: 0 items", "clear error"], timeout_s=30.0)
            page.screenshot(path=str(shots_dir / "06_settings_cache_cleared.png"), full_page=True)

            # 3) Index (orchestrator console)
            page.goto(f"{base_url}/index", wait_until="domcontentloaded")
            page.wait_for_selector("#collectionsTbody tr", timeout=60_000)
            page.screenshot(path=str(shots_dir / "07_index_loaded.png"), full_page=True)

            page.locator("#collectionsTbody input[type='checkbox'][data-coll]").first.check()
            page.click("#btnBulkStatus")
            page.wait_for_function(
                "() => (document.getElementById('status')?.textContent || '').includes('collections')",
                timeout=120_000,
            )
            page.screenshot(path=str(shots_dir / "08_index_bulk_status.png"), full_page=True)

            page.click("#btnJobsList")
            page.wait_for_function(
                "() => (document.getElementById('status')?.textContent || '').includes('jobs_count')",
                timeout=30_000,
            )
            page.screenshot(path=str(shots_dir / "09_index_jobs.png"), full_page=True)

            # 4) Search (Brave) is optional because it requires a key.
            if (os.getenv("BRAVE_SEARCH_API_KEY") or "").strip():
                page.goto(f"{base_url}/discover?parquet_root={parquet_root}", wait_until="domcontentloaded")
                page.fill("#dq", brave_query)
                page.click("#discoverForm button[type='submit']")
                page.wait_for_function(
                    "() => document.querySelector('#dstatus .badge.ok') || document.querySelector('#dstatus .badge.err')",
                    timeout=120_000,
                )
                page.screenshot(path=str(shots_dir / "10_search_brave.png"), full_page=True)
                status_text = (page.locator("#dstatus").inner_text() or "").lower()
                assert "ok" in status_text, f"Brave search failed: {status_text}"
            else:
                page.goto(f"{base_url}/discover", wait_until="domcontentloaded")
                page.screenshot(path=str(shots_dir / "10_search_brave_skipped_no_key.png"), full_page=True)

            assert page_errors == [], f"Page errors: {page_errors}"
        finally:
            if console_lines:
                (artifacts_root / "browser_console.log").write_text("\n".join(console_lines))
            browser.close()
