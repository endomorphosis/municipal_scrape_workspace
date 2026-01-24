import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest


RUN_PLAYWRIGHT = os.getenv("RUN_PLAYWRIGHT") == "1"
RUN_DASHBOARD_E2E = os.getenv("RUN_DASHBOARD_E2E") == "1"


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_http_ready(url: str, timeout_s: float = 15.0) -> None:
    deadline = time.time() + timeout_s
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


def _require_real_ccindex_assets() -> tuple[Path, Path]:
    master_db = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb")
    parquet_root = Path("/storage/ccindex_parquet")
    if not master_db.exists():
        pytest.skip(f"Missing master db: {master_db}")
    if not parquet_root.exists():
        pytest.skip(f"Missing parquet root: {parquet_root}")
    return master_db, parquet_root


@pytest.fixture()
def dashboard_real(tmp_path: Path) -> tuple[str, subprocess.Popen[str], Path, Path, Path]:
    """Start dashboard with real /storage datasets, isolated state in tmp_path."""

    master_db, parquet_root = _require_real_ccindex_assets()

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    # Isolate caches inside tmp_path to avoid polluting repo state.
    env["CCINDEX_WARC_CACHE_DIR"] = str(tmp_path / "warc_cache")
    env["CCINDEX_FULL_WARC_CACHE_DIR"] = str(tmp_path / "warc_files")

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
        cwd=str(tmp_path),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_http_ready(base_url + "/")
        yield base_url, proc, tmp_path, master_db, parquet_root
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=8)

        # Safe to read stdout only after the process has exited.
        if proc.stdout is not None:
            try:
                out = proc.stdout.read() or ""
            except Exception:  # noqa: BLE001
                out = ""
            if out:
                (tmp_path / "dashboard_stdout.log").write_text(out)


@pytest.mark.integration
@pytest.mark.skipif(not RUN_PLAYWRIGHT, reason="Set RUN_PLAYWRIGHT=1 to enable")
@pytest.mark.skipif(not RUN_DASHBOARD_E2E, reason="Set RUN_DASHBOARD_E2E=1 to enable")
def test_wayback_panel_e2e(dashboard_real, tmp_path: Path) -> None:
    base_url, proc, run_dir, _master_db, parquet_root = dashboard_real

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Playwright not installed: {e}")

    domain = os.getenv("DASHBOARD_E2E_DOMAIN") or "iana.org"

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        console_lines: list[str] = []
        page_errors: list[str] = []

        page.on("console", lambda msg: console_lines.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        try:
            search_url = f"{base_url}/?q={domain}&max_matches=5&parquet_root={parquet_root}"
            page.goto(search_url, wait_until="domcontentloaded")

            page.wait_for_selector("#status", timeout=15_000)
            page.wait_for_function(
                """() => {
                  const el = document.querySelector('#status');
                  if (!el) return false;
                  const t = (el.textContent || '').toLowerCase();
                  return t.includes('ok') || t.includes('error');
                }""",
                timeout=60_000,
            )

            status_text = (page.locator("#status").inner_text() or "").lower()
            assert "ok" in status_text, f"Wayback panel search failed: {status_text}"

            page.wait_for_selector("a[href^='/record']", timeout=20_000)
            page.locator("a[href^='/record']").first.click()

            page.wait_for_url("**/record**", timeout=20_000)
            page.wait_for_event(
                "response",
                predicate=lambda resp: resp.url.endswith("/mcp") and resp.request.method == "POST",
                timeout=60_000,
            )

            page.wait_for_selector("#recStatus", timeout=20_000)
            page.wait_for_function(
                "() => document.querySelector('#recStatus .badge.ok') || document.querySelector('#recStatus .badge.err')",
                timeout=60_000,
            )

            rec_status = (page.locator("#recStatus").inner_text() or "").lower()
            assert "ok" in rec_status, f"Record fetch failed: {rec_status}"

            page.wait_for_selector("iframe#recFrame", timeout=20_000, state="attached")
            srcdoc_len = page.evaluate(
                """() => {
                  const f = document.querySelector('iframe#recFrame');
                  return (f && (f.getAttribute('srcdoc') || '').length) || 0;
                }"""
            )
            preview_len = page.evaluate(
                """() => {
                  const p = document.querySelector('#recPreview');
                  return (p && (p.textContent || '').length) || 0;
                }"""
            )
            assert int(srcdoc_len) > 20
            assert int(preview_len) > 0

            assert page_errors == [], f"Page errors: {page_errors}"
        finally:
            if console_lines:
                (tmp_path / "wayback_console.log").write_text("\n".join(console_lines))
            browser.close()

    # Dashboard stdout is captured by the fixture teardown.


@pytest.mark.integration
@pytest.mark.skipif(not RUN_PLAYWRIGHT, reason="Set RUN_PLAYWRIGHT=1 to enable")
@pytest.mark.skipif(not RUN_DASHBOARD_E2E, reason="Set RUN_DASHBOARD_E2E=1 to enable")
def test_search_panel_brave_to_record_e2e(dashboard_real, tmp_path: Path) -> None:
    base_url, proc, run_dir, _master_db, parquet_root = dashboard_real

    if not (os.getenv("BRAVE_SEARCH_API_KEY") or "").strip():
        pytest.skip("BRAVE_SEARCH_API_KEY not set (required for real Brave search)")

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Playwright not installed: {e}")

    query = os.getenv("DASHBOARD_E2E_BRAVE_QUERY") or "site:iana.org iana"

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        console_lines: list[str] = []
        page_errors: list[str] = []

        page.on("console", lambda msg: console_lines.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        try:
            page.goto(f"{base_url}/discover?parquet_root={parquet_root}", wait_until="domcontentloaded")

            page.fill("#dq", query)
            page.click("#discoverForm button[type='submit']")

            page.wait_for_function(
                "() => document.querySelector('#dstatus .badge.ok') || document.querySelector('#dstatus .badge.err')",
                timeout=60_000,
            )

            status_text = (page.locator("#dstatus").inner_text() or "").lower()
            assert "ok" in status_text, f"Search panel failed: {status_text}"

            page.wait_for_selector("a[href^='/record']", timeout=60_000)
            page.locator("a[href^='/record']").first.click()

            page.wait_for_url("**/record**", timeout=20_000)
            page.wait_for_function(
                "() => document.querySelector('#recStatus .badge.ok') || document.querySelector('#recStatus .badge.err')",
                timeout=60_000,
            )

            rec_status = (page.locator("#recStatus").inner_text() or "").lower()
            assert "ok" in rec_status, f"Record fetch failed: {rec_status}"

            assert page_errors == [], f"Page errors: {page_errors}"
        finally:
            if console_lines:
                (tmp_path / "search_console.log").write_text("\n".join(console_lines))
            browser.close()

    # Dashboard stdout is captured by the fixture teardown.


@pytest.mark.integration
@pytest.mark.skipif(not RUN_PLAYWRIGHT, reason="Set RUN_PLAYWRIGHT=1 to enable")
@pytest.mark.skipif(not RUN_DASHBOARD_E2E, reason="Set RUN_DASHBOARD_E2E=1 to enable")
def test_settings_panel_save_and_cache_clear_affects_record_defaults(dashboard_real, tmp_path: Path) -> None:
    base_url, proc, run_dir, _master_db, parquet_root = dashboard_real

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Playwright not installed: {e}")

    # Seed fake cache files (stats/clear are purely filesystem operations).
    range_cache = run_dir / "warc_cache"
    full_cache = run_dir / "warc_files"
    range_cache.mkdir(parents=True, exist_ok=True)
    full_cache.mkdir(parents=True, exist_ok=True)
    (range_cache / "a.bin").write_bytes(b"x" * 10)
    (full_cache / "f.warc.gz").write_bytes(b"y" * 12)

    domain = os.getenv("DASHBOARD_E2E_DOMAIN") or "iana.org"

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        try:
            page.goto(f"{base_url}/settings", wait_until="domcontentloaded")

            page.click("#refreshCacheStatsBtn")
            page.wait_for_function(
                "() => (document.querySelector('#cacheStats')?.textContent || '').includes('range_cache:')",
                timeout=10_000,
            )

            page.select_option("#default_cache_mode", "full")
            page.fill("#default_max_bytes", "12345")
            page.click("#saveBtn")
            page.wait_for_function(
                "() => (document.querySelector('#status')?.textContent || '').includes('saved')",
                timeout=10_000,
            )

            page.click("#clearRangeCacheBtn")
            page.wait_for_function(
                """() => {
  const t = (document.querySelector('#cacheStats')?.textContent || '');
  return t.includes('range_cache:') && t.includes('0 items');
}""",
                timeout=10_000,
            )
            page.click("#clearFullCacheBtn")
            page.wait_for_function(
                """() => {
  const t = (document.querySelector('#cacheStats')?.textContent || '');
  return t.includes('full_warc_cache:') && t.includes('0 items');
}""",
                timeout=10_000,
            )

            # Verify files were actually removed before doing any fetch that might repopulate caches.
            assert list(range_cache.glob("*.bin")) == []
            assert list(full_cache.glob("*")) == []

            # Verify settings affect the Wayback/record panel defaults.
            search_url = f"{base_url}/?q={domain}&max_matches=3&parquet_root={parquet_root}"
            page.goto(search_url, wait_until="domcontentloaded")
            page.wait_for_function(
                """() => {
                  const el = document.querySelector('#status');
                  if (!el) return false;
                  const t = (el.textContent || '').toLowerCase();
                  return t.includes('ok') || t.includes('error');
                }""",
                timeout=60_000,
            )

            status_text = (page.locator("#status").inner_text() or "").lower()
            assert "ok" in status_text

            page.wait_for_selector("a[href^='/record']", timeout=20_000)
            page.locator("a[href^='/record']").first.click()
            page.wait_for_url("**/record**", timeout=20_000)

            page.wait_for_selector("#max_bytes", timeout=20_000)
            max_bytes_val = page.evaluate("() => document.getElementById('max_bytes')?.value")
            assert str(max_bytes_val) == "12345"

            cache_mode_val = page.evaluate("() => document.getElementById('cache_mode')?.value")
            assert str(cache_mode_val) == "full"
        finally:
            browser.close()

    # Note: later navigation may legitimately repopulate caches (especially in full mode).

    # Dashboard stdout is captured by the fixture teardown.
