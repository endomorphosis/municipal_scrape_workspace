import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest


# These are opt-in integration tests: they require Playwright + a browser install.
# Enable with:
#   RUN_PLAYWRIGHT=1 pytest -q -m integration
if os.environ.get("RUN_PLAYWRIGHT") != "1":
    pytest.skip("Playwright tests disabled. Set RUN_PLAYWRIGHT=1 to enable.", allow_module_level=True)


sync_api = pytest.importorskip("playwright.sync_api")


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


@pytest.fixture(scope="session")
def dashboard_base_url(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Start the dashboard in a subprocess and return its base URL."""

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    # Point the dashboard at a throwaway DB path. This may not contain required
    # tables, but the UI + /mcp endpoint should still load and report errors
    # cleanly.
    tmp_dir = tmp_path_factory.mktemp("ccindex_dashboard")
    master_db = Path(tmp_dir) / "cc_master_index.duckdb"

    cmd = [
        sys.executable,
        "-m",
        "common_crawl_search_engine.dashboard",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--master-db",
        str(master_db),
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_http_ready(f"{base_url}/")
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=8)


@pytest.mark.integration
def test_dashboard_home_loads_without_js_errors(dashboard_base_url: str) -> None:
    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        console_errors: list[str] = []
        page_errors: list[str] = []

        def on_console(msg):
            if msg.type == "error":
                console_errors.append(msg.text)

        page.on("console", on_console)
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        page.goto(f"{dashboard_base_url}/", wait_until="load")

        assert page.get_by_role("heading", name="ccindex").is_visible()
        assert "Enter a domain" in page.locator("#status").inner_text()

        assert page_errors == [], f"Page errors: {page_errors}"
        assert console_errors == [], f"Console errors: {console_errors}"

        browser.close()


@pytest.mark.integration
def test_dashboard_serves_sdk_js(dashboard_base_url: str) -> None:
    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        resp = page.request.get(f"{dashboard_base_url}/static/ccindex-mcp-sdk.js")
        assert resp.ok
        text = resp.text()
        assert "CcindexMcpClient" in text
        browser.close()


@pytest.mark.integration
def test_dashboard_mcp_tools_list_works(dashboard_base_url: str) -> None:
    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        page.goto(f"{dashboard_base_url}/", wait_until="load")

        # Use the browser context to POST JSON-RPC to /mcp.
        result = page.evaluate(
            """async (baseUrl) => {
              const resp = await fetch(baseUrl + '/mcp', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }),
              });
              return await resp.json();
            }""",
            dashboard_base_url,
        )

        assert result.get("jsonrpc") == "2.0"
        assert result.get("id") == 1
        tools = (result.get("result") or {}).get("tools") or []
        names = {t.get("name") for t in tools}
        assert "search_domain_meta" in names
        assert "fetch_warc_record" in names

        browser.close()


@pytest.mark.integration
def test_dashboard_search_reports_result_or_error(dashboard_base_url: str) -> None:
    """Run a search and assert the UI reports *something* (ok or error).

    This catches regressions where the JS cannot reach /mcp at all (SDK import fails,
    fetch fails, CORS issues, etc.).
    """

    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        page.goto(f"{dashboard_base_url}/", wait_until="load")

        page.fill("#q", "example.com")
        page.click("text=Search")

        page.wait_for_function(
            "() => document.querySelector('#status .badge.ok') || document.querySelector('#status .badge.err')",
            timeout=8_000,
        )

        status_text = page.locator("#status").inner_text().lower()
        assert ("ok" in status_text) or ("error" in status_text)

        browser.close()
