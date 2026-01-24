import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


RUN_PLAYWRIGHT = os.getenv("RUN_PLAYWRIGHT") == "1"
RUN_DASHBOARD_E2E = os.getenv("RUN_DASHBOARD_E2E") == "1"


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_ready(url: str, timeout_s: float = 15.0) -> None:
    import urllib.request

    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if 200 <= resp.status < 500:
                    return
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(0.2)
    raise RuntimeError(f"Dashboard not ready at {url}: {last_err}")


@pytest.mark.integration
@pytest.mark.skipif(not RUN_PLAYWRIGHT, reason="Set RUN_PLAYWRIGHT=1 to enable")
@pytest.mark.skipif(not RUN_DASHBOARD_E2E, reason="Set RUN_DASHBOARD_E2E=1 to enable")
def test_dashboard_waybackish_flow_screenshots(tmp_path: Path) -> None:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Playwright not installed: {e}")

    # Defaults expected to exist on this machine (validated earlier in this repo).
    master_db = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb")
    parquet_root = Path("/storage/ccindex_parquet")
    if not master_db.exists():
        pytest.skip(f"Missing master db: {master_db}")
    if not parquet_root.exists():
        pytest.skip(f"Missing parquet root: {parquet_root}")

    port = _get_free_port()
    base_url = f"http://127.0.0.1:{port}"

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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    shots_dir = tmp_path / "screens"
    shots_dir.mkdir(parents=True, exist_ok=True)

    try:
        _wait_ready(base_url + "/")

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()

            console_lines: list[str] = []

            def _on_console(msg):
                try:
                    console_lines.append(f"console:{msg.type}:{msg.text}")
                except Exception:
                    pass

            def _on_pageerror(err):
                try:
                    console_lines.append(f"pageerror:{err}")
                except Exception:
                    pass

            page.on("console", _on_console)
            page.on("pageerror", _on_pageerror)

            try:
                # 1) Load home page
                page.goto(base_url + "/", wait_until="domcontentloaded")
                page.wait_for_timeout(250)
                page.screenshot(path=str(shots_dir / "01_home.png"), full_page=True)

                # 2) Perform search via query params (more deterministic than typing)
                domain = os.getenv("DASHBOARD_E2E_DOMAIN") or "iana.org"
                search_url = (
                    f"{base_url}/?q={domain}"
                    f"&max_matches=5"
                    f"&parquet_root={parquet_root}"
                )
                page.goto(search_url, wait_until="domcontentloaded")

                # Status badge should resolve to ok or error.
                page.wait_for_selector("#status", timeout=15000)
                page.wait_for_function(
                    """() => {
                        const el = document.querySelector('#status');
                        if (!el) return false;
                        const t = (el.textContent || '').trim().toLowerCase();
                        return t.includes('ok') || t.includes('error');
                    }""",
                    timeout=30000,
                )
                page.screenshot(path=str(shots_dir / "02_search.png"), full_page=True)

                status_text = (page.locator("#status").text_content() or "").strip().lower()
                assert status_text, "Missing status text"

                if "error" in status_text:
                    # Save a little more context to help debug without opening screenshots.
                    (tmp_path / "page_text.txt").write_text(page.inner_text("body"))
                    pytest.fail(
                        f"Search reported error (see screenshots in {shots_dir}, context in {tmp_path/'page_text.txt'})"
                    )

                # 3) Click first 'view record' link and ensure iframe render exists.
                page.wait_for_selector("a[href^='/record']", timeout=20000)
                first = page.locator("a[href^='/record']").first
                assert first.count() == 1
                first.click()
                page.wait_for_url("**/record**", timeout=20000)
                page.wait_for_load_state("domcontentloaded")

                # Wait until the record page actually attempts the MCP fetch.
                page.wait_for_event(
                    "response",
                    predicate=lambda resp: resp.url.endswith("/mcp") and resp.request.method == "POST",
                    timeout=60000,
                )

                # Wait for record fetch to resolve (ok/error), then validate render/preview.
                page.wait_for_selector("#recStatus", timeout=20000)
                page.wait_for_function(
                    """() => {
                        const el = document.querySelector('#recStatus');
                        if (!el) return false;
                        const t = (el.textContent || '').toLowerCase();
                        return t.includes('ok') || t.includes('error');
                    }""",
                    timeout=60000,
                )

                page.screenshot(path=str(shots_dir / "03_record.png"), full_page=True)

                rec_status = (page.locator("#recStatus").text_content() or "").strip().lower()
                (tmp_path / "rec_status.txt").write_text(rec_status)
                if "error" in rec_status:
                    (tmp_path / "record_page_text.txt").write_text(page.inner_text("body"))
                    pytest.fail(
                        f"Record fetch reported error (see {shots_dir}, status in {tmp_path/'rec_status.txt'})"
                    )

                page.wait_for_selector("iframe#recFrame", timeout=20000, state="attached")
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
            finally:
                if console_lines:
                    (tmp_path / "browser_console.log").write_text("\n".join(console_lines))
                browser.close()

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            proc.kill()

        # If the test failed, dumping the dashboard output helps.
        if proc.stdout is not None:
            out = proc.stdout.read()
            if out:
                (tmp_path / "dashboard.log").write_text(out)
