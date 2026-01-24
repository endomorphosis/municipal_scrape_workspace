from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _write_lines(path: Path, lines: Iterable[str]) -> None:
    path.write_text("\n".join(str(x) for x in lines) + "\n")


def run_dashboard_analysis(
    *,
    output_dir: Path,
    domain: str,
    parquet_root: Path,
    master_db: Path,
    headless: bool = True,
    timeout_s: float = 60.0,
) -> bool:
    """Run an end-to-end dashboard analysis.

    What it checks:
    - Home page loads (no JS syntax errors)
    - Search runs and produces ok status
    - Clicking first record loads /record
    - /record triggers POST /mcp (fetch_warc_record)
    - /record status resolves ok and renders iframe+preview

    What it writes:
    - 01_home.png / 02_search.png / 03_record.png
    - browser_console.log (console + pageerror)
    - dashboard.log (server output)
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise SystemExit(
            "Playwright not installed. Install with: pip install playwright && playwright install\n"
            f"Import error: {e}"
        )

    if not Path(master_db).exists():
        _write_lines(output_dir / "analysis.txt", [f"FAIL: missing master_db: {master_db}"])
        return False
    if not Path(parquet_root).exists():
        _write_lines(output_dir / "analysis.txt", [f"FAIL: missing parquet_root: {parquet_root}"])
        return False

    port = _free_port()
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

    console_lines: list[str] = []
    findings: list[str] = []

    def wait_ready() -> None:
        import urllib.request

        deadline = time.time() + float(timeout_s)
        last_err: Exception | None = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(base_url + "/", timeout=2.0) as resp:
                    if 200 <= resp.status < 500:
                        return
            except Exception as e:  # noqa: BLE001
                last_err = e
            time.sleep(0.2)
        raise RuntimeError(f"dashboard not ready at {base_url}: {last_err}")

    ok = False
    try:
        wait_ready()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=bool(headless))
            page = browser.new_page()

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

            # 1) Home
            page.goto(base_url + "/", wait_until="domcontentloaded")
            page.wait_for_timeout(150)
            page.screenshot(path=str(output_dir / "01_home.png"), full_page=True)

            if any("pageerror:" in l for l in console_lines):
                findings.append("FAIL: page JS error on home")
                browser.close()
                ok = False
                return False

            # 2) Search
            search_url = (
                f"{base_url}/?q={domain}"
                f"&max_matches=10"
                f"&parquet_root={parquet_root}"
            )
            page.goto(search_url, wait_until="domcontentloaded")
            page.wait_for_selector("#status", timeout=int(timeout_s * 1000))
            page.wait_for_function(
                """() => {
                    const el = document.querySelector('#status');
                    if (!el) return false;
                    const t = (el.textContent || '').toLowerCase();
                    return t.includes('ok') || t.includes('error');
                }""",
                timeout=int(timeout_s * 1000),
            )
            page.screenshot(path=str(output_dir / "02_search.png"), full_page=True)
            status_text = (page.locator("#status").text_content() or "").strip().lower()
            if "error" in status_text:
                findings.append(f"FAIL: search error: {status_text}")
                browser.close()
                ok = False
                return False

            # 3) Record
            page.wait_for_selector("a[href^='/record']", timeout=int(timeout_s * 1000))
            page.locator("a[href^='/record']").first.click()
            page.wait_for_url("**/record**", timeout=int(timeout_s * 1000))
            page.wait_for_load_state("domcontentloaded")

            # Confirm record page triggers a POST /mcp
            page.wait_for_event(
                "response",
                predicate=lambda resp: resp.url.endswith("/mcp") and resp.request.method == "POST",
                timeout=int(timeout_s * 1000),
            )

            page.wait_for_selector("#recStatus", timeout=int(timeout_s * 1000))
            page.wait_for_function(
                """() => {
                    const el = document.querySelector('#recStatus');
                    if (!el) return false;
                    const t = (el.textContent || '').toLowerCase();
                    return t.includes('ok') || t.includes('error');
                }""",
                timeout=int(timeout_s * 1000),
            )

            page.screenshot(path=str(output_dir / "03_record.png"), full_page=True)
            rec_status = (page.locator("#recStatus").text_content() or "").strip().lower()
            (output_dir / "rec_status.txt").write_text(rec_status)
            if "error" in rec_status:
                findings.append(f"FAIL: record fetch error: {rec_status}")
                browser.close()
                ok = False
                return False

            # Validate record rendered something.
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
            if int(preview_len) <= 0:
                findings.append("FAIL: record preview empty")
                browser.close()
                ok = False
                return False
            if int(srcdoc_len) <= 20:
                findings.append("FAIL: iframe srcdoc not set")
                browser.close()
                ok = False
                return False

            browser.close()

        findings.append("PASS: dashboard flow ok")
        ok = True
        return True
    except Exception as e:
        findings.append(f"FAIL: exception: {type(e).__name__}: {e}")
        ok = False
        return False
    finally:
        if console_lines:
            _write_lines(output_dir / "browser_console.log", console_lines)

        if proc.stdout is not None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

            try:
                out = proc.stdout.read()
                if out:
                    (output_dir / "dashboard.log").write_text(out)
            except Exception:
                pass

        _write_lines(output_dir / "analysis.txt", findings)
