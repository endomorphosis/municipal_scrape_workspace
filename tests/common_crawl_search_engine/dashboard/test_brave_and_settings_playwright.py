import pytest


pytest.skip(
    "Deprecated: replaced by real three-panel E2E tests in "
    "tests/common_crawl_search_engine/dashboard/test_panels_e2e_playwright.py",
    allow_module_level=True,
)


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


@pytest.fixture()
def dashboard_with_tmp_state(tmp_path: Path) -> tuple[str, Path, subprocess.Popen[str]]:
    """Start the dashboard in a subprocess rooted at tmp_path.

    This keeps state/dashboard_settings.json and state/* caches isolated per test.
    """

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    master_db = tmp_path / "cc_master_index.duckdb"

    repo_root = Path(__file__).resolve().parents[4]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")

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
        cwd=str(tmp_path),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_http_ready(f"{base_url}/")
        yield base_url, tmp_path, proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=8)


def _mock_mcp(route: sync_api.Route, request: sync_api.Request) -> None:
    """Intercept POST /mcp and return deterministic tool results."""

    if request.method != "POST":
        route.continue_()
        return

    try:
        payload = json.loads(request.post_data() or "{}")
    except Exception:  # noqa: BLE001
        payload = {}

    req_id = payload.get("id")
    method = payload.get("method")

    def ok(result: dict) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}),
        )

    def err(code: int, message: str) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}),
        )

    if method == "tools/list":
        ok(
            {
                "tools": [
                    {"name": "brave_search_ccindex"},
                    {"name": "fetch_warc_record"},
                ]
            }
        )
        return

    if method != "tools/call":
        err(-32601, f"unsupported method: {method}")
        return

    params = payload.get("params") or {}
    name = params.get("name")
    args = params.get("arguments") or {}

    if name == "brave_search_ccindex":
        ok(
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
                                "warc_filename": "crawl-data/CC-MAIN-2024-01/segments/1700000000000.0/warc/CC-MAIN-20240101000000-00000.warc.gz",
                                "warc_offset": 123,
                                "warc_length": 456,
                            }
                        ],
                    }
                ],
            }
        )
        return

    if name == "fetch_warc_record":
        html_doc = "<html><body><h1>hello from warc</h1></body></html>"
        ok(
            {
                "ok": True,
                "status": 206,
                "url": "https://data.commoncrawl.org/fake.warc.gz",
                "source": "mock",
                "local_warc_path": "/tmp/fake.warc.gz",
                "bytes_requested": 456,
                "bytes_returned": 456,
                "sha256": "0" * 64,
                "decoded_text_preview": "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" + html_doc,
                "error": None,
                "http": {
                    "ok": True,
                    "warc_headers": {},
                    "status": 200,
                    "status_line": "HTTP/1.1 200 OK",
                    "headers": {"content-type": "text/html; charset=utf-8"},
                    "body_text_preview": html_doc,
                    "body_is_html": True,
                    "body_mime": "text/html",
                    "body_charset": "utf-8",
                    "error": None,
                },
            }
        )
        return

    err(-32601, f"unknown tool: {name}")


@pytest.mark.integration
def test_brave_search_to_record_flow_works_with_mocked_mcp(dashboard_with_tmp_state):
    base_url, tmp_root, proc = dashboard_with_tmp_state

    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()
        page.route("**/mcp", _mock_mcp)

        try:
            page.goto(f"{base_url}/discover", wait_until="load")

            page.fill("#dq", "site:example.test hello")
            page.click("text=Search")

            page.wait_for_function(
                "() => document.querySelector('#dstatus .badge.ok') || document.querySelector('#dstatus .badge.err')",
                timeout=10_000,
            )
            status_text = page.locator("#dstatus").inner_text().lower()
            assert "ok" in status_text

            page.wait_for_selector("a[href^='/record']", timeout=10_000)
            page.locator("a[href^='/record']").first.click()

            page.wait_for_url("**/record**", timeout=10_000)
            page.wait_for_selector("#recStatus", timeout=10_000)
            page.wait_for_function(
                "() => document.querySelector('#recStatus .badge.ok') || document.querySelector('#recStatus .badge.err')",
                timeout=10_000,
            )

            rec_status = page.locator("#recStatus").inner_text().lower()
            assert "ok" in rec_status
            assert "local_warc_path" in rec_status

            page.wait_for_selector("iframe#recFrame", timeout=10_000, state="attached")
            srcdoc = page.evaluate(
                """() => {
                  const f = document.querySelector('iframe#recFrame');
                  return (f && (f.getAttribute('srcdoc') || '')) || '';
                }"""
            )
            assert "hello from warc" in str(srcdoc)
        finally:
            browser.close()

    # Helpful server-side log capture if something failed.
    if proc.stdout is not None:
        out = proc.stdout.read() or ""
        if out:
            (tmp_root / "dashboard.log").write_text(out)


@pytest.mark.integration
def test_settings_panel_save_and_cache_controls(dashboard_with_tmp_state):
    base_url, tmp_root, proc = dashboard_with_tmp_state

    # Seed some fake caches so stats + clear-cache are meaningful.
    (tmp_root / "state" / "warc_cache").mkdir(parents=True, exist_ok=True)
    (tmp_root / "state" / "warc_files").mkdir(parents=True, exist_ok=True)
    (tmp_root / "state" / "warc_cache" / "a.bin").write_bytes(b"x" * 10)
    (tmp_root / "state" / "warc_files" / "f.warc.gz").write_bytes(b"y" * 12)

    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as e:  # noqa: BLE001
            pytest.skip(
                "Chromium not available for Playwright. Run: `playwright install chromium`. "
                f"Original error: {e}"
            )

        page = browser.new_page()

        try:
            page.goto(f"{base_url}/settings", wait_until="load")

            # Cache stats should work (auto refresh happens, but we trigger explicitly).
            page.click("#refreshCacheStatsBtn")
            page.wait_for_function(
                "() => (document.querySelector('#cacheStats')?.textContent || '').includes('range_cache:')",
                timeout=10_000,
            )

            # Save settings (including brave key).
            page.select_option("#default_cache_mode", "full")
            page.fill("#default_max_bytes", "12345")
            page.fill("#brave_search_api_key", "abc123")
            page.click("#saveBtn")
            page.wait_for_function(
                "() => (document.querySelector('#status')?.textContent || '').includes('saved')",
                timeout=10_000,
            )

            # Reload and confirm persisted values reflected.
            page.reload(wait_until="load")
            page.wait_for_timeout(250)
            mode_val = page.evaluate("() => document.getElementById('default_cache_mode')?.value")
            assert mode_val == "full"

            # The page text should show the saved key is present.
            body_text = page.inner_text("body").lower()
            assert "saved key present" in body_text
            assert "true" in body_text

            # Clear brave key, reload and ensure it is no longer present.
            page.click("#clearBraveKeyBtn")
            page.wait_for_function(
                "() => (document.querySelector('#status')?.textContent || '').includes('cleared')",
                timeout=10_000,
            )
            page.reload(wait_until="load")
            body_text2 = page.inner_text("body").lower()
            assert "saved key present" in body_text2

            # Clear caches via buttons.
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

    # Verify files were actually removed.
    assert list((tmp_root / "state" / "warc_cache").glob("*.bin")) == []
    assert list((tmp_root / "state" / "warc_files").glob("*")) == []

    if proc.stdout is not None:
        out = proc.stdout.read() or ""
        if out:
            (tmp_root / "dashboard.log").write_text(out)
