from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_ready(url: str, timeout_s: float = 15.0) -> None:
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
def dashboard_subprocess(tmp_path: Path) -> tuple[str, subprocess.Popen[str]]:
    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    # The dashboard CLI wants a master-db path; for MCP tool tests we don't
    # need it to exist, but creating an empty file avoids any future existence checks.
    master_db = tmp_path / "cc_master_index.duckdb"
    master_db.write_bytes(b"")

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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_ready(base_url + "/healthz")
        yield base_url, proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=8)

        if proc.stdout is not None:
            out = proc.stdout.read() or ""
            if out:
                (tmp_path / "dashboard_stdout.log").write_text(out, encoding="utf-8")


def test_python_mcp_client_can_list_tools(dashboard_subprocess: tuple[str, subprocess.Popen[str]]) -> None:
    base_url, _proc = dashboard_subprocess

    from common_crawl_search_engine.mcp_client import CcindexMcpClient

    c = CcindexMcpClient(endpoint=base_url)  # relies on normalize_mcp_endpoint
    tools = c.list_tools()
    assert isinstance(tools, dict)
    assert "tools" in tools

    names = {t.get("name") for t in (tools.get("tools") or []) if isinstance(t, dict)}
    assert "orchestrator_settings_get" in names
    assert "cc_collinfo_list" in names


def test_python_mcp_client_can_call_tool(dashboard_subprocess: tuple[str, subprocess.Popen[str]]) -> None:
    base_url, _proc = dashboard_subprocess

    from common_crawl_search_engine.mcp_client import CcindexMcpClient

    c = CcindexMcpClient(endpoint=base_url)
    out = c.call_tool("orchestrator_settings_get", {})
    assert isinstance(out, dict)


def test_cli_can_call_remote_mcp_tools(dashboard_subprocess: tuple[str, subprocess.Popen[str]]) -> None:
    base_url, _proc = dashboard_subprocess

    # Exercise the new generic remote subcommands.
    raw = subprocess.check_output(
        [
            sys.executable,
            "-m",
            "common_crawl_search_engine.cli",
            "mcp",
            "tools",
            "--endpoint",
            base_url,
        ],
        text=True,
        timeout=20,
    )
    obj = json.loads(raw)
    assert isinstance(obj, dict)
    assert "tools" in obj


def test_cli_index_settings_get_remote(dashboard_subprocess: tuple[str, subprocess.Popen[str]]) -> None:
    base_url, _proc = dashboard_subprocess

    raw = subprocess.check_output(
        [
            sys.executable,
            "-m",
            "common_crawl_search_engine.cli",
            "index",
            "--endpoint",
            base_url,
            "settings-get",
        ],
        text=True,
        timeout=20,
    )
    obj = json.loads(raw)
    assert isinstance(obj, dict)
