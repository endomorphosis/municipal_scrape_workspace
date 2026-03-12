import json
import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path("/home/barberb/municipal_scrape_workspace")


def _base_env() -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("PATH", os.environ.get("PATH", ""))
    return env


def test_publish_wrapper_preserves_inline_env_overrides(tmp_path):
    repo_root = _repo_root()
    local_dir = tmp_path / "state_admin_rules_parquet_cid"
    local_dir.mkdir(parents=True)
    (local_dir / "STATE-NY.parquet").write_bytes(b"PAR1testPAR1")
    (local_dir / "manifest.parquet.json").write_text("{}", encoding="utf-8")

    env = _base_env()
    env.update(
        {
            "LEGAL_PUBLISH_CORPUS": "state_admin_rules",
            "LEGAL_PUBLISH_LOCAL_DIR": str(local_dir),
            "LEGAL_PUBLISH_DRY_RUN": "1",
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_publish_canonical_legal_corpus.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    report = json.loads(result.stdout)
    assert report["status"] == "dry_run"
    assert report["corpus"] == "state_admin_rules"
    assert report["local_dir"] == str(local_dir)
    assert report["repo_id"] == "justicedao/ipfs_state_admin_rules"


def test_daemon_wrapper_preserves_inline_env_overrides():
    repo_root = _repo_root()
    env = _base_env()
    env.update(
        {
            "LEGAL_DAEMON_PRINT_RELEASE_PLAN": "1",
            "LEGAL_DAEMON_CORPUS": "state_admin_rules",
            "LEGAL_DAEMON_STATES": "CA",
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    report = json.loads(result.stdout)
    assert report["status"] == "planned"
    assert report["preview"] is True
    assert report["corpus"] == "state_admin_rules"
    merge_command = report["commands"][0]["command"]
    publish_command = report["commands"][3]["command"]
    assert "merge_state_admin_runs.py" in merge_command
    assert "--state CA" in merge_command
    assert "run_publish_canonical_legal_corpus.sh" in publish_command
    assert "LEGAL_PUBLISH_CORPUS=state_admin_rules" in publish_command


def test_daemon_wrapper_forwards_router_timeout_env_vars(tmp_path):
    repo_root = _repo_root()
    probe = tmp_path / "argv_probe.py"
    probe.write_text(
        """
#!/usr/bin/env python3
import json
import sys

print(json.dumps({"argv": sys.argv[1:]}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    probe.chmod(0o755)

    env = _base_env()
    env.update(
        {
            "LEGAL_DAEMON_PYTHON_BIN": str(probe),
            "LEGAL_DAEMON_ROUTER_LLM_TIMEOUT_SECONDS": "31",
            "LEGAL_DAEMON_ROUTER_EMBEDDINGS_TIMEOUT_SECONDS": "17",
            "LEGAL_DAEMON_ROUTER_IPFS_TIMEOUT_SECONDS": "19",
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    argv = json.loads(result.stdout)["argv"]
    assert "--router-llm-timeout-seconds" in argv
    assert argv[argv.index("--router-llm-timeout-seconds") + 1] == "31"
    assert "--router-embeddings-timeout-seconds" in argv
    assert argv[argv.index("--router-embeddings-timeout-seconds") + 1] == "17"
    assert "--router-ipfs-timeout-seconds" in argv
    assert argv[argv.index("--router-ipfs-timeout-seconds") + 1] == "19"
    
def test_daemon_wrapper_forwards_scrape_timeout_override(tmp_path):
    repo_root = _repo_root()
    probe = tmp_path / "argv_probe.py"
    probe.write_text(
        """
#!/usr/bin/env python3
import json
import sys

print(json.dumps({"argv": sys.argv[1:]}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    probe.chmod(0o755)

    env = _base_env()
    env.update(
        {
            "LEGAL_DAEMON_PYTHON_BIN": str(probe),
            "LEGAL_DAEMON_SCRAPE_TIMEOUT_SECONDS": "181",
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    argv = json.loads(result.stdout)["argv"]
    assert "--scrape-timeout-seconds" in argv
    assert argv[argv.index("--scrape-timeout-seconds") + 1] == "181"


def test_daemon_wrapper_preserves_cloudflare_env_overrides(tmp_path):
    repo_root = _repo_root()
    probe = tmp_path / "env_probe.py"
    probe.write_text(
        """
#!/usr/bin/env python3
import json
import os

keys = [
    "CLOUDFLARE_ACCOUNT_ID",
    "CLOUDFLARE_API_TOKEN",
    "IPFS_DATASETS_CLOUDFLARE_CRAWL_TIMEOUT_SECONDS",
    "LEGAL_SCRAPER_CLOUDFLARE_CRAWL_MAX_RATE_LIMIT_WAIT_SECONDS",
    "LEGAL_SCRAPER_CLOUDFLARE_CRAWL_FORMATS",
]
print(json.dumps({key: os.environ.get(key) for key in keys}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    probe.chmod(0o755)

    env = _base_env()
    env.update(
        {
            "LEGAL_DAEMON_PYTHON_BIN": str(probe),
            "CLOUDFLARE_ACCOUNT_ID": "acct-inline",
            "CLOUDFLARE_API_TOKEN": "token-inline",
            "IPFS_DATASETS_CLOUDFLARE_CRAWL_TIMEOUT_SECONDS": "77",
            "LEGAL_SCRAPER_CLOUDFLARE_CRAWL_MAX_RATE_LIMIT_WAIT_SECONDS": "900",
            "LEGAL_SCRAPER_CLOUDFLARE_CRAWL_FORMATS": "markdown,html",
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["CLOUDFLARE_ACCOUNT_ID"] == "acct-inline"
    assert payload["CLOUDFLARE_API_TOKEN"] == "token-inline"
    assert payload["IPFS_DATASETS_CLOUDFLARE_CRAWL_TIMEOUT_SECONDS"] == "77"
    assert payload["LEGAL_SCRAPER_CLOUDFLARE_CRAWL_MAX_RATE_LIMIT_WAIT_SECONDS"] == "900"
    assert payload["LEGAL_SCRAPER_CLOUDFLARE_CRAWL_FORMATS"] == "markdown,html"


def test_daemon_wrapper_summarizes_pending_retry_to_stderr(tmp_path):
    repo_root = _repo_root()
    probe = tmp_path / "pending_retry_probe.py"
    probe.write_text(
        """
#!/usr/bin/env python3
import json

print(json.dumps({
    "status": "success",
    "pending_retry": {
        "provider": "cloudflare_browser_rendering",
        "retry_after_seconds": 321,
        "retry_at_utc": "2026-03-12T12:34:56+00:00",
        "reason": "cloudflare_browser_rendering_rate_limited"
    }
}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    probe.chmod(0o755)

    env = _base_env()
    env.update(
        {
            "LEGAL_DAEMON_PYTHON_BIN": str(probe),
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["pending_retry"]["provider"] == "cloudflare_browser_rendering"
    assert "pending_retry scheduled: provider=cloudflare_browser_rendering" in result.stderr
    assert "retry_after_seconds=321" in result.stderr
    assert "retry_at_utc=2026-03-12T12:34:56+00:00" in result.stderr


def test_daemon_wrapper_summarizes_tactic_selection_to_stderr(tmp_path):
    repo_root = _repo_root()
    probe = tmp_path / "tactic_selection_probe.py"
    probe.write_text(
        """
#!/usr/bin/env python3
import json

print(json.dumps({
    "status": "success",
    "latest_cycle": {
        "cycle_state_order": ["AZ", "WA", "CA"],
        "tactic_selection": {
            "selected_tactic": "document_first",
            "mode": "exploit",
            "priority_states": ["AZ", "WA"]
        }
    }
}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    probe.chmod(0o755)

    env = _base_env()
    env.update(
        {
            "LEGAL_DAEMON_PYTHON_BIN": str(probe),
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_legal_daemon.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["latest_cycle"]["tactic_selection"]["selected_tactic"] == "document_first"
    assert "tactic_selection: selected=document_first" in result.stderr
    assert "mode=exploit" in result.stderr
    assert "priority_states=AZ,WA" in result.stderr
    assert "cycle_state_order=AZ,WA,CA" in result.stderr


def test_pending_retry_watch_wrapper_forwards_env_to_reporter(tmp_path):
    repo_root = _repo_root()
    stub = tmp_path / "python3"
    stub.write_text(
    f"""
#!{sys.executable}
import json
import sys

print(json.dumps({{"argv": sys.argv[1:]}}))
""".strip()
        + "\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    env = _base_env()
    env.update(
        {
            "PATH": f"{tmp_path}:{env.get('PATH', '')}",
            "LEGAL_DAEMON_PENDING_RETRY_CORPUS": "state_admin_rules",
            "LEGAL_DAEMON_PENDING_RETRY_OUTPUT_DIR": str(tmp_path / "daemon_out"),
            "LEGAL_DAEMON_PENDING_RETRY_WATCH": "1",
            "LEGAL_DAEMON_PENDING_RETRY_INTERVAL_SECONDS": "17",
            "LEGAL_DAEMON_PENDING_RETRY_MAX_REPORTS": "4",
        }
    )

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/ops/legal_data/run_agentic_daemon_pending_retry_watch.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )

    argv = json.loads(result.stdout)["argv"]
    assert argv[0].endswith("scripts/ops/legal_data/report_agentic_daemon_pending_retry.py")
    assert "--corpus" in argv
    assert argv[argv.index("--corpus") + 1] == "state_admin_rules"
    assert "--daemon-output-dir" in argv
    assert argv[argv.index("--daemon-output-dir") + 1] == str(tmp_path / "daemon_out")
    assert "--watch" in argv
    assert "--interval-seconds" in argv
    assert argv[argv.index("--interval-seconds") + 1] == "17"
    assert "--max-reports" in argv
    assert argv[argv.index("--max-reports") + 1] == "4"