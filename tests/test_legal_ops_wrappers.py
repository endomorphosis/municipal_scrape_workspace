import json
import os
import subprocess
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