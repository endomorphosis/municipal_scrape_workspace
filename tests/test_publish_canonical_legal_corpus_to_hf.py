import importlib.util
import json
import sys
from pathlib import Path


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_publish_canonical_legal_corpus_to_hf_dry_run(tmp_path, monkeypatch, capsys):
    module = _load_module(
        "publish_canonical_legal_corpus_to_hf_test",
        Path("/home/barberb/municipal_scrape_workspace/scripts/ops/legal_data/publish_canonical_legal_corpus_to_hf.py"),
    )

    local_dir = tmp_path / "state_laws_parquet_cid"
    local_dir.mkdir(parents=True)
    (local_dir / "STATE-NY.parquet").write_bytes(b"PAR1testPAR1")
    (local_dir / "state_laws_all_states.parquet").write_bytes(b"PAR1testPAR1")
    (local_dir / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publish_canonical_legal_corpus_to_hf.py",
            "--corpus",
            "state_laws",
            "--local-dir",
            str(local_dir),
            "--dry-run",
        ],
    )

    assert module.main() == 0

    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "dry_run"
    assert report["corpus"] == "state_laws"
    assert report["repo_id"] == "justicedao/ipfs_state_laws"
    assert report["cid_column"] == "ipfs_cid"
    assert report["counts"]["parquet"] == 2
    assert "STATE-NY.parquet" in report["sample_files"]["parquet"]