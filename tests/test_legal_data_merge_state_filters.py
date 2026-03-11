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


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_merge_state_admin_runs_filters_selected_state(tmp_path, monkeypatch):
    module = _load_module(
        "merge_state_admin_runs_test",
        Path("/home/barberb/municipal_scrape_workspace/scripts/ops/legal_data/merge_state_admin_runs.py"),
    )

    input_root = tmp_path / "artifacts" / "state_admin_rules" / "run_a"
    _write_jsonl(
        input_root / "STATE-NY.jsonld",
        [
            {
                "identifier": "NY-rule-1",
                "name": "NY Rule 1",
                "text": "New York administrative rule text",
                "legislationJurisdiction": "US-NY",
                "sourceUrl": "https://example.org/ny/rule-1",
            }
        ],
    )
    _write_jsonl(
        input_root / "STATE-CA.jsonld",
        [
            {
                "identifier": "CA-rule-1",
                "name": "CA Rule 1",
                "text": "California administrative rule text",
                "legislationJurisdiction": "US-CA",
                "sourceUrl": "https://example.org/ca/rule-1",
            }
        ],
    )

    output_dir = tmp_path / "out_admin"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "merge_state_admin_runs.py",
            "--input-root",
            str(input_root),
            "--output-dir",
            str(output_dir),
            "--state",
            "NY",
        ],
    )

    assert module.main() == 0

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["selected_states"] == ["NY"]
    assert manifest["totals"]["states_seen"] == 1
    assert sorted(manifest["states"].keys()) == ["NY"]
    assert (output_dir / "state_admin_rules_jsonld" / "STATE-NY.jsonld").exists()
    assert not (output_dir / "state_admin_rules_jsonld" / "STATE-CA.jsonld").exists()


def test_merge_state_laws_runs_filters_selected_state(tmp_path, monkeypatch):
    module = _load_module(
        "merge_state_laws_runs_test",
        Path("/home/barberb/municipal_scrape_workspace/scripts/ops/legal_data/merge_state_laws_runs.py"),
    )

    input_root = tmp_path / "state_laws_fixture"
    _write_jsonl(
        input_root / "STATE-NY.jsonld",
        [
            {
                "identifier": "NY-1",
                "name": "NY Statute",
                "text": "New York statute text",
                "legislationJurisdiction": "US-NY",
                "sourceUrl": "https://example.org/ny/statute-1",
            }
        ],
    )
    _write_jsonl(
        input_root / "STATE-CA.jsonld",
        [
            {
                "identifier": "CA-1",
                "name": "CA Statute",
                "text": "California statute text",
                "legislationJurisdiction": "US-CA",
                "sourceUrl": "https://example.org/ca/statute-1",
            }
        ],
    )

    output_dir = tmp_path / "out_laws"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "merge_state_laws_runs.py",
            "--input-root",
            str(input_root),
            "--output-dir",
            str(output_dir),
            "--state",
            "NY",
        ],
    )

    assert module.main() == 0

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["selected_states"] == ["NY"]
    assert manifest["totals"]["states_seen"] == 1
    assert sorted(manifest["states"].keys()) == ["NY"]
    assert (output_dir / "state_laws_jsonld" / "STATE-NY.jsonld").exists()
    assert not (output_dir / "state_laws_jsonld" / "STATE-CA.jsonld").exists()


def test_merge_state_court_rules_runs_filters_selected_state(tmp_path, monkeypatch):
    module = _load_module(
        "merge_state_court_rules_runs_test",
        Path("/home/barberb/municipal_scrape_workspace/scripts/ops/legal_data/merge_state_court_rules_runs.py"),
    )

    input_root = tmp_path / "court_rules"
    _write_jsonl(
        input_root / "us_state_procedural_rules_fixture.jsonl",
        [
            {
                "jurisdiction_code": "NY",
                "jurisdiction_name": "New York",
                "record": {
                    "identifier": "NY-court-1",
                    "name": "Rules of Civil Procedure",
                    "text": "New York rules of civil procedure text",
                    "sourceUrl": "https://example.org/ny/court-rules",
                },
            },
            {
                "jurisdiction_code": "CA",
                "jurisdiction_name": "California",
                "record": {
                    "identifier": "CA-court-1",
                    "name": "Rules of Civil Procedure",
                    "text": "California rules of civil procedure text",
                    "sourceUrl": "https://example.org/ca/court-rules",
                },
            },
        ],
    )

    output_dir = tmp_path / "out_court"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "merge_state_court_rules_runs.py",
            "--input-root",
            str(input_root),
            "--output-dir",
            str(output_dir),
            "--state",
            "NY",
        ],
    )

    assert module.main() == 0

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["selected_states"] == ["NY"]
    assert manifest["totals"]["states"] == 1
    assert sorted(manifest["states"].keys()) == ["NY"]
    assert (output_dir / "state_court_rules_jsonld" / "STATE-NY.jsonld").exists()
    assert not (output_dir / "state_court_rules_jsonld" / "STATE-CA.jsonld").exists()