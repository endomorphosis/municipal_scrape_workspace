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


def test_merge_state_admin_runs_ingests_corpus_jsonl_and_prefers_richer_row(tmp_path, monkeypatch):
    module = _load_module(
        "merge_state_admin_runs_corpus_test",
        Path("/home/barberb/municipal_scrape_workspace/scripts/ops/legal_data/merge_state_admin_runs.py"),
    )

    input_root = tmp_path / "artifacts" / "state_admin_rules" / "run_a"
    _write_jsonl(
        input_root / "STATE-AZ.jsonld",
        [
            {
                "identifier": "AZ-rule-1",
                "name": "AZ Rule 1",
                "text": "short",
                "legislationJurisdiction": "US-AZ",
                "sourceUrl": "https://example.org/az/rule-1",
            }
        ],
    )
    _write_jsonl(
        input_root / "state_admin_rule_kg_corpus_run.jsonl",
        [
            {
                "identifier": "AZ-rule-1",
                "name": "AZ Rule 1",
                "text": "This is the longer Arizona administrative rule text that should win during dedupe.",
                "legislationJurisdiction": "US-AZ",
                "state_code": "AZ",
                "sourceUrl": "https://example.org/az/rule-1",
            },
            {
                "identifier": "AZ-rule-2",
                "name": "AZ Rule 2",
                "text": "Second Arizona rule from the corpus JSONL.",
                "legislationJurisdiction": "US-AZ",
                "state_code": "AZ",
                "sourceUrl": "https://example.org/az/rule-2",
            },
        ],
    )

    output_dir = tmp_path / "out_admin_corpus"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "merge_state_admin_runs.py",
            "--input-root",
            str(input_root),
            "--output-dir",
            str(output_dir),
            "--include-corpus-jsonl",
            "--state",
            "AZ",
        ],
    )

    assert module.main() == 0

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    state_entry = manifest["states"]["AZ"]
    assert manifest["totals"]["source_corpus_jsonl_files"] == 1
    assert state_entry["source_corpus_rows"] == 2
    assert state_entry["merged_rows_total"] == 2

    merged_rows = [
        json.loads(line)
        for line in (output_dir / "state_admin_rules_jsonld" / "STATE-AZ.jsonld").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(merged_rows) == 2
    by_identifier = {row["identifier"]: row for row in merged_rows}
    assert by_identifier["AZ-rule-1"]["text"].startswith("This is the longer Arizona administrative rule text")
    assert by_identifier["AZ-rule-2"]["name"] == "AZ Rule 2"


def test_merge_state_admin_runs_selects_best_summary_by_status_then_count(tmp_path, monkeypatch):
    module = _load_module(
        "merge_state_admin_runs_summary_test",
        Path("/home/barberb/municipal_scrape_workspace/scripts/ops/legal_data/merge_state_admin_runs.py"),
    )

    input_root = tmp_path / "artifacts" / "state_admin_rules" / "run_a"
    _write_jsonl(
        input_root / "STATE-UT.jsonld",
        [
            {
                "identifier": "UT-rule-1",
                "name": "UT Rule 1",
                "text": "Utah administrative rule text",
                "legislationJurisdiction": "US-UT",
                "sourceUrl": "https://example.org/ut/rule-1",
            }
        ],
    )

    lower_quality_summary = {
        "status": "partial_success",
        "rules_count": 99,
        "notes": ["more rows but weaker status"],
    }
    better_summary = {
        "status": "success",
        "rules_count": 7,
        "notes": ["fewer rows but successful run"],
    }
    (input_root / "UT.json").write_text(json.dumps(lower_quality_summary), encoding="utf-8")
    sibling_dir = tmp_path / "artifacts" / "state_admin_rules" / "run_b"
    sibling_dir.mkdir(parents=True, exist_ok=True)
    (sibling_dir / "UT.json").write_text(json.dumps(better_summary), encoding="utf-8")

    output_dir = tmp_path / "out_admin_summary"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "merge_state_admin_runs.py",
            "--input-root",
            str(input_root),
            "--input-root",
            str(sibling_dir),
            "--output-dir",
            str(output_dir),
            "--state",
            "UT",
        ],
    )

    assert module.main() == 0

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    state_entry = manifest["states"]["UT"]
    summary = json.loads((output_dir / "state_summaries" / "UT.json").read_text(encoding="utf-8"))

    assert state_entry["canonical_summary_status"] == "success"
    assert state_entry["canonical_summary_rules_count"] == 7
    assert summary["status"] == "success"
    assert summary["rules_count"] == 7
    assert summary["_source_file"].endswith("UT.json")


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