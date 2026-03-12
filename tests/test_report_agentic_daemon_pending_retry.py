from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
import sys


def _load_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "ops" / "legal_data" / "report_agentic_daemon_pending_retry.py"
    spec = importlib.util.spec_from_file_location("report_agentic_daemon_pending_retry", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_pending_retry_report_returns_idle_without_artifact(tmp_path) -> None:
    module = _load_module()

    report = module.build_pending_retry_report(daemon_output_dir=tmp_path)

    assert report["status"] == "idle"
    assert report["pending_retry"] is None
    assert report["pending_retry_path"].endswith("latest_pending_retry.json")


def test_build_pending_retry_report_includes_seconds_remaining(tmp_path) -> None:
    module = _load_module()
    payload = {
        "cycle": 4,
        "timestamp": "2026-03-12T00:00:00+00:00",
        "corpus": "state_admin_rules",
        "states": ["OR"],
        "pending_retry": {
            "provider": "cloudflare_browser_rendering",
            "retry_after_seconds": 600.0,
            "retry_at_utc": "2026-03-12T12:10:00+00:00",
            "reason": "cloudflare_browser_rendering_rate_limited",
        },
    }
    (tmp_path / "latest_pending_retry.json").write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "latest_summary.json").write_text(
        json.dumps(
            {
                "latest_cycle": {
                    "cycle_state_order": ["AZ", "UT", "IN"],
                    "tactic_selection": {
                        "selected_tactic": "document_first",
                        "mode": "exploit",
                        "priority_states": ["AZ"],
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    report = module.build_pending_retry_report(daemon_output_dir=tmp_path)

    assert report["status"] == "pending_retry"
    assert report["cycle"] == 4
    assert report["corpus"] == "state_admin_rules"
    assert report["pending_retry"]["provider"] == "cloudflare_browser_rendering"
    assert report["tactic_selection"]["selected_tactic"] == "document_first"
    assert report["cycle_state_order"] == ["AZ", "UT", "IN"]
    assert isinstance(report["seconds_remaining"], float)
    assert report["seconds_remaining"] >= 0.0


def test_seconds_remaining_normalizes_z_suffix() -> None:
    module = _load_module()
    now = datetime(2026, 3, 12, 12, 0, 0, tzinfo=timezone.utc)

    remaining = module._seconds_remaining("2026-03-12T12:05:30Z", now=now)

    assert remaining == 330.0


def test_collect_pending_retry_reports_watch_stops_after_expiry() -> None:
    module = _load_module()
    emitted = [
        {
            "status": "pending_retry",
            "seconds_remaining": 2.0,
            "pending_retry": {"provider": "cloudflare_browser_rendering"},
        },
        {
            "status": "pending_retry",
            "seconds_remaining": 0.0,
            "pending_retry": {"provider": "cloudflare_browser_rendering"},
        },
    ]
    sleeps: list[float] = []

    def _fake_loader(*, daemon_output_dir):
        assert str(daemon_output_dir).endswith("agentic_daemon")
        return emitted.pop(0)

    def _fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    reports = module.collect_pending_retry_reports(
        daemon_output_dir=Path("/tmp/agentic_daemon"),
        watch=True,
        interval_seconds=5.0,
        report_loader=_fake_loader,
        sleep_func=_fake_sleep,
    )

    assert [report["seconds_remaining"] for report in reports] == [2.0, 0.0]
    assert sleeps == [2.0]


def test_collect_pending_retry_reports_watch_stops_when_idle() -> None:
    module = _load_module()
    emitted = [
        {
            "status": "pending_retry",
            "seconds_remaining": 4.0,
            "pending_retry": {"provider": "cloudflare_browser_rendering"},
        },
        {
            "status": "idle",
            "pending_retry": None,
        },
    ]
    sleeps: list[float] = []

    def _fake_loader(*, daemon_output_dir):
        return emitted.pop(0)

    reports = module.collect_pending_retry_reports(
        daemon_output_dir=Path("/tmp/agentic_daemon"),
        watch=True,
        interval_seconds=1.5,
        report_loader=_fake_loader,
        sleep_func=lambda seconds: sleeps.append(seconds),
    )

    assert [report["status"] for report in reports] == ["pending_retry", "idle"]
    assert sleeps == [1.5]