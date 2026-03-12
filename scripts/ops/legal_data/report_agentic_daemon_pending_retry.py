#!/usr/bin/env python3
"""Report the active pending retry window for the agentic legal scraper daemon."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


def _default_daemon_output_dir(corpus: str) -> Path:
    normalized = str(corpus or "state_laws").strip() or "state_laws"
    return (Path.home() / ".ipfs_datasets" / normalized / "agentic_daemon").resolve()


def _normalize_retry_at_utc(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _seconds_remaining(retry_at_utc: Optional[str], *, now: Optional[datetime] = None) -> Optional[float]:
    normalized = _normalize_retry_at_utc(retry_at_utc)
    if not normalized:
        return None
    current = now or datetime.now(timezone.utc)
    return max(0.0, (datetime.fromisoformat(normalized) - current.astimezone(timezone.utc)).total_seconds())


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def build_pending_retry_report(*, daemon_output_dir: Path) -> Dict[str, Any]:
    pending_retry_path = daemon_output_dir / "latest_pending_retry.json"
    payload = _load_json(pending_retry_path)
    if not payload:
        return {
            "status": "idle",
            "daemon_output_dir": str(daemon_output_dir),
            "pending_retry_path": str(pending_retry_path),
            "pending_retry": None,
        }

    pending_retry = payload.get("pending_retry") if isinstance(payload.get("pending_retry"), dict) else {}
    retry_at_utc = _normalize_retry_at_utc(pending_retry.get("retry_at_utc"))
    retry_after_seconds = _coerce_optional_float(pending_retry.get("retry_after_seconds"))
    seconds_remaining = _seconds_remaining(retry_at_utc)
    if seconds_remaining is None and retry_after_seconds is not None:
        seconds_remaining = max(0.0, retry_after_seconds)

    return {
        "status": "pending_retry",
        "daemon_output_dir": str(daemon_output_dir),
        "pending_retry_path": str(pending_retry_path),
        "cycle": int(payload.get("cycle", 0) or 0),
        "timestamp": payload.get("timestamp"),
        "corpus": payload.get("corpus"),
        "states": list(payload.get("states") or []),
        "pending_retry": pending_retry,
        "seconds_remaining": round(seconds_remaining, 3) if seconds_remaining is not None else None,
    }


def collect_pending_retry_reports(
    *,
    daemon_output_dir: Path,
    watch: bool = False,
    interval_seconds: float = 5.0,
    max_reports: int = 0,
    report_loader: Callable[..., Dict[str, Any]] = build_pending_retry_report,
    sleep_func: Callable[[float], None] = time.sleep,
) -> List[Dict[str, Any]]:
    reports: List[Dict[str, Any]] = []
    interval_seconds = max(0.0, float(interval_seconds or 0.0))
    max_reports = max(0, int(max_reports or 0))

    while True:
        report = report_loader(daemon_output_dir=daemon_output_dir)
        reports.append(report)

        if not watch:
            break
        if max_reports > 0 and len(reports) >= max_reports:
            break

        seconds_remaining = _coerce_optional_float(report.get("seconds_remaining"))
        if str(report.get("status") or "") == "idle":
            break
        if seconds_remaining is not None and seconds_remaining <= 0.0:
            break

        sleep_seconds = interval_seconds
        if seconds_remaining is not None:
            sleep_seconds = min(sleep_seconds, max(0.0, seconds_remaining))
        if sleep_seconds <= 0.0:
            break
        sleep_func(sleep_seconds)

    return reports


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Report the active pending retry window for the agentic daemon.")
    parser.add_argument("--corpus", default="state_laws", help="Corpus key used to resolve the default daemon output directory.")
    parser.add_argument("--daemon-output-dir", default=None, help="Explicit daemon output directory to inspect.")
    parser.add_argument("--watch", action=argparse.BooleanOptionalAction, default=False, help="Poll repeatedly until the cooldown reaches zero, the artifact disappears, or max reports is reached.")
    parser.add_argument("--interval-seconds", type=float, default=5.0, help="Polling interval used when --watch is enabled.")
    parser.add_argument("--max-reports", type=int, default=0, help="Maximum number of reports to emit in watch mode. 0 means no explicit cap.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    daemon_output_dir = (
        Path(str(args.daemon_output_dir)).expanduser().resolve()
        if args.daemon_output_dir
        else _default_daemon_output_dir(str(args.corpus))
    )
    reports = collect_pending_retry_reports(
        daemon_output_dir=daemon_output_dir,
        watch=bool(args.watch),
        interval_seconds=float(args.interval_seconds),
        max_reports=int(args.max_reports),
    )
    for report in reports:
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())