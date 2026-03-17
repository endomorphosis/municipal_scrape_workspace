#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import anyio


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ensure_local_import_paths() -> None:
    root = _repo_root()
    candidates = [root, root / "ipfs_datasets_py"]
    for candidate in candidates:
        candidate_str = str(candidate)
        if candidate.exists() and candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)


_ensure_local_import_paths()

from ipfs_datasets_py.processors.legal_scrapers.state_admin_rules_scraper import (
    scrape_state_admin_rules,
)


def _reexec_in_repo_venv() -> None:
    if os.environ.get("MUNICIPAL_SCRAPE_IN_VENV", "").lower() == "true":
        return

    venv_python = _repo_root() / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return

    try:
        in_venv = Path(sys.prefix).resolve() == venv_python.parent.parent.resolve()
    except Exception:
        in_venv = False

    if in_venv:
        return

    os.environ["MUNICIPAL_SCRAPE_IN_VENV"] = "true"
    os.execv(str(venv_python), [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run state admin-rules scrape for one state")
    p.add_argument("--state", required=True)
    p.add_argument("--output-json", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--per-state-timeout-seconds", type=float, default=90.0)
    p.add_argument("--require-substantive-rule-text", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--retry-zero-rule-states", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--agentic-max-candidates-per-state", type=int, default=40)
    p.add_argument("--agentic-max-fetch-per-state", type=int, default=16)
    p.add_argument("--agentic-max-results-per-domain", type=int, default=35)
    p.add_argument("--agentic-max-hops", type=int, default=2)
    p.add_argument("--agentic-max-pages", type=int, default=18)
    p.add_argument("--agentic-fetch-concurrency", type=int, default=6)
    p.add_argument("--parallel-workers", type=int, default=6)
    p.add_argument("--worker-direct", action="store_true", help=argparse.SUPPRESS)
    return p.parse_args()


def _payload_path(output_json: str) -> Path:
    return Path(output_json).expanduser().resolve()


def _write_payload(output_json: str, payload: dict) -> None:
    payload_path = _payload_path(output_json)
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_payload_if_present(output_json: str) -> dict | None:
    payload_path = _payload_path(output_json)
    if not payload_path.exists():
        return None
    try:
        return json.loads(payload_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _timeout_payload(state: str, *, detail: str | None = None) -> dict:
    payload = {
        "state": state,
        "status": "timeout",
        "rules_count": 0,
        "states_with_rules": [],
        "missing_rule_states": [state],
    }
    if detail:
        payload["detail"] = detail
    return payload


def _error_payload(state: str, *, detail: str) -> dict:
    return {
        "state": state,
        "status": "error",
        "rules_count": 0,
        "states_with_rules": [],
        "missing_rule_states": [state],
        "detail": detail,
    }


def _count_nonempty_lines(path: Path) -> int:
    try:
        return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    except Exception:
        return 0


def _artifact_recovery_payload(state: str, output_dir: str) -> dict | None:
    output_root = Path(output_dir).expanduser().resolve()
    jsonld_path = output_root / "state_admin_rules_jsonld" / f"STATE-{state}.jsonld"
    kg_path = output_root / "agentic_discovery" / "state_admin_rule_kg_corpus.jsonl"

    rules_count = 0
    if jsonld_path.exists():
        rules_count = max(rules_count, _count_nonempty_lines(jsonld_path))
    if kg_path.exists():
        rules_count = max(rules_count, _count_nonempty_lines(kg_path))

    if rules_count <= 0:
        return None

    return {
        "state": state,
        "status": "partial_success",
        "rules_count": rules_count,
        "states_with_rules": [state],
        "missing_rule_states": [],
        "artifact_recovered": True,
        "artifact_output_dir": str(output_root),
    }


def _timeout_payload_preserving_existing(output_json: str, state: str, *, detail: str) -> dict:
    existing_payload = _read_payload_if_present(output_json)
    if not isinstance(existing_payload, dict):
        return _timeout_payload(state, detail=detail)

    payload = dict(existing_payload)
    payload.setdefault("state", state)
    payload.setdefault("status", "timeout")
    payload["supervisor_timeout"] = True
    prior_detail = str(payload.get("detail") or "").strip()
    payload["detail"] = detail if not prior_detail else f"{prior_detail} {detail}".strip()
    return payload


def _build_payload_from_result(state: str, result: dict) -> dict:
    meta = result.get("metadata") or {}
    agentic_report = meta.get("agentic_report") or {}
    per_state_report = ((agentic_report.get("per_state") or {}).get(state) or {})
    return {
        "state": state,
        "status": result.get("status"),
        "rules_count": int(meta.get("rules_count") or 0),
        "states_with_rules": list(meta.get("states_with_rules") or []),
        "missing_rule_states": list(meta.get("missing_rule_states") or []),
        "agentic_report_status": agentic_report.get("status"),
        "agentic_report_error": agentic_report.get("error"),
        "per_state": per_state_report,
        "kg_etl_corpus_jsonl": meta.get("kg_etl_corpus_jsonl"),
        "elapsed_time_seconds": meta.get("elapsed_time_seconds"),
    }


def _forward_child_output(stdout: str | None, stderr: str | None) -> None:
    if stdout:
        sys.stdout.write(stdout)
        if not stdout.endswith("\n"):
            sys.stdout.write("\n")
        sys.stdout.flush()
    if stderr:
        sys.stderr.write(stderr)
        if not stderr.endswith("\n"):
            sys.stderr.write("\n")
        sys.stderr.flush()


def _coerce_stream_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _worker_timeout_seconds(per_state_timeout_seconds: float) -> float:
    timeout_value = float(per_state_timeout_seconds)
    return max(timeout_value + 30.0, timeout_value * 1.5)


def _supervisor_timeout_seconds(per_state_timeout_seconds: float) -> float:
    timeout_value = float(per_state_timeout_seconds)
    return max(_worker_timeout_seconds(timeout_value) + 20.0, timeout_value * 1.75)


def _run_supervised(args: argparse.Namespace) -> int:
    state = str(args.state or "").strip().upper()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    _payload_path(args.output_json).parent.mkdir(parents=True, exist_ok=True)

    child_args = [arg for arg in sys.argv[1:] if arg != "--worker-direct"]
    cmd = [sys.executable, str(Path(__file__).resolve()), *child_args, "--worker-direct"]
    env = os.environ.copy()
    env["MUNICIPAL_SCRAPE_IN_VENV"] = "true"

    timeout_seconds = _supervisor_timeout_seconds(float(args.per_state_timeout_seconds))
    proc = subprocess.Popen(
        cmd,
        cwd=str(_repo_root()),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        stdout, stderr = proc.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        stdout = _coerce_stream_text(exc.stdout)
        stderr = _coerce_stream_text(exc.stderr)
        proc.terminate()
        try:
            tail_stdout, tail_stderr = proc.communicate(timeout=10)
            stdout += _coerce_stream_text(tail_stdout)
            stderr += _coerce_stream_text(tail_stderr)
        except subprocess.TimeoutExpired:
            proc.kill()
            tail_stdout, tail_stderr = proc.communicate()
            stdout += _coerce_stream_text(tail_stdout)
            stderr += _coerce_stream_text(tail_stderr)

        _forward_child_output(stdout, stderr)
        recovered_payload = _artifact_recovery_payload(state, args.output_dir)
        if recovered_payload is not None:
            payload = dict(recovered_payload)
            payload["supervisor_timeout"] = True
            payload["detail"] = (
                "Supervisor terminated the worker after the scrape timed out or hung during shutdown. "
                "Recovered counts from worker output artifacts."
            )
        else:
            payload = _timeout_payload_preserving_existing(
                args.output_json,
                state,
                detail=(
                    "Supervisor terminated the worker after the scrape timed out or hung during shutdown."
                ),
            )
        _write_payload(args.output_json, payload)
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    _forward_child_output(stdout, stderr)

    if proc.returncode != 0:
        payload_path = _payload_path(args.output_json)
        if not payload_path.exists():
            payload = _error_payload(state, detail=f"Worker exited with status {proc.returncode}")
            _write_payload(args.output_json, payload)
            print(json.dumps(payload, ensure_ascii=False))
        return int(proc.returncode or 1)

    payload_path = _payload_path(args.output_json)
    if not payload_path.exists():
        payload = _error_payload(state, detail="Worker exited successfully but did not write output JSON")
        _write_payload(args.output_json, payload)
        print(json.dumps(payload, ensure_ascii=False))
        return 1

    return 0


async def _run(args: argparse.Namespace) -> dict:
    state = str(args.state or "").strip().upper()
    try:
        result = await asyncio.wait_for(
            scrape_state_admin_rules(
                states=[state],
                output_format="json",
                include_metadata=True,
                rate_limit_delay=0.2,
                max_rules=None,
                output_dir=args.output_dir,
                write_jsonld=True,
                strict_full_text=False,
                min_full_text_chars=200,
                hydrate_rule_text=True,
                parallel_workers=int(args.parallel_workers),
                per_state_retry_attempts=1,
                retry_zero_rule_states=bool(args.retry_zero_rule_states),
                max_base_statutes=None,
                per_state_timeout_seconds=float(args.per_state_timeout_seconds),
                include_dc=False,
                agentic_fallback_enabled=True,
                agentic_max_candidates_per_state=int(args.agentic_max_candidates_per_state),
                agentic_max_fetch_per_state=int(args.agentic_max_fetch_per_state),
                agentic_max_results_per_domain=int(args.agentic_max_results_per_domain),
                agentic_max_hops=int(args.agentic_max_hops),
                agentic_max_pages=int(args.agentic_max_pages),
                agentic_fetch_concurrency=int(args.agentic_fetch_concurrency),
                write_agentic_kg_corpus=True,
                require_substantive_rule_text=bool(args.require_substantive_rule_text),
            ),
            timeout=_worker_timeout_seconds(float(args.per_state_timeout_seconds)),
        )
    except asyncio.TimeoutError:
        recovered_payload = _artifact_recovery_payload(state, args.output_dir)
        if recovered_payload is not None:
            payload = dict(recovered_payload)
            payload["detail"] = "Worker timed out after scrape artifacts were written; recovered counts from output artifacts."
            return payload
        return {
            "state": state,
            "status": "timeout",
            "rules_count": 0,
            "states_with_rules": [],
            "missing_rule_states": [state],
        }

    return _build_payload_from_result(state, result)


def main() -> int:
    _reexec_in_repo_venv()
    args = parse_args()
    if not args.worker_direct:
        return _run_supervised(args)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    payload = loop.run_until_complete(_run(args))
    _write_payload(args.output_json, payload)
    print(json.dumps(payload, ensure_ascii=False))
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    raise SystemExit(main())
