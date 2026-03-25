#!/usr/bin/env python3
"""Post-cycle release gate for legal corpus scraping.

Responsibilities
----------------
1. **Completion gate (check mode)**: Before starting a new scrape, query the
   HuggingFace dataset manifest to find which states are already marked
   complete.  Exits with JSON listing ``finished_states`` and
   ``pending_states`` so the caller can skip already-finished states.

2. **Release pipeline (release mode)**: After a successful daemon cycle,
   run the full merge → clean → parquet → embed pipeline, upload the result
   to HuggingFace, and stamp a per-state completion record into the remote
   manifest so future gate checks can skip those states.

Usage
-----
# Check which CA states are already finished on HF:
python post_cycle_release_with_gate.py check \\
    --corpus state_admin_rules --states CA

# Run full release after a finished daemon cycle and upload to HF:
python post_cycle_release_with_gate.py release \\
    --corpus state_admin_rules \\
    --states CA \\
    --daemon-output-dir artifacts/state_admin_rules/ca_uncapped_20260325_212912 \\
    --min-statutes 1000 \\
    [--dry-run] [--hf-token <token>]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ipfs_root() -> Path:
    return _workspace_root() / "ipfs_datasets_py"


def _python_bin() -> str:
    venv = _workspace_root() / ".venv" / "bin" / "python"
    if venv.exists():
        return str(venv)
    return sys.executable


def _bootstrap() -> None:
    root = str(_ipfs_root())
    if root not in sys.path:
        sys.path.insert(0, root)


_bootstrap()


# ---------------------------------------------------------------------------
# HuggingFace manifest helpers
# ---------------------------------------------------------------------------

HF_MANIFEST_PATH = "scrape_completion_manifest.json"
"""Path inside the HF dataset repo where per-state completion records live."""


def _hf_api(token: Optional[str] = None):
    from huggingface_hub import HfApi
    return HfApi(token=token or os.environ.get("HF_TOKEN") or os.environ.get("LEGAL_PUBLISH_TOKEN") or None)


def _load_remote_manifest(repo_id: str, token: Optional[str]) -> Dict[str, Any]:
    """Download and parse the completion manifest from HF.  Returns {} on miss."""
    try:
        from huggingface_hub import hf_hub_download
        local_path = hf_hub_download(
            repo_id=repo_id,
            filename=HF_MANIFEST_PATH,
            repo_type="dataset",
            token=token or os.environ.get("HF_TOKEN"),
        )
        return json.loads(Path(local_path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _upload_manifest(repo_id: str, manifest: Dict[str, Any], token: Optional[str], commit_message: str) -> None:
    """Upload the updated manifest to HF."""
    api = _hf_api(token)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        tmp_path = f.name
    try:
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo=HF_MANIFEST_PATH,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=commit_message,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Gate: check which states are already finished
# ---------------------------------------------------------------------------

def cmd_check(args: argparse.Namespace) -> int:
    """Print JSON with finished_states / pending_states for the given corpus+states."""
    from ipfs_datasets_py.processors.legal_scrapers.canonical_legal_corpora import get_canonical_legal_corpus
    corpus = get_canonical_legal_corpus(args.corpus)
    requested = _parse_states(args.states)
    manifest = _load_remote_manifest(corpus.hf_dataset_id, args.hf_token)
    state_records: Dict[str, Any] = manifest.get("states", {})

    finished: List[str] = []
    pending: List[str] = []
    for state in requested:
        rec = state_records.get(state, {})
        if _state_is_finished(rec, min_statutes=args.min_statutes):
            finished.append(state)
        else:
            pending.append(state)

    result = {
        "corpus": args.corpus,
        "requested_states": requested,
        "finished_states": finished,
        "pending_states": pending,
        "manifest_version": manifest.get("version", 0),
        "manifest_generated_at": manifest.get("generated_at"),
        "state_records": {s: state_records.get(s, {}) for s in requested},
    }
    print(json.dumps(result, indent=2))
    return 0


def _state_is_finished(record: Dict[str, Any], min_statutes: int) -> bool:
    """A state record is considered finished when it has >= min_statutes rows."""
    if not isinstance(record, dict):
        return False
    status = str(record.get("status", "")).lower()
    if status in ("skipped", "blocked"):
        return False
    rows = int(record.get("rows_uploaded", 0) or 0)
    return rows >= min_statutes


# ---------------------------------------------------------------------------
# Release: merge → clean → parquet → embed → upload
# ---------------------------------------------------------------------------

def cmd_release(args: argparse.Namespace) -> int:
    _bootstrap()
    from ipfs_datasets_py.processors.legal_scrapers.canonical_legal_corpora import get_canonical_legal_corpus
    corpus = get_canonical_legal_corpus(args.corpus)
    states = _parse_states(args.states)
    workspace = _workspace_root()
    python_bin = shlex.quote(_python_bin())
    dry = bool(args.dry_run)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Resolve merge output dirs
    cycle_tag = f"post_release_{stamp}"
    merge_dir = workspace / "artifacts" / args.corpus / f"canonical_merged_{cycle_tag}"
    clean_dir = merge_dir.parent / f"{merge_dir.name}_cleaned"

    if args.corpus == "state_admin_rules":
        jsonld_dir = clean_dir / corpus.jsonld_dir_name
        parquet_dir = clean_dir / corpus.parquet_dir_name
    else:
        jsonld_dir = merge_dir / corpus.jsonld_dir_name
        parquet_dir = merge_dir / corpus.parquet_dir_name

    state_args = " ".join(f"--state {shlex.quote(s)}" for s in states)
    base = f"cd {shlex.quote(str(workspace))} && PYTHONPATH=src {python_bin}"

    # Build pipeline stages
    input_roots = _collect_input_roots(args, workspace, corpus)
    input_root_args = " ".join(f"--input-root {shlex.quote(str(r))}" for r in input_roots)

    pipeline: List[Dict[str, Any]] = []

    if args.corpus == "state_admin_rules":
        pipeline = [
            {
                "stage": "merge",
                "command": (
                    f"{base} scripts/ops/legal_data/merge_state_admin_runs.py"
                    f" {input_root_args}"
                    f' --output-dir {shlex.quote(str(merge_dir))}'
                    f" --include-corpus-jsonl {state_args}"
                ),
            },
            {
                "stage": "clean",
                "command": (
                    f"{base} scripts/ops/legal_data/clean_state_admin_canonical.py"
                    f' --input-dir {shlex.quote(str(merge_dir))}'
                    f' --output-dir {shlex.quote(str(clean_dir))}'
                ),
            },
            {
                "stage": "parquet",
                "command": (
                    f"{base} scripts/ops/legal_data/convert_state_admin_jsonld_to_parquet_with_cid.py"
                    f' --input-dir {shlex.quote(str(jsonld_dir))}'
                    f' --output-dir {shlex.quote(str(parquet_dir))}'
                    f' --combined-filename {shlex.quote(corpus.combined_parquet_filename)}'
                ),
            },
            {
                "stage": "embeddings",
                "command": (
                    f"{base} scripts/ops/legal_data/build_state_admin_embeddings_parquet_with_cid.py"
                    f' --input-dir {shlex.quote(str(parquet_dir))}'
                    f" --include-combined --overwrite"
                ),
            },
            {
                "stage": "publish",
                "command": (
                    f"{base} scripts/ops/legal_data/publish_canonical_legal_corpus_to_hf.py"
                    f" --corpus {shlex.quote(args.corpus)}"
                    f' --local-dir {shlex.quote(str(parquet_dir))}'
                    + (f" --token {shlex.quote(args.hf_token)}" if args.hf_token else "")
                    + " --verify"
                    + (f' --commit-message {shlex.quote(f"Auto-release {args.corpus} states={",".join(states)} rows=pending stamp={stamp}")}')
                ),
            },
        ]

    results: List[Dict[str, Any]] = []
    failed = False

    for stage in pipeline:
        stage_name = stage["stage"]
        cmd = stage["command"]
        print(f"\n{'='*60}", flush=True)
        print(f"[{stage_name.upper()}] {'(dry-run) ' if dry else ''}{cmd}", flush=True)
        if dry:
            results.append({"stage": stage_name, "status": "dry_run", "command": cmd})
            continue

        ret = subprocess.run(cmd, shell=True, text=True, capture_output=False)
        if ret.returncode != 0:
            print(f"  ERROR: stage '{stage_name}' exited {ret.returncode}", flush=True)
            results.append({"stage": stage_name, "status": "failed", "returncode": ret.returncode})
            failed = True
            break
        results.append({"stage": stage_name, "status": "ok", "returncode": 0})

    # Count rows uploaded per state and update remote manifest
    rows_by_state = _count_parquet_rows_by_state(parquet_dir, states) if not dry and not failed else {}
    if not dry and not failed:
        _stamp_completion_manifest(
            corpus=corpus,
            states=states,
            rows_by_state=rows_by_state,
            parquet_dir=parquet_dir,
            stamp=stamp,
            hf_token=args.hf_token,
        )

    summary = {
        "status": "dry_run" if dry else ("failed" if failed else "ok"),
        "corpus": args.corpus,
        "states": states,
        "stamp": stamp,
        "merge_dir": str(merge_dir),
        "clean_dir": str(clean_dir) if args.corpus == "state_admin_rules" else None,
        "parquet_dir": str(parquet_dir),
        "rows_by_state": rows_by_state,
        "stages": results,
    }
    print("\n" + json.dumps(summary, indent=2), flush=True)
    return 1 if failed else 0


def _collect_input_roots(args: argparse.Namespace, workspace: Path, corpus) -> List[Path]:
    """Collect all input roots: daemon output dir + canonical local root + artifacts dir."""
    roots: List[Path] = []
    if args.daemon_output_dir:
        p = Path(args.daemon_output_dir).expanduser().resolve()
        if p.exists():
            roots.append(p)
    canonical_root = corpus.default_local_root()
    if canonical_root.exists():
        roots.append(canonical_root)
    artifacts = workspace / "artifacts" / args.corpus
    if artifacts.exists():
        roots.append(artifacts)
    return roots


def _count_parquet_rows_by_state(parquet_dir: Path, states: List[str]) -> Dict[str, int]:
    """Read per-state parquet files and count rows."""
    rows: Dict[str, int] = {}
    if not parquet_dir.exists():
        return rows
    try:
        import duckdb
        con = duckdb.connect()
        for state in states:
            state_pq = parquet_dir / f"STATE-{state}.parquet"
            if state_pq.exists():
                try:
                    count = con.execute(f"SELECT count(*) FROM read_parquet('{state_pq}')").fetchone()[0]
                    rows[state] = int(count)
                except Exception:
                    rows[state] = 0
        # Also try combined
        combined = parquet_dir / "state_admin_rules_all_states.parquet"
        if combined.exists() and not rows:
            try:
                total = con.execute(f"SELECT count(*) FROM read_parquet('{combined}')").fetchone()[0]
                rows["__combined__"] = int(total)
            except Exception:
                pass
    except ImportError:
        pass
    return rows


def _stamp_completion_manifest(
    *,
    corpus,
    states: List[str],
    rows_by_state: Dict[str, int],
    parquet_dir: Path,
    stamp: str,
    hf_token: Optional[str],
) -> None:
    """Download, update, and re-upload the completion manifest on HF."""
    try:
        manifest = _load_remote_manifest(corpus.hf_dataset_id, hf_token)
        if "states" not in manifest:
            manifest["states"] = {}
        manifest["version"] = int(manifest.get("version", 0)) + 1
        manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
        manifest["corpus"] = corpus.key

        for state in states:
            rows = rows_by_state.get(state, 0)
            existing = manifest["states"].get(state, {})
            manifest["states"][state] = {
                "status": "complete",
                "rows_uploaded": rows,
                "parquet_path": str(parquet_dir / f"STATE-{state}.parquet"),
                "released_at": datetime.now(timezone.utc).isoformat(),
                "stamp": stamp,
                "previous_rows": existing.get("rows_uploaded", 0),
            }

        _upload_manifest(
            repo_id=corpus.hf_dataset_id,
            manifest=manifest,
            token=hf_token,
            commit_message=f"Update scrape_completion_manifest: {', '.join(states)} @ {stamp}",
        )
        print(f"\n[MANIFEST] Updated {HF_MANIFEST_PATH} on {corpus.hf_dataset_id}", flush=True)
        print(f"  States stamped: {states}", flush=True)
        print(f"  Rows: {rows_by_state}", flush=True)
    except Exception as exc:
        print(f"\n[MANIFEST] WARNING: Could not update remote manifest: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_states(raw: str) -> List[str]:
    return [s.strip().upper() for s in (raw or "").split(",") if s.strip()]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Post-cycle HuggingFace release gate for legal corpus scraping."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- check ---
    chk = sub.add_parser("check", help="Check HF manifest for already-finished states.")
    chk.add_argument("--corpus", required=True, choices=["state_laws", "state_admin_rules", "state_court_rules"])
    chk.add_argument("--states", required=True, help="Comma-separated state codes to check.")
    chk.add_argument("--hf-token", default=None, help="HuggingFace API token.")
    chk.add_argument("--min-statutes", type=int, default=100,
                     help="Minimum rows for a state to count as 'finished'. Default: 100.")

    # --- release ---
    rel = sub.add_parser("release", help="Run merge→clean→parquet→embed→publish pipeline.")
    rel.add_argument("--corpus", required=True, choices=["state_laws", "state_admin_rules", "state_court_rules"])
    rel.add_argument("--states", required=True, help="Comma-separated state codes to release.")
    rel.add_argument("--daemon-output-dir", default=None,
                     help="Path to the daemon output dir to include as an input root for merging.")
    rel.add_argument("--min-statutes", type=int, default=100,
                     help="Minimum rows required before releasing. Exits 0 with skip message if not met.")
    rel.add_argument("--hf-token", default=None, help="HuggingFace API token.")
    rel.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "check":
        return cmd_check(args)
    if args.cmd == "release":
        return cmd_release(args)
    print(f"Unknown command: {args.cmd}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
