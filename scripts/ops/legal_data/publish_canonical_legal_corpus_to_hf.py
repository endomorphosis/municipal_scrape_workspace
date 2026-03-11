#!/usr/bin/env python3
"""Publish canonical legal-corpus parquet artifacts to Hugging Face.

This is a generic wrapper around the ipfs_datasets_py parquet uploader with
defaults derived from the canonical legal-corpus registry.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ipfs_repo_root() -> Path:
    return _workspace_root() / "ipfs_datasets_py"


def _bootstrap_imports() -> None:
    repo_root = _ipfs_repo_root()
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


def _load_publish_function() -> Callable[..., Dict[str, Any]]:
    script_path = _ipfs_repo_root() / "scripts" / "repair" / "publish_parquet_to_hf.py"
    spec = importlib.util.spec_from_file_location("publish_parquet_to_hf_script", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load uploader script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.publish


def _default_local_dir(corpus_key: str) -> Path:
    _bootstrap_imports()
    from ipfs_datasets_py.processors.legal_scrapers.canonical_legal_corpora import get_canonical_legal_corpus

    corpus = get_canonical_legal_corpus(corpus_key)
    return corpus.parquet_dir()


def _default_repo_id(corpus_key: str) -> str:
    _bootstrap_imports()
    from ipfs_datasets_py.processors.legal_scrapers.canonical_legal_corpora import get_canonical_legal_corpus

    return get_canonical_legal_corpus(corpus_key).hf_dataset_id


def _default_cid_column(corpus_key: str) -> str:
    _bootstrap_imports()
    from ipfs_datasets_py.processors.legal_scrapers.canonical_legal_corpora import get_canonical_legal_corpus

    return get_canonical_legal_corpus(corpus_key).cid_field


def _default_commit_message(corpus_key: str) -> str:
    _bootstrap_imports()
    from ipfs_datasets_py.processors.legal_scrapers.canonical_legal_corpora import get_canonical_legal_corpus

    corpus = get_canonical_legal_corpus(corpus_key)
    return f"Publish {corpus.display_name} canonical parquet artifacts"


def _dry_run_report(local_dir: Path, args: argparse.Namespace) -> Dict[str, Any]:
    parquet_files = sorted(str(path.relative_to(local_dir)) for path in local_dir.rglob("*.parquet"))
    json_files = sorted(str(path.relative_to(local_dir)) for path in local_dir.rglob("*.json"))
    jsonl_files = sorted(str(path.relative_to(local_dir)) for path in local_dir.rglob("*.jsonl"))
    md_files = sorted(str(path.relative_to(local_dir)) for path in local_dir.rglob("*.md"))
    return {
        "status": "dry_run",
        "corpus": args.corpus,
        "local_dir": str(local_dir),
        "repo_id": args.repo_id,
        "path_in_repo": args.path_in_repo,
        "create_repo": bool(args.create_repo),
        "verify": bool(args.verify),
        "cid_column": args.cid_column,
        "counts": {
            "parquet": len(parquet_files),
            "json": len(json_files),
            "jsonl": len(jsonl_files),
            "md": len(md_files),
        },
        "sample_files": {
            "parquet": parquet_files[:5],
            "json": json_files[:5],
            "jsonl": jsonl_files[:5],
            "md": md_files[:5],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish canonical legal-corpus parquet artifacts to Hugging Face")
    parser.add_argument(
        "--corpus",
        required=True,
        choices=["state_laws", "state_admin_rules", "state_court_rules"],
        help="Canonical legal corpus to publish.",
    )
    parser.add_argument("--local-dir", default=None, help="Local parquet directory to upload.")
    parser.add_argument("--repo-id", default=None, help="Hugging Face dataset repo id.")
    parser.add_argument("--path-in-repo", default="", help="Destination path inside the dataset repo.")
    parser.add_argument("--token", default=None, help="HF token (optional if already authenticated).")
    parser.add_argument("--create-repo", action="store_true", help="Create the dataset repo if it does not exist.")
    parser.add_argument("--verify", action="store_true", help="Run remote parquet verification after upload.")
    parser.add_argument("--dry-run", action="store_true", help="Print the upload plan without pushing files.")
    parser.add_argument("--cid-column", default=None, help="CID column name for remote verification.")
    parser.add_argument("--commit-message", default=None, help="Commit message for the dataset upload.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    local_dir = Path(args.local_dir).expanduser().resolve() if args.local_dir else _default_local_dir(args.corpus)
    if not local_dir.exists():
        raise SystemExit(f"Local directory does not exist: {local_dir}")

    if args.repo_id is None:
        args.repo_id = _default_repo_id(args.corpus)
    if args.cid_column is None:
        args.cid_column = _default_cid_column(args.corpus)
    if args.commit_message is None:
        args.commit_message = _default_commit_message(args.corpus)

    if args.dry_run:
        report = _dry_run_report(local_dir=local_dir, args=args)
    else:
        publish = _load_publish_function()
        report = publish(
            local_dir=local_dir,
            repo_id=args.repo_id,
            commit_message=args.commit_message,
            create_repo=bool(args.create_repo),
            token=args.token,
            path_in_repo=args.path_in_repo,
            allow_patterns=["*.parquet", "*.json", "*.jsonl", "*.md"],
            do_verify=bool(args.verify),
            cid_column=args.cid_column,
        )

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())