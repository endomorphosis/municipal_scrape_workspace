#!/usr/bin/env python3
"""Check archived status for URLs previously submitted to archives.

This script is meant to pair with the unified scraper's async archive submission
feature, which appends JSONL events to a callback file (e.g. `state/archive_jobs.jsonl`).

Because the async job registry is process-local, `job_id` values cannot be queried
across separate runs. Instead, this tool re-checks archival presence by URL.

Typical usage:
  .venv/bin/python check_archive_callbacks.py \
    --callback-file out_async_test4/state/archive_jobs.jsonl \
    --out-file out_async_test4/state/archive_jobs_status.jsonl

It will print a short summary and optionally append status events to --out-file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _prefer_local_ipfs_datasets_py_repo() -> None:
    """Prefer a sibling checkout of ipfs_datasets_py on sys.path.

    Preferred order:
      1) Already installed / importable
      2) Local checkout pointed to by $IPFS_DATASETS_PY_ROOT
      3) Sibling checkout at ../ipfs_datasets_py (dev convenience)
    """

    try:
        import ipfs_datasets_py  # noqa: F401
        return
    except Exception:
        pass

    env_root = os.environ.get("IPFS_DATASETS_PY_ROOT")
    if env_root:
        candidate = Path(env_root).expanduser().resolve()
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            return

    repo = Path(__file__).resolve().parent.parent / "ipfs_datasets_py"
    if repo.exists():
        sys.path.insert(0, str(repo))


def _read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                # Ignore malformed lines.
                continue


@dataclass(frozen=True)
class ArchiveSubmission:
    url: str
    job_id: Optional[str]


def _collect_latest_submissions(callback_file: Path) -> List[ArchiveSubmission]:
    latest_by_url: Dict[str, ArchiveSubmission] = {}
    for evt in _read_jsonl(callback_file):
        if evt.get("event") != "archive_job_submitted":
            continue
        url = (evt.get("url") or "").strip()
        if not url:
            continue
        job_id = evt.get("job_id")
        latest_by_url[url] = ArchiveSubmission(url=url, job_id=job_id)
    return list(latest_by_url.values())


async def _check_one(
    url: str,
    *,
    timeout_seconds: int,
) -> Dict[str, Any]:
    # Import lazily so the script can still show help even if deps are missing.
    from ipfs_datasets_py.mcp_server.tools.web_archive_tools.archive_check_submit import (
        check_and_submit_to_archives,
    )

    # We *check* only; do not submit again here.
    return await check_and_submit_to_archives(
        url,
        check_archive_org=True,
        check_archive_is=True,
        submit_if_missing=False,
        wait_for_archive_completion=False,
        archive_timeout=max(1, int(timeout_seconds)),
    )


async def _run_checks(
    submissions: List[ArchiveSubmission],
    *,
    max_concurrent: int,
    timeout_seconds: int,
    limit: Optional[int],
    out_file: Optional[Path],
) -> Tuple[int, int, int]:
    sem = asyncio.Semaphore(max(1, int(max_concurrent)))

    async def worker(sub: ArchiveSubmission) -> Tuple[str, Dict[str, Any]]:
        async with sem:
            try:
                resp = await _check_one(sub.url, timeout_seconds=timeout_seconds)
                return sub.url, resp
            except Exception as e:
                return sub.url, {"status": "error", "url": sub.url, "error": f"{type(e).__name__}: {e}"}

    selected = submissions[: int(limit)] if limit else submissions
    results = await asyncio.gather(*(worker(s) for s in selected))

    archived_both = 0
    archived_any = 0
    errors = 0

    if out_file:
        out_file.parent.mkdir(parents=True, exist_ok=True)

    for url, resp in results:
        ao = bool(resp.get("archive_org_present"))
        ai = bool(resp.get("archive_is_present"))

        if resp.get("status") != "success":
            errors += 1
        if ao or ai:
            archived_any += 1
        if ao and ai:
            archived_both += 1

        if out_file:
            event = {
                "event": "archive_job_checked",
                "url": url,
                "archive_org_present": ao,
                "archive_is_present": ai,
                "archive_org_url": resp.get("archive_org_url"),
                "archive_is_url": resp.get("archive_is_url"),
                "status": resp.get("status"),
                "error": resp.get("error"),
            }
            with out_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, default=str) + "\n")

    return archived_any, archived_both, errors


def main() -> int:
    p = argparse.ArgumentParser(description="Check Wayback/Archive.is status for prior async archive submissions")
    p.add_argument("--callback-file", required=True, help="Path to archive_jobs.jsonl callback file")
    p.add_argument(
        "--out-file",
        default=None,
        help="Optional JSONL file to append status events (e.g. state/archive_jobs_status.jsonl)",
    )
    p.add_argument("--max-concurrent", type=int, default=5)
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    _prefer_local_ipfs_datasets_py_repo()

    callback_file = Path(args.callback_file)
    if not callback_file.exists():
        print(f"ERROR: callback file not found: {callback_file}", file=sys.stderr)
        return 2

    submissions = _collect_latest_submissions(callback_file)
    if not submissions:
        print("No archive_job_submitted events found.")
        return 0

    out_file = Path(args.out_file) if args.out_file else None

    archived_any, archived_both, errors = asyncio.run(
        _run_checks(
            submissions,
            max_concurrent=args.max_concurrent,
            timeout_seconds=args.timeout,
            limit=args.limit,
            out_file=out_file,
        )
    )

    total = len(submissions[: int(args.limit)]) if args.limit else len(submissions)
    print(
        json.dumps(
            {
                "checked": total,
                "archived_any": archived_any,
                "archived_both": archived_both,
                "errors": errors,
                "out_file": str(out_file) if out_file else None,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
