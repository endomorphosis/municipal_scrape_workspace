#!/usr/bin/env python3
"""Upload cc_pointers_by_collection to a Hugging Face *dataset* repo without copying.

This uses the Hugging Face Hub HTTP API (via huggingface_hub) to stream file
contents directly from the source directory.

Typical usage:
  huggingface-cli login
  python3 scripts/ops/hf_upload_cc_pointers_by_collection.py \
    --repo-id endomorphosis/common_crawl_pointers_by_collection \
    --src /storage/ccindex_parquet/cc_pointers_by_collection \
    --years 2023 2024 2025 \
    --create-repo

Notes:
- No local git clone, no rsync, no extra on-disk duplication.
- Upload is done per-year to keep commits manageable.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import shutil
import socket
import threading
import time
import hashlib
import urllib.request
from pathlib import Path
from typing import Iterable, List

from huggingface_hub import HfApi


logger = logging.getLogger("hf_upload_cc_pointers_by_collection")


_HF_ULF_PATCHED = False
_RATE_LIMIT_WARMUP_DIRS: list[Path] = []


def _set_rate_limit_warmup_dirs(dirs: list[Path]) -> None:
    global _RATE_LIMIT_WARMUP_DIRS
    _RATE_LIMIT_WARMUP_DIRS = dirs


def _rate_limit_warmup(max_seconds: int) -> None:
    # Best-effort: keep CPU busy and warm page cache while waiting.
    try:
        _warm_hash_dirs(list(_RATE_LIMIT_WARMUP_DIRS), max_seconds=max_seconds)
    except Exception:
        logger.debug("Warmup during rate limit failed", exc_info=True)


def _patch_hf_upload_large_folder(*, max_get_upload_mode_workers: int, max_preupload_workers: int) -> None:
    """Patch huggingface_hub internals to reduce rate-limit bursts.

    huggingface_hub.upload_large_folder starts N worker threads and lets many threads
    concurrently call the 'preupload' endpoint ('get upload mode'). With N=8 and many
    collections, this can quickly hit the 1000 requests / 5 min quota.

    This patch caps the number of concurrent workers allowed to run the API-heavy stages
    and adds a 429 sleep-until-reset retry inside those stages.
    """

    global _HF_ULF_PATCHED
    if _HF_ULF_PATCHED:
        return

    try:
        import huggingface_hub._upload_large_folder as ulf
    except Exception:
        logger.debug("Could not import huggingface_hub._upload_large_folder for patching", exc_info=True)
        return

    max_get_upload_mode_workers = max(1, int(max_get_upload_mode_workers))
    max_preupload_workers = max(1, int(max_preupload_workers))

    # 1) Patch job selection to cap concurrent API-heavy jobs.
    orig_determine = getattr(ulf, "_determine_next_job", None)
    if callable(orig_determine):

        def _determine_next_job_capped(status):  # type: ignore[no-untyped-def]
            # Copy of upstream logic with extra caps on GET_UPLOAD_MODE/PREUPLOAD.
            with status.lock:
                # 1. Commit if more than 5 minutes since last commit attempt (and at least 1 file)
                if (
                    status.nb_workers_commit == 0
                    and status.queue_commit.qsize() > 0
                    and status.last_commit_attempt is not None
                    and time.time() - status.last_commit_attempt > 5 * 60
                ):
                    status.nb_workers_commit += 1
                    return (ulf.WorkerJob.COMMIT, ulf._get_n(status.queue_commit, status.target_chunk()))

                # 2. Commit if at least 150 files are ready to commit
                elif status.nb_workers_commit == 0 and status.queue_commit.qsize() >= 150:
                    status.nb_workers_commit += 1
                    return (ulf.WorkerJob.COMMIT, ulf._get_n(status.queue_commit, status.target_chunk()))

                # 3. Get upload mode if at least MAX_NB_FILES_FETCH_UPLOAD_MODE files (capped)
                elif (
                    status.queue_get_upload_mode.qsize() >= ulf.MAX_NB_FILES_FETCH_UPLOAD_MODE
                    and status.nb_workers_get_upload_mode < max_get_upload_mode_workers
                ):
                    status.nb_workers_get_upload_mode += 1
                    return (
                        ulf.WorkerJob.GET_UPLOAD_MODE,
                        ulf._get_n(status.queue_get_upload_mode, ulf.MAX_NB_FILES_FETCH_UPLOAD_MODE),
                    )

                # 4. Preupload LFS/Xet batches (capped)
                elif (
                    status.queue_preupload_lfs.qsize() >= status.upload_batch_size
                    and status.nb_workers_preupload_lfs < max_preupload_workers
                ):
                    status.nb_workers_preupload_lfs += 1
                    return (ulf.WorkerJob.PREUPLOAD_LFS, ulf._get_n(status.queue_preupload_lfs, status.upload_batch_size))

                # 5. Compute sha256 if at least 1 file and no worker is computing sha256
                elif status.queue_sha256.qsize() > 0 and status.nb_workers_sha256 == 0:
                    status.nb_workers_sha256 += 1
                    return (ulf.WorkerJob.SHA256, ulf._get_one(status.queue_sha256))

                # 6. Get upload mode if at least 1 file and no worker is getting upload mode (still capped)
                elif (
                    status.queue_get_upload_mode.qsize() > 0
                    and status.nb_workers_get_upload_mode == 0
                    and status.nb_workers_get_upload_mode < max_get_upload_mode_workers
                ):
                    status.nb_workers_get_upload_mode += 1
                    return (
                        ulf.WorkerJob.GET_UPLOAD_MODE,
                        ulf._get_n(status.queue_get_upload_mode, ulf.MAX_NB_FILES_FETCH_UPLOAD_MODE),
                    )

                # 7. Compute sha256 if at least 1 file
                elif status.queue_sha256.qsize() > 0:
                    status.nb_workers_sha256 += 1
                    return (ulf.WorkerJob.SHA256, ulf._get_one(status.queue_sha256))

                # 8. Commit if at least 1 file and 1 min since last commit attempt
                elif (
                    status.nb_workers_commit == 0
                    and status.queue_commit.qsize() > 0
                    and status.last_commit_attempt is not None
                    and time.time() - status.last_commit_attempt > 1 * 60
                ):
                    status.nb_workers_commit += 1
                    return (ulf.WorkerJob.COMMIT, ulf._get_n(status.queue_commit, status.target_chunk()))

                # 9. Commit final batch
                elif (
                    status.nb_workers_commit == 0
                    and status.queue_commit.qsize() > 0
                    and status.queue_sha256.qsize() == 0
                    and status.queue_get_upload_mode.qsize() == 0
                    and status.queue_preupload_lfs.qsize() == 0
                    and status.nb_workers_sha256 == 0
                    and status.nb_workers_get_upload_mode == 0
                    and status.nb_workers_preupload_lfs == 0
                ):
                    status.nb_workers_commit += 1
                    return (ulf.WorkerJob.COMMIT, ulf._get_n(status.queue_commit, status.target_chunk()))

                # 10. If all queues are empty, exit
                elif all(metadata.is_committed or metadata.should_ignore for _, metadata in status.items):
                    return None

                # 11. If no task is available, wait
                else:
                    status.nb_workers_waiting += 1
                    return (ulf.WorkerJob.WAIT, [])

        ulf._determine_next_job = _determine_next_job_capped  # type: ignore[assignment]
        logger.info(
            "Patched huggingface_hub upload_large_folder: max_get_upload_mode_workers=%s max_preupload_workers=%s",
            max_get_upload_mode_workers,
            max_preupload_workers,
        )

    # 2) Patch GET_UPLOAD_MODE to sleep-until-reset on 429 instead of tight-loop retry.
    orig_get_upload_mode = getattr(ulf, "_get_upload_mode", None)
    if callable(orig_get_upload_mode):

        def _get_upload_mode_retry(items, api, repo_id, repo_type, revision):  # type: ignore[no-untyped-def]
            while True:
                try:
                    return orig_get_upload_mode(items, api=api, repo_id=repo_id, repo_type=repo_type, revision=revision)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    if _is_rate_limited_error(e):
                        sleep_for = _extract_retry_after_seconds(e) or 300
                        sleep_for = int(sleep_for + random.uniform(1, 5))
                        warm_secs = max(0, min(int(sleep_for) - 5, 295))
                        if warm_secs:
                            _rate_limit_warmup(warm_secs)
                        _sleep_with_reset_log(sleep_for, reason="HF 429 during get upload mode")
                        continue
                    raise

        ulf._get_upload_mode = _get_upload_mode_retry  # type: ignore[assignment]

    # 3) Patch PREUPLOAD_LFS similarly (Xet-enabled repos still call into this path).
    orig_preupload = getattr(ulf, "_preupload_lfs", None)
    if callable(orig_preupload):

        def _preupload_lfs_retry(items, api, repo_id, repo_type, revision):  # type: ignore[no-untyped-def]
            while True:
                try:
                    return orig_preupload(items, api=api, repo_id=repo_id, repo_type=repo_type, revision=revision)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    if _is_rate_limited_error(e):
                        sleep_for = _extract_retry_after_seconds(e) or 300
                        sleep_for = int(sleep_for + random.uniform(1, 5))
                        warm_secs = max(0, min(int(sleep_for) - 5, 295))
                        if warm_secs:
                            _rate_limit_warmup(warm_secs)
                        _sleep_with_reset_log(sleep_for, reason="HF 429 during preupload")
                        continue
                    raise

        ulf._preupload_lfs = _preupload_lfs_retry  # type: ignore[assignment]

    _HF_ULF_PATCHED = True


def _is_rate_limited_error(exc: Exception) -> bool:
    msg = str(exc)
    return ("429" in msg and "Too Many Requests" in msg) or "We had to rate limit you" in msg


def _extract_retry_after_seconds(exc: Exception) -> int | None:
    """Best-effort extraction of server-advised backoff.

    Hugging Face may include Retry-After or rate limit reset headers.
    """

    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None)
    if not headers:
        return None

    def _get(name: str) -> str | None:
        try:
            return headers.get(name)
        except Exception:
            return None

    # Prefer explicit Retry-After (seconds)
    ra = _get("Retry-After") or _get("retry-after")
    if ra:
        try:
            return max(1, int(float(ra.strip())))
        except Exception:
            pass

    # Some services return an epoch timestamp for reset
    reset = (
        _get("RateLimit-Reset")
        or _get("ratelimit-reset")
        or _get("X-RateLimit-Reset")
        or _get("x-ratelimit-reset")
    )
    if reset:
        try:
            reset_val = int(float(reset.strip()))
            now = int(time.time())
            # If it's an epoch timestamp, convert to delta.
            if reset_val > now:
                return max(1, reset_val - now)
            # If it's already seconds, use directly.
            return max(1, reset_val)
        except Exception:
            pass

    return None


def _sleep_with_reset_log(seconds: int, *, reason: str) -> None:
    seconds = max(1, int(seconds))
    reset_at = time.time() + seconds
    logger.warning(
        "%s; sleeping %ss (reset ETA %s)",
        reason,
        seconds,
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reset_at)),
    )
    time.sleep(seconds)


def _warm_hash_dirs(dirs: list[Path], *, max_seconds: int) -> None:
    """Hash files locally to keep CPU busy and warm OS page cache during rate-limit waits.

    This does not reduce HF API request count directly, but it can make subsequent hashing
    in upload_large_folder faster by priming the filesystem cache.
    """

    if max_seconds <= 0 or not dirs:
        return

    end = time.time() + max_seconds
    hashed_files = 0
    hashed_bytes = 0

    logger.info(
        "Rate-limited: warming hash/page cache for %s collection(s) up to %ss",
        len(dirs),
        max_seconds,
    )

    for d in dirs:
        if time.time() >= end:
            break
        if not d.is_dir():
            continue
        # Hash parquet files only (dominant size). Keep deterministic order.
        files = sorted(p for p in d.rglob("*.parquet") if p.is_file())
        for p in files:
            if time.time() >= end:
                break
            try:
                h = hashlib.sha256()
                with p.open("rb") as f:
                    while True:
                        chunk = f.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        h.update(chunk)
                        hashed_bytes += len(chunk)
                        if time.time() >= end:
                            break
                _ = h.hexdigest()
                hashed_files += 1
            except Exception:
                logger.debug("Warm-hash failed for %s", p, exc_info=True)

    logger.info(
        "Warm-hash done: files=%s bytes=%.2fGiB",
        hashed_files,
        hashed_bytes / (1024**3),
    )


def _get_xet_enabled(repo_id: str, *, token: str | None = None, timeout_seconds: int = 10) -> bool | None:
    """Return True/False if xetEnabled can be determined, else None.

    We query the same endpoint huggingface_hub uses internally, but with an explicit timeout
    to avoid hanging during long uploads.
    """

    try:
        from huggingface_hub import constants as hf_constants

        endpoint = hf_constants.ENDPOINT.rstrip("/")
    except Exception:
        endpoint = os.environ.get("HF_ENDPOINT", "https://huggingface.co").rstrip("/")

    url = f"{endpoint}/api/datasets/{repo_id}/revision/main?expand=xetEnabled"
    headers = {"User-Agent": "municipal-scrape-workspace-hf-uploader"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        logger.debug("Failed to query xetEnabled via %s", url, exc_info=True)
        return None

    val = data.get("xetEnabled")
    if isinstance(val, bool):
        return val
    return None


def _require_xet(repo_id: str, *, token: str | None) -> None:
    xet_enabled = _get_xet_enabled(repo_id, token=token)
    if xet_enabled is True:
        logger.info("Destination repo is Xet-enabled (xetEnabled=true)")
        return
    if xet_enabled is False:
        raise SystemExit(
            "Destination dataset repo is not Xet-enabled (xetEnabled=false). "
            "LFS is deprecated in this environment; enable Xet on the dataset repo before uploading."
        )
    raise SystemExit(
        "Could not determine whether destination repo is Xet-enabled (xetEnabled unknown). "
        "Refusing to proceed because LFS is deprecated here."
    )


def _upload_cache_dir(src: Path) -> Path:
    # upload_large_folder stores its resumable metadata under the *folder being uploaded*.
    # See HF docs: ./cache/huggingface/upload inside folder_path.
    return src / ".cache" / "huggingface" / "upload"


def _is_valid_upload_metadata_file(p: Path) -> bool:
    """Validate the upload_large_folder metadata file format.

    Observed format is 7 newline-separated fields:
      1) timestamp (float)
      2) size (int)
      3) something numeric (int)
      4) sha256 (hex)
      5) upload mode (string, e.g. 'lfs')
      6) int
      7) int

    We keep the validation conservative: if parsing fails, treat as invalid.
    """

    try:
        text = p.read_text(errors="strict")
    except Exception:
        return False

    lines = text.splitlines()
    if len(lines) < 5:
        return False
    try:
        float(lines[0])
        int(lines[1])
        int(lines[2])
    except Exception:
        return False

    sha = lines[3].strip()
    if not sha or any(c not in "0123456789abcdef" for c in sha.lower()):
        return False

    mode = lines[4].strip()
    if not mode:
        return False

    # Optional trailing ints (older/newer hub versions may vary); validate if present.
    for tail in lines[5:7]:
        if tail.strip() == "":
            return False
        try:
            int(tail)
        except Exception:
            return False

    return True


def _purge_upload_cache(
    src: Path,
    *,
    year: str | None = None,
    collection: str | None = None,
    mode: str = "invalid",
) -> None:
    """Clean upload_large_folder local cache.

    mode:
      - 'none': do nothing
      - 'invalid': delete only corrupted *.metadata files
      - 'all': delete the whole cache subtree for year/collection
    """

    if mode == "none":
        return
    if mode not in {"invalid", "all"}:
        raise SystemExit(f"Unsupported purge mode: {mode}")

    base = _upload_cache_dir(src)
    if year is not None:
        base = base / year
    if collection is not None:
        base = base / collection

    if not base.exists():
        return

    if mode == "all":
        logger.warning("Purging upload cache directory: %s", base)
        shutil.rmtree(base, ignore_errors=True)
        return

    # mode == "invalid"
    deleted = 0
    for meta in base.rglob("*.metadata"):
        if not _is_valid_upload_metadata_file(meta):
            try:
                meta.unlink()
                deleted += 1
            except Exception:
                logger.debug("Failed to delete invalid metadata file: %s", meta, exc_info=True)
    if deleted:
        logger.warning("Deleted %s invalid upload metadata files under %s", deleted, base)


def _find_year_dirs(src: Path) -> List[str]:
    years: List[str] = []
    for child in sorted(src.iterdir()):
        if child.is_dir() and child.name.isdigit() and len(child.name) == 4:
            years.append(child.name)
    return years


def _iter_years(src: Path, years_arg: List[str] | None) -> List[str]:
    if years_arg:
        return years_arg
    return _find_year_dirs(src)


def _upload_one_year(
    api: HfApi,
    repo_id: str,
    src: Path,
    year: str,
    collections: list[str] | None,
    num_workers: int | None,
    print_report_every: int,
    max_retries: int,
    retry_sleep_seconds: int,
    fallback_to_single_worker: bool,
    chunk_by: str,
    purge_upload_cache: str,
    max_get_upload_mode_workers: int,
    max_preupload_workers: int,
) -> None:
    year_dir = src / year
    if not year_dir.is_dir():
        raise SystemExit(f"Year directory not found: {year_dir}")

    # Lightweight stats for operator visibility (avoid a full recursive scan).
    try:
        collection_dirs = sorted([p.name for p in year_dir.iterdir() if p.is_dir()])
    except Exception:
        collection_dirs = []

    if collections:
        wanted = set(collections)
        before = list(collection_dirs)
        collection_dirs = [c for c in collection_dirs if c in wanted]
        missing = sorted(wanted.difference(before))
        if missing:
            logger.warning(
                "Requested collections not found under %s/%s: %s",
                src,
                year,
                missing,
            )

    logger.info(
        "Uploading year=%s (collections=%s) from %s -> repo=%s chunk_by=%s",
        year,
        len(collection_dirs),
        src,
        repo_id,
        chunk_by,
    )

    # Prevent known crash in some huggingface_hub versions when encountering corrupted
    # upload_large_folder metadata files.
    _purge_upload_cache(src, year=year, mode=purge_upload_cache)

    # IMPORTANT: upload_large_folder doesn't accept path_in_repo.
    # To keep remote paths as "{year}/...", we upload from the SRC root and
    # restrict to a year prefix using allow_patterns.
    def _do_upload(
        allow_patterns: list[str],
        label: str,
        *,
        warmup_dirs: list[Path] | None = None,
    ) -> None:
        attempt = 0
        while True:
            attempt += 1
            effective_workers = num_workers
            if attempt > 1 and fallback_to_single_worker:
                effective_workers = 1

            logger.info(
                "upload_large_folder %s attempt=%s/%s workers=%s patterns=%s",
                label,
                attempt,
                max_retries,
                effective_workers,
                allow_patterns,
            )

            try:
                _patch_hf_upload_large_folder(
                    max_get_upload_mode_workers=max_get_upload_mode_workers,
                    max_preupload_workers=max_preupload_workers,
                )
                api.upload_large_folder(
                    repo_id=repo_id,
                    folder_path=src,
                    repo_type="dataset",
                    allow_patterns=allow_patterns,
                    num_workers=effective_workers,
                    print_report=True,
                    print_report_every=print_report_every,
                )
                return
            except KeyboardInterrupt:
                raise
            except Exception as e:
                # Common root cause when uploading thousands of files on a free account.
                # Example seen in logs:
                #   429 Client Error: Too Many Requests ... quota of 1000 api requests per 5 minutes
                msg = str(e)

                if _is_rate_limited_error(e):
                    # Compute reset/backoff. If headers are missing, default to 5 minutes.
                    header_sleep = _extract_retry_after_seconds(e)
                    sleep_for = header_sleep if header_sleep is not None else 300
                    # Add a small jitter to avoid stampeding.
                    sleep_for = int(sleep_for + random.uniform(1, 5))
                    sleep_for = max(sleep_for, 30)

                    logger.error(
                        "Detected Hugging Face rate limiting (HTTP 429). Quota is typically 1000 requests / 5 minutes."
                    )
                    logger.exception("Rate-limited during %s on attempt=%s", label, attempt)
                    if attempt >= max_retries:
                        raise

                    # While waiting for reset, do local work: hash/warm-cache upcoming collections.
                    warm_secs = max(0, min(int(sleep_for) - 5, 295))
                    if warmup_dirs and warm_secs > 0:
                        _warm_hash_dirs(warmup_dirs, max_seconds=warm_secs)

                    _sleep_with_reset_log(sleep_for, reason=f"Rate limited for {label}")
                    # On the next retry, we will reduce concurrency if configured.
                    continue

                logger.exception("Upload failed for %s on attempt=%s", label, attempt)
                if attempt >= max_retries:
                    raise

                sleep_for = retry_sleep_seconds * (2 ** (attempt - 1))
                sleep_for = min(sleep_for, 600)
                logger.info("Retrying %s after %ss", label, sleep_for)
                time.sleep(sleep_for)

    if chunk_by == "year":
        _set_rate_limit_warmup_dirs([])
        _do_upload([f"{year}/**"], label=f"year={year}")
    elif chunk_by == "collection":
        if collections is not None and not collection_dirs:
            logger.warning("No matching collections to upload for year=%s; skipping", year)
            return
        if not collection_dirs:
            # Fallback if we couldn't enumerate collections
            _do_upload([f"{year}/**"], label=f"year={year}")
        else:
            for i, coll in enumerate(collection_dirs):
                _purge_upload_cache(src, year=year, collection=coll, mode=purge_upload_cache)
                warmup = []
                # During rate limiting, use the wait time to hash upcoming collections.
                # Keep this conservative: warm just the next 1 collection by default.
                if i + 1 < len(collection_dirs):
                    warmup.append(src / year / collection_dirs[i + 1])
                _set_rate_limit_warmup_dirs(warmup)
                _do_upload(
                    [f"{year}/{coll}/**"],
                    label=f"year={year} coll={coll}",
                    warmup_dirs=warmup,
                )
    else:
        raise SystemExit(f"Unsupported chunk_by: {chunk_by} (expected 'year' or 'collection')")

    logger.info("Finished year=%s", year)


def _start_heartbeat(label: str, every_seconds: int) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _run() -> None:
        start = time.time()
        while not stop_event.wait(timeout=every_seconds):
            elapsed = int(time.time() - start)
            logger.info("Heartbeat: %s elapsed=%ss", label, elapsed)

    t = threading.Thread(target=_run, name="hf-upload-heartbeat", daemon=True)
    t.start()
    return stop_event, t


def _configure_logging(log_file: str | None, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        fh = logging.FileHandler(log_file)
        handlers.append(fh)

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)

    # Keep our logger verbosity, but avoid third-party DEBUG noise (notably file locks)
    # which can generate enormous logs and slow down long-running uploads.
    logging.getLogger("filelock").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("huggingface_hub").setLevel(logging.INFO)


def main(argv: Iterable[str] | None = None) -> int:
    # Avoid tqdm writing to a closed pipe (e.g. when stdout/stderr is piped).
    # Heartbeat + logging provide operator visibility without progress bars.
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

    ap = argparse.ArgumentParser(
        description="Upload /storage/ccindex_parquet/cc_pointers_by_collection to a HF dataset without copying",
    )
    ap.add_argument(
        "--repo-id",
        required=True,
        help="HF dataset repo id, e.g. endomorphosis/common_crawl_pointers_by_collection",
    )
    ap.add_argument(
        "--src",
        default="/storage/ccindex_parquet/cc_pointers_by_collection",
        help="Source folder containing year directories",
    )
    ap.add_argument(
        "--years",
        nargs="+",
        help="Years to upload (default: auto-detect 4-digit year dirs in --src)",
    )
    ap.add_argument(
        "--num-workers",
        type=int,
        default=None,
        help="Parallel upload workers (passed to upload_large_folder)",
    )
    ap.add_argument(
        "--print-report-every",
        type=int,
        default=60,
        help="Seconds between progress reports during upload_large_folder",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries per year on transient upload failures.",
    )
    ap.add_argument(
        "--retry-sleep-seconds",
        type=int,
        default=10,
        help="Initial retry sleep; exponential backoff is applied.",
    )
    ap.add_argument(
        "--fallback-to-single-worker",
        action="store_true",
        help="On retry attempts, force num_workers=1 to reduce concurrency-related failures.",
    )
    ap.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=60,
        help="Emit a local heartbeat log line every N seconds.",
    )
    ap.add_argument(
        "--log-file",
        default=None,
        help="Optional path to write logs (in addition to stdout).",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging (DEBUG).",
    )
    ap.add_argument(
        "--hf-cache-dir",
        default=None,
        help=(
            "Optional Hugging Face cache directory. If set, exports HF_HOME to keep upload locks/cache out of the dataset tree."
        ),
    )
    ap.add_argument(
        "--chunk-by",
        choices=["year", "collection"],
        default="collection",
        help=(
            "Upload granularity. 'collection' reduces API request burst and helps avoid Hub rate limits."
        ),
    )
    ap.add_argument(
        "--collections",
        nargs="*",
        default=None,
        help=(
            "Optional collection names to upload (e.g. CC-MAIN-2023-06). "
            "If provided, only these collections are uploaded for each selected year."
        ),
    )
    ap.add_argument(
        "--token",
        default=None,
        help="HF token (optional). If omitted, uses cached token from `huggingface-cli login`.",
    )
    ap.add_argument(
        "--require-xet",
        action="store_true",
        help="Fail fast unless the destination dataset repo is Xet-enabled (deprecates LFS).",
    )
    ap.add_argument(
        "--allow-lfs",
        action="store_true",
        help="Allow proceeding even if Xet is not enabled (NOT recommended; LFS is deprecated here).",
    )
    ap.add_argument(
        "--purge-upload-cache",
        choices=["none", "invalid", "all"],
        default="invalid",
        help=(
            "Cleanup strategy for upload_large_folder local cache under SRC/.cache/huggingface/upload. "
            "Use 'invalid' to delete corrupted metadata entries that can crash the uploader. "
            "Use 'all' to drop resume state for selected year/collection."
        ),
    )
    ap.add_argument(
        "--max-get-upload-mode-workers",
        type=int,
        default=1,
        help=(
            "Cap concurrent 'get upload mode' workers inside huggingface_hub upload_large_folder to reduce 429 bursts. "
            "Keep low (1-2) for free-tier rate limits."
        ),
    )
    ap.add_argument(
        "--max-preupload-workers",
        type=int,
        default=1,
        help=(
            "Cap concurrent preupload workers inside huggingface_hub upload_large_folder. "
            "Keep low (1) to reduce rate-limit spikes."
        ),
    )
    ap.add_argument(
        "--create-repo",
        action="store_true",
        help="Create the dataset repo if missing (public by default)",
    )
    ap.add_argument(
        "--private",
        action="store_true",
        help="When used with --create-repo, create as private.",
    )
    # Note: upload_large_folder doesn't accept custom commit messages.

    args = ap.parse_args(list(argv) if argv is not None else None)

    _configure_logging(args.log_file, args.verbose)

    logger.info(
        "HF_HUB_DISABLE_PROGRESS_BARS=%s",
        os.environ.get("HF_HUB_DISABLE_PROGRESS_BARS"),
    )

    try:
        import huggingface_hub

        logger.info("huggingface_hub version: %s", getattr(huggingface_hub, "__version__", "unknown"))
    except Exception:
        pass

    try:
        from huggingface_hub import constants as hf_constants

        logger.info("Hugging Face Hub endpoint: %s", hf_constants.ENDPOINT)
    except Exception:
        # Non-fatal; purely diagnostic.
        pass

    if os.environ.get("HF_HUB_DISABLE_XET"):
        logger.warning("HF_HUB_DISABLE_XET is set; hf_xet/Xet backend disabled")
    else:
        try:
            import hf_xet  # type: ignore

            logger.info("hf_xet importable (Xet backend available)")
        except Exception:
            logger.warning(
                "hf_xet is not importable. For best large-upload performance, upgrade: pip install -U 'huggingface_hub>=0.32.0'"
            )

    if args.hf_cache_dir:
        hf_home = Path(args.hf_cache_dir)
        hf_home.mkdir(parents=True, exist_ok=True)
        os.environ["HF_HOME"] = str(hf_home)
        logger.info("Set HF_HOME=%s", os.environ["HF_HOME"])

    src = Path(args.src)
    if not src.is_dir():
        raise SystemExit(f"Source directory not found: {src}")

    # Enable hf_transfer acceleration if user configured it.
    # (No-op if package isn't installed; HF hub will fall back.)
    if os.environ.get("HF_HUB_ENABLE_HF_TRANSFER") is None:
        # Leave unset by default; user can opt-in.
        pass

    api = HfApi(token=args.token)

    if args.create_repo:
        api.create_repo(
            repo_id=args.repo_id,
            repo_type="dataset",
            private=bool(args.private),
            exist_ok=True,
        )

    # LFS is unreliable in this environment; default behavior is to require Xet.
    # If both flags are omitted, we require Xet.
    require_xet = True
    if args.allow_lfs:
        require_xet = False
    if args.require_xet:
        require_xet = True

    if require_xet:
        _require_xet(args.repo_id, token=args.token)
    else:
        logger.warning("Proceeding with --allow-lfs (LFS deprecated; uploads may fail)")

    years = _iter_years(src, args.years)
    if not years:
        raise SystemExit(f"No year directories found in {src}")

    stop_hb, hb_thread = _start_heartbeat(
        label=f"repo={args.repo_id} years={','.join(years)}",
        every_seconds=max(5, int(args.heartbeat_seconds)),
    )

    try:
        for year in years:
            _upload_one_year(
                api,
                args.repo_id,
                src,
                year,
                collections=list(args.collections) if args.collections else None,
                num_workers=args.num_workers,
                print_report_every=args.print_report_every,
                max_retries=max(1, int(args.max_retries)),
                retry_sleep_seconds=max(1, int(args.retry_sleep_seconds)),
                fallback_to_single_worker=bool(args.fallback_to_single_worker),
                chunk_by=str(args.chunk_by),
                purge_upload_cache=str(args.purge_upload_cache),
                max_get_upload_mode_workers=max(1, int(args.max_get_upload_mode_workers)),
                max_preupload_workers=max(1, int(args.max_preupload_workers)),
            )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (KeyboardInterrupt)")
        return 130
    finally:
        stop_hb.set()
        hb_thread.join(timeout=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
