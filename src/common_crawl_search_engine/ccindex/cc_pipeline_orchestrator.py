#!/usr/bin/env python3
"""
Common Crawl Pipeline Orchestrator

Unified system that orchestrates all pipeline phases:
1. Download CC index .tar.gz files
2. Convert to .gz.parquet files
3. Sort parquet files by domain
4. Build DuckDB pointer indexes
5. Verify completeness and integrity

Replaces the older 1-year, 2-year, 5-year scripts with a unified approach.
Uses existing validator and HUD scripts for consistency.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import selectors
import shutil
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from urllib.error import HTTPError, URLError

import psutil

from .validate_collection_completeness import CollectionValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Default behavior: run the full pipeline for 2023 collections.
# This can be overridden via --filter, pipeline_config.json (collections_filter),
# or by setting $CC_DEFAULT_COLLECTION_FILTER.
DEFAULT_COLLECTION_FILTER = os.getenv("CC_DEFAULT_COLLECTION_FILTER", "2023")
DEFAULT_MAX_WORKERS = 8


@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    ccindex_root: Path
    parquet_root: Path
    duckdb_collection_root: Path
    duckdb_year_root: Path
    duckdb_master_root: Path
    max_workers: int
    memory_limit_gb: float
    min_free_space_gb: float
    collections_filter: Optional[str] = None
    heartbeat_seconds: int = 30
    # Default to reclaiming disk once a collection is fully complete.
    # Can be overridden via pipeline_config.json or CLI flags.
    cleanup_extraneous: bool = True
    cleanup_dry_run: bool = False
    cleanup_source_archives: bool = True
    sort_workers: Optional[int] = None
    sort_memory_per_worker_gb: float = 4.0
    sort_temp_dir: Optional[Path] = None
    force_reindex: bool = False
    
    def __post_init__(self):
        self.ccindex_root = Path(self.ccindex_root)
        self.parquet_root = Path(self.parquet_root)
        self.duckdb_collection_root = Path(self.duckdb_collection_root)
        self.duckdb_year_root = Path(self.duckdb_year_root)
        self.duckdb_master_root = Path(self.duckdb_master_root)
    
    @classmethod
    def from_json(cls, path: Path) -> 'PipelineConfig':
        """Load configuration from JSON file"""
        with open(path) as f:
            data = json.load(f)
        return cls(**data)
    
    @classmethod
    def from_args(cls, args) -> 'PipelineConfig':
        """Create config from command-line args, with JSON config as fallback"""
        config_file = Path(args.config) if hasattr(args, 'config') and args.config else Path('pipeline_config.json')
        
        # Load defaults from config file if it exists
        if config_file.exists():
            logger.info(f"Loading configuration from {config_file}")
            config = cls.from_json(config_file)
            # Override with command-line args if provided
            if hasattr(args, 'ccindex_root') and args.ccindex_root:
                logger.info(f"Overriding ccindex_root: {args.ccindex_root}")
                config.ccindex_root = Path(args.ccindex_root)
            if hasattr(args, 'parquet_root') and args.parquet_root:
                logger.info(f"Overriding parquet_root: {args.parquet_root}")
                config.parquet_root = Path(args.parquet_root)
            if hasattr(args, 'workers') and args.workers is not None:
                logger.info(f"Overriding workers: {args.workers}")
                config.max_workers = int(args.workers)
            if hasattr(args, 'filter') and args.filter is not None:
                config.collections_filter = args.filter

            # Ensure a sane default even if the config file omits max_workers.
            if not getattr(config, "max_workers", None):
                config.max_workers = DEFAULT_MAX_WORKERS
            return config
        else:
            logger.info(f"Config file {config_file} not found, using defaults")
            # Use command-line args or hardcoded defaults
            return cls(
                ccindex_root=Path(args.ccindex_root) if hasattr(args, 'ccindex_root') and args.ccindex_root else Path('/storage/ccindex'),
                parquet_root=Path(args.parquet_root) if hasattr(args, 'parquet_root') and args.parquet_root else Path('/storage/ccindex_parquet'),
                duckdb_collection_root=Path('/storage/ccindex_duckdb/cc_pointers_by_collection'),
                duckdb_year_root=Path('/storage/ccindex_duckdb/cc_pointers_by_year'),
                duckdb_master_root=Path('/storage/ccindex_duckdb/cc_pointers_master'),
                max_workers=(int(args.workers) if hasattr(args, 'workers') and args.workers is not None else DEFAULT_MAX_WORKERS),
                memory_limit_gb=10.0,
                min_free_space_gb=50.0,
                collections_filter=args.filter if hasattr(args, 'filter') and args.filter is not None else None
            )


def _normalize_collections_filter(value: Optional[str]) -> Optional[str]:
    """Normalize user/config collection filters.

    - None / empty -> None
    - 'all', '*', 'none' -> None
    - otherwise -> unchanged string
    """

    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in {"all", "*", "none"}:
        return None
    return s


class PipelineOrchestrator:
    """Orchestrates the complete CC pipeline"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.validator = CollectionValidator(
            ccindex_dir=config.ccindex_root,
            parquet_dir=config.parquet_root,
            pointer_dir=config.duckdb_collection_root  # Use collection-level indexes
        )
        self.collections: List[str] = []
        self.collection_status: Dict[str, dict] = {}
        self.force_reindex: bool = bool(getattr(config, "force_reindex", False))
        self._last_subprocess_output_tail: List[str] = []

    def _invalidate_duckdb_index(self, collection: str) -> None:
        """Delete per-collection DuckDB index + marker files to force rebuild."""

        duckdb_path = self.config.duckdb_collection_root / f"{collection}.duckdb"
        candidates = [
            duckdb_path,
            Path(str(duckdb_path) + ".sorted"),
            Path(str(duckdb_path) + ".wal"),
            Path(str(duckdb_path) + ".shm"),
            Path(str(duckdb_path) + "-wal"),
            Path(str(duckdb_path) + "-shm"),
        ]
        removed = 0
        for p in candidates:
            try:
                if p.exists():
                    p.unlink(missing_ok=True)
                    removed += 1
            except Exception as e:
                logger.warning(f"Failed to remove index artifact {p}: {e}")

        if removed:
            logger.info(f"  Force-reindex: removed {removed} DuckDB artifact(s) for {collection}")

    def _run_subprocess_with_heartbeat(
        self,
        cmd: List[str],
        *,
        cwd: Optional[Path] = None,
        heartbeat_label: str = "",
        capture_tail_lines: int = 0,
    ) -> int:
        """Run a subprocess while streaming output and printing periodic heartbeats.

        This avoids long silent stretches that look like a stall when the child
        process is doing work without producing output.
        """

        hb_seconds = max(1, int(getattr(self.config, "heartbeat_seconds", 30) or 30))
        label = f"[{heartbeat_label}] " if heartbeat_label else ""
        logger.info(f"{label}Running: {' '.join(cmd)}")

        start = time.monotonic()

        tail: deque[str] | None = None
        if capture_tail_lines and int(capture_tail_lines) > 0:
            tail = deque(maxlen=int(capture_tail_lines))

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(cwd) if cwd else None,
        )

        sel = selectors.DefaultSelector()
        assert proc.stdout is not None
        sel.register(proc.stdout, selectors.EVENT_READ)

        try:
            while True:
                if proc.poll() is not None:
                    # Drain remaining output
                    for line in proc.stdout:
                        s = line.rstrip()
                        logger.info(f"{label}{s}" )
                        if tail is not None:
                            tail.append(s)
                    break

                events = sel.select(timeout=hb_seconds)
                if events:
                    for key, _mask in events:
                        line = key.fileobj.readline()
                        if line:
                            s = line.rstrip()
                            logger.info(f"{label}{s}" )
                            if tail is not None:
                                tail.append(s)
                else:
                    now = time.monotonic()
                    elapsed = now - start
                    logger.info(f"{label}Heartbeat: still running (elapsed {elapsed/60:.1f} min)")
        finally:
            try:
                sel.unregister(proc.stdout)
            except Exception:
                pass

        # Store output tail for downstream error handling.
        try:
            self._last_subprocess_output_tail = list(tail) if tail is not None else []
        except Exception:
            self._last_subprocess_output_tail = []

        return int(proc.returncode or 0)

    def _plan_collection_cleanup(self, collection: str) -> List[Tuple[Path, str]]:
        """Compute a safe cleanup plan for a collection.

        Returns a list of (path, reason) to remove.
        """

        plan: List[Tuple[Path, str]] = []

        parquet_dir = self._get_collection_parquet_dir(collection)
        if parquet_dir.exists():
            # 1) Remove leftover temp files in the parquet output tree.
            for p in parquet_dir.rglob("*.tmp"):
                plan.append((p, "leftover tmp"))
            for p in parquet_dir.rglob("*.tmp.parquet"):
                plan.append((p, "leftover tmp parquet"))

            # 2) Remove zero-byte parquet files (failed/partial writes).
            for p in parquet_dir.glob("*.parquet"):
                try:
                    if p.is_file() and p.stat().st_size == 0:
                        plan.append((p, "zero-byte parquet"))
                except OSError:
                    pass

            # 3) Remove duplicate unsorted shards when a sorted twin exists.
            for sorted_file in parquet_dir.glob("cdx-*.gz.sorted.parquet"):
                unsorted_candidate = sorted_file.with_name(
                    sorted_file.name.replace(".gz.sorted.parquet", ".gz.parquet")
                )
                if unsorted_candidate.exists():
                    plan.append((unsorted_candidate, "duplicate unsorted (sorted exists)"))

            # 4) Remove empty per-sort work dirs if they landed in the collection folder.
            for d in parquet_dir.glob("cc_sort_*"):
                if d.is_dir():
                    try:
                        if not any(d.iterdir()):
                            plan.append((d, "empty sort work dir"))
                    except OSError:
                        pass

        # 5) Optionally remove source archives once the collection is fully complete.
        if getattr(self.config, "cleanup_source_archives", False):
            status = self.validator.validate_collection(collection)
            if status.get("complete"):
                src_dir = self.config.ccindex_root / collection
                if src_dir.exists():
                    for p in src_dir.glob("*.tar.gz"):
                        plan.append((p, "source tar.gz (collection complete)"))
                    for p in src_dir.glob("cdx-*.gz"):
                        plan.append((p, "source shard gz (collection complete)"))
                    # Remove empty directory after deleting contents.
                    try:
                        if not any(src_dir.iterdir()):
                            plan.append((src_dir, "empty source dir"))
                    except OSError:
                        pass

        # De-dupe, preserve order.
        seen: Set[Path] = set()
        out: List[Tuple[Path, str]] = []
        for p, reason in plan:
            if p in seen:
                continue
            seen.add(p)
            out.append((p, reason))
        return out

    def _execute_cleanup_plan(self, collection: str, plan: List[Tuple[Path, str]]) -> None:
        dry_run = bool(getattr(self.config, "cleanup_dry_run", False))

        removed = 0
        skipped = 0
        bytes_total = 0

        for path, reason in plan:
            try:
                if not path.exists():
                    continue

                size = 0
                try:
                    if path.is_file():
                        size = path.stat().st_size
                except OSError:
                    size = 0

                if dry_run:
                    logger.info(f"[cleanup] would remove {path} ({reason})")
                    skipped += 1
                    bytes_total += size
                    continue

                if path.is_dir():
                    path.rmdir()
                else:
                    path.unlink()
                removed += 1
                bytes_total += size
            except OSError as e:
                logger.warning(f"[cleanup] failed to remove {path}: {e}")

        if dry_run:
            logger.info(
                f"[cleanup] dry-run complete for {collection}: would remove {skipped} item(s), approx {bytes_total/1024**3:.2f} GB"
            )
        else:
            logger.info(
                f"[cleanup] removed {removed} item(s) for {collection}, freed approx {bytes_total/1024**3:.2f} GB"
            )

    def _split_cleanup_plan(self, collection: str, plan: List[Tuple[Path, str]]) -> Dict[str, List[Tuple[Path, str]]]:
        """Split a cleanup plan into human-friendly categories for preview logs."""

        categories: Dict[str, List[Tuple[Path, str]]] = defaultdict(list)
        src_dir = (self.config.ccindex_root / collection).resolve()
        parquet_dir = self._get_collection_parquet_dir(collection).resolve()

        for path, reason in plan:
            cat = "other"
            # Prefer grouping by meaning/reason.
            if "source" in reason:
                cat = "source archives"
            else:
                # Fallback to path-based grouping.
                try:
                    if path.resolve().is_relative_to(src_dir):
                        cat = "source archives"
                    elif path.resolve().is_relative_to(parquet_dir):
                        cat = "parquet artifacts"
                except Exception:
                    # Older Python/path edge-cases: ignore.
                    pass

            if cat == "other":
                cat = "parquet artifacts"
            categories[cat].append((path, reason))

        # Deterministic order for logging
        ordered: Dict[str, List[Tuple[Path, str]]] = {}
        for key in ["parquet artifacts", "source archives"]:
            if key in categories and categories[key]:
                ordered[key] = categories[key]
        if "other" in categories and categories["other"]:
            ordered["other"] = categories["other"]
        return ordered

    def cleanup_collection_extraneous(self, collection: str) -> None:
        """Best-effort cleanup of safe-to-delete extraneous artifacts for a collection."""

        plan = self._plan_collection_cleanup(collection)
        if not plan:
            return
        self._execute_cleanup_plan(collection, plan)

    def run_cleanup_only(self, *, assume_yes: bool = False) -> None:
        """Run cleanup sweeps only (no pipeline stages).

        If not in dry-run mode, prompts for confirmation unless assume_yes=True.
        """

        if not getattr(self.config, "cleanup_extraneous", False):
            logger.info("[cleanup] cleanup-only requested; enabling safe extraneous cleanup")
            self.config.cleanup_extraneous = True

        self.collections = self.get_all_collections()
        logger.info(f"[cleanup] Found {len(self.collections)} collection(s) to scan")

        # Build a plan first so we can show what will be deleted.
        all_plans: Dict[str, List[Tuple[Path, str]]] = {}
        total_items = 0
        total_bytes = 0
        total_by_cat_items: Dict[str, int] = defaultdict(int)
        total_by_cat_bytes: Dict[str, int] = defaultdict(int)

        for collection in self.collections:
            plan = self._plan_collection_cleanup(collection)
            if not plan:
                continue
            all_plans[collection] = plan
            total_items += len(plan)
            split = self._split_cleanup_plan(collection, plan)
            for cat, items in split.items():
                total_by_cat_items[cat] += len(items)
                for p, _reason in items:
                    try:
                        if p.is_file():
                            total_by_cat_bytes[cat] += p.stat().st_size
                    except OSError:
                        pass

            # Total bytes across all categories.
            for p, _reason in plan:
                try:
                    if p.is_file():
                        total_bytes += p.stat().st_size
                except OSError:
                    pass

        logger.info(
            f"[cleanup] Plan: {len(all_plans)} collection(s) have cleanup items; total {total_items} item(s), approx {total_bytes/1024**3:.2f} GB"
        )
        if total_by_cat_items:
            for cat in ["parquet artifacts", "source archives"]:
                if total_by_cat_items.get(cat):
                    logger.info(
                        f"[cleanup]   - {cat}: {total_by_cat_items[cat]} item(s), approx {total_by_cat_bytes[cat]/1024**3:.2f} GB"
                    )

        # Always show the plan when cleanup-only is requested.
        original_dry_run = bool(self.config.cleanup_dry_run)
        self.config.cleanup_dry_run = True
        try:
            for collection, plan in all_plans.items():
                split = self._split_cleanup_plan(collection, plan)
                logger.info(f"[cleanup] Preview for {collection} ({len(plan)} item(s))")
                for cat, cat_plan in split.items():
                    logger.info(f"[cleanup]   Section: {cat} ({len(cat_plan)} item(s))")
                    self._execute_cleanup_plan(collection, cat_plan)
        finally:
            self.config.cleanup_dry_run = original_dry_run

        if original_dry_run:
            logger.info("[cleanup] Dry-run requested; no files deleted")
            return

        if not all_plans:
            logger.info("[cleanup] Nothing to delete")
            return

        if not assume_yes:
            if not sys.stdin.isatty():
                logger.error("[cleanup] Refusing to delete without confirmation in non-interactive mode. Re-run with --yes or --cleanup-dry-run")
                return
            answer = input("Proceed with deletion? Type 'yes' to continue: ").strip().lower()
            if answer != "yes":
                logger.info("[cleanup] Aborted by user")
                return

        # Execute deletion.
        logger.info("[cleanup] Proceeding with deletion...")
        for collection, plan in all_plans.items():
            logger.info(f"[cleanup] Sweeping {collection}...")
            self._execute_cleanup_plan(collection, plan)

    def _collection_year(self, collection: str) -> Optional[str]:
        parts = collection.split('-')
        if len(parts) >= 3 and parts[2].isdigit():
            return parts[2]
        return None

    def _resolve_ccindex_helper_script(self, filename: str) -> Path:
        """Resolve a helper script shipped alongside this module.

        The orchestrator is typically invoked via `python -m ...`, so relying on
        the current working directory for helper script discovery is fragile.
        """

        return (Path(__file__).resolve().parent / filename)

    def _get_collection_parquet_dir(self, collection: str) -> Path:
        """Return the on-disk parquet directory for a collection.

        Canonical (historical) layout:
            <parquet_root>/cc_pointers_by_collection/<year>/<collection>/

        Back-compat fallbacks:
            <parquet_root>/<year>/<collection>/
            <parquet_root>/<collection>/

        Selection behavior:
        - If any candidate already contains parquet files, prefer that path (resume-friendly).
        - Otherwise, default to the canonical (historical) layout.
        """

        year = self._collection_year(collection)
        if year:
            canonical = self.config.parquet_root / "cc_pointers_by_collection" / year / collection
            backcompat = self.config.parquet_root / year / collection

            # Prefer whichever already has data on disk.
            for candidate in (canonical, backcompat):
                try:
                    if candidate.exists() and any(candidate.glob("*.parquet")):
                        return candidate
                except Exception:
                    continue

            # Default to canonical even if it doesn't exist yet.
            return canonical

        return self.config.parquet_root / collection
        
    def get_all_collections(self) -> List[str]:
        """Get all available CC collections using validator"""
        collections = list(self.validator.get_all_collections())
        
        # Apply filter if specified
        if self.config.collections_filter:
            collections = [c for c in collections if self.config.collections_filter in c]
        
        return sorted(collections)
    
    def scan_all_collections(self):
        """Scan status of all collections using validator"""
        self.collections = self.get_all_collections()
        logger.info(f"Found {len(self.collections)} collections")
        
        for collection in self.collections:
            status = self.validator.validate_collection(collection)
            self.collection_status[collection] = status
    
    def get_available_memory_gb(self) -> float:
        """Get available system memory in GB"""
        mem = psutil.virtual_memory()
        return mem.available / (1024 ** 3)
    
    def get_free_space_gb(self, path: Path) -> float:
        """Get free disk space in GB"""
        usage = shutil.disk_usage(str(path))
        return usage.free / (1024 ** 3)
    
    def check_resources(self) -> bool:
        """Check if we have enough resources to proceed"""
        mem_gb = self.get_available_memory_gb()
        if mem_gb < self.config.memory_limit_gb:
            # Allow a small tolerance for normal fluctuations so we don't abort
            # when we're within ~5% (or 0.5GB) of the configured target.
            limit = float(self.config.memory_limit_gb)
            tolerance = max(0.5, 0.05 * limit)
            logger.warning(f"Low memory: {mem_gb:.1f} GB available, need {limit:.1f} GB")
            if mem_gb < (limit - tolerance):
                return False
            logger.warning(
                f"Proceeding despite low memory (within {tolerance:.1f} GB tolerance); performance may be reduced"
            )
        
        for path in [self.config.ccindex_root, self.config.parquet_root, self.config.duckdb_collection_root]:
            free_gb = self.get_free_space_gb(path)
            if free_gb < self.config.min_free_space_gb:
                logger.warning(f"Low disk space at {path}: {free_gb:.1f} GB free, need {self.config.min_free_space_gb:.1f} GB")
                return False
        
        return True
    
    def download_collection(self, collection: str) -> bool:
        """Download a collection's .gz files using existing download script"""
        logger.info(f"Downloading {collection}...")

        download_script = self._resolve_download_script()
        if not download_script:
            logger.error(
                "Download script not found. Expected scripts/ops/download_cc_indexes.sh in the repo, "
                "or set $CCINDEX_DOWNLOAD_SCRIPT to an explicit path."
            )
            return False
        
        # Download to collection-specific directory
        collection_dir = self.config.ccindex_root / collection
        collection_dir.mkdir(parents=True, exist_ok=True)

        parallel = int(self.config.max_workers or 8)
        cmd = [
            "bash",
            str(download_script),
            collection,
            str(collection_dir),
            str(parallel),
        ]

        repo_root = self._resolve_repo_root()
        rc = self._run_subprocess_with_heartbeat(
            cmd,
            cwd=repo_root,
            heartbeat_label=f"download:{collection}",
        )
        if rc == 0:
            logger.info(f"Downloaded {collection} successfully")
            return True
        logger.error(f"Failed to download {collection} (exit {rc})")
        return False

    def _resolve_repo_root(self) -> Optional[Path]:
        """Best-effort repo root detection.

        Works for editable installs (module lives under the repo). If not found,
        falls back to current working directory.
        """

        try:
            here = Path(__file__).resolve()
            for parent in [here.parent, *here.parents]:
                if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
                    return parent
        except Exception:
            pass
        return Path.cwd()

    def _resolve_download_script(self) -> Optional[Path]:
        """Locate the canonical download script after repo refactors."""

        env = os.getenv("CCINDEX_DOWNLOAD_SCRIPT")
        if env:
            p = Path(env).expanduser()
            if p.exists() and p.is_file():
                return p

        repo_root = self._resolve_repo_root()
        candidates = [
            repo_root / "scripts" / "ops" / "download_cc_indexes.sh",
            repo_root / "download_cc_indexes.sh",
            Path.cwd() / "scripts" / "ops" / "download_cc_indexes.sh",
            Path.cwd() / "download_cc_indexes.sh",
        ]
        for p in candidates:
            if p.exists() and p.is_file():
                return p
        return None

    def run_download_only(self, *, resume: bool) -> None:
        """Only run Stage 1 (download) for the selected collections."""

        self.scan_all_collections()
        targets = [c for c, s in self.collection_status.items() if s.get("tar_gz_count", 0) < s.get("tar_gz_expected", 0)]
        if not targets:
            logger.info("All selected collections already have their cdx-*.gz shards downloaded")
            return

        logger.info(f"Download-only mode: {len(targets)} collections need downloads")
        for collection in sorted(targets):
            status = self.collection_status.get(collection) or {}
            have = int(status.get("tar_gz_count", 0) or 0)
            exp = int(status.get("tar_gz_expected", 0) or 0)
            logger.info(f"Downloading {collection} ({have}/{exp} shards present)")
            ok = self.download_collection(collection)
            # Refresh status after attempt.
            self.collection_status[collection] = self.validator.validate_collection(collection)
            if not ok:
                raise SystemExit(f"Download failed for {collection}")
    
    def convert_collection(self, collection: str, sort_after: bool = True) -> bool:
        """Convert a collection's .gz files to parquet, optionally sorting immediately"""
        logger.info(f"Converting {collection} to parquet (sort_after={sort_after})...")
        
        ccindex_dir = self.config.ccindex_root / collection
        
        parquet_dir = self._get_collection_parquet_dir(collection)
        parquet_dir.mkdir(parents=True, exist_ok=True)

        # If a prior run produced an empty *.sorted.parquet shard (0 rows) without
        # an explicit empty-marker, treat it as incomplete and force a reconvert.
        # Also invalidate obviously broken/unreadable sorted shards so resume doesn't
        # skip forever.
        invalidated = 0
        try:
            import pyarrow.parquet as pq

            def _marker_for_sorted(sorted_path: Path) -> Path:
                unsorted_candidate = sorted_path.with_name(
                    sorted_path.name.replace(".gz.sorted.parquet", ".gz.parquet")
                )
                return unsorted_candidate.with_suffix(unsorted_candidate.suffix + ".empty")

            for sorted_file in parquet_dir.glob("cdx-*.gz.sorted.parquet"):
                try:
                    pf = pq.ParquetFile(sorted_file)
                    md = pf.metadata
                    if md is None:
                        continue
                    if int(md.num_rows) != 0:
                        continue

                    marker = _marker_for_sorted(sorted_file)
                    if marker.exists():
                        continue

                    # Unconfirmed empty: remove sorted shard so conversion+sorting can rebuild it.
                    sorted_file.unlink(missing_ok=True)
                    work_dir = parquet_dir / f".cc_sort_work_{sorted_file.name.replace('.gz.sorted.parquet', '.gz.parquet')}"
                    if work_dir.exists() and work_dir.is_dir():
                        shutil.rmtree(work_dir, ignore_errors=True)
                    invalidated += 1
                except Exception as e:
                    # Corrupt parquet can cause resume to skip forever; force rebuild.
                    try:
                        sorted_file.unlink(missing_ok=True)
                        invalidated += 1
                        logger.warning(f"Invalidated unreadable sorted shard {sorted_file}: {e}")
                    except Exception:
                        logger.warning(f"Failed to inspect/invalidate {sorted_file}: {e}")

            if invalidated:
                # Force rebuild of the downstream index if we had to invalidate any shard.
                duckdb_path = self.config.duckdb_collection_root / f"{collection}.duckdb"
                try:
                    if duckdb_path.exists():
                        duckdb_path.unlink()
                    sorted_marker = Path(str(duckdb_path) + ".sorted")
                    if sorted_marker.exists():
                        sorted_marker.unlink()
                except Exception as e:
                    logger.warning(f"Failed to invalidate DuckDB index for {collection}: {e}")

                logger.warning(
                    f"Invalidated {invalidated} unconfirmed empty sorted shard(s) for {collection}; will reconvert"
                )
        except Exception as e:
            logger.warning(f"Empty-sorted preflight skipped due to error: {e}")
        
        # Count existing parquet files to track resume progress
        existing_parquet = list(parquet_dir.glob("cdx-*.gz.parquet"))
        existing_sorted = list(parquet_dir.glob("cdx-*.gz.sorted.parquet"))
        logger.info(f"  Resume: {len(existing_parquet)} parquet, {len(existing_sorted)} sorted already exist")
        
        # Use bulk_convert_gz_to_parquet.py to convert (it has skip_existing logic)
        convert_script = self._resolve_ccindex_helper_script("bulk_convert_gz_to_parquet.py")
        cmd = [
            sys.executable,
            str(convert_script),
            "--input-dir", str(ccindex_dir),
            "--output-dir", str(parquet_dir),
            "--workers", str(self.config.max_workers),
            "--heartbeat-seconds", str(int(getattr(self.config, "heartbeat_seconds", 30) or 30)),
        ]
        
        def _count_converted_unique() -> Tuple[int, int]:
            """Return (converted_unique, expected_gz_count).

            Treat either *.gz.parquet or *.gz.sorted.parquet as converted.
            """

            gz_files = sorted(ccindex_dir.glob("cdx-*.gz"))
            expected = len(gz_files)
            if expected == 0:
                return 0, 0

            present = set()
            for p in parquet_dir.glob("cdx-*.gz.parquet"):
                present.add(p.name)
            for p in parquet_dir.glob("cdx-*.gz.sorted.parquet"):
                present.add(p.name.replace(".gz.sorted.parquet", ".gz.parquet"))

            return len(present), expected

        def _missing_examples(limit: int = 5) -> List[str]:
            gz_files = sorted(ccindex_dir.glob("cdx-*.gz"))
            expected_names = [f"{p.name}.parquet" for p in gz_files]
            present = {p.name for p in parquet_dir.glob("cdx-*.gz.parquet")}
            present |= {p.name.replace(".gz.sorted.parquet", ".gz.parquet") for p in parquet_dir.glob("cdx-*.gz.sorted.parquet")}
            missing = [n for n in expected_names if n not in present]
            return missing[: max(0, int(limit))]

        def _missing_all() -> List[str]:
            gz_files = sorted(ccindex_dir.glob("cdx-*.gz"))
            expected_names = [f"{p.name}.parquet" for p in gz_files]
            present = {p.name for p in parquet_dir.glob("cdx-*.gz.parquet")}
            present |= {
                p.name.replace(".gz.sorted.parquet", ".gz.parquet")
                for p in parquet_dir.glob("cdx-*.gz.sorted.parquet")
            }
            return [n for n in expected_names if n not in present]

        def _heal_broken_downloads_for_missing_parquets() -> int:
            """Attempt to repair missing conversions by re-downloading corrupt/missing .gz shards."""

            missing_parquets = _missing_all()
            if not missing_parquets:
                return 0

            missing_gz_names: List[str] = []
            for name in missing_parquets:
                if name.endswith(".parquet"):
                    missing_gz_names.append(name[: -len(".parquet")])

            if not missing_gz_names:
                return 0

            return self._heal_collection_gz_shards(collection, missing_gz_names)

        # Retry a few times for resume robustness: bulk_convert exits 1 if any shard fails.
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            before_converted, expected = _count_converted_unique()
            if expected == 0:
                logger.error(f"No input shards found in {ccindex_dir}")
                return False

            if before_converted >= expected:
                logger.info(f"Converted {collection} already complete ({before_converted}/{expected})")
                break

            if attempt > 1:
                logger.warning(
                    f"Retrying conversion for {collection}: attempt {attempt}/{max_attempts} (have {before_converted}/{expected})"
                )

            rc = self._run_subprocess_with_heartbeat(cmd, heartbeat_label=f"convert:{collection}")
            after_converted, _expected2 = _count_converted_unique()

            if after_converted >= expected:
                logger.info(f"Converted {collection} successfully ({after_converted}/{expected})")
                break

            # If conversion is incomplete and the converter reported an error,
            # proactively heal missing shards (corrupt/missing .gz) and retry.
            if rc != 0 and attempt < max_attempts:
                healed = 0
                try:
                    healed = _heal_broken_downloads_for_missing_parquets()
                except Exception as e:
                    logger.warning(f"Auto-heal attempt failed for {collection}: {e}")

                if healed > 0:
                    logger.warning(
                        f"Healed {healed} shard(s) for {collection} after conversion error; retrying conversion"
                    )
                    continue

            if after_converted > before_converted:
                logger.warning(
                    f"Conversion incomplete for {collection} after attempt {attempt}: {after_converted}/{expected} (exit {rc})"
                )
                continue

            missing = _missing_examples()
            logger.error(
                f"Failed to convert {collection}: no progress made (exit {rc}); missing examples: {missing}"
            )

            healed = 0
            try:
                healed = _heal_broken_downloads_for_missing_parquets()
            except Exception as e:
                logger.warning(f"Auto-heal attempt failed for {collection}: {e}")

            if healed > 0 and attempt < max_attempts:
                logger.warning(
                    f"Healed {healed} shard(s) for {collection} (redownloaded corrupt/missing .gz); retrying conversion"
                )
                continue

            return False

        final_converted, expected = _count_converted_unique()
        if final_converted < expected:
            missing = _missing_examples(limit=10)
            logger.error(
                f"Failed to fully convert {collection}: {final_converted}/{expected} shards present; missing examples: {missing}"
            )
            return False

        # If requested, immediately sort the newly converted files
        if sort_after:
            logger.info(f"Sorting newly converted files for {collection}...")
            return self.sort_collection(collection)
        return True
    
    def sort_collection(self, collection: str) -> bool:
        """Sort a collection's parquet files by (host_rev, url, ts).

        Uses validate_and_mark_sorted.py (compat tool) to:
        - validate files,
        - mark already-sorted shards as *.gz.sorted.parquet, and
        - sort + mark unsorted shards.
        """
        logger.info(f"Sorting {collection} (validate + sort + mark)...")

        parquet_dir = self._get_collection_parquet_dir(collection)
        ccindex_dir = self.config.ccindex_root / collection
        
        if not parquet_dir.exists():
            logger.error(f"Parquet directory does not exist: {parquet_dir}")
            return False

        # Some older/partial runs produced parquet files without the required
        # columns for downstream sorting/indexing (host_rev/url/ts). Detect and
        # rebuild those in-place before attempting to sort.
        try:
            import pyarrow.parquet as pq  # local import to keep module deps light

            def _has_required_cols(p: Path) -> bool:
                try:
                    pf = pq.ParquetFile(p)
                    names = set(pf.schema_arrow.names)
                    return {"host_rev", "url", "ts"}.issubset(names)
                except Exception:
                    return False

            legacy_files = [
                p
                for p in parquet_dir.glob("cdx-*.gz.parquet")
                if ".sorted." not in p.name and not _has_required_cols(p)
            ]
            if legacy_files:
                logger.warning(
                    f"Found {len(legacy_files)} parquet file(s) with legacy/invalid schema; rebuilding before sorting"
                )
                convert_script = self._resolve_ccindex_helper_script("bulk_convert_gz_to_parquet.py")
                rebuild_cmd = [
                    sys.executable,
                    str(convert_script),
                    "--input-dir",
                    str(ccindex_dir),
                    "--output-dir",
                    str(parquet_dir),
                    "--workers",
                    str(self.config.max_workers),
                ]
                rebuild = subprocess.run(rebuild_cmd)
                if rebuild.returncode != 0:
                    logger.error(f"Failed to rebuild legacy parquet files for {collection} (exit {rebuild.returncode})")
                    return False
        except Exception as e:
            logger.warning(f"Legacy schema check skipped due to error: {e}")
        
        try:
            sort_workers = int(self.config.sort_workers) if self.config.sort_workers else max(1, int(self.config.max_workers))
            sort_mem_gb = float(getattr(self.config, "sort_memory_per_worker_gb", 4.0) or 4.0)

            # Avoid oversubscribing memory and getting workers OOM-killed (which
            # manifests as BrokenProcessPool / "terminated abruptly").
            # Use a conservative fraction of *available system memory* for parallel sorts.
            # Only auto-cap when sort_workers was not explicitly set by the user.
            try:
                avail_gb = float(psutil.virtual_memory().available) / (1024.0**3)
                # Keep some headroom for Python/Arrow/OS page cache.
                mem_budget = max(1.0, avail_gb * 0.8)
                max_parallel_by_mem = max(1, int(mem_budget // max(0.1, sort_mem_gb)))
                if sort_workers > max_parallel_by_mem:
                    if self.config.sort_workers is None:
                        logger.warning(
                            f"Reducing sort-workers for {collection} from {sort_workers} to {max_parallel_by_mem} "
                            f"to fit available RAM (avail≈{avail_gb:.1f}GB, mem_budget≈{mem_budget:.1f}GB, mem_per_sort={sort_mem_gb}GB)"
                        )
                        sort_workers = max_parallel_by_mem
                    else:
                        logger.warning(
                            f"sort-workers={sort_workers} may exceed safe parallelism for available RAM "
                            f"(avail≈{avail_gb:.1f}GB, mem_budget≈{mem_budget:.1f}GB, mem_per_sort={sort_mem_gb}GB). "
                            "Proceeding because --sort-workers was explicitly set."
                        )
            except Exception:
                pass

            # Prefer a temp dir on the same filesystem as parquet output.
            sort_temp_dir = self.config.sort_temp_dir
            if sort_temp_dir is None:
                sort_temp_dir = parquet_dir / ".duckdb_sort_tmp"
            try:
                sort_temp_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                # Fall back to system temp if we can't create it.
                sort_temp_dir = None

            sort_script = self._resolve_ccindex_helper_script("validate_and_mark_sorted.py")
            cmd = [
                sys.executable,
                str(sort_script),
                "--parquet-root",
                str(parquet_dir),
                "--sort-unsorted",
                "--workers",
                str(self.config.max_workers),
                "--sort-workers",
                str(sort_workers),
                "--memory-per-sort",
                str(sort_mem_gb),
                "--heartbeat-seconds",
                str(int(getattr(self.config, "heartbeat_seconds", 30) or 30)),
            ]
            if sort_temp_dir is not None:
                cmd.extend(["--temp-dir", str(sort_temp_dir)])

            # Stream output so progress is visible during long sorts.
            result = subprocess.run(cmd)
            if result.returncode != 0:
                logger.error(f"Failed to sort/mark parquet for {collection} (exit {result.returncode})")

                # Auto-heal sort failures by retrying targeted sorts (with safer settings),
                # then reconverting the failing parquet(s), and finally re-downloading the
                # corresponding source .gz shard(s) if needed.
                healed = self._autoheal_failed_sorts(
                    collection=collection,
                    parquet_dir=parquet_dir,
                    ccindex_dir=ccindex_dir,
                    sort_temp_dir=sort_temp_dir,
                    baseline_sort_mem_gb=sort_mem_gb,
                )
                if not healed:
                    return False

                # Re-run the full validate+mark pass to ensure everything is marked and
                # cleanup logic can run.
                result2 = subprocess.run(cmd)
                if result2.returncode != 0:
                    logger.error(
                        f"Sort auto-heal ran but final validation still failed for {collection} (exit {result2.returncode})"
                    )
                    return False

            # Cleanup: if a prior sorter produced *.sorted.parquet but left the
            # original *.parquet behind, the validator can report parquet_count > expected.
            # Remove those duplicates so counts reflect unique shards.
            removed = 0
            for sorted_file in parquet_dir.glob("cdx-*.gz.sorted.parquet"):
                unsorted_candidate = sorted_file.with_name(sorted_file.name.replace(".gz.sorted.parquet", ".gz.parquet"))
                if unsorted_candidate.exists():
                    try:
                        unsorted_candidate.unlink()
                        removed += 1
                    except Exception as e:
                        logger.warning(f"Failed to remove duplicate unsorted parquet {unsorted_candidate}: {e}")
            if removed:
                logger.info(f"Removed {removed} duplicate unsorted parquet file(s) for {collection}")

            if getattr(self.config, "cleanup_extraneous", False):
                self.cleanup_collection_extraneous(collection)

            logger.info(f"Sorted/marked parquet files for {collection}")
            return True
        except Exception as e:
            logger.error(f"Failed to sort {collection}: {e}")
            return False

    def _autoheal_failed_sorts(
        self,
        *,
        collection: str,
        parquet_dir: Path,
        ccindex_dir: Path,
        sort_temp_dir: Path | None,
        baseline_sort_mem_gb: float,
    ) -> bool:
        """Attempt to heal shard-level sort failures.

        Strategy per missing shard:
        1) Retry sorting that shard alone with reduced parallelism and higher memory.
        2) If still failing, delete/reconvert the parquet and retry sorting.
        3) If still failing, re-download the source .gz shard, reconvert, and retry sorting.

        Returns True if all missing shards are healed.
        """

        expected = {f"cdx-{i:05d}" for i in range(300)}

        def _sorted_path(stem: str) -> Path:
            return parquet_dir / f"{stem}.gz.sorted.parquet"

        def _unsorted_path(stem: str) -> Path:
            return parquet_dir / f"{stem}.gz.parquet"

        present_sorted = {p.name[: -len(".gz.sorted.parquet")] for p in parquet_dir.glob("cdx-*.gz.sorted.parquet")}
        missing = sorted(expected - present_sorted)
        if not missing:
            # Might be a non-count-based failure; treat as not healable here.
            logger.warning(f"Sort failed for {collection}, but no missing shards were detected")
            return False

        logger.warning(f"Attempting sort auto-heal for {collection}: missing {len(missing)} sorted shard(s): {missing[:10]}{'...' if len(missing) > 10 else ''}")

        sort_script = self._resolve_ccindex_helper_script("validate_and_mark_sorted.py")
        convert_script = self._resolve_ccindex_helper_script("bulk_convert_gz_to_parquet.py")

        def _run_targeted_sort(stem: str, *, memory_gb: float) -> bool:
            unsorted = _unsorted_path(stem)
            if not unsorted.exists():
                # If the unsorted file is missing but sorted is too, we need reconvert.
                return False

            cmd = [
                sys.executable,
                str(sort_script),
                "--parquet-root",
                str(parquet_dir),
                "--only",
                unsorted.name,
                "--sort-unsorted",
                "--workers",
                "1",
                "--sort-workers",
                "1",
                "--memory-per-sort",
                str(float(memory_gb)),
                "--heartbeat-seconds",
                str(int(getattr(self.config, "heartbeat_seconds", 30) or 30)),
            ]
            if sort_temp_dir is not None:
                cmd.extend(["--temp-dir", str(sort_temp_dir)])
            r = subprocess.run(cmd)
            return r.returncode == 0 and _sorted_path(stem).exists()

        def _cleanup_sort_artifacts(stem: str) -> None:
            # Remove per-file work dir(s) created by validate_and_mark_sorted.
            # It uses work_dir = src.parent / f".cc_sort_work_{safe}", where safe=src.name.
            src_name = f"{stem}.gz.parquet"
            work_dir = parquet_dir / f".cc_sort_work_{src_name}"
            try:
                if work_dir.exists() and work_dir.is_dir():
                    shutil.rmtree(work_dir, ignore_errors=True)
            except Exception:
                pass

            # Remove per-file DuckDB spill dir(s).
            if sort_temp_dir is not None:
                try:
                    duckdb_tmp = sort_temp_dir / f"duckdb_sort_{src_name}"
                    if duckdb_tmp.exists() and duckdb_tmp.is_dir():
                        shutil.rmtree(duckdb_tmp, ignore_errors=True)
                except Exception:
                    pass

        def _reconvert_shard(stem: str) -> bool:
            # Delete existing parquet so bulk_convert will rebuild it.
            u = _unsorted_path(stem)
            s = _sorted_path(stem)
            try:
                s.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                u.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                (u.with_suffix(u.suffix + ".empty")).unlink(missing_ok=True)
            except Exception:
                pass

            _cleanup_sort_artifacts(stem)

            cmd = [
                sys.executable,
                str(convert_script),
                "--input-dir",
                str(ccindex_dir),
                "--output-dir",
                str(parquet_dir),
                "--workers",
                "1",
                "--heartbeat-seconds",
                str(int(getattr(self.config, "heartbeat_seconds", 30) or 30)),
            ]
            r = subprocess.run(cmd)
            return r.returncode == 0 and u.exists()

        for stem in missing:
            # (A) Retry sorting with escalating memory.
            mem_candidates: list[float] = []
            base = float(baseline_sort_mem_gb or 4.0)
            for mult in (1.0, 2.0, 4.0):
                mem_candidates.append(min(32.0, max(2.0, base * mult)))
            mem_candidates = sorted(dict.fromkeys(mem_candidates).keys())

            for mem in mem_candidates:
                if _sorted_path(stem).exists():
                    break
                logger.warning(f"Retrying sort for {collection}/{stem} with memory {mem}GB")
                if _run_targeted_sort(stem, memory_gb=mem):
                    logger.info(f"Healed sort for {collection}/{stem} by targeted re-sort")
                    break

            if _sorted_path(stem).exists():
                continue

            # (B) Reconvert -> resort.
            logger.warning(f"Re-converting parquet for {collection}/{stem} and retrying sort")
            if _reconvert_shard(stem):
                for mem in mem_candidates:
                    if _run_targeted_sort(stem, memory_gb=mem):
                        logger.info(f"Healed sort for {collection}/{stem} after reconvert")
                        break

            if _sorted_path(stem).exists():
                continue

            # (C) Re-download -> reconvert -> resort.
            logger.warning(f"Re-downloading source shard for {collection}/{stem} and retrying")
            try:
                self._heal_collection_gz_shards(collection, [f"{stem}.gz"])
            except Exception as e:
                logger.warning(f"Failed to auto-heal source shard {collection}/{stem}.gz: {e}")

            if _reconvert_shard(stem):
                for mem in mem_candidates:
                    if _run_targeted_sort(stem, memory_gb=mem):
                        logger.info(f"Healed sort for {collection}/{stem} after re-download + reconvert")
                        break

            if not _sorted_path(stem).exists():
                logger.error(f"Unable to auto-heal sort for {collection}/{stem}")
                return False

        return True

    def _heal_collection_gz_shards(self, collection: str, gz_filenames: List[str]) -> int:
        """Ensure specific cdx-*.gz shards are present and gzip-valid.

        Returns the number of shards re-downloaded.
        """

        ccindex_dir = self.config.ccindex_root / collection
        ccindex_dir.mkdir(parents=True, exist_ok=True)

        url_map = self._get_ccindex_shard_url_map(collection=collection, ccindex_dir=ccindex_dir)

        redownloaded = 0
        for gz_name in sorted(set(gz_filenames)):
            gz_path = ccindex_dir / gz_name
            needs_fetch = (
                (not gz_path.exists())
                or (gz_path.stat().st_size == 0)
                or (not self._gzip_is_valid(gz_path))
            )

            if not needs_fetch:
                continue

            url = url_map.get(gz_name)
            if not url:
                logger.warning(f"No known download URL for {collection}/{gz_name}; cannot auto-heal this shard")
                continue

            try:
                gz_path.unlink(missing_ok=True)
            except Exception:
                pass

            ok = self._download_to_file(url=url, dest_path=gz_path, retries=3, timeout_seconds=120)
            if not ok:
                logger.warning(f"Failed to re-download {collection}/{gz_name} from {url}")
                continue

            if not self._gzip_is_valid(gz_path):
                logger.warning(
                    f"Re-downloaded {collection}/{gz_name} but gzip validation still fails; leaving it deleted"
                )
                try:
                    gz_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            redownloaded += 1
            logger.info(f"Healed shard {collection}/{gz_name} (re-downloaded + gzip-validated)")

        return redownloaded

    def _get_ccindex_shard_url_map(self, *, collection: str, ccindex_dir: Path) -> Dict[str, str]:
        """Return basename (cdx-*.gz) -> full download URL for a collection."""

        index_list_path = ccindex_dir / "index_files.txt"
        paths: List[str] = []

        if index_list_path.exists():
            try:
                paths = [
                    line.strip()
                    for line in index_list_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                    if line.strip().endswith(".gz")
                ]
            except Exception:
                paths = []

        if not paths:
            list_url = f"https://data.commoncrawl.org/crawl-data/{collection}/cc-index.paths.gz"
            logger.info(f"Fetching shard list for {collection} from {list_url}")
            try:
                req = urllib.request.Request(
                    list_url,
                    headers={"User-Agent": "municipal-scrape-workspace/ccindex"},
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                text = gzip.decompress(raw).decode("utf-8", errors="ignore")
                paths = [line.strip() for line in text.splitlines() if line.strip().endswith(".gz")]

                try:
                    index_list_path.write_text("\n".join(paths) + "\n", encoding="utf-8")
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Failed to fetch shard list for {collection}: {e}")
                paths = []

        url_map: Dict[str, str] = {}
        for p in paths:
            name = os.path.basename(p)
            if not name:
                continue
            url_map[name] = f"https://data.commoncrawl.org/{p}"
        return url_map

    def _download_to_file(self, *, url: str, dest_path: Path, retries: int, timeout_seconds: int) -> bool:
        """Download a URL to a local file atomically."""

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

        last_err: Optional[Exception] = None
        for attempt in range(1, max(1, int(retries)) + 1):
            try:
                req = urllib.request.Request(
                    url,
                    headers={"User-Agent": "municipal-scrape-workspace/ccindex"},
                )
                with urllib.request.urlopen(req, timeout=int(timeout_seconds)) as resp:
                    with open(tmp_path, "wb") as f:
                        shutil.copyfileobj(resp, f)
                os.replace(tmp_path, dest_path)
                return True
            except (HTTPError, URLError, TimeoutError, OSError) as e:
                last_err = e
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                time.sleep(min(10.0, 1.5 * attempt))

        if last_err is not None:
            logger.warning(f"Download failed after {retries} attempts: {url} ({last_err})")
        return False

    def _gzip_is_valid(self, path: Path) -> bool:
        """Return True if the .gz file can be fully decompressed without errors."""

        try:
            if not path.exists() or path.stat().st_size == 0:
                return False
            with gzip.open(path, "rb") as f:
                while f.read(8 * 1024 * 1024):
                    pass
            return True
        except Exception:
            return False
    
    def build_index_for_collection(self, collection: str) -> bool:
        """Build DuckDB pointer index for a collection"""
        logger.info(f"Building DuckDB index for {collection}...")
        
        parquet_dir = self._get_collection_parquet_dir(collection)
        
        if not parquet_dir.exists():
            logger.error(f"Parquet directory does not exist: {parquet_dir}")
            return False
        
        # Store per-collection indexes in cc_pointers_by_collection
        duckdb_dir = self.config.duckdb_collection_root
        duckdb_dir.mkdir(parents=True, exist_ok=True)
        duckdb_path = duckdb_dir / f"{collection}.duckdb"

        # Build index FROM the (sorted) parquet shards.
        # Note: build_cc_pointer_duckdb.py ingests raw cdx-*.gz files, so it will
        # find zero inputs if pointed at the parquet folder.
        build_script = self._resolve_ccindex_helper_script("build_index_from_parquet.py")
        cmd = [
            sys.executable,
            "-u",
            str(build_script),
            "--parquet-root", str(parquet_dir),
            "--output-db", str(duckdb_path),
            "--extract-rowgroups",
        ]

        def _extract_indexing_shard_stem(output_tail: List[str]) -> Optional[str]:
            # Look for the last emitted "Indexing cdx-00000.gz.sorted.parquet..." line.
            import re

            pat = re.compile(r"Indexing\s+(cdx-\d{5})\.gz\.sorted\.parquet")
            for line in reversed(output_tail or []):
                m = pat.search(line)
                if m:
                    return m.group(1)
            return None

        def _tail_has_corrupt_parquet_signal(output_tail: List[str]) -> bool:
            s = "\n".join(output_tail or [])
            # DuckDB raises this for invalid UTF-8 bytes in a VARCHAR column.
            return "Invalid string encoding found in Parquet file" in s or "not valid UTF8" in s

        def _ensure_sort_temp_dir() -> Path | None:
            td = self.config.sort_temp_dir
            if td is None:
                td = parquet_dir / ".duckdb_sort_tmp"
            try:
                td.mkdir(parents=True, exist_ok=True)
                return td
            except Exception:
                return None

        try:
            rc = self._run_subprocess_with_heartbeat(
                cmd,
                heartbeat_label=f"index:{collection}",
                capture_tail_lines=500,
            )
            if rc != 0:
                logger.error(f"Failed to build index for {collection} (exit {rc})")

                # Auto-heal common shard-level corruption cases (e.g. invalid UTF-8 in parquet).
                tail = list(getattr(self, "_last_subprocess_output_tail", []) or [])
                if _tail_has_corrupt_parquet_signal(tail):
                    stem = _extract_indexing_shard_stem(tail)
                    if stem:
                        logger.warning(
                            f"Index build failed due to corrupt parquet signal; attempting shard-level heal for {collection}/{stem}"
                        )

                        # Remove the likely-bad sorted shard so the sort auto-heal will rebuild it.
                        try:
                            (parquet_dir / f"{stem}.gz.sorted.parquet").unlink(missing_ok=True)
                        except Exception:
                            pass

                        # Ensure any partial index artifacts are removed before retry.
                        self._invalidate_duckdb_index(collection)

                        healed = self._autoheal_failed_sorts(
                            collection=collection,
                            parquet_dir=parquet_dir,
                            ccindex_dir=self.config.ccindex_root / collection,
                            sort_temp_dir=_ensure_sort_temp_dir(),
                            baseline_sort_mem_gb=float(getattr(self.config, "sort_memory_per_worker_gb", 4.0) or 4.0),
                        )
                        if healed:
                            logger.info(f"Shard heal complete; retrying index build for {collection}")
                            rc2 = self._run_subprocess_with_heartbeat(
                                cmd,
                                heartbeat_label=f"index:{collection}",
                                capture_tail_lines=500,
                            )
                            if rc2 != 0:
                                logger.error(f"Index rebuild still failing for {collection} (exit {rc2})")
                                return False
                            logger.info(f"Built DuckDB index for {collection} after shard heal")
                        else:
                            logger.error(f"Shard heal failed; cannot continue indexing for {collection}")
                            return False
                    else:
                        logger.error(
                            f"Index build failed with corrupt parquet signal but could not identify shard; not auto-healing"
                        )
                        return False
                else:
                    return False

            logger.info(f"Built DuckDB index for {collection}")
            
            # Verify the index was created and has data
            import duckdb
            import time
            try:
                # If file is locked by another process, just check existence
                if duckdb_path.exists():
                    file_size = duckdb_path.stat().st_size
                    if file_size > 0:
                        logger.info(f"  Index file exists ({file_size:,} bytes)")
                        # Try to verify, but don't fail if locked
                        try:
                            conn = duckdb.connect(str(duckdb_path), read_only=True)
                            # Check which table exists
                            tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
                            
                            if 'domain_pointers' in tables:
                                row_count = conn.execute("SELECT COUNT(*) FROM domain_pointers").fetchone()[0]
                                first_domain_query = "SELECT domain FROM domain_pointers ORDER BY domain LIMIT 1"
                            elif 'cc_domain_shards' in tables:
                                row_count = conn.execute("SELECT COUNT(*) FROM cc_domain_shards").fetchone()[0]
                                first_domain_query = "SELECT host FROM cc_domain_shards ORDER BY host LIMIT 1"
                            else:
                                logger.warning(f"  Unknown table schema in index, tables: {tables}")
                                conn.close()
                                return True
                            
                            logger.info(f"  Index contains {row_count:,} entries")
                            
                            # Check first entry
                            first_domain = conn.execute(first_domain_query).fetchone()
                            if first_domain:
                                logger.info(f"  First domain: {first_domain[0]}")
                            
                            # Check if sorted
                            is_sorted = False
                            if 'domain_pointers' in tables:
                                domains = [row[0] for row in conn.execute("SELECT domain FROM domain_pointers LIMIT 1000").fetchall()]
                                is_sorted = domains == sorted(domains)
                                sort_column = "domain"
                                table_name = "domain_pointers"
                            elif 'cc_domain_shards' in tables:
                                hosts = [row[0] for row in conn.execute("SELECT host_rev FROM cc_domain_shards LIMIT 1000").fetchall()]
                                is_sorted = hosts == sorted(hosts)
                                sort_column = "host_rev"
                                table_name = "cc_domain_shards"
                            
                            logger.info(f"  Index is sorted: {is_sorted}")
                            conn.close()
                            
                            if not is_sorted:
                                logger.info(f"  Sorting index by {sort_column}...")
                                conn = duckdb.connect(str(duckdb_path))
                                
                                if table_name == "domain_pointers":
                                    conn.execute("""
                                        CREATE TABLE domain_pointers_sorted AS 
                                        SELECT * FROM domain_pointers 
                                        ORDER BY domain, parquet_file, row_start;
                                    """)
                                    conn.execute("DROP TABLE domain_pointers;")
                                    conn.execute("ALTER TABLE domain_pointers_sorted RENAME TO domain_pointers;")
                                elif table_name == "cc_domain_shards":
                                    conn.execute("""
                                        CREATE TABLE cc_domain_shards_sorted AS 
                                        SELECT * FROM cc_domain_shards 
                                        ORDER BY host_rev, shard_file;
                                    """)
                                    conn.execute("DROP TABLE cc_domain_shards;")
                                    conn.execute("ALTER TABLE cc_domain_shards_sorted RENAME TO cc_domain_shards;")
                                
                                conn.close()
                                logger.info(f"  ✓ Index sorted by {sort_column}")
                            
                            # Mark as sorted
                            sorted_marker = duckdb_path.with_suffix('.duckdb.sorted')
                            sorted_marker.touch()
                            logger.info(f"  ✓ Index marked as sorted")
                        except Exception as lock_error:
                            if "lock" in str(lock_error).lower() or "conflicting" in str(lock_error).lower():
                                logger.warning(f"  Index is locked by another process, skipping verification")
                            else:
                                raise
                        return True
                    else:
                        logger.error(f"  Index file is empty")
                        return False
                else:
                    logger.error(f"  Index file was not created")
                    return False
            except Exception as verify_error:
                logger.error(f"Failed to verify index: {verify_error}")
                return False
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build index for {collection}: {e}")
            if e.stdout:
                logger.error(f"stdout: {e.stdout}")
            if e.stderr:
                logger.error(f"stderr: {e.stderr}")
            return False
    
    def process_collection(self, collection: str) -> bool:
        """Process a single collection through all pipeline stages"""
        status = self.validator.validate_collection(collection)

        if self.force_reindex:
            # Force Stage 4 to re-run even if the collection is otherwise complete.
            self._invalidate_duckdb_index(collection)
            status["duckdb_index_exists"] = False
            status["duckdb_index_sorted"] = False
            status["complete"] = False
        
        logger.info(f"\nProcessing {collection}:")
        sources_required = status['parquet_count'] < status['parquet_expected']
        sources_note = "" if sources_required else " (optional; parquet complete)"
        logger.info(f"  Sources: {status['tar_gz_count']}/{status['tar_gz_expected']}{sources_note}")
        logger.info(f"  Converted: {status['parquet_count']}/{status['parquet_expected']}")
        logger.info(f"  Sorted: {status['sorted_count']}/{status['parquet_expected']}")
        logger.info(f"  Indexed: {status['duckdb_index_exists']} (sorted: {status['duckdb_index_sorted']})")
        
        if status['complete']:
            logger.info(f"  ✓ {collection} is complete, skipping")
            # Optional post-completion cleanup (useful on resume runs).
            if getattr(self.config, "cleanup_extraneous", False) or getattr(self.config, "cleanup_source_archives", False):
                self.cleanup_collection_extraneous(collection)
            return True
        
        # Check resources before each stage
        if not self.check_resources():
            logger.error("Insufficient resources, stopping")
            return False
        
        # Stage 1: Download
        # Source shards are only required to (re)run Stage 2 conversions. If parquet
        # is already complete (e.g. after cleanup removed cdx-*.gz), do not
        # re-download sources just to satisfy tar_gz_count.
        if sources_required and status['tar_gz_count'] < status['tar_gz_expected']:
            logger.info(f"  Stage 1: Downloading {status['tar_gz_expected'] - status['tar_gz_count']} .gz files...")
            if not self.download_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.info(f"  ✓ Downloaded: {status['tar_gz_count']}/{status['tar_gz_expected']}")
        elif sources_required:
            logger.info(f"  ✓ Stage 1: Downloads complete ({status['tar_gz_count']}/{status['tar_gz_expected']})")
        else:
            if status['tar_gz_count'] < status['tar_gz_expected']:
                logger.info(
                    "  ✓ Stage 1: Sources missing but not required "
                    f"(parquet complete: {status['parquet_count']}/{status['parquet_expected']})"
                )
            else:
                logger.info(f"  ✓ Stage 1: Downloads complete ({status['tar_gz_count']}/{status['tar_gz_expected']})")
        
        # Stage 2: Convert
        if status['parquet_count'] < status['parquet_expected']:
            logger.info(f"  Stage 2: Converting {status['parquet_expected'] - status['parquet_count']} parquet files...")
            if not self.convert_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.info(f"  ✓ Converted: {status['parquet_count']}/{status['parquet_expected']}")
        else:
            logger.info(f"  ✓ Stage 2: Conversions complete ({status['parquet_count']}/{status['parquet_expected']})")
        
        # Stage 3: Sort
        if status['sorted_count'] < status['parquet_expected']:
            logger.info(f"  Stage 3: Sorting {status['parquet_expected'] - status['sorted_count']} parquet files...")
            if not self.sort_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.info(f"  ✓ Sorted: {status['sorted_count']}/{status['parquet_expected']}")
        else:
            logger.info(f"  ✓ Stage 3: Sorting complete ({status['sorted_count']}/{status['parquet_expected']})")
        
        # Stage 4: Index
        if not status['duckdb_index_exists'] or not status['duckdb_index_sorted']:
            logger.info(f"  Stage 4: Building DuckDB index (exists: {status['duckdb_index_exists']}, sorted: {status['duckdb_index_sorted']})...")
            if not self.build_index_for_collection(collection):
                return False
            # Re-verify after building
            status = self.validator.validate_collection(collection)
            logger.info(f"  ✓ Index built and verified: exists={status['duckdb_index_exists']}, sorted={status['duckdb_index_sorted']}")
        else:
            logger.info(f"  ✓ Stage 4: Index complete and sorted")
        
        # Final re-validation gate: only claim completion if validator agrees.
        status = self.validator.validate_collection(collection)
        if status.get('complete'):
            logger.info(f"  ✓ {collection} processing complete")
            # Cleanup after the collection is truly complete (index exists + sorted).
            # This is where optional source-archive cleanup can safely happen.
            if getattr(self.config, "cleanup_extraneous", False) or getattr(self.config, "cleanup_source_archives", False):
                self.cleanup_collection_extraneous(collection)
            return True

        # Recompute after final validation so the message reflects current needs.
        # (Stage 2 may have run and changed parquet_count.)
        sources_suffix_final = " (optional)" if status['parquet_count'] >= status['parquet_expected'] else ""
        logger.warning(
            f"  ⚠️  {collection} finished stages but is still incomplete: "
            f"sources={status['tar_gz_count']}/{status['tar_gz_expected']}{sources_suffix_final} "
            f"converted={status['parquet_count']}/{status['parquet_expected']} "
            f"sorted={status['sorted_count']}/{status['parquet_expected']} "
            f"indexed={status['duckdb_index_exists']} (sorted={status['duckdb_index_sorted']})"
        )
        return False
    
    def build_meta_indexes(self, *, year: Optional[str] = None) -> bool:
        """Build year-level and master meta-indexes.

        NOTE: This is intentionally only meant to run after an entire year of
        collections is complete (not after a single collection filter like
        '2024-26').
        """
        try:
            # Step 1: Build year-level indexes
            logger.info("\nStep 1: Building year-level meta-indexes...")
            year_index_script = Path(__file__).parent / "build_year_meta_indexes.py"
            if not year_index_script.exists():
                logger.error(f"Year index builder not found: {year_index_script}")
                return False
            
            cmd = [
                sys.executable,
                str(year_index_script),
                "--collection-dir", str(self.config.duckdb_collection_root),
                "--output-dir", str(self.config.duckdb_year_root),
            ]

            if year:
                cmd += ["--year", str(year)]
            
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(result.stdout)
            logger.info("✓ Year-level indexes built")
            
            # Step 2: Build master index
            logger.info("\nStep 2: Building master meta-index...")
            master_index_script = Path(__file__).parent / "build_master_index.py"
            if not master_index_script.exists():
                logger.error(f"Master index builder not found: {master_index_script}")
                return False
            
            cmd = [
                sys.executable,
                str(master_index_script),
                "--year-dir", str(self.config.duckdb_year_root),
                "--output", str(self.config.duckdb_master_root / "cc_master_index.duckdb"),
            ]
            
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(result.stdout)
            logger.info("✓ Master index built")
            
            # Print final statistics
            logger.info("\nFinal Index Statistics:")
            cmd = [
                sys.executable,
                str(master_index_script),
                "--stats",
                "--output", str(self.config.duckdb_master_root / "cc_master_index.duckdb"),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(result.stdout)
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build meta-indexes: {e}")
            if e.stdout:
                logger.error(f"stdout: {e.stdout}")
            if e.stderr:
                logger.error(f"stderr: {e.stderr}")
            return False
    
    def run_pipeline(self, resume: bool = True):
        """Run the complete pipeline"""
        logger.info("=" * 80)
        logger.info("Common Crawl Pipeline Orchestrator")
        logger.info("=" * 80)
        
        # Scan all collections
        self.scan_all_collections()
        
        # Show overall status
        complete = sum(1 for s in self.collection_status.values() if s.get('complete', False))
        total = len(self.collections)
        logger.info(f"\nOverall Status: {complete}/{total} collections complete")
        
        # Group collections by status
        incomplete = [c for c, s in self.collection_status.items() if not s.get('complete', False)]
        
        if not incomplete and not self.force_reindex:
            logger.info("\n✓ All collections are complete!")

            # Even if no work is needed, we may still want to reclaim disk by
            # sweeping completed collections for cleanup items.
            if getattr(self.config, "cleanup_extraneous", False) or getattr(self.config, "cleanup_source_archives", False):
                logger.info("\n[cleanup] Sweeping completed collections...")
                for collection in self.collections:
                    try:
                        self.cleanup_collection_extraneous(collection)
                    except Exception as e:
                        logger.warning(f"[cleanup] Sweep failed for {collection}: {e}")
            return

        targets = list(incomplete)
        if self.force_reindex:
            # When forcing reindex, we still want to process collections even if complete.
            targets = list(self.collections)
            logger.info(f"\nForce-reindex enabled: processing {len(targets)} collections for DuckDB rebuild")
        else:
            logger.info(f"\nProcessing {len(targets)} incomplete collections...")
        
        # Process incomplete collections
        for collection in targets:
            ok = self.process_collection(collection)
            # Rescan to update status (even on failure) so the summary reflects
            # any successful work done before the failure.
            self.collection_status[collection] = self.validator.validate_collection(collection)
            if not ok:
                logger.error(f"Failed to process {collection}, stopping pipeline")
                break
        
        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("Pipeline Summary")
        logger.info("=" * 80)
        
        complete = sum(1 for s in self.collection_status.values() if s.get('complete', False))
        logger.info(f"Complete: {complete}/{total} collections")
        
        incomplete = [c for c, s in self.collection_status.items() if not s.get('complete', False)]
        if incomplete:
            logger.info(f"\nIncomplete collections ({len(incomplete)}):")
            for c in incomplete:
                s = self.collection_status[c]
                pct = (s['sorted_count'] / s['parquet_expected'] * 100) if s['parquet_expected'] > 0 else 0
                logger.info(f"  {c}: {pct:.1f}% sorted ({s['sorted_count']}/{s['parquet_expected']})")
        
        # Build meta-indexes only after a full-year run is complete.
        # If the user filtered to a single collection (e.g. '2024-26'), skip.
        if not incomplete:
            filter_str = (self.config.collections_filter or "").strip()
            is_full_year = len(filter_str) == 4 and filter_str.isdigit()
            if is_full_year:
                logger.info("\n" + "=" * 80)
                logger.info("Building Meta-Indexes")
                logger.info("=" * 80)
                try:
                    self.build_meta_indexes(year=filter_str)
                except Exception as e:
                    logger.error(f"Meta-index build failed (non-fatal): {e}")
            else:
                logger.info(
                    f"Skipping meta-index build (filter={filter_str or 'None'}; only runs for full-year filters like '2024')."
                )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Common Crawl Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default="pipeline_config.json",
        help="Path to JSON configuration file (default: pipeline_config.json)"
    )
    parser.add_argument(
        "--ccindex-root",
        type=Path,
        help="Root directory for downloaded .gz files (overrides config file)"
    )
    parser.add_argument(
        "--parquet-root",
        type=Path,
        help="Root directory for parquet files (overrides config file)"
    )
    parser.add_argument(
        "--duckdb-root",
        type=Path,
        help="Root directory for DuckDB indexes (overrides config file)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        help=f"Maximum worker processes (overrides config file; default: {DEFAULT_MAX_WORKERS} when not set)"
    )
    parser.add_argument(
        "--filter",
        type=str,
        help=(
            "Filter collections (e.g., '2024' or '2025-05'). "
            f"Default: '{DEFAULT_COLLECTION_FILTER}' when not set in config/CLI. "
            "Use '--filter all' to process all collections."
        ),
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        default=False,
        help="Only download CC URL index shards (cdx-*.gz); do not convert/sort/index",
    )
    parser.add_argument(
        "--force-reindex",
        action="store_true",
        default=False,
        help="Force rebuilding DuckDB indexes even if collections are complete",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Resume from where pipeline left off (default: True)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging"
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=30,
        help="Print a periodic heartbeat every N seconds during long phases (default: 30)",
    )
    parser.add_argument(
        "--cleanup-extraneous",
        dest="cleanup_extraneous",
        action="store_true",
        default=None,
        help="Enable cleanup of safe-to-delete artifacts (default: enabled)",
    )
    parser.add_argument(
        "--no-cleanup-extraneous",
        dest="cleanup_extraneous",
        action="store_false",
        default=None,
        help="Disable cleanup of safe-to-delete artifacts",
    )
    parser.add_argument(
        "--cleanup-dry-run",
        action="store_true",
        help="Dry-run cleanup (log what would be removed, without deleting)",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Only run cleanup sweeps (no download/convert/sort/index); supports --cleanup-dry-run",
    )
    parser.add_argument(
        "--cleanup-source-archives",
        dest="cleanup_source_archives",
        action="store_true",
        default=None,
        help="Enable removal of source archives (cdx-*.gz, *.tar.gz) once a collection is fully complete (default: enabled)",
    )
    parser.add_argument(
        "--no-cleanup-source-archives",
        dest="cleanup_source_archives",
        action="store_false",
        default=None,
        help="Disable removal of source archives",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Assume yes for cleanup confirmations (use with care)",
    )
    parser.add_argument(
        "--sort-workers",
        type=int,
        default=None,
        help="Parallel workers for sorting unsorted parquet (default: uses --workers; beware memory/disk)",
    )
    parser.add_argument(
        "--sort-memory-per-worker-gb",
        type=float,
        default=4.0,
        help="DuckDB memory limit per sort worker in GB (default: 4.0)",
    )
    parser.add_argument(
        "--sort-temp-dir",
        type=Path,
        default=None,
        help="Temp directory for DuckDB sort spill (default: system temp)",
    )
    
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration from file with command-line overrides
    config = PipelineConfig.from_args(args)

    # Apply runtime-only args (these are safe defaults if config file doesn't include them).
    config.heartbeat_seconds = int(args.heartbeat_seconds)
    if getattr(args, "cleanup_extraneous", None) is not None:
        config.cleanup_extraneous = bool(args.cleanup_extraneous)
    config.cleanup_dry_run = bool(args.cleanup_dry_run)
    if getattr(args, "cleanup_source_archives", None) is not None:
        config.cleanup_source_archives = bool(args.cleanup_source_archives)
    config.sort_workers = args.sort_workers
    config.sort_memory_per_worker_gb = float(args.sort_memory_per_worker_gb)
    config.sort_temp_dir = args.sort_temp_dir
    config.force_reindex = bool(args.force_reindex)

    # Normalize/assign defaults for core behavior.
    config.max_workers = int(getattr(config, "max_workers", 0) or 0)
    if config.max_workers <= 0:
        config.max_workers = DEFAULT_MAX_WORKERS

    # Default to 2023 collections unless the user/config explicitly specifies a filter.
    raw_config_filter = getattr(config, "collections_filter", None)
    if getattr(args, "filter", None) is not None:
        # CLI always wins, including special values like 'all'/'none'/'*'.
        config.collections_filter = _normalize_collections_filter(args.filter)
    elif raw_config_filter is not None:
        # Config explicitly specified a filter string.
        config.collections_filter = _normalize_collections_filter(raw_config_filter)
    else:
        # No CLI filter and no config filter field -> apply default.
        config.collections_filter = _normalize_collections_filter(DEFAULT_COLLECTION_FILTER)

    # Note: cleanup is enabled by default; use --no-cleanup-* flags to disable.
    
    # Log the active configuration
    logger.info("")
    logger.info("Active Configuration:")
    logger.info(f"  ccindex_root:          {config.ccindex_root}")
    logger.info(f"  parquet_root:          {config.parquet_root}")
    logger.info(f"  duckdb_collection_root:{config.duckdb_collection_root}")
    logger.info(f"  duckdb_year_root:      {config.duckdb_year_root}")
    logger.info(f"  duckdb_master_root:    {config.duckdb_master_root}")
    logger.info(f"  max_workers:           {config.max_workers}")
    logger.info(f"  memory_limit:          {config.memory_limit_gb} GB")
    logger.info(f"  min_free:              {config.min_free_space_gb} GB")
    if config.collections_filter:
        logger.info(f"  filter:                {config.collections_filter}")
    logger.info(f"  heartbeat_seconds:      {config.heartbeat_seconds}")
    logger.info(f"  cleanup_extraneous:     {config.cleanup_extraneous}")
    logger.info(f"  cleanup_dry_run:        {config.cleanup_dry_run}")
    logger.info(f"  cleanup_source_archives:{config.cleanup_source_archives}")
    logger.info(f"  cleanup_only:           {bool(args.cleanup_only)}")
    logger.info(f"  force_reindex:          {config.force_reindex}")
    logger.info(f"  sort_workers:           {config.sort_workers if config.sort_workers else config.max_workers}")
    logger.info(f"  sort_mem_per_worker_gb: {config.sort_memory_per_worker_gb}")
    logger.info(f"  sort_temp_dir:          {config.sort_temp_dir}")
    logger.info("")
    
    orchestrator = PipelineOrchestrator(config)

    if args.cleanup_only:
        orchestrator.run_cleanup_only(assume_yes=bool(args.yes))
        return 0

    if args.download_only:
        orchestrator.run_download_only(resume=bool(args.resume))
        return 0

    orchestrator.run_pipeline(resume=args.resume)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
