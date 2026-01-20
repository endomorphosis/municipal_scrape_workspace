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
import json
import logging
import os
import selectors
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import psutil

from municipal_scrape_workspace.ccindex.validate_collection_completeness import CollectionValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


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
    cleanup_extraneous: bool = False
    cleanup_dry_run: bool = False
    cleanup_source_archives: bool = False
    sort_workers: Optional[int] = None
    sort_memory_per_worker_gb: float = 4.0
    sort_temp_dir: Optional[Path] = None
    
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
            if hasattr(args, 'workers') and args.workers:
                logger.info(f"Overriding workers: {args.workers}")
                config.max_workers = args.workers
            if hasattr(args, 'filter') and args.filter:
                config.collections_filter = args.filter
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
                max_workers=args.workers if hasattr(args, 'workers') else 8,
                memory_limit_gb=10.0,
                min_free_space_gb=50.0,
                collections_filter=args.filter if hasattr(args, 'filter') and args.filter else None
            )


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

    def _run_subprocess_with_heartbeat(
        self,
        cmd: List[str],
        *,
        cwd: Optional[Path] = None,
        heartbeat_label: str = "",
    ) -> int:
        """Run a subprocess while streaming output and printing periodic heartbeats.

        This avoids long silent stretches that look like a stall when the child
        process is doing work without producing output.
        """

        hb_seconds = max(1, int(getattr(self.config, "heartbeat_seconds", 30) or 30))
        label = f"[{heartbeat_label}] " if heartbeat_label else ""
        logger.info(f"{label}Running: {' '.join(cmd)}")

        start = time.monotonic()

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
                        logger.info(f"{label}{line.rstrip()}" )
                    break

                events = sel.select(timeout=hb_seconds)
                if events:
                    for key, _mask in events:
                        line = key.fileobj.readline()
                        if line:
                            logger.info(f"{label}{line.rstrip()}" )
                else:
                    now = time.monotonic()
                    elapsed = now - start
                    logger.info(f"{label}Heartbeat: still running (elapsed {elapsed/60:.1f} min)")
        finally:
            try:
                sel.unregister(proc.stdout)
            except Exception:
                pass

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

    def _get_collection_parquet_dir(self, collection: str) -> Path:
        """Return the on-disk parquet directory for a collection.

        Prefer the validator's primary layout:
          <parquet_root>/cc_pointers_by_collection/<year>/<collection>/
        Fall back to:
          <parquet_root>/<year>/<collection>/
          <parquet_root>/<collection>/
        """

        year = self._collection_year(collection)
        if year:
            primary = self.config.parquet_root / "cc_pointers_by_collection" / year / collection
            if primary.exists():
                return primary
            secondary = self.config.parquet_root / year / collection
            if secondary.exists():
                return secondary

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
            logger.warning(f"Low memory: {mem_gb:.1f} GB available, need {self.config.memory_limit_gb:.1f} GB")
            return False
        
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
        
        # Prefer validator's primary structure.
        year = self._collection_year(collection)
        if year:
            parquet_dir = self.config.parquet_root / "cc_pointers_by_collection" / year / collection
        else:
            parquet_dir = self.config.parquet_root / collection
        parquet_dir.mkdir(parents=True, exist_ok=True)
        
        # Count existing parquet files to track resume progress
        existing_parquet = list(parquet_dir.glob("cdx-*.gz.parquet"))
        existing_sorted = list(parquet_dir.glob("cdx-*.gz.sorted.parquet"))
        logger.info(f"  Resume: {len(existing_parquet)} parquet, {len(existing_sorted)} sorted already exist")
        
        # Use bulk_convert_gz_to_parquet.py to convert (it has skip_existing logic)
        cmd = [
            sys.executable,
            "bulk_convert_gz_to_parquet.py",
            "--input-dir", str(ccindex_dir),
            "--output-dir", str(parquet_dir),
            "--workers", str(self.config.max_workers),
            "--heartbeat-seconds", str(int(getattr(self.config, "heartbeat_seconds", 30) or 30)),
        ]
        
        try:
            logger.debug(f"Running: {' '.join(cmd)}")
            # Stream subprocess output so the pipeline doesn't look "stalled" for long runs.
            subprocess.run(cmd, check=True)
            logger.info(f"Converted {collection} successfully")
            
            # If requested, immediately sort the newly converted files
            if sort_after:
                logger.info(f"Sorting newly converted files for {collection}...")
                return self.sort_collection(collection)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to convert {collection}: {e}")
            # stderr/stdout were streamed to the console; include only minimal context here.
            return False
    
    def sort_collection(self, collection: str) -> bool:
        """Sort a collection's parquet files by (host_rev, url, ts).

        Uses validate_and_mark_sorted.py which:
        - validates files,
        - sorts any unsorted files, and
        - renames them to *.gz.sorted.parquet (validator convention).
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
                rebuild_cmd = [
                    sys.executable,
                    "bulk_convert_gz_to_parquet.py",
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
            sort_workers = int(self.config.sort_workers) if self.config.sort_workers else max(2, int(self.config.max_workers))
            # Keep a conservative default per-sort memory unless user overrides.
            sort_mem_gb = float(getattr(self.config, "sort_memory_per_worker_gb", 4.0) or 4.0)

            # Use a temp dir on the same filesystem as the parquet output by default.
            # This avoids /tmp space pressure and speeds up large external sorts.
            sort_temp_dir = self.config.sort_temp_dir
            if sort_temp_dir is None:
                sort_temp_dir = parquet_dir / ".duckdb_sort_tmp"
                try:
                    sort_temp_dir.mkdir(parents=True, exist_ok=True)
                except Exception:
                    sort_temp_dir = None
            cmd = [
                sys.executable,
                "validate_and_mark_sorted.py",
                "--parquet-root", str(parquet_dir),
                "--sort-unsorted",
                "--workers", str(self.config.max_workers),
                "--sort-workers", str(sort_workers),
                "--memory-per-sort", str(sort_mem_gb),
                "--heartbeat-seconds", str(int(getattr(self.config, "heartbeat_seconds", 30) or 30)),
            ]

            if sort_temp_dir:
                cmd.extend(["--temp-dir", str(sort_temp_dir)])

            # Stream output so progress is visible during long sorts.
            result = subprocess.run(cmd)
            if result.returncode != 0:
                logger.error(f"Failed to sort/mark parquet for {collection} (exit {result.returncode})")
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
        cmd = [
            sys.executable,
            "-u",
            "build_index_from_parquet.py",
            "--parquet-root", str(parquet_dir),
            "--output-db", str(duckdb_path),
            "--extract-rowgroups",
        ]

        try:
            rc = self._run_subprocess_with_heartbeat(cmd, heartbeat_label=f"index:{collection}")
            if rc != 0:
                logger.error(f"Failed to build index for {collection} (exit {rc})")
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
        
        logger.info(f"\nProcessing {collection}:")
        logger.info(f"  Downloaded: {status['tar_gz_count']}/{status['tar_gz_expected']}")
        logger.info(f"  Converted: {status['parquet_count']}/{status['parquet_expected']}")
        logger.info(f"  Sorted: {status['sorted_count']}/{status['parquet_expected']}")
        logger.info(f"  Indexed: {status['duckdb_index_exists']} (sorted: {status['duckdb_index_sorted']})")
        
        if status['complete']:
            logger.info(f"  ✓ {collection} is complete, skipping")
            return True
        
        # Check resources before each stage
        if not self.check_resources():
            logger.error("Insufficient resources, stopping")
            return False
        
        # Stage 1: Download
        if status['tar_gz_count'] < status['tar_gz_expected']:
            logger.info(f"  Stage 1: Downloading {status['tar_gz_expected'] - status['tar_gz_count']} .gz files...")
            if not self.download_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.info(f"  ✓ Downloaded: {status['tar_gz_count']}/{status['tar_gz_expected']}")
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
            return True

        logger.warning(
            f"  ⚠️  {collection} finished stages but is still incomplete: "
            f"downloaded={status['tar_gz_count']}/{status['tar_gz_expected']} "
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
        
        if not incomplete:
            logger.info("\n✓ All collections are complete!")
            return
        
        logger.info(f"\nProcessing {len(incomplete)} incomplete collections...")
        
        # Process incomplete collections
        for collection in incomplete:
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
        help="Maximum worker processes (overrides config file)"
    )
    parser.add_argument(
        "--filter",
        type=str,
        help="Filter collections (e.g., '2024' or '2025-05')"
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        default=False,
        help="Only download CC URL index shards (cdx-*.gz); do not convert/sort/index",
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
        action="store_true",
        help="Remove safe-to-delete extraneous artifacts (tmp files, duplicate unsorted shards, zero-byte outputs)",
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
        action="store_true",
        help="Also remove source archives (cdx-*.gz, *.tar.gz) for collections that are fully complete",
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
    config.cleanup_extraneous = bool(args.cleanup_extraneous)
    config.cleanup_dry_run = bool(args.cleanup_dry_run)
    config.cleanup_source_archives = bool(args.cleanup_source_archives)
    config.sort_workers = args.sort_workers
    config.sort_memory_per_worker_gb = float(args.sort_memory_per_worker_gb)
    config.sort_temp_dir = args.sort_temp_dir
    
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
