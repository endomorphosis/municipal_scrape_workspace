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
import multiprocessing
import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import psutil

# Import existing validation logic
sys.path.insert(0, str(Path(__file__).parent))
from validate_collection_completeness import CollectionValidator

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
    duckdb_root: Path
    max_workers: int
    memory_limit_gb: float
    min_free_space_gb: float
    collections_filter: Optional[str] = None
    
    def __post_init__(self):
        self.ccindex_root = Path(self.ccindex_root)
        self.parquet_root = Path(self.parquet_root)
        self.duckdb_root = Path(self.duckdb_root)
    
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
            if hasattr(args, 'duckdb_root') and args.duckdb_root:
                logger.info(f"Overriding duckdb_root: {args.duckdb_root}")
                config.duckdb_root = Path(args.duckdb_root)
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
                duckdb_root=Path(args.duckdb_root) if hasattr(args, 'duckdb_root') and args.duckdb_root else Path('/storage/ccindex_duckdb/cc_pointers_by_collection'),
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
            pointer_dir=config.duckdb_root
        )
        self.collections: List[str] = []
        self.collection_status: Dict[str, dict] = {}
        
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
        
        for path in [self.config.ccindex_root, self.config.parquet_root, self.config.duckdb_root]:
            free_gb = self.get_free_space_gb(path)
            if free_gb < self.config.min_free_space_gb:
                logger.warning(f"Low disk space at {path}: {free_gb:.1f} GB free, need {self.config.min_free_space_gb:.1f} GB")
                return False
        
        return True
    
    def download_collection(self, collection: str) -> bool:
        """Download a collection's .gz files using existing download script"""
        logger.info(f"Downloading {collection}...")
        
        # Use the actual download script with collection-specific logic
        download_script = Path(__file__).parent / "download_cc_indexes.sh"
        if not download_script.exists():
            logger.error(f"Download script not found: {download_script}")
            return False
        
        # Download to collection-specific directory
        collection_dir = self.config.ccindex_root / collection
        collection_dir.mkdir(parents=True, exist_ok=True)
        
        cmd = [
            "bash", str(download_script),
            collection
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=str(self.config.ccindex_root))
            logger.info(f"Downloaded {collection} successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to download {collection}: {e.stderr if e.stderr else e}")
            return False
    
    def convert_collection(self, collection: str, sort_after: bool = True) -> bool:
        """Convert a collection's .gz files to parquet, optionally sorting immediately"""
        logger.info(f"Converting {collection} to parquet (sort_after={sort_after})...")
        
        year = collection.split('-')[2]
        ccindex_dir = self.config.ccindex_root / collection
        parquet_dir = self.config.parquet_root / year / collection
        parquet_dir.mkdir(parents=True, exist_ok=True)
        
        # Use bulk_convert_gz_to_parquet.py to convert
        cmd = [
            sys.executable,
            "bulk_convert_gz_to_parquet.py",
            "--input-dir", str(ccindex_dir),
            "--output-dir", str(parquet_dir),
            "--workers", str(self.config.max_workers)
        ]
        
        try:
            logger.debug(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Converted {collection} successfully")
            
            # If requested, immediately sort the newly converted files
            if sort_after:
                logger.info(f"Sorting newly converted files for {collection}...")
                return self.sort_collection(collection)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to convert {collection}: {e}")
            logger.error(f"STDERR: {e.stderr}")
            return False
    
    def sort_collection(self, collection: str) -> bool:
        """Sort a collection's parquet files by domain using external merge sort"""
        logger.info(f"Sorting {collection} with external merge sort...")
        
        # Find all unsorted files for this collection in the correct locations
        unsorted_files = []
        
        # Extract year from collection name
        year = collection.split('-')[2] if len(collection.split('-')) > 2 else None
        
        # Check in organized subdirectories (primary location)
        if year:
            collection_subdir = self.config.parquet_root / "cc_pointers_by_collection" / year / collection
            if collection_subdir.exists():
                for parquet_file in collection_subdir.glob("cdx-*.gz.parquet"):
                    # Skip files that have a .sorted.parquet counterpart or are already .sorted.parquet
                    if '.sorted.parquet' in parquet_file.name:
                        continue
                    # Check if sorted version exists
                    sorted_version = parquet_file.parent / parquet_file.name.replace('.gz.parquet', '.gz.sorted.parquet')
                    if sorted_version.exists():
                        continue
                    unsorted_files.append(parquet_file)
        
        # Also check year-organized subdirectories
        if year and not unsorted_files:
            year_subdir = self.config.parquet_root / year / collection
            if year_subdir.exists():
                for parquet_file in year_subdir.glob("cdx-*.gz.parquet"):
                    if '.sorted.parquet' in parquet_file.name:
                        continue
                    sorted_version = parquet_file.parent / parquet_file.name.replace('.gz.parquet', '.gz.sorted.parquet')
                    if sorted_version.exists():
                        continue
                    unsorted_files.append(parquet_file)
        
        # Fallback: flat directory (legacy)
        if not unsorted_files:
            for parquet_file in self.config.parquet_root.glob(f"{collection}-*.gz.parquet"):
                if '.sorted.parquet' in parquet_file.name:
                    continue
                sorted_version = parquet_file.parent / parquet_file.name.replace('.gz.parquet', '.gz.sorted.parquet')
                if sorted_version.exists():
                    continue
                unsorted_files.append(parquet_file)
        
        if not unsorted_files:
            logger.info(f"No unsorted files found for {collection}")
            return True
        
        logger.info(f"Found {len(unsorted_files)} unsorted files for {collection}")
        
        # Use external merge sort for memory-efficient sorting
        try:
            # Sort each file using external merge sort
            cmd = [
                sys.executable,
                "sort_parquet_external_merge.py",
                "--workers", str(self.config.max_workers),
                "--chunk-size", "50000",  # Process 50k rows at a time
            ] + [str(f) for f in unsorted_files]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Sorted {len(unsorted_files)} files for {collection}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to sort {collection}: {e}")
            if e.stdout:
                logger.error(f"stdout: {e.stdout}")
            if e.stderr:
                logger.error(f"stderr: {e.stderr}")
            return False
        finally:
            # Clean up temp file
            import os
            if os.path.exists(unsorted_list_path):
                os.unlink(unsorted_list_path)
    
    def build_index_for_collection(self, collection: str) -> bool:
        """Build DuckDB pointer index for a collection"""
        logger.info(f"Building DuckDB index for {collection}...")
        
        year = collection.split('-')[2]
        parquet_dir = self.config.parquet_root / year / collection
        duckdb_dir = self.config.duckdb_root / "cc_pointers_by_collection"
        duckdb_dir.mkdir(parents=True, exist_ok=True)
        duckdb_path = duckdb_dir / f"{collection}.duckdb"
        
        # Use build_cc_pointer_duckdb.py
        cmd = [
            sys.executable,
            "build_cc_pointer_duckdb.py",
            "--input-root", str(self.config.parquet_root),
            "--db", str(duckdb_path),
            "--collections", collection,
            "--threads", str(self.config.max_workers),
            "--duckdb-index-mode", "domain",
            "--domain-range-index"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Built DuckDB index for {collection}")
            
            # Mark as sorted
            sorted_marker = duckdb_path.with_suffix('.sorted')
            sorted_marker.touch()
            
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build index for {collection}: {e}")
            return False
    
    def process_collection(self, collection: str) -> bool:
        """Process a single collection through all pipeline stages"""
        status = self.validator.validate_collection(collection)
        
        logger.info(f"\nProcessing {collection}:")
        logger.info(f"  Downloaded: {status['tar_gz_count']}/{status['tar_gz_expected']}")
        logger.info(f"  Converted: {status['parquet_count']}/{status['parquet_expected']}")
        logger.info(f"  Sorted: {status['sorted_count']}/{status['parquet_expected']}")
        logger.info(f"  Indexed: {status['duckdb_index_exists']} (sorted: {status['duckdb_index_sorted']})")
        
        logger.debug(f"Status details: {status}")
        
        if status['complete']:
            logger.info(f"  ✓ {collection} is complete, skipping")
            return True
        
        # Check resources before each stage
        if not self.check_resources():
            logger.error("Insufficient resources, stopping")
            return False
        
        # Stage 1: Download
        if status['tar_gz_count'] < status['tar_gz_expected']:
            logger.debug(f"Stage 1: Need to download {status['tar_gz_expected'] - status['tar_gz_count']} .gz files")
            if not self.download_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.debug(f"After download: {status['tar_gz_count']}/{status['tar_gz_expected']}")
        else:
            logger.debug(f"Stage 1: Downloads complete ({status['tar_gz_count']}/{status['tar_gz_expected']})")
        
        # Stage 2: Convert
        if status['parquet_count'] < status['parquet_expected']:
            logger.debug(f"Stage 2: Need to convert {status['parquet_expected'] - status['parquet_count']} parquet files")
            if not self.convert_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.debug(f"After convert: {status['parquet_count']}/{status['parquet_expected']}")
        else:
            logger.debug(f"Stage 2: Conversions complete ({status['parquet_count']}/{status['parquet_expected']})")
        
        # Stage 3: Sort
        if status['sorted_count'] < status['parquet_expected']:
            logger.debug(f"Stage 3: Need to sort {status['parquet_expected'] - status['sorted_count']} parquet files")
            if not self.sort_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.debug(f"After sort: {status['sorted_count']}/{status['parquet_expected']}")
        else:
            logger.debug(f"Stage 3: Sorting complete ({status['sorted_count']}/{status['parquet_expected']})")
        
        # Stage 4: Index
        if not status['duckdb_index_exists'] or not status['duckdb_index_sorted']:
            logger.debug(f"Stage 4: Need to build/update index (exists: {status['duckdb_index_exists']}, sorted: {status['duckdb_index_sorted']})")
            if not self.build_index_for_collection(collection):
                return False
            status = self.validator.validate_collection(collection)
            logger.debug(f"After index: exists={status['duckdb_index_exists']}, sorted={status['duckdb_index_sorted']}")
        else:
            logger.debug(f"Stage 4: Index complete and sorted")
        
        logger.info(f"  ✓ {collection} processing complete")
        return True
    
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
            if not self.process_collection(collection):
                logger.error(f"Failed to process {collection}, stopping pipeline")
                break
            
            # Rescan to update status
            self.collection_status[collection] = self.validator.validate_collection(collection)
        
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


def main():
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
    
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration from file with command-line overrides
    config = PipelineConfig.from_args(args)
    
    # Log the active configuration
    logger.info("")
    logger.info("Active Configuration:")
    logger.info(f"  ccindex_root:  {config.ccindex_root}")
    logger.info(f"  parquet_root:  {config.parquet_root}")
    logger.info(f"  duckdb_root:   {config.duckdb_root}")
    logger.info(f"  max_workers:   {config.max_workers}")
    logger.info(f"  memory_limit:  {config.memory_limit_gb} GB")
    logger.info(f"  min_free:      {config.min_free_space_gb} GB")
    if config.collections_filter:
        logger.info(f"  filter:        {config.collections_filter}")
    logger.info("")
    
    orchestrator = PipelineOrchestrator(config)
    orchestrator.run_pipeline(resume=args.resume)


if __name__ == "__main__":
    main()
