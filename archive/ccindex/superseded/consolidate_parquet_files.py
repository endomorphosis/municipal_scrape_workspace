#!/usr/bin/env python3
"""
Consolidate scattered parquet files into a single consistent structure.

Finds all parquet files across different directory structures and consolidates
them into /storage/ccindex_parquet/CC-MAIN-*/ with consistent naming.
"""

import logging
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_all_parquet_files(root: Path) -> Dict[str, List[Path]]:
    """Find all parquet files and group by collection"""
    files_by_collection = defaultdict(list)
    
    # Search for all parquet files
    for parquet_file in root.rglob("cdx-*.gz.parquet*"):
        # Extract collection from path
        parts = parquet_file.parts
        collection = None
        
        # Look for CC-MAIN-YYYY-WW pattern in path
        for part in parts:
            if part.startswith('CC-MAIN-'):
                collection = part
                break
        
        if collection:
            files_by_collection[collection].append(parquet_file)
        else:
            logger.warning(f"Could not determine collection for: {parquet_file}")
    
    return files_by_collection


def consolidate_collection(collection: str, files: List[Path], 
                          parquet_root: Path, dry_run: bool = True) -> Dict:
    """Consolidate all files for a collection into target directory"""
    target_dir = parquet_root / collection
    target_dir.mkdir(parents=True, exist_ok=True)
    
    stats = {
        'moved': 0,
        'skipped_duplicate': 0,
        'kept_sorted': 0,
        'errors': 0
    }
    
    # Group files by base name (without .sorted extension)
    files_by_base = defaultdict(list)
    for f in files:
        # Determine if file is sorted
        is_sorted = '.sorted' in f.name or f.name.endswith('.parquet.sorted')
        base_name = f.name.replace('.sorted', '').replace('.parquet.sorted', '.parquet')
        
        files_by_base[base_name].append((f, is_sorted))
    
    # Process each unique file
    for base_name, file_list in files_by_base.items():
        # Find the best version (prefer sorted)
        sorted_files = [f for f, is_sorted in file_list if is_sorted]
        unsorted_files = [f for f, is_sorted in file_list if not is_sorted]
        
        # Determine target files
        target_unsorted = target_dir / base_name
        target_sorted = target_dir / f"{base_name}.sorted"
        
        # Handle sorted version
        if sorted_files:
            source_sorted = sorted_files[0]  # Take first sorted version
            if target_sorted.exists():
                # Check if it's the same file
                if source_sorted.resolve() == target_sorted.resolve():
                    logger.debug(f"  Already in place: {target_sorted.name}")
                    stats['skipped_duplicate'] += 1
                else:
                    logger.info(f"  Target sorted exists, keeping existing: {target_sorted.name}")
                    stats['skipped_duplicate'] += 1
            else:
                if dry_run:
                    logger.info(f"  [DRY RUN] Would move: {source_sorted} -> {target_sorted}")
                else:
                    logger.info(f"  Moving sorted: {source_sorted.name} -> {target_sorted}")
                    shutil.move(str(source_sorted), str(target_sorted))
                stats['moved'] += 1
                stats['kept_sorted'] += 1
        
        # Handle unsorted version
        if unsorted_files:
            source_unsorted = unsorted_files[0]
            # Only keep unsorted if no sorted version exists
            if not sorted_files and not target_sorted.exists():
                if target_unsorted.exists():
                    if source_unsorted.resolve() == target_unsorted.resolve():
                        logger.debug(f"  Already in place: {target_unsorted.name}")
                        stats['skipped_duplicate'] += 1
                    else:
                        logger.info(f"  Target unsorted exists, keeping existing: {target_unsorted.name}")
                        stats['skipped_duplicate'] += 1
                else:
                    if dry_run:
                        logger.info(f"  [DRY RUN] Would move: {source_unsorted} -> {target_unsorted}")
                    else:
                        logger.info(f"  Moving unsorted: {source_unsorted.name} -> {target_unsorted}")
                        shutil.move(str(source_unsorted), str(target_unsorted))
                    stats['moved'] += 1
            else:
                # Sorted version exists, can delete unsorted
                if source_unsorted.resolve() != target_unsorted.resolve():
                    if dry_run:
                        logger.info(f"  [DRY RUN] Would delete (sorted exists): {source_unsorted}")
                    else:
                        logger.info(f"  Deleting unsorted (sorted exists): {source_unsorted.name}")
                        source_unsorted.unlink()
    
    return stats


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Consolidate parquet files")
    parser.add_argument('--parquet-root', type=Path, default=Path('/storage/ccindex_parquet'),
                       help='Root directory for parquet files')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('--collection', type=str,
                       help='Only consolidate this collection')
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("Parquet File Consolidation")
    logger.info("="*80)
    logger.info(f"Parquet root: {args.parquet_root}")
    logger.info(f"Dry run: {args.dry_run}")
    if args.collection:
        logger.info(f"Collection filter: {args.collection}")
    logger.info("")
    
    # Find all parquet files
    logger.info("Scanning for parquet files...")
    files_by_collection = find_all_parquet_files(args.parquet_root)
    
    # Filter if requested
    if args.collection:
        if args.collection in files_by_collection:
            files_by_collection = {args.collection: files_by_collection[args.collection]}
        else:
            logger.error(f"Collection {args.collection} not found")
            return 1
    
    logger.info(f"Found {len(files_by_collection)} collections with parquet files\n")
    
    # Consolidate each collection
    total_stats = defaultdict(int)
    for collection in sorted(files_by_collection.keys()):
        files = files_by_collection[collection]
        logger.info(f"Processing {collection} ({len(files)} files)...")
        
        stats = consolidate_collection(collection, files, args.parquet_root, args.dry_run)
        
        for key, value in stats.items():
            total_stats[key] += value
        
        logger.info(f"  Stats: moved={stats['moved']}, skipped={stats['skipped_duplicate']}, "
                   f"sorted={stats['kept_sorted']}, errors={stats['errors']}\n")
    
    # Summary
    logger.info("="*80)
    logger.info("Consolidation Summary")
    logger.info("="*80)
    logger.info(f"Collections processed: {len(files_by_collection)}")
    logger.info(f"Files moved: {total_stats['moved']}")
    logger.info(f"Files skipped (duplicate): {total_stats['skipped_duplicate']}")
    logger.info(f"Sorted files preserved: {total_stats['kept_sorted']}")
    logger.info(f"Errors: {total_stats['errors']}")
    
    if args.dry_run:
        logger.info("\n*** DRY RUN - No changes were made ***")
        logger.info("Run without --dry-run to perform consolidation")
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
