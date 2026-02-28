#!/usr/bin/env python3
"""
Convert all DuckDB index files to Parquet format.

This script:
1. Finds all *.duckdb files in ~/common_crawl_meta_indexes/
2. Exports all tables from each duckdb file to corresponding parquet files
3. Optionally removes the duckdb files after successful conversion

Usage:
    python convert_duckdb_to_parquet.py [--dry-run] [--remove-duckdb] [--year 2025] [--workers 4]
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

try:
    import duckdb
    import pyarrow.parquet as pq
except ImportError as e:
    print(f"Error: Missing dependency. Install with: pip install duckdb pyarrow", file=sys.stderr)
    sys.exit(1)


def find_duckdb_files(base_dir: Path, years: Optional[List[str]] = None) -> List[Path]:
    """Find all .duckdb files in the meta indexes directory."""
    duckdb_files = []
    
    for path in base_dir.glob("*/"):
        year = path.name
        if years and year not in years:
            continue
        
        for duckdb_file in path.glob("**/*.duckdb"):
            if ".sorted" not in duckdb_file.name:  # Skip .duckdb.sorted marker files
                duckdb_files.append(duckdb_file)
    
    return sorted(duckdb_files)


def export_duckdb_to_parquet(duckdb_file: Path, dry_run: bool = False, compression: str = "zstd") -> Dict[str, bool]:
    """
    Export all tables from a duckdb file to parquet files.
    
    Args:
        duckdb_file: Path to the duckdb file to export
        dry_run: If True, don't actually write files
        compression: Compression codec (default: zstd). Use None for no compression.
    
    Returns a dict with success status for each export.
    """
    results = {}
    
    try:
        con = duckdb.connect(str(duckdb_file), read_only=True)
        
        # Get all tables
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        
        if not tables:
            print(f"⚠ No tables found in {duckdb_file.name}")
            con.close()
            return {"empty": False}
        
        for (table_name,) in tables:
            # Determine output parquet filename
            # For CC-MAIN-2025-05.duckdb with table cc_domain_shards
            # Output: CC-MAIN-2025-05__cc_domain_shards.parquet
            base_name = duckdb_file.stem.replace(".duckdb", "")
            output_file = duckdb_file.parent / f"{base_name}__{table_name}.parquet"
            
            if not dry_run:
                # Export table to parquet with zstd compression
                compress_opt = f"COMPRESSION '{compression}'" if compression else ""
                copy_cmd = f"COPY {table_name} TO '{output_file}' (FORMAT 'parquet'{(',' + compress_opt) if compress_opt else ''})"
                con.execute(copy_cmd)
            
            results[str(output_file)] = True
        
        con.close()
        
    except Exception as e:
        print(f"❌ Error processing {duckdb_file}: {e}", file=sys.stderr)
        results["error"] = False
        return results
    
    return results


def convert_all(
    base_dir: Path,
    years: Optional[List[str]] = None,
    dry_run: bool = False,
    remove_parquet: bool = False,
    workers: int = 4,
    compression: str = "zstd",
) -> None:
    """Convert all duckdb files to parquet."""
    
    print("=" * 70)
    print("DuckDB to Parquet Converter")
    print("=" * 70)
    print(f"Base directory: {base_dir}")
    print(f"Years: {years or 'all'}")
    print(f"Dry run: {dry_run}")
    print(f"Remove existing parquet files: {remove_parquet}")
    print(f"Compression: {compression}")
    print(f"Workers: {workers}")
    print()
    
    # Find all duckdb files
    duckdb_files = find_duckdb_files(base_dir, years)
    
    if not duckdb_files:
        print("No duckdb files found")
        return
    
    print(f"Found {len(duckdb_files)} duckdb files to process:")
    for f in duckdb_files[:5]:
        print(f"  - {f}")
    if len(duckdb_files) > 5:
        print(f"  ... and {len(duckdb_files) - 5} more")
    print()
    
    if dry_run:
        print("DRY RUN: No files will be modified")
        print()
    
    # Process files
    successful = 0
    failed = 0
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(export_duckdb_to_parquet, f, dry_run, compression): f
            for f in duckdb_files
        }
        
        with tqdm(total=len(duckdb_files), desc="Converting") as pbar:
            for future in as_completed(futures):
                duckdb_file = futures[future]
                try:
                    results = future.result()
                    if "error" not in results:
                        successful += 1
                    else:
                        failed += 1
                        pbar.write(f"✗ Failed: {duckdb_file.name}")
                except Exception as e:
                    failed += 1
                    pbar.write(f"✗ Error: {duckdb_file.name}: {e}")
                
                pbar.update(1)
    
    print()
    print("=" * 70)
    print(f"Conversion complete: {successful} successful, {failed} failed")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Convert DuckDB files to Parquet")
    parser.add_argument(
        "--base",
        type=Path,
        default=Path.home() / "common_crawl_meta_indexes",
        help="Base directory with collections (default: ~/common_crawl_meta_indexes)",
    )
    parser.add_argument(
        "--year",
        type=str,
        action="append",
        dest="years",
        help="Specific years to process (default: all). Can be specified multiple times.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without modifying files",
    )
    parser.add_argument(
        "--remove-parquet",
        action="store_true",
        help="Remove existing parquet files before conversion",
    )
    parser.add_argument(
        "--compression",
        type=str,
        default="zstd",
        choices=["zstd", "snappy", "gzip", "brotli", None],
        help="Compression codec (default: zstd)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    
    args = parser.parse_args()
    
    if not args.base.is_dir():
        print(f"Error: Base directory not found: {args.base}", file=sys.stderr)
        sys.exit(1)
    
    convert_all(
        base_dir=args.base,
        years=args.years,
        dry_run=args.dry_run,
        remove_parquet=args.remove_parquet,
        workers=args.workers,
        compression=args.compression,
    )


if __name__ == "__main__":
    main()
