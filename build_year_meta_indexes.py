#!/usr/bin/env python3
"""
Build year-level meta-indexes that aggregate per-collection DuckDB indexes.

This creates an index-of-indexes structure where:
- cc_domain_by_collection/ contains one .duckdb file per collection
- cc_domain_by_year/ contains one .duckdb file per year that references all collections

This allows efficient querying either at the collection level or year level.
"""

import argparse
import duckdb
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_collection_indexes(collection_dir: Path) -> Dict[str, List[Path]]:
    """Group collection indexes by year"""
    indexes_by_year = defaultdict(list)

    # Support both legacy naming ('cc_pointers_CC-MAIN-....duckdb') and
    # current per-collection naming ('CC-MAIN-....duckdb').
    candidates = list(collection_dir.glob("cc_pointers_CC-MAIN-*.duckdb")) + list(collection_dir.glob("CC-MAIN-*.duckdb"))
    for db_file in sorted(set(candidates)):
        stem = db_file.stem
        collection = stem.replace("cc_pointers_", "") if stem.startswith("cc_pointers_") else stem
        parts = collection.split('-')
        # Extract year from collection: CC-MAIN-2024-10 -> 2024
        if len(parts) >= 3 and parts[2].isdigit():
            year = parts[2]
            indexes_by_year[year].append(db_file)
    
    return dict(indexes_by_year)


def build_year_meta_index(year: str, collection_dbs: List[Path], output_dir: Path) -> None:
    """Build a year-level meta-index that references all collection indexes"""
    output_path = output_dir / f"cc_pointers_{year}.duckdb"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Building year meta-index for {year} ({len(collection_dbs)} collections)")
    logger.info(f"  Output: {output_path}")
    
    # Create new database
    conn = duckdb.connect(str(output_path))
    
    # Create meta-index table that tracks which collections are available
    conn.execute("""
        CREATE TABLE IF NOT EXISTS collection_registry (
            collection TEXT PRIMARY KEY,
            db_path TEXT NOT NULL,
            domain_count INTEGER,
            file_count INTEGER,
            indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create a view that unions all collection indexes
    # We'll use ATTACH DATABASE to reference each collection database
    total_domains = 0
    total_files = 0
    
    for i, db_path in enumerate(collection_dbs):
        stem = db_path.stem
        collection = stem.replace("cc_pointers_", "") if stem.startswith("cc_pointers_") else stem
        alias = f"coll_{i}"
        
        try:
            # Attach the collection database
            conn.execute(f"ATTACH DATABASE '{db_path}' AS {alias} (READ_ONLY)")

            # Detect schema.
            tables = {row[0] for row in conn.execute(
                f"SELECT table_name FROM {alias}.information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()}

            if 'domain_pointers' in tables:
                domain_count = conn.execute(f"SELECT COUNT(*) FROM {alias}.domain_pointers").fetchone()[0]
                # Legacy schema uses file_path.
                file_count = conn.execute(f"SELECT COUNT(DISTINCT file_path) FROM {alias}.domain_pointers").fetchone()[0]
            elif 'cc_domain_shards' in tables:
                # Domain-only schema: one row per (host_rev, parquet_relpath, ...)
                domain_count = conn.execute(f"SELECT COUNT(*) FROM {alias}.cc_domain_shards").fetchone()[0]
                file_count = conn.execute(f"SELECT COUNT(DISTINCT parquet_relpath) FROM {alias}.cc_domain_shards").fetchone()[0]
            else:
                raise RuntimeError(f"Unsupported schema (no domain_pointers/cc_domain_shards). tables={sorted(tables)}")
            
            # Register this collection
            conn.execute("""
                INSERT OR REPLACE INTO collection_registry (collection, db_path, domain_count, file_count)
                VALUES (?, ?, ?, ?)
            """, [collection, str(db_path), domain_count, file_count])
            
            total_domains += domain_count
            total_files += file_count
            
            logger.info(f"  Registered {collection}: {domain_count:,} domains, {file_count:,} files")
            
        except Exception as e:
            logger.error(f"  Failed to process {collection}: {e}")
            continue
    
    # Create a metadata table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta_info (
            year TEXT PRIMARY KEY,
            collection_count INTEGER,
            total_domains INTEGER,
            total_files INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        INSERT OR REPLACE INTO meta_info (year, collection_count, total_domains, total_files)
        VALUES (?, ?, ?, ?)
    """, [year, len(collection_dbs), total_domains, total_files])
    
    conn.close()
    logger.info(f"✓ Built year index for {year}: {total_domains:,} domains across {len(collection_dbs)} collections")


def main():
    parser = argparse.ArgumentParser(description="Build year-level meta-indexes")
    parser.add_argument(
        "--collection-dir",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_domain_by_collection"),
        help="Directory containing per-collection indexes"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_domain_by_year"),
        help="Directory for year-level meta-indexes"
    )
    parser.add_argument(
        "--year",
        help="Build index for specific year only"
    )
    args = parser.parse_args()
    
    if not args.collection_dir.exists():
        logger.error(f"Collection directory does not exist: {args.collection_dir}")
        return
    
    # Get all collection indexes grouped by year
    indexes_by_year = get_collection_indexes(args.collection_dir)
    
    if not indexes_by_year:
        logger.warning("No collection indexes found")
        return
    
    logger.info(f"Found {len(indexes_by_year)} years: {sorted(indexes_by_year.keys())}")
    
    # Build meta-indexes
    for year in sorted(indexes_by_year.keys()):
        if args.year and year != args.year:
            continue
        
        build_year_meta_index(year, indexes_by_year[year], args.output_dir)


if __name__ == "__main__":
    main()
