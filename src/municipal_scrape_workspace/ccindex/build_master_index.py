#!/usr/bin/env python3
"""
Build master meta-index that aggregates all year-level indexes.

This creates a top-level index-of-indexes structure:
- cc_domain_by_collection/ contains one .duckdb file per collection
- cc_domain_by_year/ contains one .duckdb file per year
- cc_master_index.duckdb contains references to all years

This allows efficient querying at any level: collection, year, or entire corpus.
"""

import argparse
import duckdb
import logging
from pathlib import Path
from typing import List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_year_indexes(year_dir: Path) -> List[Tuple[str, Path]]:
    """Get all year-level index files"""
    indexes = []
    
    for db_file in sorted(year_dir.glob("cc_pointers_*.duckdb")):
        # Extract year from filename: cc_pointers_2024.duckdb -> 2024
        year = db_file.stem.replace("cc_pointers_", "")
        if year.isdigit():
            indexes.append((year, db_file))
    
    return indexes


def build_master_index(year_indexes: List[Tuple[str, Path]], output_path: Path) -> None:
    """Build master index that references all year-level indexes"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Building master index for {len(year_indexes)} years")
    logger.info(f"  Output: {output_path}")
    
    # Create new database
    conn = duckdb.connect(str(output_path))
    
    # Create master registry table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS year_registry (
            year TEXT PRIMARY KEY,
            db_path TEXT NOT NULL,
            collection_count INTEGER,
            total_domains INTEGER,
            total_files INTEGER,
            indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create collection summary table (denormalized for performance)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS collection_summary (
            collection TEXT PRIMARY KEY,
            year TEXT NOT NULL,
            year_db_path TEXT NOT NULL,
            collection_db_path TEXT NOT NULL,
            domain_count INTEGER,
            file_count INTEGER,
            indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    total_collections = 0
    total_domains = 0
    total_files = 0
    
    for year, db_path in year_indexes:
        try:
            # Attach the year database
            alias = f"year_{year}"
            conn.execute(f"ATTACH DATABASE '{db_path}' AS {alias} (READ_ONLY)")
            
            # Get metadata from year index
            meta = conn.execute(f"""
                SELECT collection_count, total_domains, total_files 
                FROM {alias}.meta_info 
                WHERE year = ?
            """, [year]).fetchone()
            
            if meta:
                collection_count, domain_count, file_count = meta
                
                # Register this year
                conn.execute("""
                    INSERT OR REPLACE INTO year_registry (year, db_path, collection_count, total_domains, total_files)
                    VALUES (?, ?, ?, ?, ?)
                """, [year, str(db_path), collection_count, domain_count, file_count])
                
                # Get all collections from this year
                collections = conn.execute(f"""
                    SELECT collection, db_path, domain_count, file_count, indexed_at
                    FROM {alias}.collection_registry
                """).fetchall()
                
                for coll_name, coll_db_path, coll_domains, coll_files, coll_indexed_at in collections:
                    conn.execute("""
                        INSERT OR REPLACE INTO collection_summary 
                        (collection, year, year_db_path, collection_db_path, domain_count, file_count, indexed_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [coll_name, year, str(db_path), coll_db_path, coll_domains, coll_files, coll_indexed_at])
                
                total_collections += collection_count
                total_domains += domain_count
                total_files += file_count
                
                logger.info(f"  Registered {year}: {collection_count} collections, {domain_count:,} domains, {file_count:,} files")
            else:
                logger.warning(f"  No metadata found for {year}")
            
        except Exception as e:
            logger.error(f"  Failed to process {year}: {e}")
            continue
    
    # Create master metadata table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS master_info (
            id INTEGER PRIMARY KEY,
            year_count INTEGER,
            collection_count INTEGER,
            total_domains INTEGER,
            total_files INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        INSERT OR REPLACE INTO master_info (id, year_count, collection_count, total_domains, total_files)
        VALUES (1, ?, ?, ?, ?)
    """, [len(year_indexes), total_collections, total_domains, total_files])
    
    # Create convenience views
    conn.execute("""
        CREATE OR REPLACE VIEW year_summary AS
        SELECT 
            year,
            collection_count,
            total_domains,
            total_files,
            indexed_at
        FROM year_registry
        ORDER BY year DESC
    """)
    
    conn.execute("""
        CREATE OR REPLACE VIEW collections_by_year AS
        SELECT 
            year,
            collection,
            domain_count,
            file_count,
            indexed_at
        FROM collection_summary
        ORDER BY year DESC, collection
    """)
    
    conn.close()
    
    logger.info("=" * 80)
    logger.info("✓ Master Index Summary:")
    logger.info(f"  Years:        {len(year_indexes)}")
    logger.info(f"  Collections:  {total_collections}")
    logger.info(f"  Domains:      {total_domains:,}")
    logger.info(f"  Files:        {total_files:,}")
    logger.info("=" * 80)


def print_master_stats(db_path: Path) -> None:
    """Print statistics from master index"""
    if not db_path.exists():
        logger.error(f"Master index does not exist: {db_path}")
        return
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # Master info
    master_info = conn.execute("SELECT * FROM master_info").fetchone()
    if master_info:
        _, year_count, collection_count, total_domains, total_files, created_at, updated_at = master_info
        logger.info("=" * 80)
        logger.info("Master Index Statistics")
        logger.info("=" * 80)
        logger.info(f"Years:        {year_count}")
        logger.info(f"Collections:  {collection_count}")
        logger.info(f"Domains:      {total_domains:,}")
        logger.info(f"Files:        {total_files:,}")
        logger.info(f"Created:      {created_at}")
        logger.info(f"Updated:      {updated_at}")
        logger.info("")
    
    # Year breakdown
    logger.info("Year Breakdown:")
    logger.info("-" * 80)
    years = conn.execute("SELECT * FROM year_summary").fetchall()
    for year, coll_count, domains, files, indexed_at in years:
        logger.info(f"  {year}: {coll_count:2d} collections, {domains:12,} domains, {files:5,} files")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Build master meta-index of all years")
    parser.add_argument(
        "--year-dir",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_domain_by_year"),
        help="Directory containing year-level indexes"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_master_index.duckdb"),
        help="Output path for master index"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print statistics from existing master index"
    )
    args = parser.parse_args()
    
    if args.stats:
        print_master_stats(args.output)
        return
    
    if not args.year_dir.exists():
        logger.error(f"Year directory does not exist: {args.year_dir}")
        return
    
    # Get all year indexes
    year_indexes = get_year_indexes(args.year_dir)
    
    if not year_indexes:
        logger.warning("No year indexes found")
        return
    
    logger.info(f"Found {len(year_indexes)} years: {[y for y, _ in year_indexes]}")
    
    # Build master index
    build_master_index(year_indexes, args.output)


if __name__ == "__main__":
    main()
