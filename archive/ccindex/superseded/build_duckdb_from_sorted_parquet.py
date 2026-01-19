#!/usr/bin/env python3
"""Build DuckDB domain pointer index from sorted parquet files.

This script reads sorted parquet files containing Common Crawl CDX data
and builds an optimized DuckDB pointer database that maps domains to 
parquet files with row offsets and ranges for fast lookups.

Expected input: /storage/ccindex_parquet/cc_pointers_by_year/YYYY/CC-MAIN-YYYY-WW/cdx-*.gz.parquet
Output: /storage/ccindex_duckdb/cc_domain_pointer.duckdb

The pointer DB contains:
  - domain_index: (domain, parquet_file, row_offset, row_count)
  - Allows fast lookup of which parquet files contain data for a given domain
  - Row offset/count enable efficient reading of only relevant rows
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Tuple
import duckdb
import pyarrow.parquet as pq

def find_sorted_parquet_files(root_dir: Path) -> List[Path]:
    """Find all sorted parquet files in the directory structure."""
    parquet_files = []
    for year_dir in sorted(root_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        for collection_dir in sorted(year_dir.iterdir()):
            if not collection_dir.is_dir():
                continue
            for parquet_file in sorted(collection_dir.glob("cdx-*.gz.parquet")):
                if parquet_file.is_file():
                    parquet_files.append(parquet_file)
    return parquet_files

def extract_domain_ranges(parquet_file: Path) -> List[Tuple[str, int, int]]:
    """Extract domain ranges from a sorted parquet file.
    
    Returns list of (domain, start_row, count) tuples.
    Assumes file is sorted by host_rev (reversed domain).
    """
    try:
        table = pq.read_table(parquet_file, columns=['host', 'host_rev'])
        
        if len(table) == 0:
            return []
        
        # Extract host column
        hosts = table.column('host').to_pylist()
        
        # Build domain ranges
        ranges = []
        current_domain = hosts[0]
        start_row = 0
        count = 1
        
        for i in range(1, len(hosts)):
            domain = hosts[i]
            if domain == current_domain:
                count += 1
            else:
                # Save previous range
                if current_domain:
                    ranges.append((current_domain, start_row, count))
                # Start new range
                current_domain = domain
                start_row = i
                count = 1
        
        # Save final range
        if current_domain:
            ranges.append((current_domain, start_row, count))
        
        return ranges
        
    except Exception as e:
        print(f"Error processing {parquet_file}: {e}", file=sys.stderr)
        return []

def build_domain_index(db_path: Path, input_root: Path, batch_size: int = 10):
    """Build the DuckDB domain pointer index."""
    
    print(f"Finding sorted parquet files in {input_root}...")
    parquet_files = find_sorted_parquet_files(input_root)
    print(f"Found {len(parquet_files)} parquet files")
    
    if len(parquet_files) == 0:
        print("No parquet files found!", file=sys.stderr)
        return
    
    # Create database
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    
    # Create schema
    con.execute("""
        CREATE TABLE IF NOT EXISTS domain_index (
            domain VARCHAR,
            parquet_file VARCHAR,
            row_offset BIGINT,
            row_count BIGINT,
            collection VARCHAR,
            year INTEGER,
            shard_file VARCHAR
        )
    """)
    
    # Create indexes table to track processed files
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            parquet_file VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            row_count BIGINT,
            domain_count BIGINT
        )
    """)
    
    # Get already processed files
    processed = set()
    try:
        result = con.execute("SELECT parquet_file FROM processed_files").fetchall()
        processed = {row[0] for row in result}
        print(f"Already processed {len(processed)} files")
    except:
        pass
    
    # Process each parquet file
    total_processed = 0
    total_domains = 0
    
    for idx, parquet_file in enumerate(parquet_files, 1):
        parquet_path_str = str(parquet_file)
        
        # Skip if already processed
        if parquet_path_str in processed:
            continue
        
        print(f"[{idx}/{len(parquet_files)}] Processing {parquet_file.name}...")
        
        # Extract collection and year from path
        try:
            parts = parquet_file.parts
            collection = parts[-2]  # CC-MAIN-YYYY-WW
            year = int(parts[-3])  # YYYY
            shard_file = parquet_file.name  # cdx-XXXXX.gz.parquet
        except:
            print(f"  Warning: Could not parse collection/year from path", file=sys.stderr)
            collection = "unknown"
            year = 0
            shard_file = parquet_file.name
        
        # Extract domain ranges
        ranges = extract_domain_ranges(parquet_file)
        
        if len(ranges) == 0:
            print(f"  No domains found")
            continue
        
        # Insert into database
        rows_to_insert = [
            (domain, parquet_path_str, offset, count, collection, year, shard_file)
            for domain, offset, count in ranges
        ]
        
        con.executemany(
            """INSERT INTO domain_index 
               (domain, parquet_file, row_offset, row_count, collection, year, shard_file)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            rows_to_insert
        )
        
        # Mark as processed
        total_rows = sum(count for _, _, count in ranges)
        con.execute(
            """INSERT INTO processed_files (parquet_file, row_count, domain_count)
               VALUES (?, ?, ?)""",
            [parquet_path_str, total_rows, len(ranges)]
        )
        
        total_processed += 1
        total_domains += len(ranges)
        
        print(f"  Added {len(ranges)} domain ranges, {total_rows} total rows")
        
        # Commit periodically
        if total_processed % batch_size == 0:
            con.commit()
            print(f"Committed batch (processed {total_processed} files, {total_domains} domain ranges)")
    
    # Final commit
    con.commit()
    
    # Create indexes for fast lookups
    print("\nCreating indexes...")
    con.execute("CREATE INDEX IF NOT EXISTS idx_domain ON domain_index(domain)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_collection ON domain_index(collection)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_year ON domain_index(year)")
    
    # Print statistics
    stats = con.execute("""
        SELECT 
            COUNT(DISTINCT domain) as unique_domains,
            COUNT(*) as total_entries,
            COUNT(DISTINCT parquet_file) as files_indexed,
            SUM(row_count) as total_rows
        FROM domain_index
    """).fetchone()
    
    print(f"\n=== Index Statistics ===")
    print(f"Unique domains: {stats[0]:,}")
    print(f"Domain-file entries: {stats[1]:,}")
    print(f"Parquet files indexed: {stats[2]:,}")
    print(f"Total rows indexed: {stats[3]:,}")
    print(f"\nDatabase written to: {db_path}")
    
    con.close()

def main():
    parser = argparse.ArgumentParser(
        description="Build DuckDB domain pointer index from sorted parquet files"
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        required=True,
        help="Root directory containing sorted parquet files (e.g., /storage/ccindex_parquet/cc_pointers_by_year)"
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Output DuckDB database path (e.g., /storage/ccindex_duckdb/cc_domain_pointer.duckdb)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of files to process before committing (default: 10)"
    )
    
    args = parser.parse_args()
    
    if not args.input_root.exists():
        print(f"Error: Input root does not exist: {args.input_root}", file=sys.stderr)
        sys.exit(1)
    
    build_domain_index(args.db, args.input_root, args.batch_size)

if __name__ == "__main__":
    main()
