#!/usr/bin/env python3
"""
Build DuckDB index from EXISTING parquet files (fast method).
Reads parquet metadata only, processes in batches to manage memory.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set

import duckdb
import pyarrow.parquet as pq


def extract_domain_mappings_from_parquet(parquet_file: Path, parquet_root: Path) -> List[tuple]:
    """
    Extract unique domains from a parquet file.
    Returns list of (collection, year, shard_file, parquet_relpath, host, host_rev)
    """
    try:
        # Get relative path
        try:
            rel_path = parquet_file.relative_to(parquet_root).as_posix()
        except:
            rel_path = str(parquet_file)
        
        # Parse collection from path
        parts = parquet_file.parts
        collection = None
        year = None
        for i, part in enumerate(parts):
            if part.startswith('CC-MAIN-'):
                collection = part
                year_match = part.split('-')[2]
                try:
                    year = int(year_match)
                except:
                    pass
                break
        
        shard_file = parquet_file.name
        
        # Read unique host_rev values using DuckDB (faster than pyarrow for aggregation)
        con = duckdb.connect(":memory:")
        domains = con.execute(
            """
            SELECT DISTINCT host, host_rev
            FROM read_parquet(?)
            WHERE host IS NOT NULL AND host_rev IS NOT NULL
            """,
            [str(parquet_file)]
        ).fetchall()
        con.close()
        
        results = []
        for host, host_rev in domains:
            results.append((
                str(parquet_file),  # source_path
                collection,
                year,
                shard_file,
                rel_path,
                host,
                host_rev
            ))
        
        return results
        
    except Exception as e:
        print(f"Error processing {parquet_file}: {e}", file=sys.stderr)
        return []


def extract_rowgroup_ranges(parquet_file: Path, parquet_root: Path) -> List[tuple]:
    """
    Extract row group range metadata.
    Returns list of (source_path, collection, year, shard_file, parquet_relpath, 
                     row_group, row_start, row_end, host_rev_min, host_rev_max)
    """
    try:
        pf = pq.ParquetFile(parquet_file)
        md = pf.metadata
        
        if md is None or md.num_row_groups == 0:
            return []
        
        # Get relative path
        try:
            rel_path = parquet_file.relative_to(parquet_root).as_posix()
        except:
            rel_path = str(parquet_file)
        
        # Parse collection
        parts = parquet_file.parts
        collection = None
        year = None
        for part in parts:
            if part.startswith('CC-MAIN-'):
                collection = part
                year_match = part.split('-')[2]
                try:
                    year = int(year_match)
                except:
                    pass
                break
        
        shard_file = parquet_file.name
        
        # Find host_rev column
        host_rev_idx = None
        try:
            host_rev_idx = list(pf.schema_arrow.names).index('host_rev')
        except:
            try:
                host_rev_idx = list(pf.schema.names).index('host_rev')
            except:
                return []
        
        results = []
        row_start = 0
        
        for rg_idx in range(md.num_row_groups):
            rg = md.row_group(rg_idx)
            num_rows = int(rg.num_rows or 0)
            row_end = row_start + num_rows
            
            # Get min/max from statistics
            host_rev_min = None
            host_rev_max = None
            
            if host_rev_idx is not None:
                try:
                    col = rg.column(host_rev_idx)
                    stats = getattr(col, 'statistics', None)
                    if stats:
                        mn = getattr(stats, 'min', None)
                        mx = getattr(stats, 'max', None)
                        if isinstance(mn, bytes):
                            host_rev_min = mn.decode('utf-8', errors='ignore')
                        elif mn:
                            host_rev_min = str(mn)
                        if isinstance(mx, bytes):
                            host_rev_max = mx.decode('utf-8', errors='ignore')
                        elif mx:
                            host_rev_max = str(mx)
                except:
                    pass
            
            results.append((
                str(parquet_file),  # source_path
                collection,
                year,
                shard_file,
                rel_path,
                rg_idx,
                row_start,
                row_end,
                host_rev_min,
                host_rev_max
            ))
            
            row_start = row_end
        
        return results
        
    except Exception as e:
        print(f"Error extracting row groups from {parquet_file}: {e}", file=sys.stderr)
        return []


def main() -> int:
    ap = argparse.ArgumentParser(description="Build index from existing parquet files")
    ap.add_argument("--parquet-root", required=True, help="Root directory of parquet files")
    ap.add_argument("--output-db", required=True, help="Output DuckDB file")
    ap.add_argument("--batch-size", type=int, default=100, help="Files per batch")
    ap.add_argument("--extract-rowgroups", action="store_true", help="Extract row group ranges")
    
    args = ap.parse_args()
    
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    output_db = Path(args.output_db).expanduser().resolve()
    
    print(f"Parquet root: {parquet_root}")
    print(f"Output DB:    {output_db}")
    print(f"Batch size:   {args.batch_size}")
    print()
    
    # Find all parquet files
    all_files = sorted(parquet_root.rglob("*.parquet"))
    print(f"Found {len(all_files)} parquet files")
    print()
    
    # Create output directory
    output_db.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to output database
    con = duckdb.connect(str(output_db))
    
    # Create tables
    con.execute("""
        CREATE TABLE IF NOT EXISTS cc_domain_shards (
            source_path VARCHAR,
            collection VARCHAR,
            year INTEGER,
            shard_file VARCHAR,
            parquet_relpath VARCHAR,
            host VARCHAR,
            host_rev VARCHAR
        )
    """)
    
    if args.extract_rowgroups:
        con.execute("""
            CREATE TABLE IF NOT EXISTS cc_parquet_rowgroups (
                source_path VARCHAR,
                collection VARCHAR,
                year INTEGER,
                shard_file VARCHAR,
                parquet_relpath VARCHAR,
                row_group INTEGER,
                row_start BIGINT,
                row_end BIGINT,
                host_rev_min VARCHAR,
                host_rev_max VARCHAR
            )
        """)
    
    # Process in batches
    batch_size = args.batch_size
    total_domains = 0
    total_rowgroups = 0
    
    for batch_start in range(0, len(all_files), batch_size):
        batch_end = min(batch_start + batch_size, len(all_files))
        batch = all_files[batch_start:batch_end]
        
        print(f"Processing batch {batch_start//batch_size + 1}/{(len(all_files) + batch_size - 1)//batch_size}")
        print(f"  Files: {batch_start+1} to {batch_end}")
        
        # Extract domain mappings
        batch_domains = []
        for pq_file in batch:
            domains = extract_domain_mappings_from_parquet(pq_file, parquet_root)
            batch_domains.extend(domains)
        
        if batch_domains:
            con.executemany(
                "INSERT INTO cc_domain_shards VALUES (?, ?, ?, ?, ?, ?, ?)",
                batch_domains
            )
            total_domains += len(batch_domains)
            print(f"  Inserted {len(batch_domains)} domain mappings")
        
        # Extract row group ranges if requested
        if args.extract_rowgroups:
            batch_rowgroups = []
            for pq_file in batch:
                rowgroups = extract_rowgroup_ranges(pq_file, parquet_root)
                batch_rowgroups.extend(rowgroups)
            
            if batch_rowgroups:
                con.executemany(
                    "INSERT INTO cc_parquet_rowgroups VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    batch_rowgroups
                )
                total_rowgroups += len(batch_rowgroups)
                print(f"  Inserted {len(batch_rowgroups)} row group ranges")
        
        print()
    
    # Create indexes
    print("Creating indexes...")
    con.execute("CREATE INDEX IF NOT EXISTS idx_domain_shards_host_rev ON cc_domain_shards(host_rev)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_domain_shards_host ON cc_domain_shards(host)")
    
    if args.extract_rowgroups:
        con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_host_rev_min ON cc_parquet_rowgroups(host_rev_min)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_host_rev_max ON cc_parquet_rowgroups(host_rev_max)")
    
    con.close()
    
    print()
    print("=" * 80)
    print("COMPLETE")
    print("=" * 80)
    print(f"Total domain mappings: {total_domains:,}")
    if args.extract_rowgroups:
        print(f"Total row group ranges: {total_rowgroups:,}")
    print(f"Output: {output_db}")
    print(f"Size: {output_db.stat().st_size / (1024**3):.2f} GB")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
