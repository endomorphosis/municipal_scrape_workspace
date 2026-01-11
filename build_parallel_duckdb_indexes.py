#!/usr/bin/env python3
"""
Build DuckDB pointer indexes in parallel - one per collection
Each index contains domain -> (parquet_file, row_offset, row_count) mappings
"""

import duckdb
import os
import sys
import json
import time
import multiprocessing as mp
from pathlib import Path
from datetime import datetime
from collections import defaultdict

PARQUET_DIR = Path("/storage/ccindex_parquet/cc_pointers_by_year")
INDEX_DIR = Path("/storage/ccindex_duckdb/cc_pointers_by_collection")
LOG_DIR = Path("logs")

def get_all_collections():
    """Extract all unique collection names from organized parquet directory structure"""
    collections = set()
    # Scan YEAR/COLLECTION subdirectories
    for year_dir in PARQUET_DIR.glob("*/"):
        if year_dir.is_dir():
            for collection_dir in year_dir.glob("*/"):
                if collection_dir.is_dir():
                    collections.add(collection_dir.name)
    return sorted(collections)

def get_collection_files(collection):
    """Get all parquet files for a specific collection from organized structure"""
    files = []
    # Look in all year directories for this collection
    for year_dir in PARQUET_DIR.glob("*/"):
        collection_path = year_dir / collection
        if collection_path.exists() and collection_path.is_dir():
            files.extend(sorted(collection_path.glob("*.parquet")))
    return files

def build_collection_index(collection):
    """Build DuckDB index for a single collection"""
    start_time = time.time()
    pid = os.getpid()
    
    # Setup paths
    db_path = INDEX_DIR / f"{collection}.duckdb"
    progress_file = INDEX_DIR / f"{collection}_progress.json"
    log_file = LOG_DIR / f"build_{collection}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    
    def log(msg):
        timestamp = datetime.now().isoformat()
        log_msg = f"[{timestamp}] [{pid}] {msg}\n"
        print(log_msg.strip())
        with open(log_file, "a") as f:
            f.write(log_msg)
    
    try:
        # Get all files for this collection
        parquet_files = get_collection_files(collection)
        if not parquet_files:
            log(f"No files found for collection {collection}")
            return {"collection": collection, "status": "no_files", "files_processed": 0}
        
        log(f"Building index for {collection} with {len(parquet_files)} files")
        
        # Connect to DuckDB
        con = duckdb.connect(str(db_path))
        
        # Create the pointer table
        con.execute("""
            CREATE TABLE IF NOT EXISTS domain_pointers (
                domain VARCHAR,
                parquet_file VARCHAR,
                row_offset BIGINT,
                row_count BIGINT,
                PRIMARY KEY (domain, parquet_file)
            )
        """)
        
        # Create index on domain for fast lookups
        con.execute("CREATE INDEX IF NOT EXISTS idx_domain ON domain_pointers(domain)")
        
        files_processed = 0
        domains_indexed = 0
        
        for parquet_file in parquet_files:
            file_start = time.time()
            filename = parquet_file.name
            
            log(f"Processing {filename} ({files_processed+1}/{len(parquet_files)})")
            
            try:
                # Query the parquet file to get domain ranges
                # Extract domain from URL - order by domain and URL only (schema-agnostic)
                result = con.execute(f"""
                    WITH domain_ranges AS (
                        SELECT 
                            regexp_extract(url, 'https?://([^/:]+)', 1) as domain,
                            ROW_NUMBER() OVER (ORDER BY regexp_extract(url, 'https?://([^/:]+)', 1), url) - 1 as row_num
                        FROM read_parquet('{parquet_file}')
                        WHERE regexp_extract(url, 'https?://([^/:]+)', 1) IS NOT NULL
                    ),
                    domain_groups AS (
                        SELECT 
                            domain,
                            MIN(row_num) as first_row,
                            COUNT(*) as row_count
                        FROM domain_ranges
                        GROUP BY domain
                    )
                    SELECT domain, first_row, row_count
                    FROM domain_groups
                    ORDER BY domain
                """).fetchall()
                
                # Insert the pointers
                if result:
                    con.executemany("""
                        INSERT OR REPLACE INTO domain_pointers 
                        (domain, parquet_file, row_offset, row_count)
                        VALUES (?, ?, ?, ?)
                    """, [(domain, filename, offset, count) for domain, offset, count in result])
                    
                    domains_in_file = len(result)
                    domains_indexed += domains_in_file
                    
                    file_elapsed = time.time() - file_start
                    log(f"  Indexed {domains_in_file} domains in {file_elapsed:.1f}s")
                
                files_processed += 1
                
                # Save progress
                progress = {
                    "collection": collection,
                    "files_processed": files_processed,
                    "total_files": len(parquet_files),
                    "domains_indexed": domains_indexed,
                    "last_file": filename,
                    "timestamp": datetime.now().isoformat()
                }
                with open(progress_file, "w") as f:
                    json.dump(progress, f, indent=2)
                
            except Exception as e:
                log(f"  ERROR processing {filename}: {e}")
                # Delete corrupted parquet file and mark collection as dirty
                log(f"  Deleting corrupted file: {parquet_file}")
                try:
                    parquet_file.unlink()
                    # Mark collection as dirty
                    dirty_marker = INDEX_DIR / f"{collection}_DIRTY.marker"
                    with open(dirty_marker, "a") as f:
                        f.write(f"{datetime.now().isoformat()}: {filename} - {str(e)}\n")
                    log(f"  Collection {collection} marked as DIRTY")
                except Exception as del_err:
                    log(f"  Failed to delete corrupted file: {del_err}")
                continue
        
        # Final statistics
        total_domains = con.execute("SELECT COUNT(DISTINCT domain) FROM domain_pointers").fetchone()[0]
        total_pointers = con.execute("SELECT COUNT(*) FROM domain_pointers").fetchone()[0]
        
        con.close()
        
        elapsed = time.time() - start_time
        log(f"COMPLETED {collection}: {total_domains} unique domains, {total_pointers} pointers in {elapsed/60:.1f} min")
        
        return {
            "collection": collection,
            "status": "success",
            "files_processed": files_processed,
            "domains": total_domains,
            "pointers": total_pointers,
            "elapsed_seconds": elapsed
        }
        
    except Exception as e:
        log(f"FAILED {collection}: {e}")
        import traceback
        log(traceback.format_exc())
        return {
            "collection": collection,
            "status": "failed",
            "error": str(e)
        }

def build_master_index(results):
    """Build a master index that lists all collection indexes"""
    master_db = INDEX_DIR / "master_index.duckdb"
    
    con = duckdb.connect(str(master_db))
    
    # Create master table
    con.execute("""
        CREATE TABLE IF NOT EXISTS collection_indexes (
            collection VARCHAR PRIMARY KEY,
            db_file VARCHAR,
            num_domains BIGINT,
            num_pointers BIGINT,
            status VARCHAR,
            last_updated TIMESTAMP
        )
    """)
    
    # Insert collection metadata
    for result in results:
        if result["status"] == "success":
            con.execute("""
                INSERT OR REPLACE INTO collection_indexes 
                (collection, db_file, num_domains, num_pointers, status, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                result["collection"],
                f"{result['collection']}.duckdb",
                result.get("domains", 0),
                result.get("pointers", 0),
                result["status"],
                datetime.now()
            ))
    
    con.close()
    print(f"\nMaster index created at {master_db}")

def main():
    start_time = time.time()
    
    # Get all collections
    collections = get_all_collections()
    print(f"Found {len(collections)} collections to index")
    print(f"Collections: {', '.join(collections)}")
    
    # Determine parallelism - with 41GB available RAM, each worker uses ~400-500MB
    # We can safely run 15-20 workers, leaving headroom for system
    num_workers = min(len(collections), 15)
    print(f"Using {num_workers} parallel workers (out of {mp.cpu_count()} CPUs, {len(collections)} collections)")
    
    # Build indexes in parallel
    with mp.Pool(num_workers) as pool:
        results = pool.map(build_collection_index, collections)
    
    # Build master index
    build_master_index(results)
    
    # Print summary
    print("\n" + "="*80)
    print("INDEXING COMPLETE")
    print("="*80)
    
    for result in results:
        status = result["status"]
        collection = result["collection"]
        if status == "success":
            print(f"✓ {collection}: {result['domains']} domains, {result['pointers']} pointers")
        else:
            print(f"✗ {collection}: {status}")
    
    total_elapsed = time.time() - start_time
    print(f"\nTotal time: {total_elapsed/60:.1f} minutes")
    print(f"Index directory: {INDEX_DIR}")

if __name__ == "__main__":
    main()
