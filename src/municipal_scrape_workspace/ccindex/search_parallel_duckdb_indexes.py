#!/usr/bin/env python3
"""
Search across all parallel DuckDB pointer indexes
"""
import duckdb
import sys
import time
from pathlib import Path
import multiprocessing as mp

INDEX_DIR = Path("/storage/ccindex_duckdb/cc_pointers_by_collection")
PARQUET_BASE = Path("/storage/ccindex_parquet/cc_pointers_by_year")

def get_collection_indexes():
    return sorted(INDEX_DIR.glob("CC-MAIN-*.duckdb"))

def search_collection(args):
    db_path, domain = args
    collection = db_path.stem
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        results = con.execute("SELECT domain, parquet_file, row_offset, row_count FROM domain_pointers WHERE domain = ? ORDER BY parquet_file", [domain]).fetchall()
        con.close()
        return {"collection": collection, "results": results, "error": None}
    except Exception as e:
        return {"collection": collection, "results": [], "error": str(e)}

def search_domain(domain, max_workers=10):
    start_time = time.time()
    indexes = get_collection_indexes()
    if not indexes:
        print("No collection indexes found!")
        return {}
    
    print(f"Searching {len(indexes)} collections for: {domain}")
    search_args = [(db, domain) for db in indexes]
    
    with mp.Pool(max_workers) as pool:
        search_results = pool.map(search_collection, search_args)
    
    all_results = {}
    total_pointers = 0
    
    for result in search_results:
        if result["results"]:
            collection = result["collection"]
            all_results[collection] = []
            for domain_name, parquet_file, row_offset, row_count in result["results"]:
                all_results[collection].append({
                    "parquet_file": parquet_file,
                    "row_offset": row_offset,
                    "row_count": row_count
                })
                total_pointers += 1
    
    elapsed = time.time() - start_time
    print(f"\nSearch time: {elapsed:.3f}s")
    print(f"Collections with results: {len(all_results)}")
    print(f"Total pointers: {total_pointers}\n")
    
    for collection, pointers in sorted(all_results.items()):
        print(f"{collection}:")
        for p in pointers:
            print(f"  {p['parquet_file']}: rows {p['row_offset']}-{p['row_offset']+p['row_count']} ({p['row_count']} records)")
    
    return all_results

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: search_parallel_duckdb_indexes.py <domain>")
        sys.exit(1)
    search_domain(sys.argv[1])
