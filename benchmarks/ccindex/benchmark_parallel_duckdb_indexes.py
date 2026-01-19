#!/usr/bin/env python3
"""
Benchmark the parallel DuckDB pointer index architecture
"""
import duckdb
import time
import sys
import random
from pathlib import Path
import multiprocessing as mp
import json

INDEX_DIR = Path("/storage/ccindex_duckdb/cc_pointers_by_collection")
PARQUET_BASE = Path("/storage/ccindex_parquet/cc_pointers_by_year")

def get_collection_indexes():
    return sorted(INDEX_DIR.glob("CC-MAIN-*.duckdb"))

def get_sample_domains(num_samples=100):
    indexes = get_collection_indexes()
    if not indexes:
        return []
    sample_db = random.choice(indexes)
    con = duckdb.connect(str(sample_db), read_only=True)
    domains = con.execute(f"SELECT DISTINCT domain FROM domain_pointers ORDER BY RANDOM() LIMIT {num_samples}").fetchall()
    con.close()
    return [d[0] for d in domains]

def search_collection_timed(args):
    db_path, domain = args
    start = time.time()
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        results = con.execute("SELECT domain, parquet_file, row_offset, row_count FROM domain_pointers WHERE domain = ?", [domain]).fetchall()
        con.close()
        elapsed = time.time() - start
        return {"collection": db_path.stem, "found": len(results) > 0, "num_pointers": len(results), "time": elapsed, "error": None}
    except Exception as e:
        elapsed = time.time() - start
        return {"collection": db_path.stem, "found": False, "num_pointers": 0, "time": elapsed, "error": str(e)}

def benchmark_search(domains, max_workers=10):
    indexes = get_collection_indexes()
    print(f"Benchmarking {len(domains)} domains across {len(indexes)} collections")
    print(f"Using {max_workers} workers")
    
    results = {"searches_per_domain": [], "overall_stats": {}}
    overall_start = time.time()
    
    for i, domain in enumerate(domains, 1):
        domain_start = time.time()
        search_args = [(db, domain) for db in indexes]
        
        with mp.Pool(max_workers) as pool:
            search_results = pool.map(search_collection_timed, search_args)
        
        domain_elapsed = time.time() - domain_start
        found_in = sum(1 for r in search_results if r["found"])
        total_pointers = sum(r["num_pointers"] for r in search_results)
        
        results["searches_per_domain"].append({
            "domain": domain,
            "found_in_collections": found_in,
            "total_pointers": total_pointers,
            "wall_time": domain_elapsed
        })
        
        if i % 10 == 0:
            print(f"  {i}/{len(domains)} domains ({found_in} hits, {domain_elapsed:.3f}s)")
    
    overall_elapsed = time.time() - overall_start
    all_wall_times = [s["wall_time"] for s in results["searches_per_domain"]]
    
    results["overall_stats"] = {
        "total_wall_time": overall_elapsed,
        "avg_wall_time_per_domain": sum(all_wall_times) / len(all_wall_times),
        "min_wall_time": min(all_wall_times),
        "max_wall_time": max(all_wall_times),
        "searches_per_second": (len(domains) * len(indexes)) / overall_elapsed
    }
    
    return results

def benchmark_index_size():
    indexes = get_collection_indexes()
    print("\nIndex Size Analysis:")
    print("="*80)
    
    total_size = 0
    for db_path in indexes:
        size_bytes = db_path.stat().st_size
        total_size += size_bytes
        con = duckdb.connect(str(db_path), read_only=True)
        num_domains = con.execute("SELECT COUNT(DISTINCT domain) FROM domain_pointers").fetchone()[0]
        num_pointers = con.execute("SELECT COUNT(*) FROM domain_pointers").fetchone()[0]
        con.close()
        print(f"  {db_path.stem}: {size_bytes/(1024*1024):.2f} MB, {num_domains:,} domains, {num_pointers:,} pointers")
    
    print(f"\nTotal: {total_size/(1024*1024):.2f} MB")
    return {"total_size_mb": total_size / (1024*1024)}

def main():
    print("="*80)
    print("DuckDB Parallel Pointer Index Benchmark")
    print("="*80)
    
    indexes = get_collection_indexes()
    if not indexes:
        print("No indexes found!")
        sys.exit(1)
    
    print(f"\nFound {len(indexes)} collection indexes")
    
    size_results = benchmark_index_size()
    
    print("\n" + "="*80)
    print("Search Performance Benchmark")
    print("="*80)
    
    domains = get_sample_domains(50)
    if not domains:
        print("No domains found!")
        sys.exit(1)
    
    search_results = benchmark_search(domains, max_workers=10)
    
    print("\nResults:")
    stats = search_results["overall_stats"]
    print(f"Total time: {stats['total_wall_time']:.2f}s")
    print(f"Avg per domain: {stats['avg_wall_time_per_domain']:.3f}s")
    print(f"Searches/sec: {stats['searches_per_second']:.1f}")
    
    with open("benchmark_results_parallel_duckdb.json", "w") as f:
        json.dump({"size": size_results, "search": search_results}, f, indent=2)
    
    print("\nResults saved to benchmark_results_parallel_duckdb.json")

if __name__ == "__main__":
    main()
