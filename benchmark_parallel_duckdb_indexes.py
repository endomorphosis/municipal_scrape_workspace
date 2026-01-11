#!/usr/bin/env python3
"""
Benchmark parallel DuckDB pointer index searches
Tests search performance with different query patterns
"""

import time
import json
import sys
from pathlib import Path
from typing import List, Dict, Any
import random

# Import the search function
sys.path.insert(0, str(Path(__file__).parent))
from search_parallel_duckdb_indexes import search_domain, get_available_collections

def benchmark_search(domain: str, iterations: int = 3, **kwargs) -> Dict[str, Any]:
    """Benchmark a single domain search"""
    times = []
    results_count = None
    
    for i in range(iterations):
        start = time.time()
        results = search_domain(domain, **kwargs)
        elapsed = time.time() - start
        times.append(elapsed)
        
        if results_count is None:
            results_count = len(results)
    
    return {
        "domain": domain,
        "results_count": results_count,
        "min_time": min(times),
        "max_time": max(times),
        "avg_time": sum(times) / len(times),
        "iterations": iterations
    }

def run_benchmark_suite():
    """Run comprehensive benchmark suite"""
    print("="*80)
    print("PARALLEL DUCKDB INDEX BENCHMARK SUITE")
    print("="*80)
    
    # Get available collections
    collections = get_available_collections()
    if not collections:
        print("ERROR: No collection indexes found!")
        return
    
    print(f"\nAvailable collections: {len(collections)}")
    print(f"Collections: {', '.join(collections)}")
    
    # Define test domains (you should customize these)
    test_domains = [
        "example.com",
        "wikipedia.org", 
        "github.com",
        "google.com",
        "amazon.com"
    ]
    
    results = {
        "timestamp": time.time(),
        "collections": collections,
        "benchmarks": []
    }
    
    # Benchmark 1: Single collection search
    print("\n" + "="*80)
    print("TEST 1: Single Collection Search")
    print("="*80)
    
    if collections:
        test_collection = collections[0]
        print(f"\nSearching in: {test_collection}")
        
        for domain in test_domains[:3]:
            print(f"\nBenchmarking: {domain}")
            bench = benchmark_search(domain, collections=[test_collection], iterations=3)
            print(f"  Results: {bench['results_count']}")
            print(f"  Time: {bench['avg_time']:.3f}s (min: {bench['min_time']:.3f}s, max: {bench['max_time']:.3f}s)")
            bench["test"] = "single_collection"
            bench["collections"] = [test_collection]
            results["benchmarks"].append(bench)
    
    # Benchmark 2: All collections search (parallel)
    print("\n" + "="*80)
    print("TEST 2: All Collections Search (Parallel)")
    print("="*80)
    
    for domain in test_domains[:3]:
        print(f"\nBenchmarking: {domain}")
        bench = benchmark_search(domain, parallel=True, iterations=3)
        print(f"  Results: {bench['results_count']}")
        print(f"  Time: {bench['avg_time']:.3f}s (min: {bench['min_time']:.3f}s, max: {bench['max_time']:.3f}s)")
        bench["test"] = "all_collections_parallel"
        results["benchmarks"].append(bench)
    
    # Benchmark 3: All collections search (sequential)
    print("\n" + "="*80)
    print("TEST 3: All Collections Search (Sequential)")
    print("="*80)
    
    for domain in test_domains[:2]:
        print(f"\nBenchmarking: {domain}")
        bench = benchmark_search(domain, parallel=False, iterations=3)
        print(f"  Results: {bench['results_count']}")
        print(f"  Time: {bench['avg_time']:.3f}s (min: {bench['min_time']:.3f}s, max: {bench['max_time']:.3f}s)")
        bench["test"] = "all_collections_sequential"
        results["benchmarks"].append(bench)
    
    # Benchmark 4: Limited results
    print("\n" + "="*80)
    print("TEST 4: Limited Results (First 100)")
    print("="*80)
    
    for domain in test_domains[:3]:
        print(f"\nBenchmarking: {domain}")
        bench = benchmark_search(domain, limit=100, parallel=True, iterations=3)
        print(f"  Results: {bench['results_count']}")
        print(f"  Time: {bench['avg_time']:.3f}s (min: {bench['min_time']:.3f}s, max: {bench['max_time']:.3f}s)")
        bench["test"] = "limited_100"
        results["benchmarks"].append(bench)
    
    # Summary
    print("\n" + "="*80)
    print("BENCHMARK SUMMARY")
    print("="*80)
    
    by_test = {}
    for bench in results["benchmarks"]:
        test = bench["test"]
        if test not in by_test:
            by_test[test] = []
        by_test[test].append(bench["avg_time"])
    
    for test, times in sorted(by_test.items()):
        avg = sum(times) / len(times)
        print(f"\n{test}:")
        print(f"  Average: {avg:.3f}s")
        print(f"  Min: {min(times):.3f}s")
        print(f"  Max: {max(times):.3f}s")
    
    # Save results
    output_file = Path("benchmark_results.json")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: {output_file}")
    
    # Calculate speedup
    if "all_collections_parallel" in by_test and "all_collections_sequential" in by_test:
        parallel_avg = sum(by_test["all_collections_parallel"]) / len(by_test["all_collections_parallel"])
        sequential_avg = sum(by_test["all_collections_sequential"]) / len(by_test["all_collections_sequential"])
        speedup = sequential_avg / parallel_avg
        print(f"\nParallel speedup: {speedup:.2f}x")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--custom":
        # Custom benchmark mode
        if len(sys.argv) < 3:
            print("Usage: python benchmark_parallel_duckdb_indexes.py --custom <domain>")
            sys.exit(1)
        
        domain = sys.argv[2]
        print(f"Running custom benchmark for: {domain}\n")
        
        # Run different search modes
        print("1. Single collection:")
        collections = get_available_collections()
        if collections:
            bench = benchmark_search(domain, collections=[collections[0]], iterations=5)
            print(f"   Time: {bench['avg_time']:.3f}s, Results: {bench['results_count']}")
        
        print("\n2. All collections (parallel):")
        bench = benchmark_search(domain, parallel=True, iterations=5)
        print(f"   Time: {bench['avg_time']:.3f}s, Results: {bench['results_count']}")
        
        print("\n3. All collections (sequential):")
        bench = benchmark_search(domain, parallel=False, iterations=5)
        print(f"   Time: {bench['avg_time']:.3f}s, Results: {bench['results_count']}")
        
        print("\n4. Limited (100 results):")
        bench = benchmark_search(domain, limit=100, parallel=True, iterations=5)
        print(f"   Time: {bench['avg_time']:.3f}s, Results: {bench['results_count']}")
    else:
        run_benchmark_suite()

if __name__ == "__main__":
    main()
