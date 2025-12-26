#!/usr/bin/env python3
"""Compare scraping results with and without website crawling."""

import duckdb
import glob
import sys
from pathlib import Path

def analyze_run(out_dir: str) -> dict:
    """Analyze scrape results from a run directory."""
    dbs = sorted(glob.glob(f"{out_dir}/state/worker_*.duckdb"))
    
    if not dbs:
        print(f"No databases found in {out_dir}")
        return {}
    
    total_success = 0
    total_attempts = 0
    total_blobs = 0
    total_discovered = 0
    
    # Try to read each worker DB (read-only to avoid locking issues)
    for db_path in dbs:
        try:
            con = duckdb.connect(db_path, read_only=True)
            
            # Count successes
            result = con.execute("SELECT COUNT(*) FROM url_cid_latest WHERE last_status='success'").fetchone()
            if result:
                total_success += result[0]
            
            # Count attempts
            result = con.execute("SELECT COUNT(*) FROM scrape_attempts").fetchone()
            if result:
                total_attempts += result[0]
            
            # Check if discovered_links table exists and count them
            try:
                result = con.execute("SELECT COUNT(*) FROM discovered_links").fetchone()
                if result:
                    total_discovered += result[0]
            except:
                pass
            
            con.close()
        except Exception as e:
            print(f"  (Worker DB {db_path} is locked or unavailable: {e})")
    
    # Count blob files
    blob_dir = Path(out_dir) / "content_blobs"
    if blob_dir.exists():
        total_blobs = len(list(blob_dir.glob("*.bin")))
    
    return {
        "successes": total_success,
        "attempts": total_attempts,
        "blobs": total_blobs,
        "discovered_links": total_discovered,
    }

if __name__ == "__main__":
    print("\n=== Comparing Scraping Results ===\n")
    
    run1 = analyze_run("out_full_methods_w4_c12")
    print("Without Crawling (out_full_methods_w4_c12):")
    print(f"  Successes:      {run1.get('successes', 'N/A')}")
    print(f"  Total Attempts: {run1.get('attempts', 'N/A')}")
    print(f"  Blobs Written:  {run1.get('blobs', 'N/A')}")
    print(f"  Discovered Links: {run1.get('discovered_links', 'N/A')}")
    
    print()
    
    run2 = analyze_run("out_with_crawling_w4_c12")
    print("With Crawling (out_with_crawling_w4_c12):")
    print(f"  Successes:      {run2.get('successes', 'N/A')}")
    print(f"  Total Attempts: {run2.get('attempts', 'N/A')}")
    print(f"  Blobs Written:  {run2.get('blobs', 'N/A')}")
    print(f"  Discovered Links: {run2.get('discovered_links', 'N/A')}")
    
    print()
    
    # Show deltas
    if run1.get('successes') and run2.get('successes'):
        delta_success = run2['successes'] - run1['successes']
        pct = (delta_success / run1['successes'] * 100) if run1['successes'] else 0
        print(f"Delta Successes: +{delta_success} ({pct:.1f}%)")
    
    if run1.get('blobs') and run2.get('blobs'):
        delta_blobs = run2['blobs'] - run1['blobs']
        pct = (delta_blobs / run1['blobs'] * 100) if run1['blobs'] else 0
        print(f"Delta Blobs: +{delta_blobs} ({pct:.1f}%)")
    
    print()
