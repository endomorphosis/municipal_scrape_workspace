#!/usr/bin/env python3
"""Monitor progress of parallel scrape runs."""

import duckdb
import glob
import time
import sys
from pathlib import Path

def monitor_run(run_dir: str, max_wait_seconds: int = 3600):
    """Poll progress every 10 seconds until all workers complete or timeout."""
    run_path = Path(run_dir)
    start_time = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        db_files = sorted(glob.glob(str(run_path / "state" / "worker_*.duckdb")))
        if not db_files:
            print(f"No DB files found yet in {run_dir}")
            time.sleep(5)
            continue
        
        total_success = 0
        total_attempts = 0
        
        for db_file in db_files:
            worker_id = db_file.split("worker_")[-1].split(".")[0]
            try:
                con = duckdb.connect(db_file, read_only=True)
                try:
                    s = con.execute("SELECT COUNT(*) FROM url_cid_latest WHERE last_status='success'").fetchone()
                    success = int(s[0]) if s and s[0] else 0
                    a = con.execute("SELECT COUNT(*) FROM scrape_attempts").fetchone()
                    attempts = int(a[0]) if a and a[0] else 0
                    
                    total_success += success
                    total_attempts += attempts
                    print(f"  worker_{worker_id}: success {success:3d} | attempts {attempts:3d}")
                finally:
                    con.close()
            except Exception as e:
                print(f"  worker_{worker_id}: (locked or error: {type(e).__name__})")
        
        blobs = len(list((run_path / "content_blobs").glob("*.bin")))
        print(f"Total: {total_success} success | {total_attempts} attempts | {blobs} blobs")
        print(f"Time: {time.time() - start_time:.0f}s elapsed")
        print()
        
        # Check if any processes are still running
        import subprocess
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
        running = sum(1 for line in result.stdout.split("\n") if "orchestrate_municipal_scrape.py" in line and "--out " + run_dir in line)
        if running == 0:
            print(f"All workers completed!")
            break
        
        time.sleep(10)

def main() -> int:
    """Entry point for monitor_progress script."""
    if len(sys.argv) > 1:
        run_dir = sys.argv[1]
    else:
        run_dir = "out_full_methods_w4_c12"
    
    print(f"Monitoring {run_dir}...")
    monitor_run(run_dir)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
