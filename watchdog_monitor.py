#!/usr/bin/env python3
"""Verbose watchdog monitor for parallel scraping runs.

Usage:
    python watchdog_monitor.py --out out_full_methods_w4_c12 --interval 5
"""

import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

try:
    import duckdb
except ImportError:
    print("Error: duckdb not installed. Install with: pip install duckdb")
    sys.exit(1)


def get_worker_dbs(out_root: Path) -> List[Path]:
    """Find all worker_*.duckdb files in state/."""
    state_dir = out_root / "state"
    return sorted(state_dir.glob("worker_*.duckdb"))


def query_worker_stats(db_path: Path) -> Optional[Dict]:
    """Query a worker DB for current stats. Returns None if DB is locked."""
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        worker_id = db_path.stem.split("_")[-1]
        
        # Total URLs assigned to this worker
        total_urls = con.execute(
            "SELECT COUNT(*) FROM town_urls WHERE shard = ?",
            [int(worker_id)]
        ).fetchone()[0] if int(worker_id) < 10 else 0

        # Success count
        success_count = con.execute(
            "SELECT COUNT(*) FROM url_cid_latest WHERE last_status = 'success'"
        ).fetchone()[0]

        # Error count
        error_count = con.execute(
            "SELECT COUNT(*) FROM url_cid_latest WHERE last_status = 'error'"
        ).fetchone()[0]

        # Total attempts
        total_attempts = con.execute(
            "SELECT COUNT(*) FROM scrape_attempts"
        ).fetchone()[0]

        # Unique content blobs
        unique_blobs = con.execute(
            "SELECT COUNT(DISTINCT content_cid) FROM url_cid_latest WHERE last_status = 'success'"
        ).fetchone()[0]

        # Top error types
        error_types = con.execute(
            """
            SELECT last_error, COUNT(*) as cnt FROM url_cid_latest
            WHERE last_status = 'error' AND last_error IS NOT NULL
            GROUP BY last_error ORDER BY cnt DESC LIMIT 5
            """
        ).fetchall()

        # Top successful methods
        methods = con.execute(
            """
            SELECT last_method_used, COUNT(*) as cnt FROM url_cid_latest
            WHERE last_status = 'success' AND last_method_used IS NOT NULL
            GROUP BY last_method_used ORDER BY cnt DESC
            """
        ).fetchall()

        # Average bytes per successful page
        avg_bytes = con.execute(
            "SELECT AVG(last_content_bytes) FROM url_cid_latest WHERE last_status = 'success'"
        ).fetchone()[0]

        # Min/max response times
        response_times = con.execute(
            """
            SELECT
                MIN(CAST((julianday(finished_at) - julianday(started_at)) * 86400 AS INT)) as min_secs,
                MAX(CAST((julianday(finished_at) - julianday(started_at)) * 86400 AS INT)) as max_secs,
                AVG(CAST((julianday(finished_at) - julianday(started_at)) * 86400 AS INT)) as avg_secs
            FROM scrape_attempts WHERE status = 'success'
            """
        ).fetchone()

        con.close()

        return {
            "worker_id": worker_id,
            "success_count": success_count,
            "error_count": error_count,
            "total_attempts": total_attempts,
            "unique_blobs": unique_blobs,
            "error_types": error_types,
            "methods": methods,
            "avg_bytes": int(avg_bytes) if avg_bytes else 0,
            "response_times": {
                "min": int(response_times[0]) if response_times[0] else 0,
                "max": int(response_times[1]) if response_times[1] else 0,
                "avg": int(response_times[2]) if response_times[2] else 0,
            } if response_times[0] is not None else None,
        }
    except Exception as e:
        # DB likely locked or doesn't exist yet
        return None


def count_blobs(out_root: Path) -> int:
    """Count content blobs written."""
    blobs_dir = out_root / "content_blobs"
    if not blobs_dir.exists():
        return 0
    return len(list(blobs_dir.glob("*.bin")))


def get_archive_jobs_status(out_root: Path) -> Tuple[int, int]:
    """Count archive jobs submitted and completed."""
    jobs_file = out_root / "state" / "archive_jobs.jsonl"
    status_file = out_root / "state" / "archive_jobs_status.jsonl"
    
    jobs_submitted = 0
    jobs_completed = 0
    
    if jobs_file.exists():
        with open(jobs_file, "r") as f:
            jobs_submitted = sum(1 for _ in f)
    
    if status_file.exists():
        with open(status_file, "r") as f:
            jobs_completed = sum(1 for _ in f)
    
    return jobs_submitted, jobs_completed


def print_header(out_root: Path, elapsed_secs: int):
    """Print a header with timestamp and elapsed time."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elapsed_h = elapsed_secs // 3600
    elapsed_m = (elapsed_secs % 3600) // 60
    elapsed_s = elapsed_secs % 60
    print(f"\n{'='*100}")
    print(f"Monitor: {now} | Elapsed: {elapsed_h:02d}h {elapsed_m:02d}m {elapsed_s:02d}s | Target: {out_root.name}")
    print(f"{'='*100}")


def print_worker_summary(stats: Dict, start_time: float):
    """Print summary for one worker."""
    worker_id = stats["worker_id"]
    success = stats["success_count"]
    error = stats["error_count"]
    total = success + error
    total_attempts = stats["total_attempts"]
    blobs = stats["unique_blobs"]
    
    success_rate = (success / total * 100) if total > 0 else 0
    attempt_ratio = total_attempts / total if total > 0 else 0
    
    elapsed = time.time() - start_time
    rate = success / elapsed if elapsed > 0 else 0
    
    print(f"\n[Worker {worker_id}]")
    print(f"  ✓ Success: {success:4d} ({success_rate:5.1f}%) | ✗ Errors: {error:4d} | Total URLs: {total:4d}")
    print(f"  Attempts: {total_attempts:4d} (avg {attempt_ratio:.2f} per URL) | Blobs: {blobs:4d}")
    print(f"  Rate: {rate:.2f} URLs/sec | Avg bytes: {stats['avg_bytes']:,d}")
    
    if stats["response_times"]:
        rt = stats["response_times"]
        print(f"  Response time: min {rt['min']}s | avg {rt['avg']}s | max {rt['max']}s")
    
    if stats["methods"]:
        methods_str = ", ".join([f"{m[0]}:{m[1]}" for m in stats["methods"][:3]])
        print(f"  Top methods: {methods_str}")
    
    if stats["error_types"]:
        errors_str = ", ".join([f"{e[0][:20]}:{e[1]}" for e in stats["error_types"][:3]])
        print(f"  Top errors: {errors_str}")


def print_global_summary(out_root: Path, all_stats: List[Dict], blobs: int, elapsed_secs: int):
    """Print combined summary across all workers."""
    if not all_stats:
        print("  (no workers active yet)")
        return
    
    total_success = sum(s["success_count"] for s in all_stats)
    total_error = sum(s["error_count"] for s in all_stats)
    total_attempts = sum(s["total_attempts"] for s in all_stats)
    total_urls = total_success + total_error
    
    success_rate = (total_success / total_urls * 100) if total_urls > 0 else 0
    
    archive_submitted, archive_completed = get_archive_jobs_status(out_root)
    
    print(f"\n[GLOBAL SUMMARY]")
    print(f"  Total URLs processed: {total_urls:6d}")
    print(f"  ✓ Success: {total_success:6d} ({success_rate:5.1f}%) | ✗ Errors: {total_error:6d}")
    print(f"  Total attempts: {total_attempts:6d} (avg {total_attempts/total_urls if total_urls > 0 else 0:.2f} per URL)")
    print(f"  Unique content blobs: {blobs:6d}")
    print(f"  Archive jobs: {archive_submitted} submitted, {archive_completed} completed")
    
    if elapsed_secs > 0:
        rate = total_success / elapsed_secs
        print(f"  Overall rate: {rate:.2f} URLs/sec")
        
        # Estimate time to finish based on remaining URLs
        # (rough estimate assuming consistent rate and similar distribution per worker)
        remaining = 50 * 4 - total_urls  # Assuming 50 URLs per worker × 4 workers
        if remaining > 0 and rate > 0:
            eta_secs = remaining / rate
            eta_h = int(eta_secs // 3600)
            eta_m = int((eta_secs % 3600) // 60)
            if eta_h > 0:
                print(f"  ETA to completion: ~{eta_h}h {eta_m}m (estimate)")
            elif eta_m > 0:
                print(f"  ETA to completion: ~{eta_m}m (estimate)")


def monitor(out_root: Path, interval: int, max_iterations: Optional[int] = None):
    """Run the watchdog monitor loop."""
    out_root = Path(out_root).resolve()
    if not out_root.exists():
        print(f"Error: Output directory not found: {out_root}")
        return 1
    
    start_time = time.time()
    iteration = 0
    
    try:
        while True:
            iteration += 1
            elapsed_secs = int(time.time() - start_time)
            
            if max_iterations and iteration > max_iterations:
                print("\nMax iterations reached. Stopping monitor.")
                break
            
            # Gather all worker stats
            worker_dbs = get_worker_dbs(out_root)
            all_stats = []
            for db_path in worker_dbs:
                stats = query_worker_stats(db_path)
                if stats:
                    all_stats.append(stats)
            
            # Count blobs
            blobs = count_blobs(out_root)
            
            # Print
            print_header(out_root, elapsed_secs)
            for stats in all_stats:
                print_worker_summary(stats, start_time)
            print_global_summary(out_root, all_stats, blobs, elapsed_secs)
            
            # Check if all workers have finished (no new progress)
            if all_stats:
                print(f"\n[Next update in {interval}s... Press Ctrl+C to stop]")
                time.sleep(interval)
            else:
                print(f"\n[Waiting for workers to start... checking again in {interval}s]")
                time.sleep(interval)
    
    except KeyboardInterrupt:
        print("\n\nMonitor stopped by user.")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Verbose watchdog monitor for parallel municipal scraping"
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output directory (e.g., out_full_methods_w4_c12)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Polling interval in seconds (default: 10)"
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Stop after N iterations (default: run until Ctrl+C)"
    )
    
    args = parser.parse_args()
    return monitor(Path(args.out), args.interval, args.max_iterations)


if __name__ == "__main__":
    sys.exit(main())
