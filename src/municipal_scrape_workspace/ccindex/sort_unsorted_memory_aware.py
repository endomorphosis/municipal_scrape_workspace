#!/usr/bin/env python3
"""
Memory-aware parallel parquet sorting.
Checks available memory before starting each sort job.
"""

import argparse
import multiprocessing as mp
import time
from pathlib import Path
from typing import Tuple

import duckdb
import psutil


def get_available_memory_gb() -> float:
    """Get available memory in GB."""
    mem = psutil.virtual_memory()
    return mem.available / (1024 ** 3)


def sort_parquet_file(args: Tuple[Path, Path, int, float]) -> Tuple[Path, bool, str]:
    """
    Sort a single parquet file with memory limits.
    Returns (file, success, message)
    """
    parquet_file, temp_dir, worker_id, memory_limit_gb = args
    
    try:
        # Create worker-specific temp directory
        worker_temp = temp_dir / f"worker_{worker_id}"
        worker_temp.mkdir(parents=True, exist_ok=True)
        
        sorted_tmp = worker_temp / f"{parquet_file.name}.sorted.tmp"
        
        # Sort using DuckDB with strict memory limit
        con = duckdb.connect(":memory:")
        con.execute(f"SET memory_limit='{memory_limit_gb}GB'")
        con.execute(f"SET temp_directory='{worker_temp}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=1")
        
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{parquet_file}')
                ORDER BY host_rev, url, ts
            )
            TO '{sorted_tmp}' (FORMAT 'parquet', COMPRESSION 'zstd')
        """)
        con.close()
        
        # Verify it's sorted
        con = duckdb.connect(":memory:")
        result = con.execute(f"""
            SELECT COUNT(*) as unsorted_count
            FROM (
                SELECT 
                    host_rev,
                    LAG(host_rev) OVER (ORDER BY idx) as prev_host_rev
                FROM (
                    SELECT host_rev, ROW_NUMBER() OVER () as idx 
                    FROM read_parquet('{sorted_tmp}') 
                    LIMIT 1000
                )
            )
            WHERE prev_host_rev > host_rev
        """).fetchone()
        con.close()
        
        unsorted_count = result[0] if result else 0
        
        if unsorted_count > 0:
            sorted_tmp.unlink()
            return (parquet_file, False, f"Verification failed: still {unsorted_count} unsorted")
        
        # Replace original
        parquet_file.unlink()
        sorted_tmp.rename(parquet_file)
        
        # Cleanup worker temp
        for f in worker_temp.glob("duckdb_temp_*"):
            f.unlink()
        
        return (parquet_file, True, "Sorted and verified")
        
    except Exception as e:
        return (parquet_file, False, f"Error: {e}")


class MemoryAwarePool:
    """Pool that only dispatches work when memory is available."""
    
    def __init__(self, max_workers: int, memory_per_worker_gb: float, reserve_gb: float = 10):
        self.max_workers = max_workers
        self.memory_per_worker_gb = memory_per_worker_gb
        self.reserve_gb = reserve_gb
        self.pool = mp.Pool(processes=max_workers)
        self.active_jobs = 0
        
    def can_start_job(self) -> bool:
        """Check if there's enough memory to start another job."""
        available_gb = get_available_memory_gb()
        required_gb = self.memory_per_worker_gb + self.reserve_gb
        return available_gb >= required_gb
    
    def submit(self, func, args):
        """Submit a job, waiting for memory if needed."""
        while not self.can_start_job():
            print(f"  ⏳ Waiting for memory... (need {self.memory_per_worker_gb}GB, have {get_available_memory_gb():.1f}GB free)", flush=True)
            time.sleep(5)
        
        self.active_jobs += 1
        return self.pool.apply_async(func, (args,))
    
    def close(self):
        self.pool.close()
        self.pool.join()


def main() -> int:
    ap = argparse.ArgumentParser(description="Memory-aware parallel parquet sorting")
    ap.add_argument("--unsorted-list", required=True, help="File containing list of unsorted files")
    ap.add_argument("--parquet-root", required=True, help="Root directory")
    ap.add_argument("--max-workers", type=int, default=8, help="Maximum parallel workers")
    ap.add_argument("--memory-per-worker", type=float, default=2.5, help="GB per worker")
    ap.add_argument("--reserve-memory", type=float, default=10, help="GB to reserve for system")
    ap.add_argument("--temp-dir", default="/tmp/sort_temp", help="Temp directory")
    
    args = ap.parse_args()
    
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Parquet root:      {parquet_root}")
    print(f"Max workers:       {args.max_workers}")
    print(f"Memory per worker: {args.memory_per_worker}GB")
    print(f"Reserve memory:    {args.reserve_memory}GB")
    print(f"Temp dir:          {temp_dir}")
    print()
    
    # Read unsorted file list
    unsorted_files = []
    try:
        with open(args.unsorted_list, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    full_path = parquet_root / line
                    if full_path.exists():
                        unsorted_files.append(full_path)
    except Exception as e:
        print(f"Error reading unsorted list: {e}")
        return 1
    
    if not unsorted_files:
        print("No unsorted files to process")
        return 0
    
    print(f"Found {len(unsorted_files)} unsorted files to sort")
    print()
    
    total_mem_gb = psutil.virtual_memory().total / (1024 ** 3)
    available_mem_gb = get_available_memory_gb()
    
    print(f"Memory status:")
    print(f"  Total:     {total_mem_gb:.1f} GB")
    print(f"  Available: {available_mem_gb:.1f} GB")
    print(f"  Reserve:   {args.reserve_memory:.1f} GB")
    print(f"  Per worker: {args.memory_per_worker:.1f} GB")
    print()
    
    # Calculate safe number of workers based on current memory
    safe_workers = min(
        args.max_workers,
        int((available_mem_gb - args.reserve_memory) / args.memory_per_worker)
    )
    safe_workers = max(1, safe_workers)
    
    print(f"Starting with {safe_workers} workers (memory-aware)")
    print()
    
    # Create memory-aware pool
    pool = MemoryAwarePool(
        max_workers=safe_workers,
        memory_per_worker_gb=args.memory_per_worker,
        reserve_gb=args.reserve_memory
    )
    
    sorted_count = 0
    failed_count = 0
    pending_jobs = []
    
    # Submit jobs with memory checking
    for i, pq_file in enumerate(unsorted_files):
        worker_id = i % safe_workers
        job_args = (pq_file, temp_dir, worker_id, args.memory_per_worker)
        
        print(f"[{i+1}/{len(unsorted_files)}] Submitting: {pq_file.name}", flush=True)
        result = pool.submit(sort_parquet_file, job_args)
        pending_jobs.append((pq_file, result))
    
    print()
    print("All jobs submitted. Waiting for completion...")
    print()
    
    # Collect results
    for i, (pq_file, result) in enumerate(pending_jobs, 1):
        try:
            pq_file_result, success, message = result.get(timeout=300)
            
            rel_path = pq_file_result.relative_to(parquet_root)
            
            if success:
                sorted_count += 1
                print(f"✅ [{i}/{len(unsorted_files)}] {rel_path}")
            else:
                failed_count += 1
                print(f"❌ [{i}/{len(unsorted_files)}] {rel_path}: {message}")
            
            if i % 10 == 0:
                print(f"Progress: {i}/{len(unsorted_files)} - Success: {sorted_count}, Failed: {failed_count}", flush=True)
                print(f"  Memory: {get_available_memory_gb():.1f} GB available", flush=True)
        
        except mp.TimeoutError:
            failed_count += 1
            print(f"❌ [{i}/{len(unsorted_files)}] {pq_file.name}: Timeout")
        except Exception as e:
            failed_count += 1
            print(f"❌ [{i}/{len(unsorted_files)}] {pq_file.name}: {e}")
    
    pool.close()
    
    print()
    print("=" * 80)
    print("SORTING COMPLETE")
    print("=" * 80)
    print(f"Total:   {len(unsorted_files)}")
    print(f"Sorted:  {sorted_count}")
    print(f"Failed:  {failed_count}")
    
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
