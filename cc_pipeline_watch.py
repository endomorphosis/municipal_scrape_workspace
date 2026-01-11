#!/usr/bin/env python3
"""
Simple live monitoring for Common Crawl Pipeline (like watch command)
Refreshes every 2 seconds with clear screen
"""

import os
import time
import psutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class PipelineWatcher:
    def __init__(self):
        self.ccindex_dir = Path("/storage/ccindex")
        self.parquet_dir = Path("/storage/ccindex_parquet")
        self.pointer_dir = Path("/storage/ccindex_duckdb")
        
    def get_stats(self):
        """Gather all pipeline statistics"""
        stats = {}
        
        # File counts
        stats['gz_files'] = len(list(self.ccindex_dir.glob("*.gz"))) if self.ccindex_dir.exists() else 0
        stats['parquet_files'] = len(list(self.parquet_dir.glob("*.gz.parquet"))) if self.parquet_dir.exists() else 0
        stats['sorted_markers'] = len(list(self.parquet_dir.glob("*.sorted"))) if self.parquet_dir.exists() else 0
        
        # Pointer DBs by collection
        collections = defaultdict(list)
        if self.pointer_dir.exists():
            for db in self.pointer_dir.glob("*/*.duckdb"):
                year = db.parent.name
                collections[year].append(db.name)
        stats['collections'] = dict(collections)
        stats['total_dbs'] = sum(len(v) for v in collections.values())
        
        # Active processes
        active_procs = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_info']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if any(keyword in cmdline for keyword in ['build_cc_pointer', 'sort_', 'convert_', 'regenerate_parquet']):
                    active_procs.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'cmd': cmdline[:80],
                        'cpu': proc.info['cpu_percent'],
                        'mem_mb': proc.info['memory_info'].rss / 1024 / 1024
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        stats['active_processes'] = active_procs
        
        # Disk space
        stats['disk_space'] = {}
        for name, path in [('ccindex', self.ccindex_dir), ('parquet', self.parquet_dir), ('pointer', self.pointer_dir)]:
            if path.exists():
                usage = psutil.disk_usage(str(path))
                stats['disk_space'][name] = {
                    'free_gb': usage.free / (1024**3),
                    'used_gb': usage.used / (1024**3),
                    'total_gb': usage.total / (1024**3),
                    'percent': usage.percent
                }
        
        # Memory
        mem = psutil.virtual_memory()
        stats['memory'] = {
            'available_gb': mem.available / (1024**3),
            'used_gb': mem.used / (1024**3),
            'total_gb': mem.total / (1024**3),
            'percent': mem.percent
        }
        
        # CPU
        stats['cpu_percent'] = psutil.cpu_percent(interval=0.1)
        
        return stats
    
    def display_stats(self, stats):
        """Display statistics in a clean format"""
        os.system('clear')
        
        print("=" * 80)
        print(f"COMMON CRAWL PIPELINE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print()
        
        # File Progress
        print("FILE PROGRESS:")
        print(f"  Downloaded .gz files:     {stats['gz_files']:>6}")
        print(f"  Converted parquet files:  {stats['parquet_files']:>6}")
        print(f"  Sorted files:             {stats['sorted_markers']:>6}")
        print(f"  DuckDB pointer indexes:   {stats['total_dbs']:>6}")
        print()
        
        # Collections breakdown
        if stats['collections']:
            print("POINTER INDEXES BY COLLECTION:")
            for year in sorted(stats['collections'].keys()):
                dbs = stats['collections'][year]
                print(f"  {year}: {len(dbs)} databases")
            print()
        
        # Active Processes
        print(f"ACTIVE PROCESSES: {len(stats['active_processes'])}")
        if stats['active_processes']:
            print(f"  {'PID':<8} {'CPU%':<6} {'MEM(MB)':<10} {'COMMAND'}")
            print(f"  {'-'*8} {'-'*6} {'-'*10} {'-'*50}")
            for proc in stats['active_processes'][:10]:  # Top 10
                print(f"  {proc['pid']:<8} {proc['cpu']:<6.1f} {proc['mem_mb']:<10.0f} {proc['cmd'][:50]}")
        print()
        
        # System Resources
        print("SYSTEM RESOURCES:")
        print(f"  CPU Usage:    {stats['cpu_percent']:>5.1f}%")
        mem = stats['memory']
        print(f"  Memory:       {mem['used_gb']:.1f} / {mem['total_gb']:.1f} GB ({mem['percent']:.1f}% used)")
        print()
        
        print("DISK SPACE:")
        for name, disk in stats['disk_space'].items():
            print(f"  {name:>10}: {disk['free_gb']:>7.1f} GB free / {disk['total_gb']:>7.1f} GB total ({disk['percent']:>5.1f}% used)")
        
        print()
        print("=" * 80)
        print("Press Ctrl+C to exit")
        print("=" * 80)
    
    def watch(self, interval=2):
        """Continuously monitor and display stats"""
        try:
            while True:
                stats = self.get_stats()
                self.display_stats(stats)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")

def main():
    watcher = PipelineWatcher()
    watcher.watch(interval=2)

if __name__ == "__main__":
    main()
