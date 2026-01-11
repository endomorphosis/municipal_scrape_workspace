#!/usr/bin/env python3
"""
Simple live monitoring for Common Crawl Pipeline (like watch command)
Refreshes every 2 seconds with clear screen

Usage:
  python cc_pipeline_watch.py           # Interactive live mode
  python cc_pipeline_watch.py --once    # Print once and exit (CI/CD mode)
"""

import os
import sys
import time
import psutil
import json
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class PipelineWatcher:
    def __init__(self):
        self.ccindex_dir = Path("/storage/ccindex")
        self.parquet_dir = Path("/storage/ccindex_parquet")
        self.pointer_dir = Path("/storage/ccindex_duckdb")
        self.collinfo_cache_file = Path("/tmp/cc_collinfo_cache.json")
        self.collinfo_cache_ttl = 3600  # 1 hour
        
    def get_all_collections(self):
        """Fetch all Common Crawl collections from collinfo.json with caching"""
        # Check cache
        if self.collinfo_cache_file.exists():
            cache_age = time.time() - self.collinfo_cache_file.stat().st_mtime
            if cache_age < self.collinfo_cache_ttl:
                with open(self.collinfo_cache_file) as f:
                    return json.load(f)
        
        # Fetch from Common Crawl
        try:
            response = requests.get('https://index.commoncrawl.org/collinfo.json', timeout=10)
            response.raise_for_status()
            collections = response.json()
            
            # Cache it
            with open(self.collinfo_cache_file, 'w') as f:
                json.dump(collections, f)
            
            return collections
        except Exception as e:
            print(f"Warning: Could not fetch collinfo.json: {e}", file=sys.stderr)
            # Return empty list if we can't fetch
            return []
    
    def analyze_collection_status(self):
        """Check which collections are downloaded, converted, sorted, indexed"""
        all_collections = self.get_all_collections()
        
        status = {}
        for coll in all_collections:
            coll_id = coll['id']
            status[coll_id] = {
                'name': coll['name'],
                'from': coll.get('from', ''),
                'to': coll.get('to', ''),
                'downloaded': 0,
                'converted': 0,
                'sorted': 0,
                'indexed': False,
                'total_shards': 0  # We'll estimate from what we have
            }
        
        # Scan downloaded .gz files
        if self.ccindex_dir.exists():
            for gz_file in self.ccindex_dir.rglob("*.gz"):
                # Extract collection ID from path like: cdx-00299.gz or CC-MAIN-2024-51/cdx-00299.gz
                parts = gz_file.parts
                for part in parts:
                    if part.startswith('CC-MAIN-'):
                        if part in status:
                            status[part]['downloaded'] += 1
                            status[part]['total_shards'] = max(status[part]['total_shards'], status[part]['downloaded'])
        
        # Scan converted parquet files
        if self.parquet_dir.exists():
            for pq_file in self.parquet_dir.rglob("*.gz.parquet"):
                # Skip .sorted marker files
                if pq_file.suffix == '.sorted' or str(pq_file).endswith('.parquet.sorted'):
                    continue
                    
                # Extract collection from path parts like: CC-MAIN-2024-51/cdx-00299.gz.parquet
                parts = pq_file.parts
                for part in parts:
                    if part.startswith('CC-MAIN-'):
                        if part in status:
                            status[part]['converted'] += 1
                            status[part]['total_shards'] = max(status[part]['total_shards'], status[part]['converted'])
                            
                            # Check if sorted (marker file has .sorted appended)
                            sorted_marker = Path(str(pq_file) + '.sorted')
                            if sorted_marker.exists():
                                status[part]['sorted'] += 1
                        break
        
        # Check if indexed
        if self.pointer_dir.exists():
            for db_file in self.pointer_dir.rglob("*.duckdb"):
                for coll_id in status.keys():
                    if coll_id in db_file.stem:
                        status[coll_id]['indexed'] = True
        
        return status
        
    def get_stats(self):
        """Gather all pipeline statistics"""
        stats = {}
        
        # File counts (recursive search) - exclude sorted files from parquet count
        stats['gz_files'] = len(list(self.ccindex_dir.rglob("*.gz"))) if self.ccindex_dir.exists() else 0
        
        # Count all .gz.parquet files, excluding .sorted files
        all_parquet = []
        sorted_parquet = []
        if self.parquet_dir.exists():
            for f in self.parquet_dir.rglob("*.gz.parquet"):
                # Skip .sorted marker files
                if str(f).endswith('.parquet.sorted'):
                    continue
                all_parquet.append(f)
                # Check if sorted (marker file has .sorted appended)
                sorted_marker = Path(str(f) + '.sorted')
                if sorted_marker.exists():
                    sorted_parquet.append(f)
        
        stats['parquet_files'] = len(all_parquet)
        stats['sorted_files'] = len(sorted_parquet)
        stats['unsorted_files'] = len(all_parquet) - len(sorted_parquet)
        
        # Collection status analysis
        try:
            stats['collection_status'] = self.analyze_collection_status()
        except Exception as e:
            print(f"Warning: Could not analyze collection status: {e}", file=sys.stderr)
            stats['collection_status'] = {}
        
        # Pointer DBs by collection (recursive)
        collections = defaultdict(list)
        if self.pointer_dir.exists():
            for db in self.pointer_dir.rglob("*.duckdb"):
                collection = db.parent.name
                collections[collection].append(db.name)
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
        
        # Completeness check - how many .gz files should have parquet equivalents
        stats['missing_parquet'] = 0
        stats['missing_sorted'] = stats['unsorted_files']
        if self.ccindex_dir.exists():
            gz_basenames = {f.stem for f in self.ccindex_dir.rglob("*.gz")}
            parquet_basenames = {f.stem.replace('.gz.parquet', '') for f in self.parquet_dir.rglob("*.gz.parquet")} if self.parquet_dir.exists() else set()
            stats['missing_parquet'] = len(gz_basenames - parquet_basenames)
        
        return stats
    
    def display_stats(self, stats, clear_screen=True):
        """Display statistics in a clean format"""
        if clear_screen:
            os.system('clear')
        
        print("=" * 80)
        print(f"COMMON CRAWL PIPELINE MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print()
        
        # File Progress
        print("FILE PROGRESS:")
        print(f"  Downloaded .gz files:     {stats['gz_files']:>6}")
        print(f"  Converted parquet files:  {stats['parquet_files']:>6}")
        print(f"  Sorted parquet files:     {stats['sorted_files']:>6}")
        print(f"  Unsorted parquet files:   {stats['unsorted_files']:>6}")
        print(f"  DuckDB pointer indexes:   {stats['total_dbs']:>6}")
        print()
        
        # Completeness
        print("COMPLETENESS:")
        print(f"  Missing parquet conversions: {stats['missing_parquet']:>6}")
        print(f"  Missing sorts:               {stats['missing_sorted']:>6}")
        print()
        
        # Collection status (if available)
        if 'collection_status' in stats and stats['collection_status']:
            print("COLLECTION STATUS (Recent 2024-2025):")
            print(f"  {'Collection ID':<20} {'Downloaded':<12} {'Converted':<12} {'Sorted':<10} {'Indexed':<8}")
            print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*10} {'-'*8}")
            
            # Show collections with activity (downloaded, converted, sorted, or indexed)
            active_collections = [(k, v) for k, v in stats['collection_status'].items() 
                                  if v['downloaded'] > 0 or v['converted'] > 0 or v['sorted'] > 0 or v['indexed']]
            active_collections.sort(reverse=True)  # Newest first
            
            # Show collections with sorted files first, then others
            sorted_first = [c for c in active_collections if c[1]['sorted'] > 0]
            others = [c for c in active_collections if c[1]['sorted'] == 0]
            display_collections = sorted_first + others
            
            for coll_id, coll_info in display_collections[:20]:  # Top 20
                downloaded = coll_info['downloaded']
                converted = coll_info['converted']
                sorted_count = coll_info['sorted']
                indexed = 'Yes' if coll_info['indexed'] else 'No'
                total = coll_info['total_shards']
                
                dl_str = f"{downloaded}/{total}" if total > 0 else f"{downloaded}"
                cv_str = f"{converted}/{total}" if total > 0 else f"{converted}"
                st_str = f"{sorted_count}/{total}" if total > 0 else f"{sorted_count}"
                
                print(f"  {coll_id:<20} {dl_str:<12} {cv_str:<12} {st_str:<10} {indexed:<8}")
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
        if clear_screen:
            print("Press Ctrl+C to exit")
            print("=" * 80)
    
    def watch(self, interval=2):
        """Continuously monitor and display stats"""
        try:
            while True:
                stats = self.get_stats()
                self.display_stats(stats, clear_screen=True)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
    
    def print_once(self):
        """Print stats once and exit (for CI/CD)"""
        stats = self.get_stats()
        self.display_stats(stats, clear_screen=False)
        
        # Return exit code based on completeness
        if stats['missing_parquet'] > 0 or stats['missing_sorted'] > 0:
            return 1  # Incomplete
        return 0  # All complete

def main():
    watcher = PipelineWatcher()
    
    # Check for --once flag
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        exit_code = watcher.print_once()
        sys.exit(exit_code)
    else:
        watcher.watch(interval=2)

if __name__ == "__main__":
    main()
