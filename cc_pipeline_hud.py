#!/usr/bin/env python3
"""
Interactive Common Crawl Pipeline Monitor (top-style HUD)
Live monitoring of download, conversion, sorting, and indexing progress
"""

import curses
import time
import psutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import subprocess
import json

class PipelineMonitor:
    def __init__(self):
        self.ccindex_dir = Path("/storage/ccindex")
        self.parquet_dir = Path("/storage/ccindex_parquet")
        self.pointer_dir = Path("/storage/ccindex_duckdb")
        self.update_interval = 2  # seconds
        
    def get_file_counts(self):
        """Count files in each stage"""
        counts = {
            'gz_files': 0,
            'parquet_files': 0,
            'sorted_files': 0,
            'pointer_dbs': 0,
            'collections': set()
        }
        
        # Count .gz files (recursively)
        if self.ccindex_dir.exists():
            counts['gz_files'] = len(list(self.ccindex_dir.rglob("*.gz")))
        
        # Count parquet files and check if sorted (recursively)
        if self.parquet_dir.exists():
            for pq in self.parquet_dir.rglob("*.parquet"):
                counts['parquet_files'] += 1
                
                # Extract collection name
                if pq.name.endswith('.gz.parquet'):
                    name = pq.name.replace(".gz.parquet", "").replace(".sorted", "")
                elif pq.name.endswith('.gz.sorted.parquet'):
                    name = pq.name.replace(".gz.sorted.parquet", "")
                else:
                    name = pq.stem.replace(".sorted", "")
                
                counts['collections'].add(name)
                
                # Check if sorted (has .sorted. in filename)
                if '.sorted.' in pq.name or pq.name.endswith('.sorted.parquet'):
                    counts['sorted_files'] += 1
        
        # Count pointer DBs (recursively)
        if self.pointer_dir.exists():
            counts['pointer_dbs'] = len(list(self.pointer_dir.rglob("*.duckdb")))
        
        counts['collections'] = len(counts['collections'])
        return counts
    
    def get_disk_usage(self):
        """Get disk space for each directory"""
        usage = {}
        for name, path in [('ccindex', self.ccindex_dir), 
                           ('parquet', self.parquet_dir),
                           ('pointer', self.pointer_dir)]:
            if path.exists():
                stat = psutil.disk_usage(str(path))
                usage[name] = {
                    'total': stat.total / (1024**3),
                    'used': stat.used / (1024**3),
                    'free': stat.free / (1024**3),
                    'percent': stat.percent
                }
        return usage
    
    def get_active_processes(self):
        """Find active pipeline processes"""
        processes = []
        keywords = ['cc_pipeline', 'sort_', 'build_', 'convert_', 'download_cc']
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_percent']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if any(kw in cmdline for kw in keywords):
                    processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'cmd': cmdline[:80],
                        'cpu': proc.info['cpu_percent'],
                        'mem': proc.info['memory_percent']
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        return processes
    
    def get_memory_usage(self):
        """Get system memory stats"""
        mem = psutil.virtual_memory()
        return {
            'total': mem.total / (1024**3),
            'available': mem.available / (1024**3),
            'used': mem.used / (1024**3),
            'percent': mem.percent
        }
    
    def get_io_stats(self):
        """Get disk I/O statistics"""
        io = psutil.disk_io_counters()
        return {
            'read_mb': io.read_bytes / (1024**2),
            'write_mb': io.write_bytes / (1024**2),
            'read_count': io.read_count,
            'write_count': io.write_count
        }

def draw_header(stdscr, monitor):
    """Draw the header section"""
    height, width = stdscr.getmaxyx()
    
    # Title
    title = " Common Crawl Pipeline Monitor "
    stdscr.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD | curses.A_REVERSE)
    
    # Timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stdscr.addstr(0, width - len(timestamp) - 2, timestamp)
    
    return 1

def draw_summary(stdscr, y, monitor, counts, mem, disk):
    """Draw summary statistics"""
    stdscr.addstr(y, 0, "=" * stdscr.getmaxyx()[1], curses.A_BOLD)
    y += 1
    stdscr.addstr(y, 0, "PIPELINE SUMMARY", curses.A_BOLD)
    y += 1
    
    # File counts
    stdscr.addstr(y, 2, f"Downloaded .gz files:     {counts['gz_files']:>6}")
    y += 1
    stdscr.addstr(y, 2, f"Converted parquet files:  {counts['parquet_files']:>6}")
    y += 1
    stdscr.addstr(y, 2, f"Sorted parquet files:     {counts['sorted_files']:>6}")
    y += 1
    stdscr.addstr(y, 2, f"DuckDB pointer indexes:   {counts['pointer_dbs']:>6}")
    y += 1
    stdscr.addstr(y, 2, f"Unique collections:       {counts['collections']:>6}")
    y += 2
    
    # Memory
    stdscr.addstr(y, 2, f"Memory: {mem['used']:.1f}GB / {mem['total']:.1f}GB ({mem['percent']:.1f}%)")
    y += 1
    
    # Disk usage summary
    for name, usage in disk.items():
        stdscr.addstr(y, 2, f"{name:>10}: {usage['free']:.1f}GB free ({100-usage['percent']:.1f}%)")
        y += 1
    
    return y + 1

def draw_processes(stdscr, y, processes):
    """Draw active processes table"""
    height, width = stdscr.getmaxyx()
    
    stdscr.addstr(y, 0, "=" * width, curses.A_BOLD)
    y += 1
    stdscr.addstr(y, 0, "ACTIVE PROCESSES", curses.A_BOLD)
    y += 1
    
    if not processes:
        stdscr.addstr(y, 2, "No active pipeline processes")
        return y + 2
    
    # Table header
    header = f"{'PID':<8} {'CPU%':<6} {'MEM%':<6} {'COMMAND'}"
    stdscr.addstr(y, 2, header, curses.A_UNDERLINE)
    y += 1
    
    # Process rows
    for proc in processes[:height - y - 3]:  # Leave space for footer
        line = f"{proc['pid']:<8} {proc['cpu']:<6.1f} {proc['mem']:<6.1f} {proc['cmd']}"
        stdscr.addstr(y, 2, line[:width-3])
        y += 1
    
    return y + 1

def draw_footer(stdscr, monitor):
    """Draw footer with controls"""
    height, width = stdscr.getmaxyx()
    footer = " Press 'q' to quit | Refreshing every 2s "
    try:
        stdscr.addstr(height - 1, 0, footer, curses.A_REVERSE)
    except curses.error:
        pass

def main_loop(stdscr):
    """Main display loop"""
    # Setup
    curses.curs_set(0)  # Hide cursor
    stdscr.nodelay(1)   # Non-blocking input
    stdscr.timeout(100) # 100ms timeout for getch()
    
    # Initialize color pairs
    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
    
    monitor = PipelineMonitor()
    last_update = 0
    
    while True:
        current_time = time.time()
        
        # Check for quit command
        key = stdscr.getch()
        if key in [ord('q'), ord('Q'), 27]:  # q, Q, or ESC
            break
        
        # Update data periodically
        if current_time - last_update >= monitor.update_interval:
            try:
                stdscr.clear()
                
                # Gather data
                counts = monitor.get_file_counts()
                disk = monitor.get_disk_usage()
                mem = monitor.get_memory_usage()
                processes = monitor.get_active_processes()
                
                # Draw interface
                y = draw_header(stdscr, monitor)
                y = draw_summary(stdscr, y, monitor, counts, mem, disk)
                y = draw_processes(stdscr, y, processes)
                draw_footer(stdscr, monitor)
                
                stdscr.refresh()
                last_update = current_time
                
            except curses.error:
                # Handle terminal resize or other curses errors
                pass
        
        time.sleep(0.1)  # Small sleep to prevent CPU spinning

def main():
    """Entry point"""
    try:
        curses.wrapper(main_loop)
    except KeyboardInterrupt:
        pass
    print("\nPipeline monitor stopped.")

if __name__ == "__main__":
    main()
