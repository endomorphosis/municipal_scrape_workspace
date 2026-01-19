#!/usr/bin/env python3
"""
Common Crawl Pipeline HUD - Interactive CLI with Live Updates

A unified pipeline manager with real-time monitoring:
- Download CC indexes
- Convert to sorted parquet
- Build DuckDB pointer indexes  
- Search across all indexes
- Live progress tracking
- Memory-aware parallel processing
- Resume from interruption
"""

import time
import json
import curses
import psutil
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict
import threading

# ============================================================================
# Pipeline State Management
# ============================================================================

class PipelineState:
    """Tracks the state of all pipeline operations"""
    
    def __init__(self, config_file="pipeline_state.json"):
        self.config_file = Path(config_file)
        self.state = self.load_state()
        self.lock = threading.Lock()
        
    def load_state(self) -> Dict:
        """Load pipeline state from disk"""
        if self.config_file.exists():
            with open(self.config_file) as f:
                return json.load(f)
        return {
            "collections": {},
            "last_updated": None,
            "active_workers": {},
            "errors": []
        }
    
    def save_state(self):
        """Save pipeline state to disk"""
        with self.lock:
            self.state["last_updated"] = datetime.now().isoformat()
            with open(self.config_file, 'w') as f:
                json.dump(self.state, f, indent=2)
    
    def update_collection(self, collection: str, status: Dict):
        """Update status for a collection"""
        with self.lock:
            if collection not in self.state["collections"]:
                self.state["collections"][collection] = {}
            self.state["collections"][collection].update(status)
            self.save_state()


# ============================================================================
# File System Scanner
# ============================================================================

class FileSystemScanner:
    """Scans directories to track pipeline progress"""
    
    def __init__(self, ccindex_dir="/storage/ccindex",
                 parquet_dir="/storage/ccindex_parquet",
                 duckdb_dir="/storage/ccindex_duckdb"):
        self.ccindex_dir = Path(ccindex_dir)
        self.parquet_dir = Path(parquet_dir)
        self.duckdb_dir = Path(duckdb_dir)
        self.cache = {}
        self.last_scan = 0
        
    def scan_all(self, force=False) -> Dict:
        """Scan all directories and return status"""
        now = time.time()
        if not force and (now - self.last_scan) < 5:
            return self.cache
            
        result = {
            "collections": self._scan_collections(),
            "disk_space": self._get_disk_space(),
            "memory": self._get_memory_info(),
            "timestamp": datetime.now().isoformat()
        }
        
        self.cache = result
        self.last_scan = now
        return result
    
    def _scan_collections(self) -> Dict[str, Dict]:
        """Scan for collections and their status"""
        collections = defaultdict(lambda: {
            "gz_files": [],
            "parquet_files": [],
            "duckdb_files": [],
            "gz_count": 0,
            "parquet_count": 0,
            "has_index": False,
            "total_size_mb": 0
        })
        
        # Scan ccindex for .gz files by collection
        if self.ccindex_dir.exists():
            for coll_dir in self.ccindex_dir.iterdir():
                if coll_dir.is_dir():
                    coll_name = coll_dir.name
                    gz_files = list(coll_dir.glob("*.gz"))
                    collections[coll_name]["gz_files"] = [f.name for f in gz_files]
                    collections[coll_name]["gz_count"] = len(gz_files)
        
        # Scan parquet directory
        if self.parquet_dir.exists():
            for parquet_file in self.parquet_dir.glob("*.gz.parquet"):
                # Extract collection from filename (e.g., CC-MAIN-2024-10-cdx-00000.gz.parquet)
                parquet_path = Path(parquet_file) if isinstance(parquet_file, str) else parquet_file
                name = parquet_path.stem.replace('.gz', '')  # Remove .gz from stem
                parts = name.split('-')
                if len(parts) >= 4:
                    coll_name = '-'.join(parts[:4])  # CC-MAIN-2024-10
                    collections[coll_name]["parquet_files"].append(parquet_path.name)
                    collections[coll_name]["parquet_count"] += 1
                    collections[coll_name]["total_size_mb"] += parquet_path.stat().st_size / 1024 / 1024
        
        # Scan duckdb directory
        if self.duckdb_dir.exists():
            for db_file in self.duckdb_dir.glob("**/*.duckdb"):
                coll_name = db_file.parent.name
                collections[coll_name]["duckdb_files"].append(db_file.name)
                collections[coll_name]["has_index"] = True
        
        return dict(collections)
    
    def _get_disk_space(self) -> Dict:
        """Get disk space for each mount point"""
        result = {}
        for path_name, path in [("ccindex", self.ccindex_dir),
                                 ("parquet", self.parquet_dir),
                                 ("duckdb", self.duckdb_dir)]:
            if path.exists():
                usage = psutil.disk_usage(str(path))
                result[path_name] = {
                    "free_gb": usage.free / 1024**3,
                    "used_gb": usage.used / 1024**3,
                    "total_gb": usage.total / 1024**3,
                    "percent": usage.percent
                }
        return result
    
    def _get_memory_info(self) -> Dict:
        """Get current memory usage"""
        mem = psutil.virtual_memory()
        return {
            "available_gb": mem.available / 1024**3,
            "used_gb": mem.used / 1024**3,
            "total_gb": mem.total / 1024**3,
            "percent": mem.percent
        }


# ============================================================================
# Interactive HUD Display
# ============================================================================

class PipelineHUD:
    """Interactive curses-based HUD for pipeline monitoring"""
    
    def __init__(self, scanner: FileSystemScanner, state: PipelineState):
        self.scanner = scanner
        self.state = state
        self.running = True
        self.selected_menu = 0
        self.scroll_offset = 0
        self.menu_items = [
            "Overview",
            "Collections Status",
            "Active Workers",
            "Recent Errors",
            "Actions Menu"
        ]
        
    def run(self, stdscr):
        """Main HUD loop"""
        curses.curs_set(0)  # Hide cursor
        stdscr.nodelay(1)   # Non-blocking input
        stdscr.timeout(100)  # Refresh every 100ms
        
        # Color pairs
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(4, curses.COLOR_CYAN, curses.COLOR_BLACK)
        curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)
        
        while self.running:
            try:
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Draw header
                self._draw_header(stdscr, width)
                
                # Draw menu bar
                self._draw_menu_bar(stdscr, width, 2)
                
                # Draw content based on selected menu
                content_start = 4
                content_height = height - content_start - 2
                
                if self.selected_menu == 0:
                    self._draw_overview(stdscr, content_start, content_height, width)
                elif self.selected_menu == 1:
                    self._draw_collections(stdscr, content_start, content_height, width)
                elif self.selected_menu == 2:
                    self._draw_workers(stdscr, content_start, content_height, width)
                elif self.selected_menu == 3:
                    self._draw_errors(stdscr, content_start, content_height, width)
                elif self.selected_menu == 4:
                    self._draw_actions(stdscr, content_start, content_height, width)
                
                # Draw footer
                self._draw_footer(stdscr, height - 1, width)
                
                stdscr.refresh()
                
                # Handle input
                key = stdscr.getch()
                if key == ord('q') or key == ord('Q'):
                    self.running = False
                elif key == curses.KEY_LEFT or key == ord('h'):
                    self.selected_menu = max(0, self.selected_menu - 1)
                    self.scroll_offset = 0
                elif key == curses.KEY_RIGHT or key == ord('l'):
                    self.selected_menu = min(len(self.menu_items) - 1, self.selected_menu + 1)
                    self.scroll_offset = 0
                elif key == curses.KEY_UP or key == ord('k'):
                    self.scroll_offset = max(0, self.scroll_offset - 1)
                elif key == curses.KEY_DOWN or key == ord('j'):
                    self.scroll_offset += 1
                    
            except curses.error:
                pass
    
    def _draw_header(self, stdscr, width):
        """Draw header bar"""
        title = " Common Crawl Pipeline Manager "
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        stdscr.attron(curses.color_pair(5) | curses.A_BOLD)
        stdscr.addstr(0, 0, " " * width)
        stdscr.addstr(0, (width - len(title)) // 2, title)
        stdscr.addstr(0, width - len(timestamp) - 2, timestamp)
        stdscr.attroff(curses.color_pair(5) | curses.A_BOLD)
    
    def _draw_menu_bar(self, stdscr, width, y):
        """Draw menu bar"""
        x = 2
        for idx, item in enumerate(self.menu_items):
            if idx == self.selected_menu:
                stdscr.attron(curses.color_pair(5) | curses.A_BOLD)
                stdscr.addstr(y, x, f" {item} ")
                stdscr.attroff(curses.color_pair(5) | curses.A_BOLD)
            else:
                stdscr.addstr(y, x, f" {item} ", curses.color_pair(4))
            x += len(item) + 4
    
    def _draw_overview(self, stdscr, y, height, width):
        """Draw overview screen"""
        data = self.scanner.scan_all()
        
        # Summary statistics
        total_gz = sum(c["gz_count"] for c in data["collections"].values())
        total_parquet = sum(c["parquet_count"] for c in data["collections"].values())
        total_indexed = sum(1 for c in data["collections"].values() if c["has_index"])
        total_size_gb = sum(c["total_size_mb"] for c in data["collections"].values()) / 1024
        
        lines = [
            "",
            "═" * (width - 4),
            f"  PIPELINE SUMMARY",
            "═" * (width - 4),
            "",
            f"  Collections Found:      {len(data['collections'])}",
            f"  Total .gz Files:        {total_gz:,}",
            f"  Total Parquet Files:    {total_parquet:,}",
            f"  Indexed Collections:    {total_indexed}",
            f"  Total Parquet Size:     {total_size_gb:.1f} GB",
            "",
            "─" * (width - 4),
            f"  DISK SPACE",
            "─" * (width - 4),
            ""
        ]
        
        for name, info in data["disk_space"].items():
            lines.append(f"  {name:12s}: {info['free_gb']:8.1f} GB free / "
                        f"{info['total_gb']:8.1f} GB total ({info['percent']:.1f}% used)")
        
        lines.extend([
            "",
            "─" * (width - 4),
            f"  MEMORY",
            "─" * (width - 4),
            "",
            f"  Available: {data['memory']['available_gb']:.1f} GB / "
            f"{data['memory']['total_gb']:.1f} GB ({data['memory']['percent']:.1f}% used)",
            ""
        ])
        
        self._draw_lines(stdscr, y, height, width, lines)
    
    def _draw_collections(self, stdscr, y, height, width):
        """Draw collections status"""
        data = self.scanner.scan_all()
        collections = sorted(data["collections"].items())
        
        lines = [
            "",
            "═" * (width - 4),
            f"  COLLECTIONS STATUS ({len(collections)} total)",
            "═" * (width - 4),
            "",
            f"  {'Collection':<25} {'GZ':>6} {'Parquet':>8} {'Index':>7} {'Size (MB)':>12}",
            "─" * (width - 4)
        ]
        
        for coll_name, info in collections:
            idx_status = "✓" if info["has_index"] else "✗"
            color = curses.color_pair(1) if info["has_index"] else curses.color_pair(2)
            
            line = f"  {coll_name:<25} {info['gz_count']:>6} {info['parquet_count']:>8} " \
                   f"{idx_status:>7} {info['total_size_mb']:>12.1f}"
            lines.append((line, color))
        
        self._draw_lines(stdscr, y, height, width, lines)
    
    def _draw_workers(self, stdscr, y, height, width):
        """Draw active workers"""
        workers = self.state.state.get("active_workers", {})
        
        lines = [
            "",
            "═" * (width - 4),
            f"  ACTIVE WORKERS ({len(workers)} running)",
            "═" * (width - 4),
            ""
        ]
        
        if not workers:
            lines.append("  No active workers")
        else:
            for worker_id, info in workers.items():
                lines.append(f"  Worker {worker_id}:")
                lines.append(f"    Task: {info.get('task', 'unknown')}")
                lines.append(f"    File: {info.get('file', 'unknown')}")
                lines.append(f"    Started: {info.get('started', 'unknown')}")
                lines.append("")
        
        self._draw_lines(stdscr, y, height, width, lines)
    
    def _draw_errors(self, stdscr, y, height, width):
        """Draw recent errors"""
        errors = self.state.state.get("errors", [])
        
        lines = [
            "",
            "═" * (width - 4),
            f"  RECENT ERRORS ({len(errors)} total)",
            "═" * (width - 4),
            ""
        ]
        
        if not errors:
            lines.append("  No errors recorded")
        else:
            for error in errors[-20:]:  # Show last 20
                lines.append((f"  [{error.get('timestamp', 'unknown')}] {error.get('message', 'unknown')}", 
                            curses.color_pair(3)))
                lines.append("")
        
        self._draw_lines(stdscr, y, height, width, lines)
    
    def _draw_actions(self, stdscr, y, height, width):
        """Draw actions menu"""
        lines = [
            "",
            "═" * (width - 4),
            "  ACTIONS",
            "═" * (width - 4),
            "",
            "  [1] Start downloading missing indexes",
            "  [2] Convert .gz to sorted parquet",
            "  [3] Build DuckDB pointer indexes",
            "  [4] Verify file integrity",
            "  [5] Clean up ZFS snapshots",
            "  [6] Search indexes",
            "",
            "  [r] Refresh all data",
            "  [q] Quit",
            ""
        ]
        
        self._draw_lines(stdscr, y, height, width, lines)
    
    def _draw_footer(self, stdscr, y, width):
        """Draw footer bar"""
        footer = " [←→/hl] Navigate | [↑↓/jk] Scroll | [q] Quit "
        stdscr.attron(curses.color_pair(4))
        stdscr.addstr(y, 0, " " * width)
        stdscr.addstr(y, (width - len(footer)) // 2, footer)
        stdscr.attroff(curses.color_pair(4))
    
    def _draw_lines(self, stdscr, start_y, height, width, lines):
        """Draw lines with scrolling and color support"""
        y = start_y
        for idx, line in enumerate(lines[self.scroll_offset:]):
            if y >= start_y + height:
                break
            
            # Handle colored lines
            if isinstance(line, tuple):
                text, color = line
            else:
                text = line
                color = 0
            
            try:
                if color:
                    stdscr.addstr(y, 2, text[:width-4], color)
                else:
                    stdscr.addstr(y, 2, text[:width-4])
            except curses.error:
                pass
            
            y += 1


# ============================================================================
# Main Entry Point
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Common Crawl Pipeline HUD")
    parser.add_argument("--ccindex-dir", default="/storage/ccindex",
                       help="Directory containing .gz files")
    parser.add_argument("--parquet-dir", default="/storage/ccindex_parquet",
                       help="Directory containing parquet files")
    parser.add_argument("--duckdb-dir", default="/storage/ccindex_duckdb",
                       help="Directory containing DuckDB indexes")
    parser.add_argument("--state-file", default="pipeline_state.json",
                       help="Pipeline state file")
    
    args = parser.parse_args()
    
    # Initialize scanner and state
    scanner = FileSystemScanner(
        ccindex_dir=args.ccindex_dir,
        parquet_dir=args.parquet_dir,
        duckdb_dir=args.duckdb_dir
    )
    
    state = PipelineState(config_file=args.state_file)
    
    # Run HUD
    hud = PipelineHUD(scanner, state)
    
    try:
        curses.wrapper(hud.run)
        return 0
    except KeyboardInterrupt:
        print("\nExiting...")
        return 0
    

if __name__ == "__main__":
    raise SystemExit(main())
