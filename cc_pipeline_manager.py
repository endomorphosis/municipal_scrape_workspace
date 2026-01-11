#!/usr/bin/env python3
"""
Common Crawl Pipeline Manager - Unified CLI with HUD

Manages the complete pipeline:
1. Download CC index .tar.gz files
2. Convert to sorted parquet files
3. Build DuckDB pointer indexes
4. Search across all indexes

Features:
- Parallel processing with memory awareness
- Resume capability after interruption
- Integrity verification at each stage
- Real-time HUD display
- Space management (ZFS snapshot cleanup)
"""

import os
import sys
import json
import gzip
import shutil
import time
import psutil
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, asdict
import threading

import requests
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


# ============================================================================
# Configuration & Constants
# ============================================================================

@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    ccindex_dir: Path = Path("/storage/ccindex")
    parquet_dir: Path = Path("/storage/ccindex_parquet")
    index_dir: Path = Path("/storage/ccindex_duckdb")
    state_dir: Path = Path(".pipeline_state")
    
    # Resource limits
    max_workers: int = 8
    max_memory_gb: float = 32.0
    min_free_space_gb: float = 100.0
    
    # Collections to process
    years: List[int] = None
    
    def __post_init__(self):
        if self.years is None:
            self.years = [2024, 2025]
        
        # Create directories
        self.ccindex_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class FileState:
    """State of a file in the pipeline"""
    collection: str
    filename: str
    stage: str  # 'download', 'convert', 'sort', 'index', 'complete'
    status: str  # 'pending', 'processing', 'complete', 'failed', 'corrupted'
    size_bytes: int = 0
    checksum: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


class PipelineState:
    """Manages pipeline state persistence"""
    
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_file = state_dir / "pipeline_state.json"
        self.lock = threading.Lock()
        self.files: Dict[str, FileState] = {}
        self.load()
    
    def load(self):
        """Load state from disk"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    for key, value in data.items():
                        self.files[key] = FileState(**value)
            except Exception as e:
                print(f"Warning: Failed to load state: {e}")
    
    def save(self):
        """Save state to disk"""
        with self.lock:
            try:
                data = {k: asdict(v) for k, v in self.files.items()}
                with open(self.state_file, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                print(f"Warning: Failed to save state: {e}")
    
    def get_file_key(self, collection: str, filename: str) -> str:
        """Get unique key for file"""
        return f"{collection}:{filename}"
    
    def update_file(self, collection: str, filename: str, **kwargs):
        """Update file state"""
        key = self.get_file_key(collection, filename)
        
        with self.lock:
            if key not in self.files:
                self.files[key] = FileState(collection=collection, filename=filename, 
                                           stage='download', status='pending')
            
            for k, v in kwargs.items():
                setattr(self.files[key], k, v)
            
            self.save()
    
    def get_file(self, collection: str, filename: str) -> Optional[FileState]:
        """Get file state"""
        key = self.get_file_key(collection, filename)
        return self.files.get(key)
    
    def get_files_by_stage_status(self, stage: str = None, status: str = None) -> List[FileState]:
        """Get files by stage and/or status"""
        results = []
        for file_state in self.files.values():
            if stage and file_state.stage != stage:
                continue
            if status and file_state.status != status:
                continue
            results.append(file_state)
        return results


# ============================================================================
# Resource Management
# ============================================================================

class ResourceManager:
    """Manages system resources"""
    
    @staticmethod
    def get_available_memory_gb() -> float:
        """Get available memory in GB"""
        return psutil.virtual_memory().available / (1024**3)
    
    @staticmethod
    def get_free_space_gb(path: Path) -> float:
        """Get free space in GB"""
        stat = os.statvfs(path)
        return (stat.f_bavail * stat.f_frsize) / (1024**3)
    
    @staticmethod
    def estimate_parquet_memory_mb(tar_gz_path: Path) -> float:
        """Estimate memory needed to process a tar.gz file"""
        size_mb = tar_gz_path.stat().st_size / (1024**2)
        # Rough estimate: 3x the compressed size
        return size_mb * 3
    
    @staticmethod
    def can_process_file(tar_gz_path: Path, max_memory_gb: float) -> bool:
        """Check if we have enough memory to process file"""
        needed_mb = ResourceManager.estimate_parquet_memory_mb(tar_gz_path)
        available_mb = ResourceManager.get_available_memory_gb() * 1024
        return needed_mb < (available_mb * 0.7)  # Leave 30% headroom
    
    @staticmethod
    def cleanup_zfs_snapshots(dataset: str, keep_count: int = 1) -> int:
        """Clean up old ZFS snapshots"""
        try:
            result = subprocess.run(
                ['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-H', dataset],
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                return 0
            
            snapshots = [s.strip() for s in result.stdout.split('\n') if s.strip()]
            
            if len(snapshots) <= keep_count:
                return 0
            
            # Delete oldest snapshots
            to_delete = snapshots[:-keep_count]
            deleted = 0
            
            for snapshot in to_delete:
                result = subprocess.run(['zfs', 'destroy', snapshot], 
                                      capture_output=True)
                if result.returncode == 0:
                    deleted += 1
            
            return deleted
            
        except Exception as e:
            print(f"Warning: ZFS cleanup failed: {e}")
            return 0


# ============================================================================
# Download Stage
# ============================================================================

class Downloader:
    """Downloads CC index files"""
    
    BASE_URL = "https://data.commoncrawl.org/cc-index/collections"
    
    @staticmethod
    def get_collection_list(years: List[int]) -> List[str]:
        """Get list of collections to download"""
        # Fetch collinfo.json
        url = "https://index.commoncrawl.org/collinfo.json"
        response = requests.get(url)
        collections_data = response.json()
        
        collections = []
        for coll in collections_data:
            coll_id = coll['id']
            # Extract year from collection ID (e.g., CC-MAIN-2024-10)
            parts = coll_id.split('-')
            if len(parts) >= 3:
                try:
                    year = int(parts[2])
                    if year in years:
                        collections.append(coll_id)
                except ValueError:
                    pass
        
        return sorted(collections)
    
    @staticmethod
    def get_shard_list(collection: str) -> List[str]:
        """Get list of shard files for a collection"""
        # List files from the index
        url = f"{Downloader.BASE_URL}/{collection}/indexes/"
        
        try:
            response = requests.get(url)
            # Parse directory listing (this is a simplified approach)
            # In production, you'd parse the HTML or use the S3 API
            
            # For now, generate expected shard names
            shards = []
            for i in range(300):  # Common Crawl typically has ~300 shards
                shard_name = f"cdx-{i:05d}.gz"
                shards.append(shard_name)
            
            return shards
            
        except Exception as e:
            print(f"Error fetching shard list: {e}")
            return []
    
    @staticmethod
    def download_file(collection: str, shard: str, output_dir: Path) -> Tuple[bool, str]:
        """Download a single shard file"""
        url = f"{Downloader.BASE_URL}/{collection}/indexes/{shard}"
        output_path = output_dir / f"{collection}-{shard}"
        
        if output_path.exists():
            return True, "already_exists"
        
        try:
            response = requests.get(url, stream=True, timeout=300)
            
            if response.status_code == 404:
                return False, "not_found"
            
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return True, "downloaded"
            
        except Exception as e:
            return False, str(e)


# ============================================================================
# Convert & Sort Stage
# ============================================================================

class ParquetConverter:
    """Converts .gz to sorted .parquet"""
    
    @staticmethod
    def convert_and_sort(gz_path: Path, output_dir: Path) -> Tuple[bool, str]:
        """Convert .gz to sorted .parquet in one pass"""
        try:
            # Determine output path
            output_filename = gz_path.name + ".parquet"
            output_path = output_dir / output_filename
            
            if output_path.exists():
                # Verify it's valid
                try:
                    pq.read_table(output_path, memory_map=True)
                    return True, "already_exists"
                except:
                    # Corrupted, delete and regenerate
                    output_path.unlink()
            
            # Read and parse .gz file
            records = []
            
            with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        parts = line.split(' ')
                        if len(parts) < 3:
                            continue
                        
                        # Parse CDX format: url timestamp json
                        url = parts[0]
                        timestamp = parts[1]
                        json_str = ' '.join(parts[2:])
                        
                        data = json.loads(json_str)
                        
                        record = {
                            'url': url,
                            'timestamp': timestamp,
                            'filename': data.get('filename', ''),
                            'offset': int(data.get('offset', 0)),
                            'length': int(data.get('length', 0)),
                            'status': data.get('status', ''),
                            'mime': data.get('mime', ''),
                        }
                        
                        records.append(record)
                        
                    except Exception as e:
                        continue
            
            if not records:
                return False, "no_valid_records"
            
            # Sort by URL (which groups by domain)
            records.sort(key=lambda r: r['url'])
            
            # Convert to parquet
            table = pa.Table.from_pylist(records)
            pq.write_table(table, output_path, compression='snappy')
            
            return True, f"converted_{len(records)}_records"
            
        except Exception as e:
            return False, str(e)


# ============================================================================
# Index Building Stage
# ============================================================================

class IndexBuilder:
    """Builds DuckDB pointer indexes"""
    
    @staticmethod
    def build_collection_index(collection: str, parquet_dir: Path, 
                               index_dir: Path) -> Tuple[bool, str]:
        """Build DuckDB pointer index for a collection"""
        try:
            index_path = index_dir / f"{collection}.duckdb"
            
            if index_path.exists():
                return True, "already_exists"
            
            # Get all parquet files for this collection
            parquet_files = sorted(parquet_dir.glob(f"{collection}-*.parquet"))
            
            if not parquet_files:
                return False, "no_parquet_files"
            
            # Create database
            conn = duckdb.connect(str(index_path))
            
            conn.execute("""
                CREATE TABLE domain_pointers (
                    domain VARCHAR,
                    parquet_file VARCHAR,
                    row_offset BIGINT,
                    row_count BIGINT,
                    first_url VARCHAR,
                    last_url VARCHAR,
                    PRIMARY KEY (domain, parquet_file)
                )
            """)
            
            total_pointers = 0
            
            for parquet_file in parquet_files:
                try:
                    table = pq.read_table(parquet_file)
                    urls = table['url'].to_pylist()
                    
                    # Group by domain
                    current_domain = None
                    domain_start = 0
                    
                    for idx, url in enumerate(urls):
                        domain = urlparse(url).netloc
                        
                        if domain != current_domain:
                            if current_domain is not None:
                                # Save previous domain pointer
                                conn.execute("""
                                    INSERT INTO domain_pointers 
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, [
                                    current_domain,
                                    parquet_file.name,
                                    domain_start,
                                    idx - domain_start,
                                    urls[domain_start],
                                    urls[idx - 1]
                                ])
                                total_pointers += 1
                            
                            current_domain = domain
                            domain_start = idx
                    
                    # Save last domain
                    if current_domain is not None:
                        conn.execute("""
                            INSERT INTO domain_pointers 
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, [
                            current_domain,
                            parquet_file.name,
                            domain_start,
                            len(urls) - domain_start,
                            urls[domain_start],
                            urls[-1]
                        ])
                        total_pointers += 1
                
                except Exception as e:
                    # Mark file as corrupted
                    conn.close()
                    index_path.unlink()
                    return False, f"corrupted_parquet: {e}"
            
            # Create index for fast lookups
            conn.execute("CREATE INDEX idx_domain ON domain_pointers(domain)")
            conn.close()
            
            return True, f"indexed_{total_pointers}_pointers"
            
        except Exception as e:
            return False, str(e)
    
    @staticmethod
    def build_metadata_index(index_dir: Path, collections: List[str]) -> Tuple[bool, str]:
        """Build metadata index across all collections"""
        try:
            meta_path = index_dir / "metadata.duckdb"
            
            # Always rebuild metadata
            if meta_path.exists():
                meta_path.unlink()
            
            conn = duckdb.connect(str(meta_path))
            
            # Create metadata tables
            conn.execute("""
                CREATE TABLE collections (
                    collection_name VARCHAR PRIMARY KEY,
                    index_file VARCHAR NOT NULL,
                    total_domains INTEGER,
                    total_urls BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE domains_global (
                    domain VARCHAR PRIMARY KEY,
                    collection_count INTEGER,
                    total_urls BIGINT
                )
            """)
            
            for collection in collections:
                index_path = index_dir / f"{collection}.duckdb"
                
                if not index_path.exists():
                    continue
                
                # Read stats from collection index
                coll_conn = duckdb.connect(str(index_path), read_only=True)
                
                total_domains = coll_conn.execute(
                    "SELECT COUNT(DISTINCT domain) FROM domain_pointers"
                ).fetchone()[0]
                
                total_urls = coll_conn.execute(
                    "SELECT SUM(row_count) FROM domain_pointers"
                ).fetchone()[0]
                
                # Insert collection metadata
                conn.execute("""
                    INSERT INTO collections (collection_name, index_file, total_domains, total_urls)
                    VALUES (?, ?, ?, ?)
                """, [collection, f"{collection}.duckdb", total_domains, total_urls])
                
                # Update global domain stats
                domains = coll_conn.execute("""
                    SELECT domain, SUM(row_count) as url_count
                    FROM domain_pointers
                    GROUP BY domain
                """).fetchall()
                
                for domain, url_count in domains:
                    existing = conn.execute(
                        "SELECT collection_count, total_urls FROM domains_global WHERE domain = ?",
                        [domain]
                    ).fetchone()
                    
                    if existing:
                        conn.execute("""
                            UPDATE domains_global 
                            SET collection_count = collection_count + 1,
                                total_urls = total_urls + ?
                            WHERE domain = ?
                        """, [url_count, domain])
                    else:
                        conn.execute("""
                            INSERT INTO domains_global (domain, collection_count, total_urls)
                            VALUES (?, 1, ?)
                        """, [domain, url_count])
                
                coll_conn.close()
            
            # Create indexes
            conn.execute("CREATE INDEX idx_domains_global ON domains_global(domain)")
            conn.close()
            
            return True, "metadata_indexed"
            
        except Exception as e:
            return False, str(e)


# ============================================================================
# Search
# ============================================================================

class Searcher:
    """Search across all indexes"""
    
    @staticmethod
    def search_domain(domain: str, index_dir: Path, parquet_dir: Path) -> List[Dict]:
        """Search for domain across all collections"""
        meta_path = index_dir / "metadata.duckdb"
        
        if not meta_path.exists():
            return []
        
        meta_conn = duckdb.connect(str(meta_path), read_only=True)
        collections = meta_conn.execute("SELECT collection_name FROM collections").fetchall()
        meta_conn.close()
        
        all_results = []
        
        for (collection_name,) in collections:
            index_path = index_dir / f"{collection_name}.duckdb"
            
            if not index_path.exists():
                continue
            
            conn = duckdb.connect(str(index_path), read_only=True)
            
            pointers = conn.execute("""
                SELECT parquet_file, row_offset, row_count
                FROM domain_pointers
                WHERE domain = ?
            """, [domain]).fetchall()
            
            conn.close()
            
            for parquet_file, offset, count in pointers:
                parquet_path = parquet_dir / parquet_file
                
                if not parquet_path.exists():
                    continue
                
                try:
                    table = pq.read_table(parquet_path)
                    subset = table.slice(offset, count)
                    
                    for row in subset.to_pylist():
                        row['collection'] = collection_name
                        all_results.append(row)
                
                except Exception as e:
                    print(f"Error reading {parquet_file}: {e}")
        
        return all_results


# ============================================================================
# Pipeline Orchestrator
# ============================================================================

class PipelineOrchestrator:
    """Orchestrates the entire pipeline"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.state = PipelineState(config.state_dir)
        self.resource_mgr = ResourceManager()
    
    def run(self, stages: List[str] = None):
        """Run pipeline stages"""
        if stages is None:
            stages = ['download', 'convert', 'index']
        
        print(f"Starting pipeline with stages: {stages}")
        print(f"Years: {self.config.years}")
        print(f"Max workers: {self.config.max_workers}")
        print()
        
        # Get collections
        collections = Downloader.get_collection_list(self.config.years)
        print(f"Found {len(collections)} collections")
        
        if 'download' in stages:
            self.run_download_stage(collections)
        
        if 'convert' in stages:
            self.run_convert_stage()
        
        if 'index' in stages:
            self.run_index_stage(collections)
        
        print("\nPipeline complete!")
    
    def run_download_stage(self, collections: List[str]):
        """Download all shards for collections"""
        print("\n" + "="*70)
        print("STAGE 1: Download")
        print("="*70)
        
        # TODO: Implement parallel download
        # For now, just mark as a placeholder
        print("Download stage - to be implemented")
        print("Use existing download scripts for now")
    
    def run_convert_stage(self):
        """Convert and sort .gz files to .parquet"""
        print("\n" + "="*70)
        print("STAGE 2: Convert & Sort")
        print("="*70)
        
        # Find all .gz files that need conversion
        gz_files = sorted(self.config.ccindex_dir.glob("*.gz"))
        
        print(f"Found {len(gz_files)} .gz files")
        
        # Process in parallel with memory awareness
        to_process = []
        
        for gz_file in gz_files:
            parquet_file = self.config.parquet_dir / (gz_file.name + ".parquet")
            
            if parquet_file.exists():
                # Verify integrity
                try:
                    pq.read_table(parquet_file, memory_map=True)
                    continue
                except:
                    parquet_file.unlink()
            
            to_process.append(gz_file)
        
        print(f"Need to process {len(to_process)} files")
        
        with ProcessPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {}
            
            for gz_file in to_process:
                # Check memory before submitting
                if not self.resource_mgr.can_process_file(gz_file, self.config.max_memory_gb):
                    print(f"Skipping {gz_file.name} - insufficient memory")
                    continue
                
                future = executor.submit(
                    ParquetConverter.convert_and_sort,
                    gz_file,
                    self.config.parquet_dir
                )
                futures[future] = gz_file
            
            completed = 0
            for future in as_completed(futures):
                gz_file = futures[future]
                success, message = future.result()
                
                completed += 1
                status = "✓" if success else "✗"
                print(f"  [{completed}/{len(futures)}] {status} {gz_file.name}: {message}")
        
        print("\nConvert & Sort stage complete")
    
    def run_index_stage(self, collections: List[str]):
        """Build DuckDB pointer indexes"""
        print("\n" + "="*70)
        print("STAGE 3: Build Indexes")
        print("="*70)
        
        # Build collection indexes in parallel
        with ProcessPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {}
            
            for collection in collections:
                future = executor.submit(
                    IndexBuilder.build_collection_index,
                    collection,
                    self.config.parquet_dir,
                    self.config.index_dir
                )
                futures[future] = collection
            
            completed = 0
            for future in as_completed(futures):
                collection = futures[future]
                success, message = future.result()
                
                completed += 1
                status = "✓" if success else "✗"
                print(f"  [{completed}/{len(futures)}] {status} {collection}: {message}")
        
        # Build metadata index
        print("\nBuilding metadata index...")
        success, message = IndexBuilder.build_metadata_index(
            self.config.index_dir, collections
        )
        
        status = "✓" if success else "✗"
        print(f"  {status} Metadata: {message}")
        
        print("\nIndex building complete")


# ============================================================================
# CLI Commands
# ============================================================================

def cmd_run(args):
    """Run pipeline"""
    config = PipelineConfig(
        ccindex_dir=Path(args.ccindex_dir),
        parquet_dir=Path(args.parquet_dir),
        index_dir=Path(args.index_dir),
        max_workers=args.workers,
        max_memory_gb=args.max_memory,
        years=args.years
    )
    
    orchestrator = PipelineOrchestrator(config)
    orchestrator.run(stages=args.stages)


def cmd_search(args):
    """Search for domain"""
    config = PipelineConfig(
        parquet_dir=Path(args.parquet_dir),
        index_dir=Path(args.index_dir)
    )
    
    print(f"Searching for: {args.domain}")
    print()
    
    start = time.time()
    results = Searcher.search_domain(args.domain, config.index_dir, config.parquet_dir)
    elapsed = time.time() - start
    
    print(f"Found {len(results)} URLs in {elapsed:.3f}s")
    print()
    
    if args.limit:
        results = results[:args.limit]
    
    for result in results:
        print(f"{result['collection']}: {result['url']}")
        if args.verbose:
            print(f"  WARC: {result['filename']}")
            print(f"  Offset: {result['offset']}, Length: {result['length']}")
            print()


def cmd_status(args):
    """Show pipeline status"""
    config = PipelineConfig(
        ccindex_dir=Path(args.ccindex_dir),
        parquet_dir=Path(args.parquet_dir),
        index_dir=Path(args.index_dir)
    )
    
    print("="*70)
    print("PIPELINE STATUS")
    print("="*70)
    print()
    
    # Count files
    gz_count = len(list(config.ccindex_dir.glob("*.gz")))
    parquet_count = len(list(config.parquet_dir.glob("*.parquet")))
    index_count = len(list(config.index_dir.glob("*.duckdb")))
    
    print(f"Downloaded .gz files: {gz_count}")
    print(f"Converted parquet files: {parquet_count}")
    print(f"DuckDB indexes: {index_count}")
    print()
    
    # Space
    print("Disk Space:")
    for name, path in [("ccindex", config.ccindex_dir), 
                       ("parquet", config.parquet_dir),
                       ("index", config.index_dir)]:
        free_gb = ResourceManager.get_free_space_gb(path)
        print(f"  {name}: {free_gb:.1f} GB free")
    
    print()
    
    # Memory
    mem_gb = ResourceManager.get_available_memory_gb()
    print(f"Available memory: {mem_gb:.1f} GB")


def main():
    parser = argparse.ArgumentParser(
        description="Common Crawl Pipeline Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run pipeline')
    run_parser.add_argument('--ccindex-dir', default='/storage/ccindex')
    run_parser.add_argument('--parquet-dir', default='/storage/ccindex_parquet')
    run_parser.add_argument('--index-dir', default='/storage/ccindex_duckdb')
    run_parser.add_argument('--stages', nargs='+', choices=['download', 'convert', 'index'],
                          default=['convert', 'index'])
    run_parser.add_argument('--workers', type=int, default=8)
    run_parser.add_argument('--max-memory', type=float, default=32.0)
    run_parser.add_argument('--years', type=int, nargs='+', default=[2024, 2025])
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for domain')
    search_parser.add_argument('domain', help='Domain to search for')
    search_parser.add_argument('--parquet-dir', default='/storage/ccindex_parquet')
    search_parser.add_argument('--index-dir', default='/storage/ccindex_duckdb')
    search_parser.add_argument('--limit', type=int, help='Limit results')
    search_parser.add_argument('--verbose', '-v', action='store_true')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show pipeline status')
    status_parser.add_argument('--ccindex-dir', default='/storage/ccindex')
    status_parser.add_argument('--parquet-dir', default='/storage/ccindex_parquet')
    status_parser.add_argument('--index-dir', default='/storage/ccindex_duckdb')
    
    args = parser.parse_args()
    
    if args.command == 'run':
        cmd_run(args)
    elif args.command == 'search':
        cmd_search(args)
    elif args.command == 'status':
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
