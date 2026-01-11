#!/usr/bin/env python3
"""
Validate collection completeness across all pipeline stages.

Checks each Common Crawl collection to verify:
1. tar.gz files are downloaded OR
2. parquet files are converted AND sorted AND
3. DuckDB pointer index exists for the collection AND is sorted
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple
import pyarrow.parquet as pq
import duckdb
from collections import defaultdict
import argparse

class CollectionValidator:
    def __init__(self, ccindex_dir: Path, parquet_dir: Path, pointer_dir: Path):
        self.ccindex_dir = ccindex_dir
        self.parquet_dir = parquet_dir
        self.pointer_dir = pointer_dir
        
    def get_all_collections(self) -> Set[str]:
        """Get all known Common Crawl collections from collinfo.json"""
        collinfo_path = Path("collinfo.json")
        if not collinfo_path.exists():
            print("WARNING: collinfo.json not found, scanning directories instead")
            return self._scan_collections_from_files()
        
        with open(collinfo_path) as f:
            data = json.load(f)
            return {coll['id'] for coll in data}
    
    def _scan_collections_from_files(self) -> Set[str]:
        """Fallback: scan collections from existing files"""
        collections = set()
        
        # Scan tar.gz directories (collections are stored as directories with cdx-*.gz files)
        if self.ccindex_dir.exists():
            for item in self.ccindex_dir.iterdir():
                if item.is_dir() and item.name.startswith("CC-MAIN-"):
                    collections.add(item.name)
        
        # Scan parquet files (numbered files like CC-MAIN-2024-22-00000.gz.parquet)
        if self.parquet_dir.exists():
            for pq_file in self.parquet_dir.glob("CC-MAIN-*-*.gz.parquet*"):
                name = pq_file.name
                # Extract collection name: CC-MAIN-2024-22-00000.gz.parquet -> CC-MAIN-2024-22
                parts = name.split("-")
                if len(parts) >= 3:
                    # CC-MAIN-YYYY-WW or CC-MAIN-YYYY-MM-DD
                    coll_name = "-".join(parts[:3])
                    if coll_name.startswith("CC-MAIN-"):
                        collections.add(coll_name)
        
        # Also scan DuckDB indexes
        if self.duckdb_dir.exists():
            for db_dir in [self.duckdb_dir / "cc_pointers_by_collection",
                          self.duckdb_dir / "ccindex_duckdb"]:
                if db_dir.exists():
                    for db_file in db_dir.glob("CC-MAIN-*.duckdb"):
                        coll_name = db_file.stem  # Remove .duckdb
                        if coll_name.startswith("CC-MAIN-"):
                            collections.add(coll_name)
        
        return collections
    
    def check_tar_gz_downloaded(self, collection: str) -> Tuple[int, int]:
        """Check if tar.gz files exist for collection (as directory with cdx-*.gz files)
        Returns: (files_found, expected_count)
        """
        coll_dir = self.ccindex_dir / collection
        if coll_dir.exists() and coll_dir.is_dir():
            # Check if there are any .gz files in the collection directory
            gz_files = list(coll_dir.glob("cdx-*.gz"))
            return len(gz_files), 300  # Typical collection has ~300 files
        return 0, 300
    
    def check_parquet_converted(self, collection: str) -> Tuple[int, int, Path]:
        """Check if parquet files exist for collection (numbered files)
        Returns: (files_found, expected_count, path)
        """
        parquet_files = []
        
        # Look in multiple locations:
        # 1. Flat directory: CC-MAIN-2024-22-cdx-00000.gz.parquet
        parquet_files.extend(self.parquet_dir.glob(f"{collection}-cdx-*.gz.parquet"))
        parquet_files.extend(self.parquet_dir.glob(f"{collection}-cdx-*.gz.sorted.parquet"))
        
        # 2. Organized subdirectories: cc_pointers_by_collection/2024/CC-MAIN-2024-51/cdx-*.gz.parquet
        year_match = collection.split('-')[2]  # Extract year from CC-MAIN-YYYY-WW
        collection_subdir = self.parquet_dir / "cc_pointers_by_collection" / year_match / collection
        if collection_subdir.exists():
            parquet_files.extend(collection_subdir.glob("cdx-*.gz.parquet"))
            parquet_files.extend(collection_subdir.glob("cdx-*.gz.sorted.parquet"))
        
        # Each file is either .gz.parquet or .gz.sorted.parquet (not both)
        # So just count all parquet files
        total_count = len(parquet_files)
        if total_count > 0:
            return total_count, 300, self.parquet_dir
        return 0, 300, None
    
    def check_parquet_sorted(self, parquet_path: Path) -> bool:
        """Check if parquet file is sorted by url_surtkey"""
        if not parquet_path or not parquet_path.exists():
            return False
        
        # Check filename first
        if ".sorted" in parquet_path.name:
            return True
        
        # Verify by reading content
        try:
            table = pq.read_table(parquet_path, columns=['url_surtkey'])
            if len(table) < 2:
                return True  # Empty or single row is sorted
            
            # Check if sorted
            prev_key = None
            for batch in table.to_batches(max_chunksize=10000):
                keys = batch.column('url_surtkey').to_pylist()
                for key in keys:
                    if prev_key is not None and key < prev_key:
                        return False
                    prev_key = key
            return True
        except Exception as e:
            print(f"  ERROR checking sort status of {parquet_path}: {e}")
            return False
    
    def check_collection_parquet_sorted(self, collection: str) -> Tuple[int, int]:
        """Check if all parquet files for a collection are sorted
        Returns: (sorted_count, total_count)
        """
        # Check in multiple locations
        sorted_files = list(self.parquet_dir.glob(f"{collection}-cdx-*.gz.sorted.parquet"))
        unsorted_files = list(self.parquet_dir.glob(f"{collection}-cdx-*.gz.parquet"))
        
        # Also check organized subdirectories
        year_match = collection.split('-')[2] if len(collection.split('-')) > 2 else None
        if year_match:
            collection_subdir = self.parquet_dir / "cc_pointers_by_collection" / year_match / collection
            if collection_subdir.exists():
                sorted_files.extend(collection_subdir.glob("cdx-*.gz.sorted.parquet"))
                unsorted_files.extend(collection_subdir.glob("cdx-*.gz.parquet"))
        
        # Remove sorted files from unsorted list
        unsorted_files = [f for f in unsorted_files if '.sorted.' not in f.name]
        
        total = len(sorted_files) + len(unsorted_files)
        return len(sorted_files), total
    
    def check_duckdb_index_exists(self, collection: str) -> Tuple[bool, List[Path]]:
        """Check if DuckDB pointer index exists for collection"""
        db_files = []
        
        # Check various pointer index locations
        patterns = [
            f"cc_pointers_by_collection/{collection}.duckdb",
            f"cc_domain_by_collection/{collection}.duckdb",
            f"ccindex_duckdb/{collection}.duckdb",
        ]
        
        for pattern in patterns:
            path = self.pointer_dir / pattern
            if path.exists():
                db_files.append(path)
        
        return len(db_files) > 0, db_files
    
    def check_duckdb_index_sorted(self, db_path: Path) -> bool:
        """Check if DuckDB pointer index is sorted by domain"""
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            
            # Get table name
            tables = conn.execute("SHOW TABLES").fetchall()
            if not tables:
                conn.close()
                return False
            
            table_name = tables[0][0]
            
            # Check if domain column exists and is sorted using a subquery
            result = conn.execute(f"""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN is_sorted THEN 1 ELSE 0 END) as sorted_count
                FROM (
                    SELECT domain >= LAG(domain, 1, domain) OVER (ORDER BY rowid) as is_sorted
                    FROM {table_name}
                )
            """).fetchone()
            
            conn.close()
            
            if result and result[0] > 0:
                total, sorted_count = result
                # All rows should be sorted
                return sorted_count == total
            
            return True  # Empty table is sorted
            
        except Exception as e:
            # Skip locked files - they're likely being used
            if "lock" in str(e).lower() or "conflicting" in str(e).lower():
                return None  # Unknown status
            print(f"  ERROR checking DuckDB sort status of {db_path}: {e}")
            return False
    
    def validate_collection(self, collection: str) -> Dict:
        """Validate a single collection through all stages"""
        status = {
            'collection': collection,
            'tar_gz_count': 0,
            'tar_gz_expected': 300,
            'parquet_count': 0,
            'parquet_expected': 300,
            'sorted_count': 0,
            'sorted_expected': 0,
            'duckdb_index_exists': False,
            'duckdb_index_sorted': False,
            'complete': False,
            'parquet_path': None,
            'duckdb_paths': []
        }
        
        # Stage 1: Check tar.gz download
        gz_count, gz_expected = self.check_tar_gz_downloaded(collection)
        status['tar_gz_count'] = gz_count
        status['tar_gz_expected'] = gz_expected
        
        # Stage 2: Check parquet conversion
        pq_count, pq_expected, parquet_path = self.check_parquet_converted(collection)
        status['parquet_count'] = pq_count
        status['parquet_expected'] = pq_expected
        # Show full collection-specific path
        if parquet_path:
            status['parquet_path'] = str(parquet_path / f"{collection}-*.gz.parquet*")
        else:
            status['parquet_path'] = None
        
        # Stage 3: Check parquet sorted (only if converted)
        if pq_count > 0:
            sorted_count, total_count = self.check_collection_parquet_sorted(collection)
            status['sorted_count'] = sorted_count
            status['sorted_expected'] = total_count
        
        # Stage 4: Check DuckDB index exists
        has_index, db_paths = self.check_duckdb_index_exists(collection)
        status['duckdb_index_exists'] = has_index
        status['duckdb_paths'] = [str(p) for p in db_paths]
        
        # Stage 5: Check DuckDB index sorted (only if exists)
        if has_index and db_paths:
            # Check first database
            status['duckdb_index_sorted'] = self.check_duckdb_index_sorted(db_paths[0])
        
        # Overall completeness: ALL stages must be complete
        status['complete'] = (
            status['parquet_count'] >= status['parquet_expected'] and 
            status['sorted_count'] >= status['sorted_expected'] and
            status['sorted_expected'] > 0 and
            status['duckdb_index_exists'] and 
            status['duckdb_index_sorted'] is True  # Must be explicitly True, not None
        )
        
        return status
    
    def validate_all(self, verbose=False) -> Dict:
        """Validate all collections"""
        collections = self.get_all_collections()
        results = {
            'total_collections': len(collections),
            'complete': 0,
            'incomplete': 0,
            'collections': []
        }
        
        for collection in sorted(collections):
            status = self.validate_collection(collection)
            results['collections'].append(status)
            
            if status['complete']:
                results['complete'] += 1
            else:
                results['incomplete'] += 1
            
            # Always print collection status
            self._print_collection_status(status)
        
        return results
    
    def _print_collection_status(self, status: Dict):
        """Print status of a single collection"""
        print(f"\n{status['collection']}:")
        
        # tar.gz status with progress
        gz_pct = (status['tar_gz_count'] / status['tar_gz_expected'] * 100) if status['tar_gz_expected'] > 0 else 0
        print(f"  📦 tar.gz:      {status['tar_gz_count']:>3}/{status['tar_gz_expected']:<3} ({gz_pct:>5.1f}%)")
        
        # parquet status with progress
        pq_pct = (status['parquet_count'] / status['parquet_expected'] * 100) if status['parquet_expected'] > 0 else 0
        print(f"  📄 parquet:     {status['parquet_count']:>3}/{status['parquet_expected']:<3} ({pq_pct:>5.1f}%)")
        
        # sorted status with progress
        if status['sorted_expected'] > 0:
            sort_pct = (status['sorted_count'] / status['sorted_expected'] * 100)
            print(f"  ✅ sorted:      {status['sorted_count']:>3}/{status['sorted_expected']:<3} ({sort_pct:>5.1f}%)")
        else:
            print(f"  ✅ sorted:      N/A")
        
        # DuckDB index status
        idx_status = "✓" if status['duckdb_index_exists'] else "✗"
        print(f"  🗄️  index:       {idx_status} exists", end="")
        if status['duckdb_index_exists']:
            sort_status = "sorted" if status['duckdb_index_sorted'] else "unsorted"
            print(f", {sort_status}")
        else:
            print()
        
        # Overall completion
        complete_icon = "✅" if status['complete'] else "⏳"
        print(f"  {complete_icon} COMPLETE:    {status['complete']}")
        
        # Always show parquet path
        if status['parquet_path']:
            print(f"     📁 Parquet: {status['parquet_path']}")
        else:
            print(f"     📁 Parquet: /storage/ccindex_parquet")
        
        # Always show DuckDB path (exists or expected location)
        if status['duckdb_paths']:
            for db_path in status['duckdb_paths']:
                print(f"     📁 DuckDB: {db_path}")
        else:
            # Show expected location even if not created yet
            expected_path = Path("/storage/ccindex_duckdb/cc_pointers_by_collection") / f"{status['collection']}.duckdb"
            print(f"     📁 DuckDB: {expected_path} (expected)")

def main():
    parser = argparse.ArgumentParser(
        description='Validate Common Crawl collection completeness'
    )
    parser.add_argument(
        '--ccindex-dir',
        type=Path,
        default=Path('/storage/ccindex'),
        help='Directory containing tar.gz files'
    )
    parser.add_argument(
        '--parquet-dir',
        type=Path,
        default=Path('/storage/ccindex_parquet'),
        help='Directory containing parquet files'
    )
    parser.add_argument(
        '--pointer-dir',
        type=Path,
        default=Path('/storage/ccindex_duckdb'),
        help='Directory containing DuckDB pointer indexes'
    )
    parser.add_argument(
        '--collection',
        help='Validate specific collection only'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show all collections, not just incomplete ones'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )
    
    args = parser.parse_args()
    
    validator = CollectionValidator(
        args.ccindex_dir,
        args.parquet_dir,
        args.pointer_dir
    )
    
    if args.collection:
        # Validate single collection
        status = validator.validate_collection(args.collection)
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            validator._print_collection_status(status)
            sys.exit(0 if status['complete'] else 1)
    else:
        # Validate all collections
        results = validator.validate_all(verbose=args.verbose)
        
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            print(f"\n{'='*80}")
            print(f"SUMMARY")
            print(f"{'='*80}")
            print(f"Total collections:    {results['total_collections']}")
            print(f"Complete:             {results['complete']}")
            print(f"Incomplete:           {results['incomplete']}")
            print(f"Completion rate:      {results['complete']/results['total_collections']*100:.1f}%")
        
        sys.exit(0 if results['incomplete'] == 0 else 1)

if __name__ == '__main__':
    main()
