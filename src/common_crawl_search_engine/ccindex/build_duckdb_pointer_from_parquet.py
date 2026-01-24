#!/usr/bin/env python3
"""
Build DuckDB domain pointer index from existing sorted parquet files.
Creates offset/range metadata for fast domain lookups.
"""
import argparse
import duckdb
import time
from pathlib import Path
from typing import List, Tuple, Optional
import pyarrow.parquet as pq

def extract_domain_ranges(parquet_path: Path) -> List[Tuple[str, int, int, int]]:
    """
    Extract domain ranges from a sorted parquet file.
    Returns list of (domain, row_group_id, start_row, end_row, count).
    """
    pf = pq.ParquetFile(str(parquet_path))
    md = pf.metadata
    
    domain_ranges = []
    cumulative_row = 0
    
    for rg_idx in range(md.num_row_groups):
        rg = md.row_group(rg_idx)
        num_rows = rg.num_rows
        
        # Read just the URL column from this row group to extract domains
        table = pf.read_row_group(rg_idx, columns=['url'])
        urls = table.column('url').to_pylist()
        
        # Extract unique domains in this row group
        domains = {}
        for i, url in enumerate(urls):
            if url:
                # Extract domain from URL
                domain = extract_domain_from_url(url)
                if domain:
                    if domain not in domains:
                        domains[domain] = {'first': i, 'last': i, 'count': 1}
                    else:
                        domains[domain]['last'] = i
                        domains[domain]['count'] += 1
        
        # Create range entries
        for domain, info in domains.items():
            domain_ranges.append((
                domain,
                rg_idx,
                cumulative_row + info['first'],
                cumulative_row + info['last'],
                info['count']
            ))
        
        cumulative_row += num_rows
    
    return domain_ranges

def extract_domain_from_url(url: str) -> Optional[str]:
    """Extract domain from URL."""
    if not url:
        return None
    
    # Remove protocol
    if '://' in url:
        url = url.split('://', 1)[1]
    
    # Get domain (before first /)
    domain = url.split('/')[0]
    
    # Remove port if present
    if ':' in domain:
        domain = domain.split(':')[0]
    
    return domain.lower() if domain else None

def build_pointer_index(db_path: Path, parquet_root: Path, verbose: bool = False):
    """Build the domain pointer index from all parquet files."""
    
    if db_path.exists():
        print(f"Removing existing database: {db_path}")
        db_path.unlink()
        if db_path.with_suffix('.duckdb.wal').exists():
            db_path.with_suffix('.duckdb.wal').unlink()
    
    con = duckdb.connect(str(db_path))
    
    # Create the pointer table
    con.execute("""
        CREATE TABLE domain_pointers (
            domain VARCHAR,
            parquet_file VARCHAR,
            row_group_id INTEGER,
            start_row BIGINT,
            end_row BIGINT,
            domain_count INTEGER
        )
    """)
    
    # Find all parquet files
    parquet_files = sorted(parquet_root.rglob("*.parquet"))
    # Filter out test/sample files
    parquet_files = [
        f for f in parquet_files 
        if 'sample' not in f.name.lower() and 'test' not in str(f.parent).lower()
    ]
    
    print(f"Found {len(parquet_files)} parquet files to index")
    
    total_domains = 0
    start_time = time.time()
    
    for idx, pf_path in enumerate(parquet_files, 1):
        file_start = time.time()
        
        if verbose:
            print(f"[{idx}/{len(parquet_files)}] Processing {pf_path.name}...")
        
        try:
            ranges = extract_domain_ranges(pf_path)
            
            if ranges:
                # Insert into database
                con.executemany(
                    """
                    INSERT INTO domain_pointers 
                    (domain, parquet_file, row_group_id, start_row, end_row, domain_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [(d, str(pf_path), rg, start, end, cnt) for d, rg, start, end, cnt in ranges]
                )
                total_domains += len(ranges)
                
                if verbose:
                    elapsed = time.time() - file_start
                    print(f"    Found {len(ranges)} domain ranges in {elapsed:.2f}s")
        
        except Exception as e:
            print(f"    ERROR: {e}")
            continue
        
        # Progress update every 50 files
        if idx % 50 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed
            remaining = (len(parquet_files) - idx) / rate if rate > 0 else 0
            print(f"Progress: {idx}/{len(parquet_files)} files ({idx*100//len(parquet_files)}%) "
                  f"- {total_domains:,} domain entries - ETA: {remaining/60:.1f}min")
    
    # Create indexes for fast lookup
    print("\nCreating indexes...")
    con.execute("CREATE INDEX idx_domain ON domain_pointers(domain)")
    con.execute("CREATE INDEX idx_parquet_file ON domain_pointers(parquet_file)")
    
    con.close()
    
    total_time = time.time() - start_time
    print(f"\n✓ Index build complete!")
    print(f"  Total files: {len(parquet_files)}")
    print(f"  Total domain entries: {total_domains:,}")
    print(f"  Total time: {total_time/60:.1f} minutes")
    print(f"  Database: {db_path}")

def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build DuckDB domain pointer index from sorted parquet files'
    )
    parser.add_argument('--db', required=True,
                        help='Path to output DuckDB database')
    parser.add_argument('--parquet-root', required=True,
                        help='Root directory containing sorted parquet files')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    
    args = parser.parse_args()
    
    db_path = Path(args.db).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    
    if not parquet_root.exists():
        print(f"Error: Parquet root not found: {parquet_root}")
        return 1
    
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    build_pointer_index(db_path, parquet_root, args.verbose)
    
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
