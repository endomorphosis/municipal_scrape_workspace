#!/usr/bin/env python3
"""
Search the DuckDB pointer index for a domain and retrieve all WARC pointers.

This script demonstrates fast domain lookups across all indexed parquet files.
"""
import argparse
import sys
import time
from pathlib import Path
import duckdb
import pyarrow.parquet as pq

def search_domain_in_duckdb(db_path: Path, domain: str, limit: int = None):
    """
    Search for a domain in the DuckDB pointer index.
    
    Returns: list of dicts with url, timestamp, warc_filename, warc_record_offset, warc_record_length
    """
    con = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Check if table exists
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if 'cc_pointers' not in table_names:
            print(f"Error: 'cc_pointers' table not found in {db_path}", file=sys.stderr)
            print(f"Available tables: {table_names}", file=sys.stderr)
            return []
        
        # Get schema
        schema = con.execute("DESCRIBE cc_pointers").fetchdf()
        columns = schema['column_name'].tolist()
        
        # Build query based on available columns
        if 'domain' in columns:
            # Domain-indexed mode
            query = """
                SELECT url, timestamp, warc_filename, warc_record_offset, warc_record_length
                FROM cc_pointers
                WHERE domain = ?
            """
        elif 'host' in columns:
            # Host-indexed mode
            query = """
                SELECT url, timestamp, warc_filename, warc_offset as warc_record_offset, warc_length as warc_record_length
                FROM cc_pointers
                WHERE host = ?
            """
        else:
            # Full URL search fallback
            query = """
                SELECT url, timestamp, warc_filename, warc_offset as warc_record_offset, warc_length as warc_record_length
                FROM cc_pointers
                WHERE url LIKE ?
            """
            domain = f'%{domain}%'
        
        if limit:
            query += f" LIMIT {limit}"
        
        start = time.time()
        results = con.execute(query, [domain]).fetchall()
        elapsed = time.time() - start
        
        print(f"Query completed in {elapsed:.3f}s, found {len(results)} results")
        
        return [
            {
                'url': r[0],
                'timestamp': r[1],
                'warc_filename': r[2],
                'warc_record_offset': r[3],
                'warc_record_length': r[4],
            }
            for r in results
        ]
        
    finally:
        con.close()

def search_domain_in_parquet(parquet_root: Path, domain: str, limit: int = None):
    """
    Search for a domain across all sorted parquet files.
    
    This demonstrates direct parquet scanning without DuckDB.
    """
    results = []
    start = time.time()
    
    # Find all parquet files
    parquet_files = sorted(parquet_root.rglob("*.parquet"))
    print(f"Scanning {len(parquet_files)} parquet files...")
    
    files_scanned = 0
    for pq_file in parquet_files:
        try:
            table = pq.read_table(pq_file, columns=['domain', 'url', 'timestamp', 
                                                      'warc_filename', 'warc_record_offset', 
                                                      'warc_record_length'])
            
            # Filter by domain
            df = table.to_pandas()
            matches = df[df['domain'] == domain]
            
            if len(matches) > 0:
                files_scanned += 1
                for _, row in matches.iterrows():
                    results.append({
                        'url': row['url'],
                        'timestamp': row['timestamp'],
                        'warc_filename': row['warc_filename'],
                        'warc_record_offset': row['warc_record_offset'],
                        'warc_record_length': row['warc_record_length'],
                    })
                    
                    if limit and len(results) >= limit:
                        break
            
            if limit and len(results) >= limit:
                break
                
        except Exception as e:
            print(f"Error reading {pq_file}: {e}", file=sys.stderr)
            continue
    
    elapsed = time.time() - start
    print(f"Parquet scan completed in {elapsed:.3f}s, scanned {files_scanned} files, found {len(results)} results")
    
    return results[:limit] if limit else results

def main():
    parser = argparse.ArgumentParser(description='Search DuckDB pointer index for domain')
    parser.add_argument('domain', help='Domain to search for (e.g., example.com)')
    parser.add_argument('--db', type=Path, 
                       default=Path('/storage/ccindex_duckdb/cc_pointers.duckdb'),
                       help='Path to DuckDB database')
    parser.add_argument('--parquet-root', type=Path,
                       default=Path('/storage/ccindex_parquet/cc_pointers_by_year'),
                       help='Path to parquet files root')
    parser.add_argument('--mode', choices=['duckdb', 'parquet', 'both'],
                       default='duckdb',
                       help='Search mode: duckdb (fast), parquet (direct), or both (comparison)')
    parser.add_argument('--limit', type=int, default=100,
                       help='Maximum number of results to return')
    parser.add_argument('--show', action='store_true',
                       help='Show the actual results (not just count)')
    
    args = parser.parse_args()
    
    if args.mode in ['duckdb', 'both']:
        if not args.db.exists():
            print(f"Error: DuckDB database not found at {args.db}", file=sys.stderr)
            if args.mode == 'duckdb':
                return 1
        else:
            print(f"\n{'='*60}")
            print("DuckDB Search")
            print(f"{'='*60}")
            results_db = search_domain_in_duckdb(args.db, args.domain, args.limit)
            
            if args.show and results_db:
                print("\nSample results:")
                for i, r in enumerate(results_db[:10], 1):
                    print(f"\n{i}. {r['url']}")
                    print(f"   Timestamp: {r['timestamp']}")
                    print(f"   WARC: {r['warc_filename']}")
                    print(f"   Offset: {r['warc_record_offset']}, Length: {r['warc_record_length']}")
    
    if args.mode in ['parquet', 'both']:
        if not args.parquet_root.exists():
            print(f"Error: Parquet root not found at {args.parquet_root}", file=sys.stderr)
            if args.mode == 'parquet':
                return 1
        else:
            print(f"\n{'='*60}")
            print("Direct Parquet Search")
            print(f"{'='*60}")
            results_pq = search_domain_in_parquet(args.parquet_root, args.domain, args.limit)
            
            if args.show and results_pq:
                print("\nSample results:")
                for i, r in enumerate(results_pq[:10], 1):
                    print(f"\n{i}. {r['url']}")
                    print(f"   Timestamp: {r['timestamp']}")
                    print(f"   WARC: {r['warc_filename']}")
                    print(f"   Offset: {r['warc_record_offset']}, Length: {r['warc_record_length']}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
