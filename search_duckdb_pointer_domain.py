#!/usr/bin/env python3
"""
Search DuckDB pointer index by domain name with offset/range optimization.
Returns all WARC file locations for a given domain across all parquet files.
"""
import argparse
import duckdb
import time
from pathlib import Path

def search_domain(db_path: str, domain: str, verbose: bool = False):
    """Search for all URLs matching a domain in the pointer index."""
    start_time = time.time()
    
    con = duckdb.connect(str(db_path), read_only=True)
    
    # First, get domain pointers (which parquet files contain this domain)
    pointer_query = """
    SELECT 
        parquet_file,
        row_group_id,
        start_row,
        end_row,
        domain_count
    FROM domain_pointers
    WHERE domain = ?
    ORDER BY parquet_file, row_group_id
    """
    
    if verbose:
        print(f"Searching for domain: {domain}")
        print(f"Querying pointer index...")
    
    pointers = con.execute(pointer_query, [domain]).fetchall()
    
    if not pointers:
        print(f"No results found for domain: {domain}")
        con.close()
        return []
    
    pointer_time = time.time()
    if verbose:
        print(f"Found {len(pointers)} parquet file segments in {pointer_time - start_time:.3f}s")
    
    # Now query each parquet file using the offset/range information
    all_results = []
    for parquet_file, row_group_id, start_row, end_row, domain_count in pointers:
        if verbose:
            print(f"  Reading {parquet_file} (rows {start_row}-{end_row}, ~{domain_count} URLs)")
        
        # Use DuckDB's parquet reader with row group filtering
        file_query = """
        SELECT url, warc_filename, warc_record_offset, warc_record_length
        FROM read_parquet(?, hive_partitioning=false)
        WHERE url LIKE ? || '%'
        LIMIT 10000
        """
        
        pattern = f"http://{domain}/" if not domain.startswith('http') else domain
        results = con.execute(file_query, [parquet_file, pattern]).fetchall()
        all_results.extend(results)
        
        if verbose:
            print(f"    Found {len(results)} URLs")
    
    con.close()
    
    total_time = time.time() - start_time
    if verbose:
        print(f"\nTotal results: {len(all_results)} URLs")
        print(f"Total time: {total_time:.3f}s")
        print(f"  - Pointer lookup: {pointer_time - start_time:.3f}s")
        print(f"  - Data retrieval: {total_time - pointer_time:.3f}s")
    
    return all_results

def main():
    parser = argparse.ArgumentParser(
        description='Search DuckDB pointer index for domain URLs'
    )
    parser.add_argument('--db', required=True,
                        help='Path to DuckDB pointer database')
    parser.add_argument('--domain', required=True,
                        help='Domain to search for (e.g., example.com)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    parser.add_argument('--limit', type=int, default=10,
                        help='Max results to display (default: 10)')
    
    args = parser.parse_args()
    
    results = search_domain(args.db, args.domain, args.verbose)
    
    print(f"\nShowing first {min(args.limit, len(results))} results:")
    for i, (url, warc_file, offset, length) in enumerate(results[:args.limit], 1):
        print(f"{i}. {url}")
        print(f"   WARC: {warc_file}")
        print(f"   Offset: {offset}, Length: {length}")

if __name__ == '__main__':
    main()
