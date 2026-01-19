#!/usr/bin/env python3
"""
Search for all URLs from a domain using the DuckDB pointer index.
This uses offset and range for optimal I/O and fast access times.
"""

import sys
import argparse
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from urllib.parse import urlparse
import time


def search_domain(domain, db_path, parquet_dir, verbose=False, show_urls=False):
    """
    Search for all URLs from a domain using the pointer index.
    
    Args:
        domain: Domain to search for (e.g., 'example.com')
        db_path: Path to DuckDB pointer database
        parquet_dir: Directory containing sorted parquet files
        verbose: Show detailed progress
        show_urls: Display all URLs found
    
    Returns:
        List of all URL records for the domain
    """
    start_time = time.time()
    
    # Connect to pointer database
    if verbose:
        print(f"Connecting to pointer database: {db_path}")
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # Query for domain pointers
    lookup_start = time.time()
    pointers = conn.execute("""
        SELECT parquet_file, row_offset, row_count, first_url, last_url
        FROM domain_pointers
        WHERE domain = ?
        ORDER BY parquet_file
    """, [domain]).fetchall()
    lookup_time = time.time() - lookup_start
    
    conn.close()
    
    if not pointers:
        print(f"Domain '{domain}' not found in index")
        return []
    
    if verbose:
        print(f"Found {len(pointers)} parquet file(s) containing '{domain}'")
        print(f"Pointer lookup time: {lookup_time*1000:.2f}ms")
    
    # Retrieve URLs using pointers
    all_records = []
    retrieval_start = time.time()
    
    for parquet_file, offset, count, first_url, last_url in pointers:
        parquet_path = Path(parquet_dir) / parquet_file
        
        if not parquet_path.exists():
            print(f"Warning: Parquet file not found: {parquet_path}", file=sys.stderr)
            continue
        
        if verbose:
            print(f"\nReading: {parquet_file}")
            print(f"  Offset: {offset}, Count: {count}")
            print(f"  First URL: {first_url}")
            print(f"  Last URL: {last_url}")
        
        # Read only the specific rows using offset and count
        table = pq.read_table(parquet_path)
        subset = table.slice(offset, count)
        
        # Convert to list of dicts
        records = subset.to_pylist()
        all_records.extend(records)
        
        if verbose:
            print(f"  Retrieved: {len(records)} records")
    
    retrieval_time = time.time() - retrieval_start
    total_time = time.time() - start_time
    
    # Display summary
    print(f"\n{'='*70}")
    print(f"Domain: {domain}")
    print(f"Total URLs found: {len(all_records)}")
    print(f"Parquet files searched: {len(pointers)}")
    print(f"{'='*70}")
    print(f"Pointer lookup time: {lookup_time*1000:.2f}ms")
    print(f"Data retrieval time: {retrieval_time*1000:.2f}ms")
    print(f"Total time: {total_time*1000:.2f}ms")
    print(f"Throughput: {len(all_records)/total_time:.0f} URLs/sec")
    print(f"{'='*70}")
    
    # Show URLs if requested
    if show_urls and all_records:
        print("\nURLs found:")
        for i, record in enumerate(all_records[:100], 1):  # Limit to first 100
            print(f"{i:4d}. {record['url']}")
            if verbose:
                print(f"      WARC: {record['filename']}")
                print(f"      Offset: {record['offset']}, Length: {record['length']}")
        
        if len(all_records) > 100:
            print(f"\n... and {len(all_records) - 100} more URLs")
    
    return all_records


def list_all_domains(db_path, limit=None):
    """List all domains in the index with URL counts"""
    conn = duckdb.connect(str(db_path), read_only=True)
    
    query = """
        SELECT domain, SUM(row_count) as url_count, COUNT(*) as file_count
        FROM domain_pointers
        GROUP BY domain
        ORDER BY url_count DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    results = conn.execute(query).fetchall()
    conn.close()
    
    print(f"\nDomains in index (showing {len(results)} domains):")
    print(f"{'Domain':<50} {'URLs':>10} {'Files':>8}")
    print("="*70)
    
    for domain, url_count, file_count in results:
        print(f"{domain:<50} {url_count:>10,} {file_count:>8}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Search DuckDB pointer index for domain URLs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for a domain
  %(prog)s example.com
  
  # Search with verbose output
  %(prog)s example.com -v
  
  # Show all URLs found
  %(prog)s example.com --show-urls
  
  # List all domains in index
  %(prog)s --list-domains
  
  # List top 100 domains
  %(prog)s --list-domains --limit 100
  
  # Use custom database location
  %(prog)s example.com --db /custom/path/domain_pointer.duckdb
        """
    )
    
    parser.add_argument('domain', nargs='?', help='Domain to search for')
    parser.add_argument('--db', default='/storage/ccindex_duckdb/domain_pointer.duckdb',
                       help='Path to DuckDB pointer database (default: /storage/ccindex_duckdb/domain_pointer.duckdb)')
    parser.add_argument('--parquet-dir', default='/storage/ccindex_parquet',
                       help='Directory containing sorted parquet files (default: /storage/ccindex_parquet)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Show detailed progress information')
    parser.add_argument('--show-urls', action='store_true',
                       help='Display all URLs found (up to 100)')
    parser.add_argument('--list-domains', action='store_true',
                       help='List all domains in the index')
    parser.add_argument('--limit', type=int,
                       help='Limit number of domains to show with --list-domains')
    parser.add_argument('--output', '-o', help='Write results to JSON file')
    
    args = parser.parse_args()
    
    # Check if database exists
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    # List domains mode
    if args.list_domains:
        list_all_domains(db_path, limit=args.limit)
        return
    
    # Search mode requires domain
    if not args.domain:
        parser.print_help()
        sys.exit(1)
    
    # Perform search
    records = search_domain(
        args.domain,
        db_path,
        args.parquet_dir,
        verbose=args.verbose,
        show_urls=args.show_urls
    )
    
    # Write to file if requested
    if args.output and records:
        import json
        with open(args.output, 'w') as f:
            json.dump(records, f, indent=2)
        print(f"\nResults written to: {args.output}")


if __name__ == "__main__":
    main()
