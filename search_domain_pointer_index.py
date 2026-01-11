#!/usr/bin/env python3
"""Search the DuckDB domain pointer index for all URLs matching a domain.

This script searches the domain_pointer.duckdb index to find all parquet files
and row ranges containing data for a specific domain, then retrieves the actual
URL records from those parquet files.

Usage:
    python search_domain_pointer_index.py example.com
    python search_domain_pointer_index.py --domain example.com --limit 100
"""

import argparse
import sys
import time
from pathlib import Path
from typing import List, Tuple

import duckdb
import pyarrow.parquet as pq


def search_domain(
    domain: str,
    db_path: str = "/storage/ccindex_duckdb/domain_pointer.duckdb",
    parquet_root: str = "/storage/ccindex_parquet",
    limit: int = None,
    verbose: bool = False,
) -> List[dict]:
    """Search for all URLs in a domain using the pointer index.
    
    Args:
        domain: Domain to search for (e.g., "example.com")
        db_path: Path to the DuckDB pointer index
        parquet_root: Root directory containing parquet files
        limit: Maximum number of results to return
        verbose: Print timing and diagnostic information
    
    Returns:
        List of URL records with WARC pointers
    """
    start_time = time.time()
    
    # Connect to DuckDB index
    if verbose:
        print(f"Connecting to {db_path}...")
    conn = duckdb.connect(db_path, read_only=True)
    
    # Find all parquet files and row ranges for this domain
    query_time = time.time()
    query = """
        SELECT parquet_file, row_start, row_end, row_count
        FROM domain_pointers
        WHERE domain = ?
        ORDER BY parquet_file, row_start
    """
    
    ranges = conn.execute(query, [domain]).fetchall()
    query_elapsed = time.time() - query_time
    
    if verbose:
        print(f"Query took {query_elapsed:.4f}s")
        print(f"Found {len(ranges)} parquet file ranges")
    
    if not ranges:
        if verbose:
            print(f"No results found for domain: {domain}")
        return []
    
    # Read data from parquet files
    results = []
    total_rows = 0
    read_time = time.time()
    
    for parquet_file, row_start, row_end, row_count in ranges:
        parquet_path = Path(parquet_root) / parquet_file
        
        if not parquet_path.exists():
            if verbose:
                print(f"Warning: Parquet file not found: {parquet_path}")
            continue
        
        if verbose:
            print(f"Reading {parquet_file} rows {row_start}-{row_end} ({row_count} rows)")
        
        # Read only the specific row range
        try:
            table = pq.read_table(
                str(parquet_path),
                columns=None,  # Read all columns
            )
            
            # Filter to the specific row range and domain
            # Since parquet is sorted by domain, we can use the range directly
            chunk = table.slice(row_start, row_count)
            
            # Convert to list of dicts
            for i in range(chunk.num_rows):
                row_dict = {
                    col: chunk.column(col)[i].as_py()
                    for col in chunk.column_names
                }
                results.append(row_dict)
                total_rows += 1
                
                if limit and len(results) >= limit:
                    break
            
        except Exception as e:
            if verbose:
                print(f"Error reading {parquet_file}: {e}")
            continue
        
        if limit and len(results) >= limit:
            break
    
    read_elapsed = time.time() - read_time
    total_elapsed = time.time() - start_time
    
    if verbose:
        print(f"\nRead {total_rows} rows from {len(ranges)} parquet ranges")
        print(f"Read time: {read_elapsed:.4f}s")
        print(f"Total time: {total_elapsed:.4f}s")
    
    conn.close()
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Search domain pointer index for URLs"
    )
    parser.add_argument(
        "domain",
        nargs="?",
        help="Domain to search for (e.g., example.com)"
    )
    parser.add_argument(
        "--domain",
        dest="domain_flag",
        help="Domain to search for (alternative to positional arg)"
    )
    parser.add_argument(
        "--db",
        default="/storage/ccindex_duckdb/domain_pointer.duckdb",
        help="Path to DuckDB pointer index"
    )
    parser.add_argument(
        "--parquet-root",
        default="/storage/ccindex_parquet",
        help="Root directory containing parquet files"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of results to return"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print timing and diagnostic information"
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "summary"],
        default="summary",
        help="Output format"
    )
    
    args = parser.parse_args()
    
    domain = args.domain or args.domain_flag
    if not domain:
        parser.error("Domain is required (either as positional arg or --domain)")
    
    # Normalize domain (remove protocol, trailing slash, etc)
    domain = domain.lower()
    domain = domain.replace("http://", "").replace("https://", "")
    domain = domain.rstrip("/")
    if "/" in domain:
        domain = domain.split("/")[0]
    
    try:
        results = search_domain(
            domain=domain,
            db_path=args.db,
            parquet_root=args.parquet_root,
            limit=args.limit,
            verbose=args.verbose,
        )
        
        if args.format == "json":
            import json
            print(json.dumps(results, indent=2))
        
        elif args.format == "csv":
            if results:
                # Print header
                print(",".join(results[0].keys()))
                # Print rows
                for row in results:
                    print(",".join(str(v) for v in row.values()))
        
        else:  # summary
            print(f"\nFound {len(results)} URLs for domain: {domain}")
            if results:
                print(f"\nFirst {min(10, len(results))} results:")
                for i, row in enumerate(results[:10]):
                    url = row.get("url", "")
                    warc = row.get("warc_filename", "")
                    offset = row.get("warc_offset", "")
                    length = row.get("warc_length", "")
                    print(f"{i+1}. {url}")
                    print(f"   WARC: {warc} offset={offset} length={length}")
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            raise
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
