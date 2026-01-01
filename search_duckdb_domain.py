#!/usr/bin/env python3
"""Search Common Crawl data by domain using DuckDB pointer index.

This script uses the DuckDB domain pointer index to quickly locate and
retrieve all Common Crawl records for a given domain across all indexed
parquet files.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple
import duckdb
import pyarrow.parquet as pq
import time

def search_domain(db_path: Path, domain: str, limit: int = None) -> List[dict]:
    """Search for all records matching a domain.
    
    Returns list of records with WARC pointer information.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    
    # Query the pointer index
    query = """
        SELECT parquet_file, row_offset, row_count, collection, year, shard_file
        FROM domain_index
        WHERE domain = ?
        ORDER BY year DESC, collection DESC
    """
    
    results = con.execute(query, [domain]).fetchall()
    con.close()
    
    if len(results) == 0:
        return []
    
    print(f"Found domain '{domain}' in {len(results)} parquet file(s)", file=sys.stderr)
    
    # Retrieve records from parquet files
    all_records = []
    total_rows_expected = sum(row[2] for row in results)
    
    for idx, (parquet_file, row_offset, row_count, collection, year, shard_file) in enumerate(results, 1):
        print(f"  [{idx}/{len(results)}] Reading {row_count} rows from {Path(parquet_file).name} (offset {row_offset})", file=sys.stderr)
        
        try:
            # Read the specific row range
            table = pq.read_table(
                parquet_file,
                columns=['url', 'host', 'timestamp', 'status', 'mime', 'digest', 
                         'warc_filename', 'warc_offset', 'warc_length']
            )
            
            # Extract the relevant rows
            records = table.slice(row_offset, row_count).to_pylist()
            
            # Add metadata
            for record in records:
                record['collection'] = collection
                record['year'] = year
                record['shard_file'] = shard_file
            
            all_records.extend(records)
            
            # Apply limit if specified
            if limit and len(all_records) >= limit:
                all_records = all_records[:limit]
                break
                
        except Exception as e:
            print(f"  Error reading {parquet_file}: {e}", file=sys.stderr)
            continue
    
    print(f"Retrieved {len(all_records)} record(s) total", file=sys.stderr)
    return all_records

def format_record(record: dict) -> str:
    """Format a record for display."""
    return (
        f"{record.get('url', 'N/A')}\n"
        f"  Host: {record.get('host', 'N/A')}\n"
        f"  Timestamp: {record.get('timestamp', 'N/A')}\n"
        f"  Status: {record.get('status', 'N/A')}\n"
        f"  MIME: {record.get('mime', 'N/A')}\n"
        f"  Digest: {record.get('digest', 'N/A')}\n"
        f"  WARC: {record.get('warc_filename', 'N/A')}\n"
        f"  Offset: {record.get('warc_offset', 'N/A')}, Length: {record.get('warc_length', 'N/A')}\n"
        f"  Collection: {record.get('collection', 'N/A')} ({record.get('year', 'N/A')})\n"
    )

def main():
    parser = argparse.ArgumentParser(
        description="Search Common Crawl data by domain using DuckDB pointer index"
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="DuckDB pointer database path"
    )
    parser.add_argument(
        "--domain",
        type=str,
        required=True,
        help="Domain to search for (e.g., example.com)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of results (default: no limit)"
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text)"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress messages"
    )
    
    args = parser.parse_args()
    
    if not args.db.exists():
        print(f"Error: Database does not exist: {args.db}", file=sys.stderr)
        sys.exit(1)
    
    # Redirect stderr if quiet mode
    if args.quiet:
        sys.stderr = open('/dev/null', 'w')
    
    start_time = time.time()
    records = search_domain(args.db, args.domain, args.limit)
    elapsed = time.time() - start_time
    
    print(f"Search completed in {elapsed:.2f} seconds", file=sys.stderr)
    
    if len(records) == 0:
        print(f"No records found for domain: {args.domain}")
        return
    
    # Output results
    if args.format == "text":
        for record in records:
            print(format_record(record))
    elif args.format == "json":
        import json
        print(json.dumps(records, indent=2))
    elif args.format == "csv":
        import csv
        import sys
        writer = csv.DictWriter(sys.stdout, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

if __name__ == "__main__":
    main()
