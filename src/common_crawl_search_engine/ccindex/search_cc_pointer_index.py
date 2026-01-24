#!/usr/bin/env python3
"""Search the DuckDB pointer index for domains across all indexes.

This script searches the domain-mode DuckDB pointer index to find all WARC
locations for a given domain. It uses the offset/range metadata to efficiently
locate and read only the relevant portions of sorted parquet files.

Usage:
    python search_cc_pointer_index.py --domain example.com --db-dir /storage/ccindex_duckdb
    python search_cc_pointer_index.py --domain example.com --db-dir /storage/ccindex_duckdb --parquet-root /storage/ccindex_parquet
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

import duckdb
import pyarrow.parquet as pq


def reverse_domain(domain: str) -> str:
    """Convert domain to reversed format for indexing."""
    parts = [p for p in domain.lower().strip().split(".") if p]
    if not parts:
        return ""
    return ",".join(reversed(parts))


def search_domain_in_pointer_index(
    db_path: Path,
    domain: str,
    *,
    use_range_index: bool = True,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """Search for domain in a single DuckDB pointer database.
    
    Returns list of records with parquet file locations and optionally
    row group ranges for efficient access.
    """
    if not db_path.exists():
        if verbose:
            print(f"Skipping non-existent DB: {db_path}")
        return []
    
    host_rev = reverse_domain(domain)
    if not host_rev:
        return []
    
    results = []
    
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        
        # Check if we have range index
        has_range_index = False
        if use_range_index:
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                table_names = [t[0] for t in tables]
                has_range_index = "cc_parquet_rowgroups" in table_names
            except Exception:
                pass
        
        if has_range_index and use_range_index:
            # Use range index for precise row group targeting
            query = """
                SELECT DISTINCT
                    prq.source_path,
                    prq.collection,
                    prq.year,
                    prq.shard_file,
                    prq.parquet_relpath,
                    prq.row_group,
                    prq.row_start,
                    prq.row_end,
                    prq.host_rev_min,
                    prq.host_rev_max
                FROM cc_parquet_rowgroups prq
                WHERE ? BETWEEN prq.host_rev_min AND prq.host_rev_max
                ORDER BY prq.collection, prq.shard_file, prq.row_group
            """
            rows = con.execute(query, [host_rev]).fetchall()
            
            for row in rows:
                results.append({
                    "source_path": row[0],
                    "collection": row[1],
                    "year": row[2],
                    "shard_file": row[3],
                    "parquet_relpath": row[4],
                    "row_group": row[5],
                    "row_start": row[6],
                    "row_end": row[7],
                    "host_rev_min": row[8],
                    "host_rev_max": row[9],
                    "has_range": True,
                })
        else:
            # Fallback: use domain shards table (file-level only)
            query = """
                SELECT DISTINCT
                    source_path,
                    collection,
                    year,
                    shard_file,
                    parquet_relpath,
                    host_rev
                FROM cc_domain_shards
                WHERE host_rev = ?
                ORDER BY collection, shard_file
            """
            rows = con.execute(query, [host_rev]).fetchall()
            
            for row in rows:
                results.append({
                    "source_path": row[0],
                    "collection": row[1],
                    "year": row[2],
                    "shard_file": row[3],
                    "parquet_relpath": row[4],
                    "host_rev": row[5],
                    "has_range": False,
                })
        
        con.close()
        
    except Exception as e:
        if verbose:
            print(f"Error searching {db_path}: {e}", file=sys.stderr)
        return []
    
    return results


def search_all_pointer_indexes(
    db_dir: Path,
    domain: str,
    *,
    use_range_index: bool = True,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """Search for domain across all DuckDB pointer databases in a directory."""
    if not db_dir.exists() or not db_dir.is_dir():
        print(f"Error: DB directory does not exist: {db_dir}", file=sys.stderr)
        return []
    
    all_results = []
    
    # Find all .duckdb files
    db_files = sorted(db_dir.glob("*.duckdb"))
    
    if verbose:
        print(f"Searching {len(db_files)} database(s) for domain: {domain}")
    
    for db_path in db_files:
        if verbose:
            print(f"  Searching: {db_path.name}")
        
        results = search_domain_in_pointer_index(
            db_path,
            domain,
            use_range_index=use_range_index,
            verbose=verbose
        )
        
        all_results.extend(results)
    
    return all_results


def read_warc_pointers_from_parquet(
    parquet_path: Path,
    domain: str,
    *,
    row_groups: Optional[List[int]] = None,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """Read WARC pointer records for a domain from a parquet file.
    
    If row_groups is provided, only read those specific row groups.
    Otherwise, scan the entire file.
    """
    if not parquet_path.exists():
        if verbose:
            print(f"Parquet file not found: {parquet_path}")
        return []
    
    host_rev = reverse_domain(domain)
    if not host_rev:
        return []
    
    results = []
    
    try:
        pf = pq.ParquetFile(parquet_path)
        
        if row_groups:
            # Read specific row groups only
            for rg in row_groups:
                try:
                    table = pf.read_row_group(rg)
                    df = table.to_pandas()
                    
                    # Filter for matching domain
                    matching = df[df['host_rev'] == host_rev]
                    
                    for _, row in matching.iterrows():
                        results.append({
                            "url": row.get("url"),
                            "timestamp": row.get("ts"),
                            "status": row.get("status"),
                            "mime": row.get("mime"),
                            "warc_filename": row.get("warc_filename"),
                            "warc_offset": row.get("warc_offset"),
                            "warc_length": row.get("warc_length"),
                            "collection": row.get("collection"),
                        })
                except Exception as e:
                    if verbose:
                        print(f"Error reading row group {rg}: {e}")
                    continue
        else:
            # Read entire file
            table = pf.read()
            df = table.to_pandas()
            
            # Filter for matching domain
            matching = df[df['host_rev'] == host_rev]
            
            for _, row in matching.iterrows():
                results.append({
                    "url": row.get("url"),
                    "timestamp": row.get("ts"),
                    "status": row.get("status"),
                    "mime": row.get("mime"),
                    "warc_filename": row.get("warc_filename"),
                    "warc_offset": row.get("warc_offset"),
                    "warc_length": row.get("warc_length"),
                    "collection": row.get("collection"),
                })
    
    except Exception as e:
        if verbose:
            print(f"Error reading parquet file {parquet_path}: {e}")
        return []
    
    return results


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Search DuckDB pointer index for domain WARC locations"
    )
    ap.add_argument(
        "--domain",
        required=True,
        type=str,
        help="Domain to search (e.g., example.com)"
    )
    ap.add_argument(
        "--db-dir",
        required=True,
        type=str,
        help="Directory containing DuckDB pointer index files"
    )
    ap.add_argument(
        "--parquet-root",
        type=str,
        default=None,
        help="Optional: Root directory of parquet files to read full WARC pointers"
    )
    ap.add_argument(
        "--no-range-index",
        action="store_true",
        default=False,
        help="Disable row-group range index usage (slower, reads entire files)"
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of results returned"
    )
    ap.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Verbose output"
    )
    ap.add_argument(
        "--output-format",
        type=str,
        default="summary",
        choices=["summary", "full", "warc-only"],
        help="Output format: summary (file locations), full (all WARC pointers), warc-only (WARC locations only)"
    )
    
    args = ap.parse_args()
    
    db_dir = Path(args.db_dir).expanduser().resolve()
    domain = args.domain.strip()
    
    if not domain:
        print("Error: Domain cannot be empty", file=sys.stderr)
        return 1
    
    t0 = time.time()
    
    # Step 1: Search pointer indexes
    results = search_all_pointer_indexes(
        db_dir,
        domain,
        use_range_index=(not args.no_range_index),
        verbose=args.verbose
    )
    
    t1 = time.time()
    index_search_time = t1 - t0
    
    if args.verbose:
        print(f"\nIndex search completed in {index_search_time:.3f}s")
        print(f"Found {len(results)} parquet location(s)")
    
    if not results:
        print(f"No results found for domain: {domain}")
        return 0
    
    # Step 2: Optionally read full WARC pointers from parquet
    warc_pointers = []
    
    if args.output_format in ["full", "warc-only"] and args.parquet_root:
        parquet_root = Path(args.parquet_root).expanduser().resolve()
        
        if args.verbose:
            print(f"\nReading WARC pointers from parquet files...")
        
        for result in results:
            if args.limit and len(warc_pointers) >= args.limit:
                break
            
            parquet_relpath = result.get("parquet_relpath")
            if not parquet_relpath:
                continue
            
            parquet_path = parquet_root / parquet_relpath
            
            row_groups = None
            if result.get("has_range"):
                row_groups = [result.get("row_group")]
            
            pointers = read_warc_pointers_from_parquet(
                parquet_path,
                domain,
                row_groups=row_groups,
                verbose=args.verbose
            )
            
            warc_pointers.extend(pointers)
        
        t2 = time.time()
        parquet_read_time = t2 - t1
        
        if args.verbose:
            print(f"Parquet read completed in {parquet_read_time:.3f}s")
            print(f"Found {len(warc_pointers)} WARC pointer(s)")
    
    # Output results
    if args.output_format == "summary":
        print(f"\nSearch results for domain: {domain}")
        print(f"Total parquet files: {len(results)}")
        print(f"Index search time: {index_search_time:.3f}s")
        print()
        
        # Group by collection
        by_collection: Dict[str, List[Dict[str, Any]]] = {}
        for r in results:
            coll = r.get("collection", "unknown")
            by_collection.setdefault(coll, []).append(r)
        
        for coll in sorted(by_collection.keys()):
            items = by_collection[coll]
            print(f"{coll}: {len(items)} shard(s)")
            
            if args.verbose:
                for item in items:
                    if item.get("has_range"):
                        print(f"  - {item.get('shard_file')}: row_group {item.get('row_group')} (rows {item.get('row_start')}-{item.get('row_end')})")
                    else:
                        print(f"  - {item.get('shard_file')}")
    
    elif args.output_format == "warc-only":
        for ptr in warc_pointers[:args.limit] if args.limit else warc_pointers:
            print(f"{ptr.get('warc_filename')}:{ptr.get('warc_offset')}:{ptr.get('warc_length')}")
    
    else:  # full
        import json
        for ptr in warc_pointers[:args.limit] if args.limit else warc_pointers:
            print(json.dumps(ptr))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
