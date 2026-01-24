#!/usr/bin/env python3
"""Flexible search script for Common Crawl DuckDB domain pointer index.

Supports multiple search modes:
1. Domain search - find all parquet shards containing a domain
2. URL search - find specific URLs in parquet files
3. Host pattern search - find domains matching patterns
4. Collection search - search within specific collections

Designed for both interactive use and batch processing.

Examples:
  # Find all shards for a domain
  python search_cc_duckdb_index.py --duckdb-dir /storage/ccindex_duckdb \
        --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \
    --domain example.gov

  # Search for specific URLs
  python search_cc_duckdb_index.py --duckdb-dir /storage/ccindex_duckdb \
        --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \
    --url-file urls.txt --output results.jsonl

  # Search with row group range optimization
  python search_cc_duckdb_index.py --duckdb-dir /storage/ccindex_duckdb \
        --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \
    --domain example.gov --use-rowgroup-ranges
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import duckdb
import pyarrow.parquet as pq


@dataclass
class SearchResult:
    domain: str
    host_rev: str
    parquet_shards: List[str]
    collections: Set[str]
    row_groups: Optional[List[Dict[str, Any]]] = None
    search_time_ms: float = 0.0


@dataclass
class URLMatch:
    url: str
    collection: str
    timestamp: str
    warc_filename: str
    warc_offset: int
    warc_length: int
    status: Optional[int] = None
    mime: Optional[str] = None
    digest: Optional[str] = None


def _host_to_rev(host: str) -> str:
    """Convert host to reverse domain notation."""
    parts = [p for p in (host or "").lower().split(".") if p]
    return ",".join(reversed(parts))


def _normalize_domain(domain: str) -> str:
    """Normalize domain from URL or plain domain."""
    dom = (domain or "").strip().lower()
    dom = re.sub(r"^https?://", "", dom)
    dom = dom.split("/", 1)[0]
    if dom.startswith("www."):
        dom = dom[4:]
    return dom


def _iter_duckdb_files(path_or_dir: Path, pattern: str = "*.duckdb") -> List[Path]:
    """Find all DuckDB files in directory or return single file."""
    if path_or_dir.is_file():
        return [path_or_dir]
    if path_or_dir.is_dir():
        return sorted(p for p in path_or_dir.glob(pattern) if p.is_file())
    return []


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if table exists in DuckDB."""
    try:
        row = con.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            LIMIT 1
            """,
            [str(table_name)],
        ).fetchone()
        return row is not None
    except Exception:
        return False


class DuckDBSearcher:
    """Fast searcher using DuckDB domain pointer index."""

    def __init__(
        self,
        duckdb_path: Path,
        parquet_root: Optional[Path] = None,
        use_rowgroup_ranges: bool = False,
        threads: int = 4,
    ):
        self.duckdb_path = duckdb_path
        self.parquet_root = parquet_root
        self.use_rowgroup_ranges = use_rowgroup_ranges
        self.threads = threads
        self.db_files = _iter_duckdb_files(duckdb_path)
        
        if not self.db_files:
            raise ValueError(f"No DuckDB files found at {duckdb_path}")

    def search_domain(self, domain: str) -> SearchResult:
        """Search for all parquet shards containing a domain."""
        t0 = time.perf_counter()
        
        dom = _normalize_domain(domain)
        host_rev = _host_to_rev(dom)
        like_pat = host_rev + ",%"
        
        parquet_relpaths: Set[str] = set()
        collections: Set[str] = set()
        row_groups_list: List[Dict[str, Any]] = []
        
        for db_file in self.db_files:
            con = duckdb.connect(str(db_file), read_only=True)
            try:
                con.execute(f"PRAGMA threads={self.threads}")
                
                if not _duckdb_has_table(con, "cc_domain_shards"):
                    continue
                
                # Get parquet shard paths
                rows = con.execute(
                    """
                    SELECT DISTINCT parquet_relpath, collection
                    FROM cc_domain_shards
                    WHERE host_rev = ? OR host_rev LIKE ?
                    """,
                    [host_rev, like_pat],
                ).fetchall()
                
                for rel, coll in rows:
                    if rel:
                        parquet_relpaths.add(str(rel))
                    if coll:
                        collections.add(str(coll))
                
                # Optionally get row group ranges for focused scanning
                if self.use_rowgroup_ranges and _duckdb_has_table(con, "cc_parquet_rowgroups"):
                    rg_rows = con.execute(
                        """
                        SELECT 
                            parquet_relpath,
                            collection,
                            row_group,
                            row_start,
                            row_end,
                            host_rev_min,
                            host_rev_max
                        FROM cc_parquet_rowgroups
                        WHERE 
                            (host_rev_min <= ? AND host_rev_max >= ?)
                            OR (host_rev_min LIKE ? OR host_rev_max LIKE ?)
                        ORDER BY parquet_relpath, row_group
                        """,
                        [host_rev, host_rev, like_pat, like_pat],
                    ).fetchall()
                    
                    for rel, coll, rg, rs, re, mn, mx in rg_rows:
                        row_groups_list.append({
                            "parquet_relpath": rel,
                            "collection": coll,
                            "row_group": rg,
                            "row_start": rs,
                            "row_end": re,
                            "host_rev_min": mn,
                            "host_rev_max": mx,
                        })
            finally:
                con.close()
        
        dt_ms = (time.perf_counter() - t0) * 1000.0
        
        return SearchResult(
            domain=dom,
            host_rev=host_rev,
            parquet_shards=sorted(parquet_relpaths),
            collections=collections,
            row_groups=row_groups_list if self.use_rowgroup_ranges else None,
            search_time_ms=dt_ms,
        )

    def search_urls_in_parquet(
        self, 
        parquet_paths: List[Path], 
        urls: List[str],
        limit: Optional[int] = None,
    ) -> List[URLMatch]:
        """Search for specific URLs in parquet files."""
        matches: List[URLMatch] = []
        
        if not urls or not parquet_paths:
            return matches
        
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA threads={self.threads}")
            
            # Create temp table of search URLs
            con.execute("CREATE TABLE search_urls (url VARCHAR)")
            url_data = [[u] for u in urls]
            con.executemany("INSERT INTO search_urls VALUES (?)", url_data)
            
            for pq_path in parquet_paths:
                if not pq_path.exists():
                    continue
                
                lim_clause = f"LIMIT {int(limit)}" if limit else ""
                rows = con.execute(
                    f"""
                    SELECT 
                        p.url,
                        p.collection,
                        p.ts,
                        p.warc_filename,
                        p.warc_offset,
                        p.warc_length,
                        p.status,
                        p.mime,
                        p.digest
                    FROM read_parquet(?) p
                    INNER JOIN search_urls s ON p.url = s.url
                    {lim_clause}
                    """,
                    [str(pq_path)],
                ).fetchall()
                
                for row in rows:
                    matches.append(URLMatch(
                        url=row[0],
                        collection=row[1],
                        timestamp=row[2],
                        warc_filename=row[3],
                        warc_offset=row[4],
                        warc_length=row[5],
                        status=row[6],
                        mime=row[7],
                        digest=row[8],
                    ))
                    
                    if limit and len(matches) >= limit:
                        break
                
                if limit and len(matches) >= limit:
                    break
                    
        finally:
            con.close()
        
        return matches

    def count_urls_for_domain(
        self,
        domain: str,
        limit: Optional[int] = None,
    ) -> int:
        """Count total URLs for a domain across all parquet shards."""
        result = self.search_domain(domain)
        
        if not result.parquet_shards or not self.parquet_root:
            return 0
        
        parquet_paths = [
            self.parquet_root / shard 
            for shard in result.parquet_shards
        ]
        
        like_pat = result.host_rev + ",%"
        total = 0
        
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA threads={self.threads}")
            
            for pq_path in parquet_paths:
                if not pq_path.exists():
                    continue
                
                lim_clause = f"LIMIT {int(limit) - total}" if limit else ""
                row = con.execute(
                    f"""
                    SELECT count(*)
                    FROM read_parquet(?)
                    WHERE host_rev = ? OR host_rev LIKE ?
                    {lim_clause}
                    """,
                    [str(pq_path), result.host_rev, like_pat],
                ).fetchone()
                
                n = int(row[0] if row and row[0] is not None else 0)
                total += n
                
                if limit and total >= limit:
                    break
        finally:
            con.close()
        
        return total

    def list_all_domains(
        self,
        collection_pattern: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Tuple[str, str, int]]:
        """List all domains in index. Returns (host, host_rev, shard_count)."""
        domains: Dict[str, Tuple[str, Set[str]]] = {}
        
        for db_file in self.db_files:
            con = duckdb.connect(str(db_file), read_only=True)
            try:
                if not _duckdb_has_table(con, "cc_domain_shards"):
                    continue
                
                where_clause = ""
                params = []
                if collection_pattern:
                    where_clause = "WHERE collection LIKE ?"
                    params = [collection_pattern]
                
                lim_clause = f"LIMIT {int(limit)}" if limit else ""
                
                rows = con.execute(
                    f"""
                    SELECT host, host_rev, parquet_relpath
                    FROM cc_domain_shards
                    {where_clause}
                    {lim_clause}
                    """,
                    params,
                ).fetchall()
                
                for host, host_rev, shard in rows:
                    if host_rev not in domains:
                        domains[host_rev] = (host, set())
                    domains[host_rev][1].add(shard)
                    
            finally:
                con.close()
        
        result = [
            (host, host_rev, len(shards))
            for host_rev, (host, shards) in domains.items()
        ]
        result.sort(key=lambda x: x[0])
        
        if limit:
            result = result[:int(limit)]
        
        return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Search Common Crawl DuckDB domain index")
    ap.add_argument("--duckdb-dir", required=True, type=str, help="DuckDB file or directory")
    ap.add_argument("--parquet-root", type=str, help="Root directory of parquet shards")
    ap.add_argument("--threads", type=int, default=4, help="Number of threads for DuckDB")
    
    # Search modes
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--domain", type=str, help="Search for a specific domain")
    mode.add_argument("--url-file", type=str, help="File containing URLs to search (one per line)")
    mode.add_argument("--list-domains", action="store_true", help="List all domains in index")
    
    # Options
    ap.add_argument("--use-rowgroup-ranges", action="store_true", help="Use row group ranges for optimization")
    ap.add_argument("--count-urls", action="store_true", help="Count total URLs for domain")
    ap.add_argument("--limit", type=int, help="Limit results")
    ap.add_argument("--collection-pattern", type=str, help="Filter by collection pattern (SQL LIKE)")
    ap.add_argument("--output", type=str, help="Output file (JSONL format)")
    ap.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = ap.parse_args()
    
    duckdb_path = Path(args.duckdb_dir).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve() if args.parquet_root else None
    
    searcher = DuckDBSearcher(
        duckdb_path=duckdb_path,
        parquet_root=parquet_root,
        use_rowgroup_ranges=args.use_rowgroup_ranges,
        threads=args.threads,
    )
    
    output_file = None
    if args.output:
        output_file = open(args.output, "w")
    
    try:
        if args.domain:
            # Domain search
            result = searcher.search_domain(args.domain)
            
            print(f"Domain: {result.domain}")
            print(f"Host (reversed): {result.host_rev}")
            print(f"Collections: {len(result.collections)}")
            print(f"Parquet shards: {len(result.parquet_shards)}")
            print(f"Search time: {result.search_time_ms:.2f}ms")
            
            if args.verbose:
                print("\nCollections:")
                for c in sorted(result.collections):
                    print(f"  {c}")
                print("\nParquet shards:")
                for s in result.parquet_shards:
                    full_path = parquet_root / s if parquet_root else Path(s)
                    exists = full_path.exists() if parquet_root else "?"
                    size = full_path.stat().st_size if exists == True else 0
                    print(f"  {s}  exists={exists}  size={size:,}")
            
            if args.use_rowgroup_ranges and result.row_groups:
                print(f"\nRow groups: {len(result.row_groups)}")
                if args.verbose:
                    for rg in result.row_groups[:20]:  # Show first 20
                        print(f"  {rg}")
            
            if args.count_urls and parquet_root:
                print("\nCounting URLs...")
                count = searcher.count_urls_for_domain(args.domain, limit=args.limit)
                print(f"Total URLs: {count:,}")
            
            if output_file:
                json.dump({
                    "domain": result.domain,
                    "host_rev": result.host_rev,
                    "collections": sorted(result.collections),
                    "parquet_shards": result.parquet_shards,
                    "row_groups": result.row_groups,
                    "search_time_ms": result.search_time_ms,
                }, output_file)
                output_file.write("\n")
        
        elif args.url_file:
            # URL search
            if not parquet_root:
                print("ERROR: --parquet-root required for URL search", file=sys.stderr)
                return 1
            
            with open(args.url_file) as f:
                urls = [line.strip() for line in f if line.strip()]
            
            print(f"Searching for {len(urls)} URLs...")
            
            # First, determine which shards to scan based on domains
            domains = set()
            for url in urls:
                dom = _normalize_domain(url)
                if dom:
                    domains.add(dom)
            
            all_shards = set()
            for domain in domains:
                result = searcher.search_domain(domain)
                all_shards.update(result.parquet_shards)
            
            parquet_paths = [parquet_root / s for s in sorted(all_shards)]
            print(f"Scanning {len(parquet_paths)} parquet shards...")
            
            matches = searcher.search_urls_in_parquet(parquet_paths, urls, limit=args.limit)
            
            print(f"Found {len(matches)} matches")
            
            for match in matches:
                line = {
                    "url": match.url,
                    "collection": match.collection,
                    "timestamp": match.timestamp,
                    "warc_filename": match.warc_filename,
                    "warc_offset": match.warc_offset,
                    "warc_length": match.warc_length,
                    "status": match.status,
                    "mime": match.mime,
                    "digest": match.digest,
                }
                
                if output_file:
                    json.dump(line, output_file)
                    output_file.write("\n")
                elif args.verbose:
                    print(json.dumps(line, indent=2))
        
        elif args.list_domains:
            # List all domains
            print("Listing domains...")
            domains = searcher.list_all_domains(
                collection_pattern=args.collection_pattern,
                limit=args.limit,
            )
            
            print(f"Found {len(domains)} domains")
            
            for host, host_rev, shard_count in domains:
                line = f"{host}\t{host_rev}\t{shard_count}"
                print(line)
                
                if output_file:
                    json.dump({
                        "host": host,
                        "host_rev": host_rev,
                        "shard_count": shard_count,
                    }, output_file)
                    output_file.write("\n")
    
    finally:
        if output_file:
            output_file.close()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
