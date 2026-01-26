#!/usr/bin/env python3
"""
Build DuckDB index from EXISTING parquet files (fast method).
Reads parquet metadata only, processes in batches to manage memory.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, List, Set

import duckdb
import pyarrow.parquet as pq


def extract_domain_mappings_from_parquet(parquet_file: Path, parquet_root: Path) -> List[tuple]:
    """
    Extract unique domains from a parquet file.
    Returns list of (collection, year, shard_file, parquet_relpath, host, host_rev)
    """
    try:
        # Get relative path
        try:
            rel_path = parquet_file.relative_to(parquet_root).as_posix()
        except:
            rel_path = str(parquet_file)
        
        # Parse collection from path
        parts = parquet_file.parts
        collection = None
        year = None
        for i, part in enumerate(parts):
            if part.startswith('CC-MAIN-'):
                collection = part
                year_match = part.split('-')[2]
                try:
                    year = int(year_match)
                except:
                    pass
                break
        
        shard_file = parquet_file.name
        
        # Read unique host_rev values using DuckDB (faster than pyarrow for aggregation)
        con = duckdb.connect(":memory:")
        domains = con.execute(
            """
            SELECT DISTINCT host, host_rev
            FROM read_parquet(?)
            WHERE host IS NOT NULL AND host_rev IS NOT NULL
            """,
            [str(parquet_file)]
        ).fetchall()
        con.close()
        
        results = []
        for host, host_rev in domains:
            results.append((
                str(parquet_file),  # source_path
                collection,
                year,
                shard_file,
                rel_path,
                host,
                host_rev
            ))
        
        return results
        
    except Exception as e:
        print(f"Error processing {parquet_file}: {e}", file=sys.stderr)
        return []


def extract_rowgroup_ranges(parquet_file: Path, parquet_root: Path) -> List[tuple]:
    """
    Extract row group range metadata.
    Returns list of (source_path, collection, year, shard_file, parquet_relpath, 
                     row_group, row_start, row_end, host_rev_min, host_rev_max)
    """
    try:
        pf = pq.ParquetFile(parquet_file)
        md = pf.metadata
        
        if md is None or md.num_row_groups == 0:
            return []
        
        # Get relative path
        try:
            rel_path = parquet_file.relative_to(parquet_root).as_posix()
        except:
            rel_path = str(parquet_file)
        
        # Parse collection
        parts = parquet_file.parts
        collection = None
        year = None
        for part in parts:
            if part.startswith('CC-MAIN-'):
                collection = part
                year_match = part.split('-')[2]
                try:
                    year = int(year_match)
                except:
                    pass
                break
        
        shard_file = parquet_file.name
        
        # Find host_rev column
        host_rev_idx = None
        try:
            host_rev_idx = list(pf.schema_arrow.names).index('host_rev')
        except:
            try:
                host_rev_idx = list(pf.schema.names).index('host_rev')
            except:
                return []
        
        results = []
        row_start = 0
        
        for rg_idx in range(md.num_row_groups):
            rg = md.row_group(rg_idx)
            num_rows = int(rg.num_rows or 0)
            row_end = row_start + num_rows
            
            # Get min/max from statistics
            host_rev_min = None
            host_rev_max = None
            
            if host_rev_idx is not None:
                try:
                    col = rg.column(host_rev_idx)
                    stats = getattr(col, 'statistics', None)
                    if stats:
                        mn = getattr(stats, 'min', None)
                        mx = getattr(stats, 'max', None)
                        if isinstance(mn, bytes):
                            host_rev_min = mn.decode('utf-8', errors='ignore')
                        elif mn:
                            host_rev_min = str(mn)
                        if isinstance(mx, bytes):
                            host_rev_max = mx.decode('utf-8', errors='ignore')
                        elif mx:
                            host_rev_max = str(mx)
                except:
                    pass
            
            results.append((
                str(parquet_file),  # source_path
                collection,
                year,
                shard_file,
                rel_path,
                rg_idx,
                row_start,
                row_end,
                host_rev_min,
                host_rev_max
            ))
            
            row_start = row_end
        
        return results
        
    except Exception as e:
        print(f"Error extracting row groups from {parquet_file}: {e}", file=sys.stderr)
        return []


def main() -> int:
    ap = argparse.ArgumentParser(description="Build index from existing parquet files")
    ap.add_argument("--parquet-root", required=True, help="Root directory of parquet files")
    ap.add_argument("--output-db", required=True, help="Output DuckDB file")
    ap.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Commit every N files (also controls progress cadence)",
    )
    ap.add_argument("--extract-rowgroups", action="store_true", help="Extract row group ranges")
    ap.add_argument("--max-files", type=int, default=None, help="Only process up to N parquet files (for testing)")
    ap.add_argument(
        "--db-lock-retries",
        type=int,
        default=60,
        help="Retries when DuckDB file is locked by another process (default: 60)",
    )
    ap.add_argument(
        "--db-lock-sleep-seconds",
        type=float,
        default=2.0,
        help="Sleep between DuckDB lock retries (default: 2.0)",
    )
    args = ap.parse_args()
    
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    output_db = Path(args.output_db).expanduser().resolve()
    
    print(f"Parquet root: {parquet_root}")
    print(f"Output DB:    {output_db}")
    print(f"Batch size:   {args.batch_size}")
    print()
    
    # Find all parquet files (files only) and ignore hidden/temp directories.
    # Some stages create temporary work dirs; we don't want to treat directories
    # as Parquet inputs or accidentally index scratch artifacts.
    candidates: List[Path] = []
    for p in parquet_root.rglob("*.parquet"):
        try:
            if not p.is_file():
                continue
            rel = p.relative_to(parquet_root)
            # Skip any parquet files under hidden directories (e.g. .duckdb_sort_tmp)
            if any(part.startswith(".") for part in rel.parts[:-1]):
                continue
            candidates.append(p)
        except Exception:
            continue

    # Prefer sorted shards when present.
    # Pipeline convention: sorted shards end with '.sorted.parquet' (commonly '.gz.sorted.parquet').
    sorted_candidates = [p for p in candidates if p.name.endswith(".sorted.parquet")]
    all_files = sorted(sorted_candidates if sorted_candidates else candidates)

    if args.max_files is not None:
        all_files = all_files[: max(0, int(args.max_files))]

    print(f"Found {len(all_files)} parquet files")
    print()
    
    # Create output directory
    output_db.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to output database
    con = None
    retries = max(0, int(args.db_lock_retries or 0))
    sleep_s = max(0.1, float(args.db_lock_sleep_seconds or 0.1))
    for attempt in range(retries + 1):
        try:
            con = duckdb.connect(str(output_db))
            break
        except Exception as e:
            msg = str(e)
            is_lock = (
                "Conflicting lock is held" in msg
                or "Could not set lock on file" in msg
                or "lock" in msg.lower() and "conflicting" in msg.lower()
            )
            if (not is_lock) or (attempt >= retries):
                raise
            waited = (attempt + 1) * sleep_s
            print(
                f"DuckDB file is locked ({output_db}); retrying in {sleep_s:.1f}s "
                f"({attempt+1}/{retries})...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(sleep_s)

    assert con is not None
    
    # Create tables
    con.execute("""
        CREATE TABLE IF NOT EXISTS cc_domain_shards (
            source_path VARCHAR,
            collection VARCHAR,
            year INTEGER,
            shard_file VARCHAR,
            parquet_relpath VARCHAR,
            host VARCHAR,
            host_rev VARCHAR
        )
    """)
    
    if args.extract_rowgroups:
        con.execute("""
            CREATE TABLE IF NOT EXISTS cc_parquet_rowgroups (
                source_path VARCHAR,
                collection VARCHAR,
                year INTEGER,
                shard_file VARCHAR,
                parquet_relpath VARCHAR,
                row_group INTEGER,
                row_start BIGINT,
                row_end BIGINT,
                host_rev_min VARCHAR,
                host_rev_max VARCHAR
            )
        """)

    # Track progress per parquet file so reruns don't duplicate rows.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cc_indexed_parquet_files (
            parquet_path VARCHAR PRIMARY KEY,
            size_bytes BIGINT,
            mtime_ns BIGINT,
            indexed_at TIMESTAMP
        )
        """
    )

    commit_every = max(1, int(args.batch_size or 10))
    total_domains = 0
    total_rowgroups = 0
    did_files = 0
    skipped_files = 0

    for idx, pq_file in enumerate(all_files, 1):
        try:
            st = pq_file.stat()
            size_bytes = int(st.st_size)
            mtime_ns = int(st.st_mtime_ns)
        except Exception:
            size_bytes = -1
            mtime_ns = -1

        pq_path_str = str(pq_file)
        existing = con.execute(
            "SELECT size_bytes, mtime_ns FROM cc_indexed_parquet_files WHERE parquet_path = ?",
            [pq_path_str],
        ).fetchone()
        if existing and int(existing[0]) == size_bytes and int(existing[1]) == mtime_ns:
            skipped_files += 1
            if (idx % 50) == 0:
                print(f"Progress {idx}/{len(all_files)} (did={did_files}, skipped={skipped_files})")
            continue

        # Parse collection/year from path (best-effort).
        collection = None
        year = None
        for part in pq_file.parts:
            if part.startswith('CC-MAIN-'):
                collection = part
                try:
                    year = int(part.split('-')[2])
                except Exception:
                    year = None
                break

        shard_file = pq_file.name
        try:
            rel_path = pq_file.relative_to(parquet_root).as_posix()
        except Exception:
            rel_path = pq_path_str

        print(f"[{idx}/{len(all_files)}] Indexing {shard_file}...")

        # Make per-file idempotent.
        con.execute("DELETE FROM cc_domain_shards WHERE source_path = ?", [pq_path_str])
        if args.extract_rowgroups:
            con.execute("DELETE FROM cc_parquet_rowgroups WHERE source_path = ?", [pq_path_str])

        # Insert domain mappings directly via SQL (avoids huge Python lists).
        con.execute(
            """
            INSERT INTO cc_domain_shards
            SELECT ?, ?, ?, ?, ?, host, host_rev
            FROM (
                SELECT DISTINCT host, host_rev
                FROM read_parquet(?)
                WHERE host IS NOT NULL AND host_rev IS NOT NULL
            )
            """,
            [pq_path_str, collection, year, shard_file, rel_path, pq_path_str],
        )

        try:
            n_dom = con.execute(
                "SELECT COUNT(*) FROM cc_domain_shards WHERE source_path = ?",
                [pq_path_str],
            ).fetchone()[0]
        except Exception:
            n_dom = 0
        total_domains += int(n_dom or 0)

        if args.extract_rowgroups:
            rowgroups = extract_rowgroup_ranges(pq_file, parquet_root)
            if rowgroups:
                con.executemany(
                    "INSERT INTO cc_parquet_rowgroups VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rowgroups,
                )
                total_rowgroups += len(rowgroups)

        con.execute("DELETE FROM cc_indexed_parquet_files WHERE parquet_path = ?", [pq_path_str])
        con.execute(
            "INSERT INTO cc_indexed_parquet_files (parquet_path, size_bytes, mtime_ns, indexed_at) VALUES (?, ?, ?, now())",
            [pq_path_str, size_bytes, mtime_ns],
        )

        did_files += 1
        if (did_files % commit_every) == 0:
            con.commit()
            print(f"Committed (did={did_files}, skipped={skipped_files})")

        print(f"  Domains: {int(n_dom or 0):,}")
        if args.extract_rowgroups:
            print(f"  Row groups (this file): {len(rowgroups) if rowgroups else 0}")
        print()

    con.commit()
    print(f"Processed files: {did_files:,} (skipped unchanged: {skipped_files:,})")
    
    # Create indexes
    print("Creating indexes...")
    con.execute("CREATE INDEX IF NOT EXISTS idx_domain_shards_host_rev ON cc_domain_shards(host_rev)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_domain_shards_host ON cc_domain_shards(host)")
    
    if args.extract_rowgroups:
        con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_host_rev_min ON cc_parquet_rowgroups(host_rev_min)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rowgroups_host_rev_max ON cc_parquet_rowgroups(host_rev_max)")
    
    con.close()
    
    print()
    print("=" * 80)
    print("COMPLETE")
    print("=" * 80)
    print(f"Total domain mappings: {total_domains:,}")
    if args.extract_rowgroups:
        print(f"Total row group ranges: {total_rowgroups:,}")
    print(f"Output: {output_db}")
    print(f"Size: {output_db.stat().st_size / (1024**3):.2f} GB")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
