#!/usr/bin/env python3
"""Comprehensive benchmark for DuckDB domain pointer index search performance.

Tests multiple search patterns and access modes to validate:
1. Domain lookup speed (index scan time)
2. Parquet shard access time (with and without row group optimization)
3. URL search performance (join operations)
4. Scalability with different data sizes

Reports detailed metrics to help optimize the index design.

Example:
  python benchmark_cc_duckdb_search.py \
    --duckdb-dir /storage/ccindex_duckdb \
    --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
    --sample-domains 100 \
    --sample-urls 1000
"""

from __future__ import annotations

import argparse
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb


@dataclass
class BenchmarkResult:
    name: str
    iterations: int
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    stddev_ms: float
    total_time_ms: float
    throughput_per_sec: float


def _host_to_rev(host: str) -> str:
    """Convert host to reverse domain notation."""
    parts = [p for p in (host or "").lower().split(".") if p]
    return ",".join(reversed(parts))


def _iter_duckdb_files(path_or_dir: Path) -> List[Path]:
    """Find all DuckDB files."""
    if path_or_dir.is_file():
        return [path_or_dir]
    if path_or_dir.is_dir():
        return sorted(p for p in path_or_dir.glob("*.duckdb") if p.is_file())
    return []


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if table exists."""
    try:
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ? LIMIT 1",
            [str(table_name)],
        ).fetchone()
        return row is not None
    except Exception:
        return False


class SearchBenchmark:
    """Benchmark suite for DuckDB search operations."""

    def __init__(
        self,
        duckdb_path: Path,
        parquet_root: Optional[Path] = None,
        threads: int = 4,
    ):
        self.duckdb_path = duckdb_path
        self.parquet_root = parquet_root
        self.threads = threads
        self.db_files = _iter_duckdb_files(duckdb_path)
        
        if not self.db_files:
            raise ValueError(f"No DuckDB files found at {duckdb_path}")
        
        # Gather sample data
        self.sample_domains: List[Tuple[str, str]] = []  # (host, host_rev)
        self.sample_parquet_paths: List[str] = []
        self._collect_samples()

    def _collect_samples(self) -> None:
        """Collect sample domains and parquet paths from index."""
        domains_set = set()
        parquet_set = set()
        
        for db_file in self.db_files[:3]:  # Sample from first 3 DBs
            con = duckdb.connect(str(db_file), read_only=True)
            try:
                if _duckdb_has_table(con, "cc_domain_shards"):
                    rows = con.execute(
                        "SELECT DISTINCT host, host_rev, parquet_relpath FROM cc_domain_shards LIMIT 1000"
                    ).fetchall()
                    
                    for host, host_rev, pq_path in rows:
                        if host and host_rev:
                            domains_set.add((host, host_rev))
                        if pq_path:
                            parquet_set.add(pq_path)
            finally:
                con.close()
        
        self.sample_domains = list(domains_set)
        self.sample_parquet_paths = list(parquet_set)

    def benchmark_domain_lookup(self, n_samples: int = 100) -> BenchmarkResult:
        """Benchmark single domain lookups in DuckDB index."""
        samples = random.sample(self.sample_domains, min(n_samples, len(self.sample_domains)))
        times: List[float] = []
        
        for host, host_rev in samples:
            like_pat = host_rev + ",%"
            t0 = time.perf_counter()
            
            for db_file in self.db_files:
                con = duckdb.connect(str(db_file), read_only=True)
                try:
                    con.execute(f"PRAGMA threads={self.threads}")
                    if _duckdb_has_table(con, "cc_domain_shards"):
                        con.execute(
                            "SELECT DISTINCT parquet_relpath FROM cc_domain_shards WHERE host_rev = ? OR host_rev LIKE ?",
                            [host_rev, like_pat],
                        ).fetchall()
                finally:
                    con.close()
            
            dt_ms = (time.perf_counter() - t0) * 1000.0
            times.append(dt_ms)
        
        return self._compute_stats("Domain Lookup", times)

    def benchmark_rowgroup_lookup(self, n_samples: int = 50) -> Optional[BenchmarkResult]:
        """Benchmark row group range lookups."""
        samples = random.sample(self.sample_domains, min(n_samples, len(self.sample_domains)))
        times: List[float] = []
        has_rowgroups = False
        
        for host, host_rev in samples:
            like_pat = host_rev + ",%"
            t0 = time.perf_counter()
            
            for db_file in self.db_files:
                con = duckdb.connect(str(db_file), read_only=True)
                try:
                    con.execute(f"PRAGMA threads={self.threads}")
                    if _duckdb_has_table(con, "cc_parquet_rowgroups"):
                        has_rowgroups = True
                        con.execute(
                            """
                            SELECT parquet_relpath, row_group, row_start, row_end
                            FROM cc_parquet_rowgroups
                            WHERE (host_rev_min <= ? AND host_rev_max >= ?)
                               OR (host_rev_min LIKE ? OR host_rev_max LIKE ?)
                            """,
                            [host_rev, host_rev, like_pat, like_pat],
                        ).fetchall()
                finally:
                    con.close()
            
            dt_ms = (time.perf_counter() - t0) * 1000.0
            times.append(dt_ms)
        
        if not has_rowgroups:
            return None
        
        return self._compute_stats("Row Group Range Lookup", times)

    def benchmark_parquet_scan_full(self, n_samples: int = 10) -> Optional[BenchmarkResult]:
        """Benchmark full parquet file scans."""
        if not self.parquet_root:
            return None
        
        samples = random.sample(
            self.sample_parquet_paths,
            min(n_samples, len(self.sample_parquet_paths))
        )
        times: List[float] = []
        
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA threads={self.threads}")
            
            for pq_relpath in samples:
                pq_path = self.parquet_root / pq_relpath
                if not pq_path.exists():
                    continue
                
                t0 = time.perf_counter()
                con.execute(
                    "SELECT count(*) FROM read_parquet(?)",
                    [str(pq_path)],
                ).fetchone()
                dt_ms = (time.perf_counter() - t0) * 1000.0
                times.append(dt_ms)
        finally:
            con.close()
        
        if not times:
            return None
        
        return self._compute_stats("Parquet Full Scan", times)

    def benchmark_parquet_filtered_scan(self, n_samples: int = 20) -> Optional[BenchmarkResult]:
        """Benchmark filtered parquet scans by host_rev."""
        if not self.parquet_root:
            return None
        
        domain_samples = random.sample(
            self.sample_domains,
            min(n_samples, len(self.sample_domains))
        )
        times: List[float] = []
        
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA threads={self.threads}")
            
            for host, host_rev in domain_samples:
                # Find a parquet file for this domain
                pq_path = None
                for db_file in self.db_files[:1]:  # Just check first DB
                    dcon = duckdb.connect(str(db_file), read_only=True)
                    try:
                        if _duckdb_has_table(dcon, "cc_domain_shards"):
                            row = dcon.execute(
                                "SELECT parquet_relpath FROM cc_domain_shards WHERE host_rev = ? LIMIT 1",
                                [host_rev],
                            ).fetchone()
                            if row and row[0]:
                                pq_path = self.parquet_root / row[0]
                                break
                    finally:
                        dcon.close()
                
                if not pq_path or not pq_path.exists():
                    continue
                
                like_pat = host_rev + ",%"
                t0 = time.perf_counter()
                con.execute(
                    """
                    SELECT url, warc_filename, warc_offset, warc_length
                    FROM read_parquet(?)
                    WHERE host_rev = ? OR host_rev LIKE ?
                    """,
                    [str(pq_path), host_rev, like_pat],
                ).fetchall()
                dt_ms = (time.perf_counter() - t0) * 1000.0
                times.append(dt_ms)
        finally:
            con.close()
        
        if not times:
            return None
        
        return self._compute_stats("Parquet Filtered Scan", times)

    def benchmark_url_join(self, n_urls: int = 100) -> Optional[BenchmarkResult]:
        """Benchmark URL lookup via join operation."""
        if not self.parquet_root or not self.sample_parquet_paths:
            return None
        
        # Get sample URLs from a parquet file
        sample_pq = self.parquet_root / self.sample_parquet_paths[0]
        if not sample_pq.exists():
            return None
        
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA threads={self.threads}")
            
            # Extract sample URLs
            urls = con.execute(
                f"SELECT DISTINCT url FROM read_parquet(?) WHERE url IS NOT NULL LIMIT {n_urls}",
                [str(sample_pq)],
            ).fetchall()
            
            if not urls:
                return None
            
            # Create lookup table
            con.execute("CREATE TABLE search_urls (url VARCHAR)")
            con.executemany("INSERT INTO search_urls VALUES (?)", urls)
            
            # Benchmark join
            t0 = time.perf_counter()
            con.execute(
                """
                SELECT s.url, p.warc_filename, p.warc_offset, p.warc_length
                FROM search_urls s
                INNER JOIN read_parquet(?) p ON s.url = p.url
                """,
                [str(sample_pq)],
            ).fetchall()
            dt_ms = (time.perf_counter() - t0) * 1000.0
            
            return BenchmarkResult(
                name=f"URL Join ({n_urls} URLs)",
                iterations=1,
                mean_ms=dt_ms,
                median_ms=dt_ms,
                min_ms=dt_ms,
                max_ms=dt_ms,
                stddev_ms=0.0,
                total_time_ms=dt_ms,
                throughput_per_sec=n_urls / (dt_ms / 1000.0) if dt_ms > 0 else 0.0,
            )
        finally:
            con.close()

    def benchmark_index_scan_all_domains(self) -> BenchmarkResult:
        """Benchmark scanning all domains in index."""
        times: List[float] = []
        
        for db_file in self.db_files:
            con = duckdb.connect(str(db_file), read_only=True)
            try:
                con.execute(f"PRAGMA threads={self.threads}")
                if _duckdb_has_table(con, "cc_domain_shards"):
                    t0 = time.perf_counter()
                    con.execute("SELECT DISTINCT host_rev FROM cc_domain_shards").fetchall()
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    times.append(dt_ms)
            finally:
                con.close()
        
        return self._compute_stats("Full Index Scan", times)

    def _compute_stats(self, name: str, times: List[float]) -> BenchmarkResult:
        """Compute statistics from timing samples."""
        if not times:
            return BenchmarkResult(
                name=name,
                iterations=0,
                mean_ms=0.0,
                median_ms=0.0,
                min_ms=0.0,
                max_ms=0.0,
                stddev_ms=0.0,
                total_time_ms=0.0,
                throughput_per_sec=0.0,
            )
        
        mean = statistics.mean(times)
        median = statistics.median(times)
        stddev = statistics.stdev(times) if len(times) > 1 else 0.0
        total = sum(times)
        throughput = len(times) / (total / 1000.0) if total > 0 else 0.0
        
        return BenchmarkResult(
            name=name,
            iterations=len(times),
            mean_ms=mean,
            median_ms=median,
            min_ms=min(times),
            max_ms=max(times),
            stddev_ms=stddev,
            total_time_ms=total,
            throughput_per_sec=throughput,
        )


def print_result(result: Optional[BenchmarkResult]) -> None:
    """Print benchmark result in formatted table."""
    if result is None:
        return
    
    print(f"\n{result.name}")
    print("=" * 70)
    print(f"  Iterations:     {result.iterations}")
    print(f"  Mean:           {result.mean_ms:>10.3f} ms")
    print(f"  Median:         {result.median_ms:>10.3f} ms")
    print(f"  Min:            {result.min_ms:>10.3f} ms")
    print(f"  Max:            {result.max_ms:>10.3f} ms")
    print(f"  Std Dev:        {result.stddev_ms:>10.3f} ms")
    print(f"  Total Time:     {result.total_time_ms:>10.3f} ms")
    print(f"  Throughput:     {result.throughput_per_sec:>10.2f} ops/sec")


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark DuckDB search performance")
    ap.add_argument("--duckdb-dir", required=True, type=str, help="DuckDB directory or file")
    ap.add_argument("--parquet-root", type=str, help="Parquet root directory")
    ap.add_argument("--threads", type=int, default=4, help="DuckDB threads")
    ap.add_argument("--sample-domains", type=int, default=100, help="Sample size for domain lookups")
    ap.add_argument("--sample-urls", type=int, default=100, help="Sample size for URL searches")
    ap.add_argument("--quick", action="store_true", help="Run quick benchmark (fewer samples)")
    
    args = ap.parse_args()
    
    duckdb_path = Path(args.duckdb_dir).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve() if args.parquet_root else None
    
    if args.quick:
        args.sample_domains = 20
        args.sample_urls = 50
    
    print("=" * 70)
    print("Common Crawl DuckDB Index Search Benchmark")
    print("=" * 70)
    print(f"DuckDB Path:    {duckdb_path}")
    print(f"Parquet Root:   {parquet_root or 'Not provided'}")
    print(f"Threads:        {args.threads}")
    print(f"Quick Mode:     {args.quick}")
    
    bench = SearchBenchmark(
        duckdb_path=duckdb_path,
        parquet_root=parquet_root,
        threads=args.threads,
    )
    
    print(f"\nSample Data Collected:")
    print(f"  Domains:  {len(bench.sample_domains):,}")
    print(f"  Parquet:  {len(bench.sample_parquet_paths):,}")
    
    print("\n" + "=" * 70)
    print("Running Benchmarks...")
    print("=" * 70)
    
    # Run benchmarks
    results = []
    
    print("\n[1/7] Domain lookup benchmark...")
    result = bench.benchmark_domain_lookup(n_samples=args.sample_domains)
    results.append(result)
    print_result(result)
    
    print("\n[2/7] Row group range lookup benchmark...")
    result = bench.benchmark_rowgroup_lookup(n_samples=min(50, args.sample_domains))
    if result:
        results.append(result)
        print_result(result)
    else:
        print("  SKIPPED: No row group data available")
    
    print("\n[3/7] Full index scan benchmark...")
    result = bench.benchmark_index_scan_all_domains()
    results.append(result)
    print_result(result)
    
    if parquet_root:
        print("\n[4/7] Parquet full scan benchmark...")
        result = bench.benchmark_parquet_scan_full(n_samples=10)
        if result:
            results.append(result)
            print_result(result)
        else:
            print("  SKIPPED: No parquet files available")
        
        print("\n[5/7] Parquet filtered scan benchmark...")
        result = bench.benchmark_parquet_filtered_scan(n_samples=20)
        if result:
            results.append(result)
            print_result(result)
        else:
            print("  SKIPPED: Could not perform filtered scans")
        
        print("\n[6/7] URL join benchmark...")
        result = bench.benchmark_url_join(n_urls=args.sample_urls)
        if result:
            results.append(result)
            print_result(result)
        else:
            print("  SKIPPED: Could not perform URL joins")
    else:
        print("\n[4/7] Parquet benchmarks SKIPPED (no --parquet-root)")
        print("[5/7] SKIPPED")
        print("[6/7] SKIPPED")
    
    print("\n" + "=" * 70)
    print("Benchmark Summary")
    print("=" * 70)
    
    for r in results:
        print(f"{r.name:30s}  {r.mean_ms:8.2f} ms  ({r.throughput_per_sec:8.1f} ops/sec)")
    
    print("\n" + "=" * 70)
    print("Interpretation Guide:")
    print("=" * 70)
    print("Domain Lookup:      <10ms = Excellent, <50ms = Good, >100ms = Needs optimization")
    print("Row Group Lookup:   Should be similar to domain lookup if implemented")
    print("Filtered Scan:      <100ms = Good for sorted data, >500ms = Consider indexing")
    print("URL Join:           Depends on batch size; ~10-50ms per 100 URLs is typical")
    print("Full Index Scan:    Depends on DB size; useful for baseline comparison")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
