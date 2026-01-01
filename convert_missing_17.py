#!/usr/bin/env python3
"""Convert 17 missing .gz files to sorted .gz.parquet files"""

import sys
import gzip
import duckdb
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

def convert_and_sort(crawl, filename):
    """Convert .gz to sorted .parquet"""
    try:
        gz_path = Path(f"/storage/ccindex/{crawl}/{filename}")
        parquet_path = Path(f"/storage/ccindex_parquet/cc_pointers_by_collection/{crawl[:4]}/{crawl}/{filename}.parquet")
        
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        temp_dir = Path("/storage/ccindex_parquet/.convert_temp")
        temp_dir.mkdir(exist_ok=True)
        
        print(f"[{crawl}] Converting {filename}...", flush=True)
        
        # Use DuckDB to convert and sort in one step
        con = duckdb.connect(":memory:")
        con.execute("SET memory_limit='4GB'")
        con.execute(f"SET temp_directory='{temp_dir}'")
        con.execute("SET threads=2")
        
        # Create temp table
        con.execute("""
            CREATE TEMP TABLE cdx_data (
                url VARCHAR,
                ts VARCHAR,
                data VARCHAR
            )
        """)
        
        # Read gzip file in chunks
        chunk_size = 500000
        total_rows = 0
        
        with gzip.open(gz_path, 'rt') as f:
            chunk = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split(' ', 2)
                if len(parts) >= 3:
                    chunk.append(parts)
                    
                    if len(chunk) >= chunk_size:
                        con.executemany("INSERT INTO cdx_data VALUES (?, ?, ?)", chunk)
                        total_rows += len(chunk)
                        chunk = []
            
            if chunk:
                con.executemany("INSERT INTO cdx_data VALUES (?, ?, ?)", chunk)
                total_rows += len(chunk)
        
        # Extract fields, add host_rev, sort, and write
        con.execute(f"""
            COPY (
                SELECT 
                    url,
                    ts,
                    regexp_extract(data, '^[^ ]+') as warc_file,
                    regexp_extract(data, 'offset:([0-9]+)', 1)::BIGINT as warc_offset,
                    regexp_extract(data, 'length:([0-9]+)', 1)::INTEGER as warc_length,
                    reverse(regexp_extract(url, '^[a-z]+://([^/]+)', 1)) as host_rev
                FROM cdx_data
                ORDER BY host_rev, url, ts
            )
            TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd')
        """)
        
        con.close()
        
        print(f"[{crawl}] ✅ Converted and sorted {filename} ({total_rows:,} rows)", flush=True)
        return True
        
    except Exception as e:
        print(f"[{crawl}] ❌ Failed {filename}: {e}", flush=True, file=sys.stderr)
        return False

# Define tasks
tasks = [
    ("CC-MAIN-2024-30", "cdx-00000.gz"),
    ("CC-MAIN-2024-30", "cdx-00006.gz"),
    ("CC-MAIN-2024-30", "cdx-00052.gz"),
    ("CC-MAIN-2024-30", "cdx-00124.gz"),
    ("CC-MAIN-2024-30", "cdx-00181.gz"),
    ("CC-MAIN-2024-33", "cdx-00044.gz"),
    ("CC-MAIN-2024-33", "cdx-00138.gz"),
    ("CC-MAIN-2024-38", "cdx-00139.gz"),
    ("CC-MAIN-2024-38", "cdx-00165.gz"),
    ("CC-MAIN-2024-38", "cdx-00214.gz"),
    ("CC-MAIN-2024-38", "cdx-00249.gz"),
    ("CC-MAIN-2024-42", "cdx-00213.gz"),
    ("CC-MAIN-2024-42", "cdx-00270.gz"),
    ("CC-MAIN-2024-42", "cdx-00272.gz"),
    ("CC-MAIN-2024-46", "cdx-00005.gz"),
    ("CC-MAIN-2024-46", "cdx-00203.gz"),
    ("CC-MAIN-2024-51", "cdx-00014.gz"),
]

print(f"Starting conversion of {len(tasks)} files...\n", flush=True)

# Process in parallel (6 workers)
success = 0
failed = 0

with ProcessPoolExecutor(max_workers=6) as executor:
    futures = {executor.submit(convert_and_sort, crawl, fn): (crawl, fn) for crawl, fn in tasks}
    
    for future in as_completed(futures):
        crawl, fn = futures[future]
        try:
            if future.result():
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"[{crawl}] ❌ Exception for {fn}: {e}", flush=True, file=sys.stderr)
            failed += 1
        
        if (success + failed) % 5 == 0:
            print(f"\nProgress: {success + failed}/{len(tasks)} - Success: {success}, Failed: {failed}\n", flush=True)

print(f"\n{'='*80}", flush=True)
print(f"CONVERSION COMPLETE", flush=True)
print(f"{'='*80}", flush=True)
print(f"Total:   {len(tasks)}", flush=True)
print(f"Success: {success}", flush=True)
print(f"Failed:  {failed}", flush=True)

sys.exit(0 if failed == 0 else 1)
