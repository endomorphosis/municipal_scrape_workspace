#!/usr/bin/env python3
"""
Parallel conversion of missing .gz files to .parquet with memory management
"""
import os
import sys
import gzip
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import gc
from multiprocessing import Pool, cpu_count
import psutil

CCINDEX_ROOT = Path("/storage/ccindex")
PARQUET_ROOT = Path("/storage/ccindex_parquet/cc_pointers_by_year")
CHUNK_SIZE = 50000
MAX_WORKERS = 8  # Conservative for memory

def get_available_memory_gb():
    """Get available memory in GB"""
    mem = psutil.virtual_memory()
    return mem.available / (1024**3)

def get_year_from_crawl(crawl_name):
    """Extract year from crawl name"""
    return crawl_name.split("-")[2]

def find_missing_conversions():
    """Find all .gz files that don't have corresponding .parquet files"""
    missing = []
    
    for crawl_dir in sorted(CCINDEX_ROOT.glob("CC-MAIN-202[45]-*")):
        crawl_name = crawl_dir.name
        year = get_year_from_crawl(crawl_name)
        
        for gz_file in sorted(crawl_dir.glob("*.gz")):
            parquet_path = PARQUET_ROOT / year / crawl_name / f"{gz_file.name}.parquet"
            if not parquet_path.exists():
                missing.append((str(gz_file), str(parquet_path)))
    
    return missing

def convert_one_file(args):
    """Convert a single .gz to .parquet (for multiprocessing)"""
    gz_path_str, parquet_path_str = args
    gz_path = Path(gz_path_str)
    parquet_path = Path(parquet_path_str)
    
    try:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        
        schema = pa.schema([
            ('url', pa.string()),
            ('domain', pa.string()),
            ('timestamp', pa.string()),
            ('warc_filename', pa.string()),
            ('warc_record_offset', pa.int64()),
            ('warc_record_length', pa.int64()),
        ])
        
        writer = None
        chunk = []
        total_records = 0
        
        with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    
                    timestamp = parts[1]
                    json_data = json.loads(' '.join(parts[2:]))
                    
                    url = json_data.get('url', '')
                    if not url:
                        continue
                    
                    domain = url.split('/')[2] if len(url.split('/')) > 2 else ''
                    
                    chunk.append({
                        'url': url,
                        'domain': domain,
                        'timestamp': timestamp,
                        'warc_filename': json_data.get('filename', ''),
                        'warc_record_offset': int(json_data.get('offset', 0)),
                        'warc_record_length': int(json_data.get('length', 0)),
                    })
                    
                    if len(chunk) >= CHUNK_SIZE:
                        batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
                        if writer is None:
                            writer = pq.ParquetWriter(parquet_path, schema, compression='snappy')
                        writer.write_batch(batch)
                        total_records += len(chunk)
                        chunk = []
                        gc.collect()
                        
                except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                    continue
        
        if chunk:
            batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, schema, compression='snappy')
            writer.write_batch(batch)
            total_records += len(chunk)
        
        if writer:
            writer.close()
        
        size_mb = parquet_path.stat().st_size / 1024 / 1024
        return (True, gz_path.name, total_records, size_mb, None)
        
    except Exception as e:
        if parquet_path.exists():
            parquet_path.unlink()
        return (False, gz_path.name, 0, 0, str(e))

def main():
    print("Finding missing .gz to .parquet conversions...")
    missing = find_missing_conversions()
    
    if not missing:
        print("✓ All .gz files have been converted to .parquet")
        return 0
    
    print(f"Found {len(missing)} files to convert")
    
    # Adjust workers based on available memory
    avail_mem = get_available_memory_gb()
    workers = min(MAX_WORKERS, max(1, int(avail_mem / 2)))  # 2GB per worker
    print(f"Using {workers} parallel workers (available memory: {avail_mem:.1f} GB)")
    
    success_count = 0
    fail_count = 0
    
    with Pool(processes=workers) as pool:
        for i, result in enumerate(pool.imap_unordered(convert_one_file, missing), 1):
            success, filename, records, size_mb, error = result
            
            if success:
                print(f"[{i}/{len(missing)}] ✓ {filename}: {records:,} records, {size_mb:.1f} MB")
                success_count += 1
            else:
                print(f"[{i}/{len(missing)}] ✗ {filename}: {error}")
                fail_count += 1
            
            if i % 100 == 0:
                mem = psutil.virtual_memory()
                print(f"  Progress: {i}/{len(missing)} ({100*i/len(missing):.1f}%), Memory: {mem.percent}% used")
    
    print(f"\n{'='*60}")
    print(f"Conversion complete:")
    print(f"  Success: {success_count}")
    print(f"  Failed:  {fail_count}")
    print(f"{'='*60}")
    
    return 0 if fail_count == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
