#!/usr/bin/env python3
"""
Convert missing .gz files to .parquet with chunked processing to avoid OOM
"""
import os
import sys
import gzip
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import gc

CCINDEX_ROOT = Path("/storage/ccindex")
PARQUET_ROOT = Path("/storage/ccindex_parquet/cc_pointers_by_year")
CHUNK_SIZE = 50000  # Process 50k records at a time

def get_year_from_crawl(crawl_name):
    """Extract year from crawl name like CC-MAIN-2024-51"""
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
                missing.append((gz_file, parquet_path))
    
    return missing

def convert_gz_to_parquet_chunked(gz_path, parquet_path):
    """Convert .gz to .parquet using chunked processing"""
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
    
    try:
        with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    
                    # Parse CDX format
                    url_key = parts[0]
                    timestamp = parts[1]
                    json_data = json.loads(' '.join(parts[2:]))
                    
                    url = json_data.get('url', '')
                    if not url:
                        continue
                    
                    # Extract domain
                    domain = url.split('/')[2] if len(url.split('/')) > 2 else ''
                    
                    warc_filename = json_data.get('filename', '')
                    warc_record_offset = int(json_data.get('offset', 0))
                    warc_record_length = int(json_data.get('length', 0))
                    
                    chunk.append({
                        'url': url,
                        'domain': domain,
                        'timestamp': timestamp,
                        'warc_filename': warc_filename,
                        'warc_record_offset': warc_record_offset,
                        'warc_record_length': warc_record_length,
                    })
                    
                    # Write chunk when it reaches CHUNK_SIZE
                    if len(chunk) >= CHUNK_SIZE:
                        batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
                        if writer is None:
                            writer = pq.ParquetWriter(parquet_path, schema, compression='snappy')
                        writer.write_batch(batch)
                        total_records += len(chunk)
                        chunk = []
                        gc.collect()  # Force garbage collection
                        
                except (json.JSONDecodeError, KeyError, ValueError, IndexError) as e:
                    continue
        
        # Write remaining records
        if chunk:
            batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, schema, compression='snappy')
            writer.write_batch(batch)
            total_records += len(chunk)
        
        if writer:
            writer.close()
        
        return total_records
        
    except Exception as e:
        if writer:
            writer.close()
        if parquet_path.exists():
            parquet_path.unlink()
        raise

def main():
    print("Finding missing .gz to .parquet conversions...")
    missing = find_missing_conversions()
    
    if not missing:
        print("✓ All .gz files have been converted to .parquet")
        return 0
    
    print(f"Found {len(missing)} files to convert")
    
    for i, (gz_file, parquet_path) in enumerate(missing, 1):
        try:
            print(f"[{i}/{len(missing)}] Converting {gz_file.name}...", end='', flush=True)
            records = convert_gz_to_parquet_chunked(gz_file, parquet_path)
            size_mb = parquet_path.stat().st_size / 1024 / 1024
            print(f" ✓ {records:,} records, {size_mb:.1f} MB")
        except Exception as e:
            print(f" ✗ FAILED: {e}")
            return 1
    
    print(f"\n✓ Successfully converted {len(missing)} files")
    return 0

if __name__ == '__main__':
    sys.exit(main())
