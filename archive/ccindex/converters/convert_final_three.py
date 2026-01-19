#!/usr/bin/env python3
"""
Convert only the final 3 problematic files to parquet format.
"""

import gzip
import json
import os
import sys
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# The 3 files that need conversion
TARGET_FILES = [
    "cdx-00017.gz",
    "cdx-00296.gz", 
    "cdx-00394.gz"
]

CCINDEX_DIR = Path("/storage/ccindex")
PARQUET_DIR = Path("/storage/ccindex_parquet")

def process_file_in_chunks(gz_path, parquet_path, chunk_size=100000):
    """Process a large .gz file in chunks to avoid memory issues."""
    
    print(f"Processing {gz_path.name} in chunks of {chunk_size}...")
    
    # First pass: collect all records in chunks
    all_records = []
    chunk_count = 0
    
    try:
        with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
            chunk = []
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    chunk.append(record)
                    
                    if len(chunk) >= chunk_size:
                        all_records.extend(chunk)
                        chunk_count += 1
                        print(f"  Loaded chunk {chunk_count} ({len(all_records)} records so far)")
                        chunk = []
                        
                except json.JSONDecodeError as e:
                    print(f"  Warning: Skipping malformed JSON at line {line_num}: {e}")
                    continue
            
            # Add remaining records
            if chunk:
                all_records.extend(chunk)
                chunk_count += 1
                print(f"  Loaded final chunk {chunk_count} ({len(all_records)} total records)")
    
    except Exception as e:
        print(f"ERROR reading {gz_path}: {e}")
        return False
    
    if not all_records:
        print(f"  No valid records found in {gz_path.name}")
        return False
    
    print(f"  Total records loaded: {len(all_records)}")
    print(f"  Sorting by url_surtkey...")
    
    # Sort all records
    try:
        all_records.sort(key=lambda x: x.get('url_surtkey', ''))
    except Exception as e:
        print(f"ERROR sorting records: {e}")
        return False
    
    print(f"  Writing to parquet...")
    
    # Convert to Arrow table and write
    try:
        schema = pa.schema([
            ('url_surtkey', pa.string()),
            ('url', pa.string()),
            ('timestamp', pa.string()),
            ('mime', pa.string()),
            ('status', pa.int64()),
            ('digest', pa.string()),
            ('length', pa.int64()),
            ('offset', pa.int64()),
            ('filename', pa.string())
        ])
        
        arrays = [
            pa.array([r.get('url_surtkey', '') for r in all_records]),
            pa.array([r.get('url', '') for r in all_records]),
            pa.array([r.get('timestamp', '') for r in all_records]),
            pa.array([r.get('mime', '') for r in all_records]),
            pa.array([r.get('status', 0) for r in all_records]),
            pa.array([r.get('digest', '') for r in all_records]),
            pa.array([r.get('length', 0) for r in all_records]),
            pa.array([r.get('offset', 0) for r in all_records]),
            pa.array([r.get('filename', '') for r in all_records])
        ]
        
        table = pa.Table.from_arrays(arrays, schema=schema)
        
        # Write with compression
        pq.write_table(
            table,
            parquet_path,
            compression='snappy',
            row_group_size=100000
        )
        
        print(f"  ✓ Successfully wrote {parquet_path.name}")
        return True
        
    except Exception as e:
        print(f"ERROR writing parquet: {e}")
        return False

def main():
    print(f"Converting final 3 problematic files...")
    print(f"Source: {CCINDEX_DIR}")
    print(f"Destination: {PARQUET_DIR}")
    print()
    
    success_count = 0
    fail_count = 0
    
    for filename in TARGET_FILES:
        gz_path = CCINDEX_DIR / filename
        parquet_filename = filename.replace('.gz', '.gz.parquet')
        parquet_path = PARQUET_DIR / parquet_filename
        
        if not gz_path.exists():
            print(f"ERROR: Source file not found: {gz_path}")
            fail_count += 1
            continue
        
        # Remove existing parquet if it exists (since it's corrupted/incomplete)
        if parquet_path.exists():
            print(f"Removing existing (corrupted) file: {parquet_path.name}")
            parquet_path.unlink()
        
        print(f"\n[{success_count + fail_count + 1}/{len(TARGET_FILES)}] Converting {filename}...")
        
        if process_file_in_chunks(gz_path, parquet_path, chunk_size=50000):
            success_count += 1
        else:
            fail_count += 1
    
    print(f"\n{'='*60}")
    print(f"Conversion complete!")
    print(f"  Success: {success_count}/{len(TARGET_FILES)}")
    print(f"  Failed: {fail_count}/{len(TARGET_FILES)}")
    
    return 0 if fail_count == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
