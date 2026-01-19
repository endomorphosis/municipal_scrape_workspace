#!/usr/bin/env python3
"""
Convert the final 3 missing parquet files from their correct source locations
"""
import gzip
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import sys

files_to_convert = [
    {
        'source': '/storage/ccindex/CC-MAIN-2024-33/cdx-00099.gz',
        'dest': '/storage/ccindex_parquet/CC-MAIN-2024-33-cdx-00099.gz.parquet'
    },
    {
        'source': '/storage/ccindex/CC-MAIN-2024-38/cdx-00099.gz',
        'dest': '/storage/ccindex_parquet/CC-MAIN-2024-38-cdx-00099.gz.parquet'
    },
    {
        'source': '/storage/ccindex/CC-MAIN-2024-42/cdx-00099.gz',
        'dest': '/storage/ccindex_parquet/CC-MAIN-2024-42-cdx-00099.gz.parquet'
    }
]

schema = pa.schema([
    ('urlkey', pa.string()),
    ('timestamp', pa.string()),
    ('url', pa.string()),
    ('mime', pa.string()),
    ('mime_detected', pa.string()),
    ('status', pa.string()),
    ('digest', pa.string()),
    ('length', pa.string()),
    ('offset', pa.string()),
    ('filename', pa.string())
])

print("Converting final 3 files with chunking to avoid memory issues...")
print(f"Memory-efficient chunked processing (10M records per chunk)\n")

success_count = 0
failed_count = 0

for file_info in files_to_convert:
    source = Path(file_info['source'])
    dest = Path(file_info['dest'])
    
    if not source.exists():
        print(f"ERROR: Source file not found: {source}")
        failed_count += 1
        continue
    
    if dest.exists():
        print(f"SKIP: Destination already exists: {dest}")
        success_count += 1
        continue
    
    print(f"\nProcessing: {source.name}")
    print(f"  Source: {source}")
    print(f"  Dest: {dest}")
    
    try:
        chunk_size = 10_000_000
        chunk_num = 0
        
        with gzip.open(source, 'rt', encoding='utf-8') as f:
            while True:
                chunk_num += 1
                print(f"  Reading chunk {chunk_num} (up to {chunk_size:,} records)...")
                
                chunk_data = []
                for i, line in enumerate(f):
                    if i >= chunk_size:
                        break
                    
                    try:
                        parts = line.strip().split(' ', 2)
                        if len(parts) < 3:
                            continue
                        
                        urlkey = parts[0]
                        timestamp = parts[1]
                        json_str = parts[2]
                        
                        obj = json.loads(json_str)
                        
                        chunk_data.append({
                            'urlkey': urlkey,
                            'timestamp': timestamp,
                            'url': obj.get('url', ''),
                            'mime': obj.get('mime', ''),
                            'mime_detected': obj.get('mime-detected', ''),
                            'status': obj.get('status', ''),
                            'digest': obj.get('digest', ''),
                            'length': obj.get('length', ''),
                            'offset': obj.get('offset', ''),
                            'filename': obj.get('filename', '')
                        })
                    except Exception as e:
                        continue
                
                if not chunk_data:
                    break
                
                print(f"  Sorting chunk {chunk_num} ({len(chunk_data):,} records)...")
                chunk_data.sort(key=lambda x: (x['urlkey'], x['timestamp']))
                
                table = pa.Table.from_pylist(chunk_data, schema=schema)
                
                if chunk_num == 1:
                    print(f"  Writing chunk {chunk_num}...")
                    pq.write_table(table, dest, compression='snappy')
                else:
                    print(f"  Appending chunk {chunk_num}...")
                    pq.write_to_dataset(table, root_path=str(dest.parent),
                                       basename_template=dest.name,
                                       existing_data_behavior='overwrite_or_ignore')
                
                print(f"  Chunk {chunk_num} complete")
                
                if len(chunk_data) < chunk_size:
                    break
        
        print(f"✓ Successfully converted: {dest.name}")
        success_count += 1
        
    except Exception as e:
        print(f"✗ FAILED: {source.name}")
        print(f"  Error: {e}")
        failed_count += 1

print("\n" + "="*60)
print("Conversion complete!")
print(f"  Success: {success_count}/3")
print(f"  Failed: {failed_count}/3")
print("="*60)

sys.exit(0 if failed_count == 0 else 1)
