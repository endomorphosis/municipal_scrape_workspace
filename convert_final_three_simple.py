#!/usr/bin/env python3
"""
Convert the final 3 missing parquet files - simple approach with all data in memory
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

print("Converting final 3 files (simple in-memory approach)...\n")

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
        print(f"  Reading and parsing all records...")
        all_data = []
        
        with gzip.open(source, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line_num % 1_000_000 == 0:
                    print(f"    ... {line_num:,} lines read")
                
                try:
                    parts = line.strip().split(' ', 2)
                    if len(parts) < 3:
                        continue
                    
                    urlkey = parts[0]
                    timestamp = parts[1]
                    json_str = parts[2]
                    
                    obj = json.loads(json_str)
                    
                    all_data.append({
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
        
        print(f"  Total records parsed: {len(all_data):,}")
        print(f"  Sorting by (urlkey, timestamp)...")
        all_data.sort(key=lambda x: (x['urlkey'], x['timestamp']))
        
        print(f"  Writing sorted parquet file...")
        table = pa.Table.from_pylist(all_data, schema=schema)
        pq.write_table(table, dest, compression='snappy')
        
        file_size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"✓ Successfully converted: {dest.name} ({file_size_mb:.1f} MB)")
        success_count += 1
        
    except Exception as e:
        print(f"✗ FAILED: {source.name}")
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        failed_count += 1

print("\n" + "="*60)
print("Conversion complete!")
print(f"  Success: {success_count}/3")
print(f"  Failed: {failed_count}/3")
print("="*60)

sys.exit(0 if failed_count == 0 else 1)
