#!/usr/bin/env python3
"""
Regenerate a parquet file from original .gz source, with chunking for large files.
"""

import argparse
import gzip
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def parse_cdx_line(line: str) -> dict:
    """Parse a CDX line into a dict."""
    parts = line.strip().split()
    if len(parts) < 3:
        return None
    
    # Basic CDX format: surt url timestamp ...
    record = {
        'surt': parts[0] if len(parts) > 0 else None,
        'url': parts[1] if len(parts) > 1 else None,
        'ts': parts[2] if len(parts) > 2 else None,
    }
    
    # Parse additional fields (warc filename, offset, etc.)
    if len(parts) >= 9:
        record['warc_filename'] = parts[8]
    if len(parts) >= 10:
        record['warc_offset'] = parts[9]
    if len(parts) >= 11:
        record['warc_length'] = parts[10]
    
    # Extract host and host_rev from surt
    if record['surt']:
        # SURT format: reverse domain, e.g., "gov,whitehouse)/"
        host_rev = record['surt'].split(')')[0] if ')' in record['surt'] else record['surt']
        record['host_rev'] = host_rev
        
        # Convert back to normal host
        parts_rev = host_rev.replace(',', '.').split('.')
        record['host'] = '.'.join(reversed(parts_rev))
    
    return record


def regenerate_parquet(gz_file: Path, output_parquet: Path, chunk_size: int = 1_000_000) -> bool:
    """
    Regenerate parquet from .gz file in chunks to avoid OOM.
    """
    try:
        print(f"Reading: {gz_file}")
        print(f"Output:  {output_parquet}")
        print(f"Chunk size: {chunk_size:,} rows")
        print()
        
        # Create temp directory for chunks
        temp_dir = output_parquet.parent / f".temp_{output_parquet.stem}"
        temp_dir.mkdir(exist_ok=True)
        
        chunk_files = []
        current_chunk = []
        chunk_num = 0
        total_rows = 0
        
        # Read and parse .gz file in chunks
        with gzip.open(gz_file, 'rt') as f:
            for i, line in enumerate(f, 1):
                record = parse_cdx_line(line)
                if record:
                    current_chunk.append(record)
                    total_rows += 1
                
                # Write chunk when size reached
                if len(current_chunk) >= chunk_size:
                    chunk_file = temp_dir / f"chunk_{chunk_num:04d}.parquet"
                    
                    # Convert to Arrow table and write
                    table = pa.Table.from_pylist(current_chunk)
                    pq.write_table(table, chunk_file, compression='zstd')
                    
                    chunk_files.append(chunk_file)
                    print(f"  Chunk {chunk_num}: {len(current_chunk):,} rows -> {chunk_file.name}")
                    
                    current_chunk = []
                    chunk_num += 1
                
                if i % 1_000_000 == 0:
                    print(f"  Progress: {i:,} lines read, {total_rows:,} rows parsed")
        
        # Write final chunk
        if current_chunk:
            chunk_file = temp_dir / f"chunk_{chunk_num:04d}.parquet"
            table = pa.Table.from_pylist(current_chunk)
            pq.write_table(table, chunk_file, compression='zstd')
            chunk_files.append(chunk_file)
            print(f"  Chunk {chunk_num}: {len(current_chunk):,} rows -> {chunk_file.name}")
        
        print()
        print(f"Total: {total_rows:,} rows in {len(chunk_files)} chunks")
        print()
        
        # Now merge and sort chunks using DuckDB with memory limit
        print("Merging and sorting chunks...")
        
        con = duckdb.connect(":memory:")
        con.execute("SET memory_limit='2GB'")
        con.execute("SET temp_directory='/tmp'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=1")
        
        # Read all chunks, sort, and write final parquet
        chunk_pattern = str(temp_dir / "chunk_*.parquet")
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{chunk_pattern}')
                ORDER BY host_rev, url, ts
            )
            TO '{output_parquet}' (FORMAT 'parquet', COMPRESSION 'zstd')
        """)
        con.close()
        
        print(f"✅ Created: {output_parquet}")
        
        # Cleanup temp chunks
        for chunk_file in chunk_files:
            chunk_file.unlink()
        temp_dir.rmdir()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Regenerate parquet from .gz source")
    ap.add_argument("--gz-file", required=True, help="Original .gz file")
    ap.add_argument("--output", required=True, help="Output parquet file")
    ap.add_argument("--chunk-size", type=int, default=1_000_000, help="Rows per chunk")
    
    args = ap.parse_args()
    
    gz_file = Path(args.gz_file)
    output = Path(args.output)
    
    if not gz_file.exists():
        print(f"ERROR: Source file not found: {gz_file}")
        return 1
    
    # Backup existing parquet if it exists
    if output.exists():
        backup = output.with_suffix('.parquet.backup')
        print(f"Backing up existing file to: {backup}")
        output.rename(backup)
        print()
    
    success = regenerate_parquet(gz_file, output, args.chunk_size)
    
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
