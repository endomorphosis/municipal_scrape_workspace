#!/usr/bin/env python3
"""
Bulk convert Common Crawl .gz indexes to .gz.parquet format

Converts all .gz files in a collection directory to parquet format in parallel.
"""

import argparse
import gzip
import json
import logging
import multiprocessing
import sys
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def _coerce_int(value: object) -> Optional[int]:
    """Best-effort conversion of CC index numeric fields.

    Common Crawl CDX JSON often represents numeric fields as strings (e.g. "200").
    We normalize those to Python ints so Arrow can write typed parquet reliably.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        # Avoid treating booleans as ints for these fields.
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # Some parsers may yield floats; only accept integral floats.
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text == "-":
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def convert_gz_to_parquet(gz_path: Path, output_path: Path, chunk_size: int = 100000) -> bool:
    """Convert a single .gz file to parquet"""
    try:
        logger.info(f"Converting {gz_path.name}...")
        
        # Read and parse .gz file
        rows = []
        with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split(' ', 2)
                if len(parts) < 3:
                    continue
                
                surt, timestamp, rest = parts
                try:
                    json_data = json.loads(rest)
                except json.JSONDecodeError:
                    continue
                
                rows.append({
                    'surt': surt,
                    'timestamp': timestamp,
                    'url': json_data.get('url'),
                    'status': _coerce_int(json_data.get('status')),
                    'mime': json_data.get('mime'),
                    'digest': json_data.get('digest'),
                    'warc_filename': json_data.get('filename'),
                    'warc_offset': _coerce_int(json_data.get('offset')),
                    'warc_length': _coerce_int(json_data.get('length')),
                })
        
        if not rows:
            logger.warning(f"No valid rows in {gz_path.name}")
            return False
        
        # Create Arrow table
        schema = pa.schema([
            ('surt', pa.string()),
            ('timestamp', pa.string()),
            ('url', pa.string()),
            ('status', pa.int32()),
            ('mime', pa.string()),
            ('digest', pa.string()),
            ('warc_filename', pa.string()),
            ('warc_offset', pa.int64()),
            ('warc_length', pa.int64())
        ])
        
        table = pa.Table.from_pylist(rows, schema=schema)
        
        # Write parquet file
        pq.write_table(
            table,
            output_path,
            compression='zstd',
            compression_level=3
        )
        
        logger.info(f"✓ Converted {gz_path.name} ({len(rows)} rows)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to convert {gz_path.name}: {e}")
        return False


def convert_collection(
    input_dir: Path,
    output_dir: Path,
    workers: int = 4,
    skip_existing: bool = True
) -> tuple[int, int]:
    """Convert all .gz files in a collection directory"""
    
    # Find all .gz files
    gz_files = sorted(input_dir.glob("cdx-*.gz"))
    if not gz_files:
        logger.warning(f"No .gz files found in {input_dir}")
        return 0, 0
    
    logger.info(f"Found {len(gz_files)} .gz files in {input_dir}")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare work list
    work = []
    for gz_file in gz_files:
        output_file = output_dir / f"{gz_file.name}.parquet"
        sorted_output_file = output_dir / f"{gz_file.name}.sorted.parquet"

        # Treat already-sorted outputs as "already converted" to avoid duplicating work.
        if skip_existing and (output_file.exists() or sorted_output_file.exists()):
            if sorted_output_file.exists() and not output_file.exists():
                logger.info(f"Skipping existing sorted {sorted_output_file.name}")
            else:
                logger.info(f"Skipping existing {output_file.name}")
            continue
        work.append((gz_file, output_file))
    
    if not work:
        logger.info("All files already converted")
        return len(gz_files), len(gz_files)
    
    logger.info(f"Converting {len(work)} files with {workers} workers...")
    
    # Convert in parallel
    with multiprocessing.Pool(workers) as pool:
        results = pool.starmap(
            convert_gz_to_parquet,
            [(gz, out, 100000) for gz, out in work]
        )
    
    success_count = sum(1 for r in results if r)
    logger.info(f"Converted {success_count}/{len(work)} files successfully")
    
    return len(gz_files), success_count


def main():
    parser = argparse.ArgumentParser(description="Bulk convert CC .gz to parquet")
    parser.add_argument("--input-dir", type=Path, required=True, help="Input directory with .gz files")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for parquet files")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing parquet files")
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        logger.error(f"Input directory does not exist: {args.input_dir}")
        return 1
    
    total, success = convert_collection(
        args.input_dir,
        args.output_dir,
        workers=args.workers,
        skip_existing=not args.overwrite
    )
    
    logger.info(f"Final: {success}/{total} files converted")
    return 0 if success == total else 1


if __name__ == "__main__":
    sys.exit(main())
