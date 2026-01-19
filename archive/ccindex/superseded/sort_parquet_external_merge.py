#!/usr/bin/env python3
"""
External merge sort for large parquet files.
Uses disk-based sorting with minimal memory and high CPU utilization.

Strategy:
1. Split input into sorted chunks (using row groups)
2. Multi-way merge of sorted chunks with parallel I/O
3. Uses minimal memory per worker (~1-2GB)
4. Scales with CPU cores instead of RAM
"""

import argparse
import heapq
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Tuple, Iterator
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import psutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ExternalMergeSorter:
    """
    External merge sort for parquet files.
    Sorts data in small chunks and merges them on disk.
    """
    
    def __init__(self, input_file: Path, output_file: Path, 
                 temp_dir: Path, chunk_size_mb: int = 500):
        self.input_file = input_file
        self.output_file = output_file
        self.temp_dir = temp_dir
        self.chunk_size_rows = (chunk_size_mb * 1024 * 1024) // 200  # ~200 bytes per row estimate
        
        # Create temp directory
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
    def sort(self) -> bool:
        """
        Execute external merge sort.
        Returns True on success.
        """
        try:
            # Phase 1: Create sorted chunks
            logger.info(f"Phase 1: Creating sorted chunks from {self.input_file.name}")
            chunk_files = self._create_sorted_chunks()
            
            if not chunk_files:
                logger.error("No chunks created")
                return False
            
            logger.info(f"Created {len(chunk_files)} sorted chunks")
            
            # Phase 2: Multi-way merge
            logger.info(f"Phase 2: Merging {len(chunk_files)} chunks")
            success = self._merge_chunks(chunk_files)
            
            # Cleanup
            for chunk in chunk_files:
                try:
                    chunk.unlink()
                except:
                    pass
            
            return success
            
        except Exception as e:
            logger.error(f"Sort failed: {e}")
            return False
    
    def _create_sorted_chunks(self) -> List[Path]:
        """
        Read input file in chunks, sort each chunk, write to temp files.
        Returns list of sorted chunk files.
        """
        chunk_files = []
        
        try:
            parquet_file = pq.ParquetFile(self.input_file)
            
            # Process each row group
            for i, row_group in enumerate(parquet_file.iter_batches(batch_size=self.chunk_size_rows)):
                chunk_path = self.temp_dir / f"chunk_{i:04d}.parquet"
                
                # Convert to table and sort
                table = pa.Table.from_batches([row_group])
                
                # Sort by host_rev, url, ts
                indices = pc.sort_indices(
                    table,
                    sort_keys=[
                        ("host_rev", "ascending"),
                        ("url", "ascending"),
                        ("ts", "ascending")
                    ]
                )
                sorted_table = pc.take(table, indices)
                
                # Write sorted chunk
                pq.write_table(sorted_table, chunk_path, compression='snappy')
                chunk_files.append(chunk_path)
                
                if i % 10 == 0:
                    logger.info(f"  Created chunk {i+1}: {len(sorted_table)} rows")
            
            return chunk_files
            
        except Exception as e:
            logger.error(f"Failed to create chunks: {e}")
            # Cleanup partial chunks
            for chunk in chunk_files:
                try:
                    chunk.unlink()
                except:
                    pass
            return []
    
    def _merge_chunks(self, chunk_files: List[Path]) -> bool:
        """
        Multi-way merge of sorted chunk files.
        Uses heap-based merge with minimal memory.
        """
        try:
            # Open all chunk files
            readers = []
            for chunk_file in chunk_files:
                try:
                    pf = pq.ParquetFile(chunk_file)
                    readers.append(pf)
                except Exception as e:
                    logger.error(f"Failed to open chunk {chunk_file}: {e}")
                    return False
            
            # Initialize iterators for each chunk
            iterators = []
            for reader in readers:
                try:
                    it = reader.iter_batches(batch_size=10000)
                    iterators.append(it)
                except Exception as e:
                    logger.error(f"Failed to create iterator: {e}")
                    return False
            
            # Create heap for k-way merge
            # Each heap entry: (sort_key, chunk_index, row_index_in_batch, batch)
            heap = []
            current_batches = [None] * len(iterators)
            batch_positions = [0] * len(iterators)
            
            # Initialize heap with first row from each chunk
            for i, it in enumerate(iterators):
                try:
                    batch = next(it)
                    current_batches[i] = batch
                    if len(batch) > 0:
                        row = self._get_row_dict(batch, 0)
                        sort_key = self._make_sort_key(row)
                        heapq.heappush(heap, (sort_key, i, 0, batch))
                except StopIteration:
                    pass
                except Exception as e:
                    logger.error(f"Failed to initialize heap for chunk {i}: {e}")
            
            # Merge to output
            writer = None
            output_schema = None
            rows_written = 0
            batch_buffer = []
            WRITE_BATCH_SIZE = 100000
            
            while heap:
                # Get minimum element
                sort_key, chunk_idx, row_idx, batch = heapq.heappop(heap)
                
                # Extract row
                row_dict = self._get_row_dict(batch, row_idx)
                batch_buffer.append(row_dict)
                rows_written += 1
                
                # Write batch if buffer is full
                if len(batch_buffer) >= WRITE_BATCH_SIZE:
                    if writer is None:
                        output_schema = batch.schema
                        writer = pq.ParquetWriter(self.output_file, output_schema, compression='snappy')
                    
                    output_batch = pa.Table.from_pylist(batch_buffer, schema=output_schema)
                    writer.write_table(output_batch)
                    batch_buffer = []
                    
                    if rows_written % 1000000 == 0:
                        logger.info(f"  Merged {rows_written:,} rows")
                
                # Get next row from same chunk
                next_row_idx = row_idx + 1
                if next_row_idx < len(batch):
                    # More rows in current batch
                    row = self._get_row_dict(batch, next_row_idx)
                    sort_key = self._make_sort_key(row)
                    heapq.heappush(heap, (sort_key, chunk_idx, next_row_idx, batch))
                else:
                    # Need next batch from this chunk
                    try:
                        next_batch = next(iterators[chunk_idx])
                        if len(next_batch) > 0:
                            row = self._get_row_dict(next_batch, 0)
                            sort_key = self._make_sort_key(row)
                            heapq.heappush(heap, (sort_key, chunk_idx, 0, next_batch))
                    except StopIteration:
                        pass
            
            # Write remaining rows
            if batch_buffer:
                if writer is None:
                    # Handle case where all chunks were empty
                    logger.warning("No data to write")
                    return False
                
                output_batch = pa.Table.from_pylist(batch_buffer, schema=output_schema)
                writer.write_table(output_batch)
            
            if writer:
                writer.close()
            
            logger.info(f"  Merge complete: {rows_written:,} total rows")
            return True
            
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            return False
    
    def _get_row_dict(self, batch: pa.RecordBatch, idx: int) -> dict:
        """Extract row as dictionary from batch."""
        return {
            col: batch.column(col)[idx].as_py()
            for col in batch.schema.names
        }

    def _make_sort_key(self, row: dict) -> tuple:
        """Return a heap-comparable sort key matching Arrow's null-last semantics.

        We sort by (host_rev, url, ts). Some rows can contain NULLs; Python cannot
        compare None with strings, so we normalize to (is_null, value) pairs.
        """

        def key_part(value: object) -> tuple[int, str]:
            if value is None:
                return (1, "")  # nulls last
            return (0, str(value))

        return (
            key_part(row.get("host_rev")),
            key_part(row.get("url")),
            key_part(row.get("ts")),
        )


def sort_file_external(args: Tuple[Path, Path, Path, int]) -> Tuple[Path, bool, str]:
    """
    Sort a single parquet file using external merge sort.
    Returns (file, success, message)
    """
    input_file, output_dir, temp_base, worker_id = args
    
    try:
        # Create worker-specific temp directory
        worker_temp = temp_base / f"worker_{worker_id}"
        worker_temp.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{input_file.stem}.sorted{input_file.suffix}"
        
        # Check if already sorted
        if output_file.exists():
            return (input_file, True, "Already sorted")
        
        # Create temporary output
        temp_output = worker_temp / f"{input_file.name}.tmp"
        
        # Execute external merge sort
        sorter = ExternalMergeSorter(
            input_file=input_file,
            output_file=temp_output,
            temp_dir=worker_temp / "chunks",
            chunk_size_mb=500  # 500MB chunks
        )
        
        success = sorter.sort()
        
        if success and temp_output.exists():
            # Move to final location
            temp_output.rename(output_file)
            return (input_file, True, f"Sorted successfully")
        else:
            return (input_file, False, "Sort failed")
            
    except Exception as e:
        return (input_file, False, f"Error: {e}")
    finally:
        # Cleanup worker temp
        try:
            if worker_temp.exists():
                shutil.rmtree(worker_temp)
        except:
            pass


def find_unsorted_files(input_dir: Path, output_dir: Path, collection: str = None) -> List[Path]:
    """Find parquet files that need sorting."""
    unsorted = []
    
    if collection:
        search_dir = input_dir / collection
        if not search_dir.exists():
            logger.warning(f"Collection directory not found: {search_dir}")
            return []
    else:
        search_dir = input_dir
    
    for parquet_file in search_dir.rglob("*.gz.parquet"):
        # Check if .sorted version exists
        if collection:
            relative = parquet_file.relative_to(search_dir)
            sorted_file = output_dir / collection / f"{relative.stem}.sorted{relative.suffix}"
        else:
            sorted_file = output_dir / f"{parquet_file.stem}.sorted{parquet_file.suffix}"
        
        if not sorted_file.exists():
            unsorted.append(parquet_file)
    
    return unsorted


def main():
    parser = argparse.ArgumentParser(
        description="External merge sort for large parquet files"
    )
    parser.add_argument(
        '--input-dir',
        type=Path,
        default=Path('/storage/ccindex_parquet'),
        help='Input directory containing parquet files'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('/storage/ccindex_parquet_sorted'),
        help='Output directory for sorted files'
    )
    parser.add_argument(
        '--temp-dir',
        type=Path,
        default=Path('/storage/ccindex_parquet_sorted/.temp'),
        help='Temporary directory for chunks'
    )
    parser.add_argument(
        '--collection',
        type=str,
        help='Process specific collection (e.g., CC-MAIN-2024-10)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=max(1, mp.cpu_count() // 2),
        help='Number of parallel workers'
    )
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Find files to sort
    logger.info(f"Scanning for unsorted files in {args.input_dir}")
    if args.collection:
        logger.info(f"Filtering for collection: {args.collection}")
    
    unsorted_files = find_unsorted_files(args.input_dir, args.output_dir, args.collection)
    
    if not unsorted_files:
        logger.info("No unsorted files found")
        return 0
    
    logger.info(f"Found {len(unsorted_files)} files to sort")
    logger.info(f"Using {args.workers} parallel workers")
    logger.info(f"Memory per worker: ~1-2GB")
    
    # Prepare work items
    work_items = [
        (f, args.output_dir, args.temp_dir, i % args.workers)
        for i, f in enumerate(unsorted_files)
    ]
    
    # Process in parallel
    start_time = time.time()
    completed = 0
    failed = 0
    
    with mp.Pool(processes=args.workers) as pool:
        for file, success, message in pool.imap_unordered(sort_file_external, work_items):
            if success:
                completed += 1
                logger.info(f"✓ [{completed}/{len(unsorted_files)}] {file.name}: {message}")
            else:
                failed += 1
                logger.error(f"✗ [{completed+failed}/{len(unsorted_files)}] {file.name}: {message}")
    
    elapsed = time.time() - start_time
    
    logger.info("=" * 80)
    logger.info(f"Sort complete:")
    logger.info(f"  Completed: {completed}/{len(unsorted_files)}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Time: {elapsed/60:.1f} minutes")
    logger.info(f"  Rate: {completed/(elapsed/60):.1f} files/minute")
    
    # Cleanup temp directory
    try:
        shutil.rmtree(args.temp_dir)
    except:
        pass
    
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
