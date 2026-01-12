# Common Crawl Pipeline Orchestrator

## Overview
The orchestrator (`cc_pipeline_orchestrator.py`) is the unified control system that manages the entire Common Crawl index processing pipeline. It replaces the older 1-year, 2-year, and 5-year scripts with a single, resumable, resource-aware system.

## Architecture

### Integration with Existing Scripts
The orchestrator **reuses** existing scripts instead of reimplementing functionality:

1. **Status & Validation**: `validate_collection_completeness.py`
2. **Monitoring**: `cc_pipeline_watch.py` and `cc_pipeline_hud.py`
3. **Conversion**: `bulk_convert_gz_to_parquet.py`
4. **Sorting**: `parallel_validate_parquet.py` + `sort_unsorted_memory_aware.py`
5. **Indexing**: `build_cc_pointer_duckdb.py`

## Pipeline Stages

### Stage 1: Download
- Downloads `.tar.gz` files from Common Crawl
- Stores in `/storage/ccindex/{collection}/`
- Uses existing download scripts
- Validates integrity

### Stage 2: Convert to Parquet (with optional immediate sorting)
- Converts `.gz` files to `.gz.parquet`
- Stores in `/storage/ccindex_parquet/{year}/{collection}/`
- Uses `bulk_convert_gz_to_parquet.py`
- Parallel processing with worker pool
- **NEW**: Can immediately sort after conversion to pipeline files efficiently

### Stage 3: Sort by Domain
- Validates if files are already sorted (checks file contents, not just extension)
- Sorts unsorted files by domain name using external merge sort
- Marks sorted files with `.sorted.parquet` extension
- Uses `parallel_validate_parquet.py` + `sort_parquet_external_merge.py`
- CPU-optimized with low memory usage for large files

### Stage 4: Build DuckDB Pointer Index
- Creates pointer index with offset+range for fast lookups
- Stores in `/storage/ccindex_duckdb/cc_pointers_by_collection/{collection}.duckdb`
- Uses `build_cc_pointer_duckdb.py`
- Marks complete indexes with `.sorted` extension

## Usage

### Basic Usage
```bash
# Process all collections with defaults
python cc_pipeline_orchestrator.py

# Process only 2024 collections
python cc_pipeline_orchestrator.py --filter 2024

# Process specific collection
python cc_pipeline_orchestrator.py --filter CC-MAIN-2025-05

# Adjust parallelism and memory
python cc_pipeline_orchestrator.py --workers 8 --memory-limit-gb 10.0
```

### Resource Management
```bash
# Conservative settings for low memory
python cc_pipeline_orchestrator.py \
    --workers 2 \
    --memory-limit-gb 5.0 \
    --min-free-space-gb 100.0

# Aggressive settings for high-end system
python cc_pipeline_orchestrator.py \
    --workers 16 \
    --memory-limit-gb 2.0 \
    --min-free-space-gb 50.0
```

## Monitoring

### Real-time Monitoring
```bash
# Watch pipeline progress with live HUD
python cc_pipeline_watch.py

# Interactive curses-based HUD
python cc_pipeline_hud.py
```

### Status Checks
```bash
# Check collection completeness
python validate_collection_completeness.py

# CI/CD-friendly output
python cc_pipeline_watch.py --print-only
```

## Features

### Resumability
- Automatically detects completed work
- Skips already-processed collections
- Resumes from last incomplete stage
- Safe to stop and restart at any time

### Resource Awareness
- Monitors available memory
- Checks free disk space
- Respects worker limits
- Prevents OOM situations

### Integrity Checking
- Validates downloaded files
- Checks parquet file integrity
- Verifies sorting correctness
- Ensures index completeness

### Parallel Processing
- Multi-worker conversion
- Parallel sorting validation
- Concurrent index building
- Memory-aware scheduling

## Directory Structure

```
/storage/
├── ccindex/                    # Downloaded .tar.gz files
│   ├── CC-MAIN-2024-10/
│   ├── CC-MAIN-2024-18/
│   └── ...
├── ccindex_parquet/           # Converted parquet files
│   ├── 2024/
│   │   ├── CC-MAIN-2024-10/
│   │   │   ├── cdx-00000.gz.parquet
│   │   │   ├── cdx-00000.gz.parquet.sorted
│   │   │   └── ...
│   └── 2025/
│       └── CC-MAIN-2025-05/
└── ccindex_duckdb/            # DuckDB pointer indexes
    └── cc_pointers_by_collection/
        ├── CC-MAIN-2024-10.duckdb
        ├── CC-MAIN-2024-10.duckdb.sorted
        └── ...
```

## Collection Status

Each collection goes through these states:

1. **Not Started**: No files downloaded
2. **Downloading**: `.tar.gz` files being fetched
3. **Downloaded**: All `.tar.gz` files present
4. **Converting**: Converting to `.gz.parquet`
5. **Converted**: All parquet files created
6. **Sorting**: Validating and sorting parquet files
7. **Sorted**: All parquet files sorted by domain
8. **Indexing**: Building DuckDB pointer index
9. **Complete**: Index built and marked sorted

## Troubleshooting

### Collection Won't Complete
```bash
# Check detailed status
python validate_collection_completeness.py | grep "CC-MAIN-2025-05"

# Regenerate corrupted parquet files
python regenerate_parquet_from_gz.py /storage/ccindex/CC-MAIN-2025-05

# Manually sort if needed
python sort_unsorted_memory_aware.py /storage/ccindex_parquet/2025/CC-MAIN-2025-05
```

### Out of Memory
```bash
# Reduce workers and increase memory limit
python cc_pipeline_orchestrator.py --workers 2 --memory-limit-gb 8.0

# Or sort sequentially
python sort_unsorted_sequential.sh
```

### Disk Space Issues
```bash
# Clean up old snapshots
sudo zfs list -t snapshot
sudo zfs destroy storage/ccindex@auto_2025-01-10_daily

# Or increase minimum free space threshold
python cc_pipeline_orchestrator.py --min-free-space-gb 200.0
```

## Integration with Other Tools

### Search Tools
After indexing is complete, use these tools to search:
```bash
# Search for domain across all indexes
python search_parallel_duckdb_indexes.py "example.gov"

# Benchmark search performance
python benchmark_parallel_duckdb_indexes.py "example.gov"
```

### Validation Tools
```bash
# Validate all parquet files are sorted
python validate_urlindex_sorted.py

# Check search completeness
python validate_search_completeness.py "example.gov"
```

## Performance Notes

- **Download**: ~1-2 hours per collection (network dependent)
- **Convert**: ~30-60 minutes per collection (300 files, 8 workers)
- **Sort**: ~2-4 hours per collection (depends on memory)
- **Index**: ~10-20 minutes per collection

## Future Enhancements

1. **Incremental Updates**: Detect new collections automatically
2. **Distributed Processing**: Split work across multiple machines
3. **Cloud Integration**: S3/GCS support for storage
4. **Advanced Scheduling**: Priority queues for collections
5. **Metrics Dashboard**: Web-based monitoring interface

