# Orchestrator Refactoring Analysis

## Current Issues in cc_pipeline_orchestrator.py

### 1. Download Phase (line 122-137)
**Current:** Calls `bash download_cc_indexes.sh collection`
**Problem:** This script doesn't exist with that interface
**Should use:** 
- `download_cc_indexes_1year.sh` or `download_cc_indexes_2years.sh`
- OR better: use the downloader from `cc_pipeline_manager.py` (line 275)

### 2. Convert Phase (line 139-163)
**Current:** Calls `bulk_convert_gz_to_parquet.py` with specific args
**Better:** Should check which script to use:
- `bulk_convert_gz_to_parquet.py` - for batch conversion
- `parallel_convert_missing.py` - for missing files
- `regenerate_parquet_from_gz.py` - for regeneration

### 3. Sort Phase (line 165-204)
**Current:** Calls both `parallel_validate_parquet.py` then `sort_unsorted_memory_aware.py`
**Good!** This is correct - uses existing scripts

### 4. Index Build Phase (line 206-237)
**Current:** Calls `build_cc_pointer_duckdb.py`
**Better:** Should use the parallel system:
- `launch_cc_pointer_build.py` - launches parallel builds
- `queue_cc_pointer_build.py` - queues builds
- `build_duckdb_from_sorted_parquet.py` - builds from sorted files

### 5. Status Scanning (line 241)
**Current:** Calls `self.scan_collection_status(collection)` - NOT DEFINED!
**Should use:** `self.validator.validate_collection(collection)` from validate_collection_completeness.py

## Scripts Being Duplicated vs Scripts We Should Use

### Download Scripts (should reuse):
- `download_cc_indexes_1year.sh`
- `download_cc_indexes_2years.sh`
- `download_cc_indexes_5years.sh`

### Conversion Scripts (should reuse):
- `bulk_convert_gz_to_parquet.py` ✓ (already using)
- `parallel_convert_missing.py` (for missing files)
- `regenerate_parquet_from_gz.py` (for corrupted files)

### Sorting Scripts (should reuse):
- `validate_and_mark_sorted.py` (validates+marks)
- `parallel_validate_parquet.py` ✓ (already using)
- `sort_unsorted_memory_aware.py` ✓ (already using)

### Indexing Scripts (should reuse):
- `launch_cc_pointer_build.py` (launches parallel)
- `build_cc_pointer_duckdb.py` ✓ (already using)
- `build_duckdb_from_sorted_parquet.py` (alternative)

### Monitoring Scripts (should integrate):
- `validate_collection_completeness.py` ✓ (already using)
- `cc_pipeline_watch.py` (for live monitoring)
- `cc_pipeline_hud.py` (for interactive display)

## Key Missing Method
Line 241 calls `self.scan_collection_status(collection)` which doesn't exist!
Should be `self.validator.validate_collection(collection)`
