# Orchestrator Fixes - 2026-01-12

## Problems Identified
1. **Scattered parquet files** across multiple directory structures
2. **Inconsistent path handling** between convert, sort, and index operations
3. **Wrong sorted file extension** - was using `.sorted.parquet` instead of `.parquet.sorted`
4. **No proper resume logic** - couldn't track progress when interrupted
5. **Status reporting bugs** - showing `exists=False` when files clearly exist

## Solutions Implemented

### 1. Consolidated Directory Structure
**New flat structure:**
```
/storage/ccindex_parquet/
  └── CC-MAIN-2024-10/
      ├── cdx-00000.gz.parquet          (unsorted)
      ├── cdx-00000.gz.parquet.sorted   (sorted)
      └── ...
```

**Benefits:**
- Single source of truth for file locations
- Easy to verify completeness
- Proper resume support
- Clear sort status via `.sorted` extension

### 2. Fixed Orchestrator Paths
- `convert_collection()`: Now writes to `/storage/ccindex_parquet/{collection}/`
- `sort_collection()`: Reads from same location, appends `.sorted` extension
- `build_index_for_collection()`: Reads from same location

### 3. Created Consolidation Script
`consolidate_parquet_files.py` - Migrates scattered files to new structure:
```bash
# Dry run (shows what would happen)
python consolidate_parquet_files.py --dry-run

# Actually consolidate
python consolidate_parquet_files.py

# Consolidate specific collection
python consolidate_parquet_files.py --collection CC-MAIN-2024-10
```

### 4. File Naming Convention
- **Unsorted**: `cdx-XXXXX.gz.parquet`
- **Sorted**: `cdx-XXXXX.gz.parquet.sorted`
- Sorting appends `.sorted` (doesn't move or rename base file)

### 5. Resume Logic
- Convert checks for existing `.gz.parquet` files - skips them
- Sort checks for existing `.gz.parquet.sorted` files - skips them
- Index checks for existing `.duckdb` files - can rebuild or skip

## Migration Steps

1. **Run consolidation** (DRY RUN first!):
   ```bash
   python consolidate_parquet_files.py --dry-run
   python consolidate_parquet_files.py
   ```

2. **Verify with HUD**:
   ```bash
   python cc_pipeline_watch.py
   ```

3. **Run orchestrator**:
   ```bash
   python cc_pipeline_orchestrator.py --filter 2024 --workers 8
   ```

## Testing Checklist
- [ ] Consolidation script moves files correctly
- [ ] No data loss during consolidation
- [ ] Orchestrator uses consistent paths
- [ ] Sort properly appends `.sorted` extension
- [ ] Resume works after interruption
- [ ] Status reporting is accurate
- [ ] DuckDB indexes build correctly
- [ ] Search works across all indexes

## Configuration
Updated `pipeline_config.json` structure:
```json
{
  "ccindex_root": "/storage/ccindex",
  "parquet_root": "/storage/ccindex_parquet",
  "duckdb_collection_root": "/storage/ccindex_duckdb/cc_pointers_by_collection",
  "duckdb_year_root": "/storage/ccindex_duckdb/cc_pointers_by_year",
  "duckdb_master_root": "/storage/ccindex_duckdb/cc_pointers_master",
  "max_workers": 8,
  "memory_limit_gb": 10.0,
  "min_free_space_gb": 50.0
}
```
