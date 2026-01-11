# Collection Tracking Feature

## Overview
Added chronological collection tracking to the pipeline monitor (`cc_pipeline_watch.py`) to help identify which Common Crawl collections have been completed and which are missing.

## What Was Added

### 1. Collection Status Fetching
- Fetches collection info from `https://index.commoncrawl.org/collinfo.json`
- Caches results for 1 hour to avoid hammering the API
- Shows collection ID, name, and date ranges

### 2. Per-Collection Progress Tracking
For each collection, tracks:
- **Downloaded**: Number of `.gz` index shards downloaded
- **Converted**: Number of `.gz.parquet` files created
- **Sorted**: Number of sorted parquet files (has `.sorted` marker)
- **Indexed**: Whether a DuckDB pointer index exists for this collection
- **Total Shards**: Estimated from the highest count we've seen

### 3. Display Format
Shows recent 2024-2025 collections in a table:
```
COLLECTION STATUS (Recent 2024-2025):
  Collection ID        Downloaded   Converted    Sorted     Indexed 
  -------------------- ------------ ------------ ---------- --------
  CC-MAIN-2025-51      300/300      300/300      0/300      Yes     
  CC-MAIN-2025-47      300/300      300/300      0/300      Yes     
```

## Usage

### Print Once (CI/CD Mode)
```bash
python cc_pipeline_watch.py --once
```

### Live Monitoring
```bash
python cc_pipeline_watch.py
```

## What This Shows

### Current Status (as of test run):
- ✅ **2024-2025 Collections**: Mostly downloaded and converted
- ⚠️ **Sorting**: 0 files sorted (needs attention!)
- ✅ **Indexing**: Most collections have pointer indexes
- ⚠️ **Incomplete Collections**: 
  - CC-MAIN-2024-51: 299/300 converted (1 missing)
  - CC-MAIN-2024-46: 298/300 converted (2 missing)

### Next Steps Indicated:
1. **Sort all parquet files** - Currently 7,315 unsorted files
2. **Complete missing conversions** - Fix the 2-3 missing files
3. **Verify integrity** - Some collections show slight mismatches

## Technical Details

### How Collection Detection Works
1. Scans directory structure looking for `CC-MAIN-*` patterns in paths
2. Counts files within each collection directory
3. Checks for `.sorted` marker files
4. Searches for DuckDB files matching collection IDs

### File Organization Expected
```
/storage/ccindex/CC-MAIN-2025-51/cdx-00000.gz
/storage/ccindex_parquet/CC-MAIN-2025-51/cdx-00000.gz.parquet
/storage/ccindex_parquet/CC-MAIN-2025-51/cdx-00000.gz.parquet.sorted
/storage/ccindex_duckdb/cc_pointers_by_collection/CC-MAIN-2025-51.duckdb
```

## Dependencies
- `requests`: For fetching collinfo.json
- `psutil`: For system metrics
- Standard library: `json`, `pathlib`, `datetime`

## Future Enhancements
- Show older collections (2023, 2022) with flag
- Download progress percentage
- ETA for remaining work
- Health check warnings for stalled collections
