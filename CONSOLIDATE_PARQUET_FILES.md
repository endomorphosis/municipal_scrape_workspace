# Parquet File Consolidation Plan

## Problem
Parquet files are scattered across multiple directory structures:
- `/storage/ccindex_parquet/cc_pointers_by_year/2024/CC-MAIN-2024-XX/`
- `/storage/ccindex_parquet/cc_pointers_by_collection/2024/CC-MAIN-2024-XX/`
- `/storage/ccindex_parquet/2024/CC-MAIN-2024-XX/`
- `/storage/ccindex_parquet/CC-MAIN-2024-XX/` (flat)

## Solution
Use a single, consistent structure:
```
/storage/ccindex_parquet/
  └── CC-MAIN-2024-10/
      ├── cdx-00000.gz.parquet
      ├── cdx-00000.gz.parquet.sorted
      ├── cdx-00001.gz.parquet  
      ├── cdx-00001.gz.parquet.sorted
      ...
```

## File Naming Convention
- Unsorted: `cdx-XXXXX.gz.parquet`
- Sorted: `cdx-XXXXX.gz.parquet.sorted`
- Sort appends `.sorted` extension (no moving to different directory)

## Index Structure
```
/storage/ccindex_duckdb/
  └── cc_pointers_by_collection/
      ├── CC-MAIN-2024-10.duckdb
      ├── CC-MAIN-2024-10.duckdb.sorted  (marker file)
      └── ...
  └── cc_pointers_by_year/
      ├── 2024.duckdb  (meta-index of all 2024 collections)
      └── 2025.duckdb
  └── cc_pointers_master/
      └── master.duckdb  (meta-index of all years)
```

## Migration Steps
1. Find all parquet files across all locations
2. Move to flat structure under `/storage/ccindex_parquet/CC-MAIN-*/`
3. Preserve `.sorted` status
4. Update orchestrator to use consistent paths
5. Rebuild indexes from consolidated files
