# Parquet File Consolidation Plan

## Problem
Parquet files are scattered across multiple directory structures:
- `/storage/ccindex_parquet/cc_pointers_by_year/2024/CC-MAIN-2024-XX/` (legacy; safe to remove once migrated)
- `/storage/ccindex_parquet/cc_pointers_by_collection/2024/CC-MAIN-2024-XX/`
- `/storage/ccindex_parquet/2024/CC-MAIN-2024-XX/`
- `/storage/ccindex_parquet/CC-MAIN-2024-XX/` (flat)

## Solution
Use a single, consistent structure (canonical):
```
/storage/ccindex_parquet/
    └── cc_pointers_by_collection/
            └── 2024/
                    └── CC-MAIN-2024-10/
                            ├── cdx-00000.gz.sorted.parquet
                            ├── cdx-00001.gz.sorted.parquet
                            └── ...
```

## File Naming Convention
- Unsorted: `cdx-XXXXX.gz.parquet`
- Sorted: `cdx-XXXXX.gz.sorted.parquet`
- Sort writes the `.gz.sorted.parquet` file (and may also create a tiny `.parquet.sorted` marker)

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
2. Move into the canonical structure under `/storage/ccindex_parquet/cc_pointers_by_collection/<year>/<collection>/`
3. Preserve `.sorted` status
4. Update any scripts still pointing at `cc_pointers_by_year` (parquet)
5. Rebuild indexes from consolidated files

## Notes
- `/storage/ccindex_parquet/cc_pointers_by_year/` is a legacy parquet tree from early development; the current pipeline uses `cc_pointers_by_collection`.
- `/storage/ccindex_duckdb/cc_pointers_by_year/` is still current (DuckDB meta-index per year).
