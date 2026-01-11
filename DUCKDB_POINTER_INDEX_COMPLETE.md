# DuckDB Domain Pointer Index - Complete System

## Overview
A fast domain-based search system for Common Crawl indexes using DuckDB with offset/range optimization.

## Components Created

### 1. Index Builder: `build_duckdb_pointer_from_parquet.py`
Builds the DuckDB pointer index from existing sorted parquet files.

**Features:**
- Reads sorted parquet files from disk
- Extracts domain ranges for each row group
- Creates compact pointer index: domain → (parquet_file, row_group, start_row, end_row, count)
- Progress tracking with ETA
- Creates indexes on domain and parquet_file for fast lookups

**Usage:**
```bash
./build_duckdb_pointer_from_parquet.py \
    --db /storage/ccindex_duckdb/domain_pointer.duckdb \
    --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
    --verbose
```

### 2. Search Script: `search_duckdb_pointer_domain.py`
Fast domain search across all indexed parquet files.

**Features:**
- Two-phase query: pointer lookup (ms) + targeted data retrieval
- Only reads parquet files/row groups that contain the domain
- Returns all WARC file locations for a domain
- Detailed timing breakdown

**Usage:**
```bash
./search_duckdb_pointer_domain.py \
    --db /storage/ccindex_duckdb/domain_pointer.duckdb \
    --domain example.com \
    --verbose \
    --limit 20
```

### 3. Benchmark Tool: `benchmark_duckdb_pointer_domain.py`
Comprehensive performance testing and validation.

**Features:**
- Tests multiple domain queries
- Validates index completeness (all parquet files indexed)
- Statistics: min/max/mean/median query times
- Throughput measurement (URLs/second)
- Identifies missing or extra files

**Usage:**
```bash
./benchmark_duckdb_pointer_domain.py \
    --db /storage/ccindex_duckdb/domain_pointer.duckdb \
    --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
    --domains 20 \
    --validate
```

### 4. Overnight Build Script: `overnight_build_duckdb_pointer.sh`
Automated build and validation pipeline.

**Process:**
1. Removes old database
2. Builds new pointer index from all sorted parquet files
3. Runs validation benchmark
4. Logs everything to timestamped file

**Usage:**
```bash
./overnight_build_duckdb_pointer.sh
# or run in background:
nohup ./overnight_build_duckdb_pointer.sh > build_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

## Current Status

**Running Now:**
- Process ID: 2795488
- Started: 2026-01-11 19:10 CET
- Input: 711 sorted parquet files in `/storage/ccindex_parquet/cc_pointers_by_year/`
- Output: `/storage/ccindex_duckdb/domain_pointer.duckdb`
- Log: `overnight_duckdb_pointer_20260111_191018.log`

## Database Schema

```sql
CREATE TABLE domain_pointers (
    domain VARCHAR,           -- Domain name (e.g., "example.com")
    parquet_file VARCHAR,     -- Full path to parquet file
    row_group_id INTEGER,     -- Row group within parquet file
    start_row BIGINT,         -- First row for this domain in this row group
    end_row BIGINT,           -- Last row for this domain in this row group
    domain_count INTEGER      -- Number of URLs for this domain in range
);

CREATE INDEX idx_domain ON domain_pointers(domain);
CREATE INDEX idx_parquet_file ON domain_pointers(parquet_file);
```

## Design Benefits

### 1. Fast Lookups
- Pointer query: O(log n) using B-tree index on domain
- Only reads relevant parquet files/row groups
- Avoids scanning entire dataset

### 2. Flexible Searching
- Search by exact domain
- Can extend to wildcard/regex patterns
- Returns ALL matching URLs across all collections

### 3. Optimal I/O
- Row-group level granularity
- DuckDB's efficient parquet reader
- Parallel file reading possible

### 4. Completeness
- Indexes ALL parquet files recursively
- Validation ensures no files missed
- Tracks multiple collections (2023-2025)

## Example Query Flow

```
User searches: "example.com"
↓
1. Query pointer index (< 10ms)
   → Returns 5 parquet files with row group ranges
↓
2. For each file, read only relevant row groups
   → File A: row group 3, rows 10000-12000
   → File B: row group 1, rows 5000-5500
   → ...
↓
3. Return all URLs and WARC locations (< 1s total)
```

## Monitoring Progress

```bash
# Check if running
ps aux | grep build_duckdb_pointer_from_parquet

# Watch log in real-time
tail -f overnight_duckdb_pointer_20260111_191018.log

# Check database size (grows as indexing progresses)
ls -lh /storage/ccindex_duckdb/domain_pointer.duckdb*

# Estimated completion: ~30-60 minutes for 711 files
```

## Next Steps After Completion

1. **Run Benchmark:**
   ```bash
   ./benchmark_duckdb_pointer_domain.py \
       --db /storage/ccindex_duckdb/domain_pointer.duckdb \
       --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
       --domains 50 \
       --validate
   ```

2. **Test Search:**
   ```bash
   # Search for a known domain
   ./search_duckdb_pointer_domain.py \
       --db /storage/ccindex_duckdb/domain_pointer.duckdb \
       --domain your-domain.com \
       --verbose
   ```

3. **Integrate with Municipal Scraper:**
   - Use pointer index to find all URLs for municipal domains
   - Retrieve WARC files efficiently
   - Extract and process HTML content

## Files Modified/Created

- ✅ `build_duckdb_pointer_from_parquet.py` - New index builder
- ✅ `search_duckdb_pointer_domain.py` - New search tool
- ✅ `benchmark_duckdb_pointer_domain.py` - New benchmark tool
- ✅ `overnight_build_duckdb_pointer.sh` - New automation script

All scripts are executable and include help text (`--help`).
