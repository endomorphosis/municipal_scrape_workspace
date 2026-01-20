# DuckDB Pointer Index - Overnight Build Summary

## What's Running Now

### 1. Parallel .gz → .parquet Conversion
- **Status**: ⏳ ACTIVE (8 workers at ~100% CPU)
- **Progress**: Creating sorted parquet files from 6,396 .gz sources
- **Files Created**: 361/6,396 parquet files (5.6%)
- **Memory Usage**: ~0.1% per worker (well within limits)
- **Script**: `parallel_convert_missing.py`

### 2. Overnight Orchestration Job
- **Status**: ⏳ WAITING for conversion to complete
- **Script**: `overnight_duckdb_complete.sh`
- **Will Execute**:
  1. Wait for all conversions to finish
  2. Validate all parquet files are sorted by domain
  3. Build DuckDB pointer index with domain→file mappings
  4. Run search tests on sample domains
  5. Run performance benchmarks
  6. Generate completion report

## Architecture Design

### Parquet Files (Sorted by Domain)
```
/storage/ccindex_parquet/cc_pointers_by_year/
├── 2024/
│   ├── CC-MAIN-2024-10/
│   │   ├── cdx-00000.gz.parquet (sorted by domain)
│   │   ├── cdx-00001.gz.parquet (sorted by domain)
│   │   └── ...
│   ├── CC-MAIN-2024-18/
│   └── ... (all 2024 crawls)
└── 2025/
    ├── CC-MAIN-2025-05/
    └── ... (all 2025 crawls)
```

**Each parquet file contains**:
- `url`: Full URL
- `domain`: Extracted domain (sorted!)
- `timestamp`: Capture timestamp
- `warc_filename`: WARC file location
- `warc_record_offset`: Byte offset in WARC
- `warc_record_length`: Record length in bytes

### DuckDB Pointer Index (Domain Mode)

**Purpose**: Fast domain → parquet file mapping with offset/range optimization

**Schema** (will be created):
```sql
CREATE TABLE cc_pointers (
    domain VARCHAR,
    parquet_file VARCHAR,
    row_offset BIGINT,    -- First row in parquet for this domain
    row_count BIGINT,     -- Number of rows for this domain
    collection VARCHAR,
    shard_num INTEGER
);

CREATE INDEX idx_domain ON cc_pointers(domain);
```

**How It Works**:
1. For each sorted parquet file, scan to find domain boundaries
2. Store only: `(domain, file_path, offset, count)`
3. When searching for "example.com":
   - Query DuckDB: `SELECT * FROM cc_pointers WHERE domain = 'example.com'`
   - Get list of (file, offset, count) tuples
   - Read ONLY those specific row ranges from parquet files
   - Return all WARC pointers for that domain

**Benefits**:
- ✓ Minimal DuckDB index size (only domain boundaries, not full URLs)
- ✓ Fast domain lookups (indexed on domain)
- ✓ Efficient I/O (offset+range avoids full file scans)
- ✓ Full URL data stays in sorted parquet (compressed, columnar)
- ✓ Scalable to billions of URLs

## Search & Benchmark Tools

Setup (portable):

```bash
REPO_ROOT="/path/to/municipal_scrape_workspace"
VENV_PYTHON="${VENV_PYTHON:-${REPO_ROOT}/.venv/bin/python}"
if [[ ! -x "${VENV_PYTHON}" ]]; then VENV_PYTHON="python3"; fi
```

### 1. `search_cc_domain.py`
Search for a domain and retrieve all WARC pointers.

```bash
# Search using DuckDB index (fast)
"${VENV_PYTHON}" "${REPO_ROOT}/search_cc_domain.py" example.com --limit 100

# Compare DuckDB vs direct parquet scan
"${VENV_PYTHON}" "${REPO_ROOT}/search_cc_domain.py" example.com --mode both --show
```

**Output**: List of URLs with WARC file locations and byte offsets

### 2. `benchmarks/ccindex/benchmark_cc_domain_search.py`
Comprehensive performance benchmarking.

```bash
# Basic benchmark
"${VENV_PYTHON}" "${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_domain_search.py" --domains example.com google.com github.com

# With cache clearing (requires sudo)
"${VENV_PYTHON}" "${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_domain_search.py" --clear-cache --output results.json
```

**Metrics**:
- Cold start query time
- Warm cache query time
- Throughput (rows/second)
- Min/max/mean/median query times
- Index size and row counts

## Monitoring Progress

```bash
# Check current status
"${REPO_ROOT}/monitor_overnight_build.sh"

# Watch conversion log
tail -f "${REPO_ROOT}/conversion_progress.log"

# Watch orchestration log
tail -f "${REPO_ROOT}"/overnight_duckdb_*.log

# Check disk usage
df -h /storage

# Check memory
free -h
```

## Expected Timeline

Based on current progress (361/6,396 files in ~2 minutes):

1. **Conversion**: ~35-40 minutes remaining
2. **Validation**: ~5-10 minutes (check all files sorted)
3. **DuckDB Build**: ~10-20 minutes (scan parquet, build index)
4. **Testing**: ~2-3 minutes (run searches)
5. **Benchmarking**: ~3-5 minutes (performance tests)

**Total**: ~1-1.5 hours for complete build

## Design Rationale

### Why Sorted Parquet Files?
- Enables binary search within files
- Groups same-domain records together (cache-friendly)
- Allows offset/range optimization
- Standard format (works with DuckDB, pandas, etc.)

### Why DuckDB for Pointers Only?
- Minimal index size (~100-1000x smaller than full data)
- SQL interface for flexible queries
- Fast indexed lookups
- Can scale to billions of domains
- Embedded (no server needed)

### Why Not Store Everything in DuckDB?
- Would require ~terabytes of database storage
- Slower than optimized parquet for bulk scans
- Less flexible for adding new columns
- Harder to distribute/share

### Why Offset + Range?
- Skip irrelevant rows in parquet files
- Only read the exact rows needed
- Massive I/O savings for sparse domains
- Near-constant time lookups regardless of file size

## Files Created

- `parallel_convert_missing.py` - Multi-process gz→parquet with memory management
- `search_cc_domain.py` - Domain search with DuckDB and parquet modes
- `benchmarks/ccindex/benchmark_cc_domain_search.py` - Comprehensive performance testing
- `overnight_duckdb_complete.sh` - Full orchestration script
- `monitor_overnight_build.sh` - Progress monitoring
- `archive/ccindex/converters/convert_missing_with_chunks.py` - Single-threaded chunked converter (backup; archived)

## Next Steps (Automatic)

The overnight job will:
1. ✓ Complete all conversions (in progress)
2. ⏳ Validate sorting
3. ⏳ Build DuckDB index
4. ⏳ Run search tests
5. ⏳ Run benchmarks
6. ⏳ Generate report

You can check status anytime with `${REPO_ROOT}/monitor_overnight_build.sh`

## Manual Override (If Needed)

If you need to stop/restart:

```bash
# Stop conversion
pkill -f parallel_convert_missing.py

# Stop orchestration
pkill -f overnight_duckdb_complete.sh

# Restart orchestration manually
"${REPO_ROOT}/overnight_duckdb_complete.sh"
```

## Success Criteria

- [ ] All 6,396 .gz files converted to sorted .parquet
- [ ] All parquet files validated as sorted by domain
- [ ] DuckDB pointer index created with <1GB size
- [ ] Domain searches return results in <100ms (warm cache)
- [ ] Benchmark shows consistent sub-second query times
- [ ] All WARC pointers retrievable for test domains

---

**Status**: Build in progress, monitoring active
**ETA**: ~1-1.5 hours to completion
**Monitor**: `${REPO_ROOT}/monitor_overnight_build.sh`
