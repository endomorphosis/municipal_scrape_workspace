# DuckDB Pointer Index - Search and Benchmark Tools

## Overview

This directory contains three comprehensive tools for testing, searching, and benchmarking the DuckDB pointer index system:

1. **test_cc_pipeline.py** - Complete pipeline test suite
2. **search_domain_duckdb_pointer.py** - Domain search tool
3. **benchmark_duckdb_pointer.py** - Performance benchmark suite

## Design Philosophy

The DuckDB pointer index uses **offsets and ranges** to provide:
- ✅ **Fast lookups** - Domain queries use indexed lookups
- ✅ **Optimal I/O** - Only read specific row ranges from parquet files
- ✅ **Memory efficient** - No need to load entire parquet files
- ✅ **Flexible searches** - Can query across all indexes for any domain

## 1. Pipeline Test Suite

Tests all phases of the Common Crawl processing pipeline.

### Usage

```bash
# Run all tests (temp directory, auto-cleanup)
./test_cc_pipeline.py

# Run tests and keep artifacts
./test_cc_pipeline.py --keep

# Run tests in specific directory
./test_cc_pipeline.py --test-dir /tmp/my_test
```

### Test Phases

1. **Phase 1: Download** - Creates sample .gz files with test data
2. **Phase 2: Convert** - Converts .gz files to .parquet format
3. **Phase 3: Sort** - Sorts parquet files by URL (domain-sorted)
4. **Phase 4: Index** - Builds DuckDB pointer index with offset/range
5. **Phase 5: Search** - Tests search functionality across all files
6. **Phase 6: Benchmark** - Measures search performance

### Example Output

```
======================================================================
COMMON CRAWL PIPELINE TEST SUITE
======================================================================

=== Phase 1: Download/Create .gz files ===
✓ Created sample .gz file: CC-MAIN-2024-10-index00000.gz (15 records)
✓ Phase 1 PASSED: 3 .gz files created

=== Phase 2: Convert .gz to .parquet ===
✓ Phase 2 PASSED: 3 .parquet files created

=== Phase 3: Sort .parquet files by domain ===
✓ Phase 3 PASSED: All .parquet files sorted by URL

=== Phase 4: Build DuckDB pointer index ===
✓ Phase 4 PASSED: DuckDB index created with 8 domain pointers

=== Phase 5: Search functionality ===
  Test 1: Search for domain 'example.com'
    ✓ Found in CC-MAIN-2024-10-index00000.gz.parquet: offset=0, count=5
    ✓ Verified 5 URLs from example.com

=== Phase 6: Benchmark search performance ===
    ✓ Pointer lookup: 0.0035s (6 domains)
    ✓ Average per domain: 0.58ms
    ✓ Throughput: 2856 URLs/sec

✓ ALL TESTS PASSED
```

## 2. Domain Search Tool

Search for all URLs from a specific domain across all Common Crawl indexes.

### Usage

```bash
# Search for a domain
./search_domain_duckdb_pointer.py example.com

# Search with verbose output
./search_domain_duckdb_pointer.py example.com -v

# Show all URLs found
./search_domain_duckdb_pointer.py example.com --show-urls

# List all domains in index
./search_domain_duckdb_pointer.py --list-domains

# List top 100 domains
./search_domain_duckdb_pointer.py --list-domains --limit 100

# Save results to JSON
./search_domain_duckdb_pointer.py example.com -o results.json

# Use custom database location
./search_domain_duckdb_pointer.py example.com --db /custom/path/domain_pointer.duckdb
```

### How It Works

1. **Pointer Lookup** - Queries DuckDB index for domain pointers
2. **Targeted Retrieval** - Uses offset/count to read only relevant rows from parquet files
3. **No Full Scan** - Never scans entire parquet files, only specific ranges

### Performance Characteristics

- **Pointer lookup**: Sub-millisecond for indexed domains
- **Data retrieval**: Only reads necessary rows using offset/count
- **Throughput**: Thousands of URLs per second
- **Memory**: Minimal - only loads required data chunks

## 3. Performance Benchmark

Comprehensive benchmark suite to validate performance characteristics.

### Usage

```bash
# Full benchmark
./benchmark_duckdb_pointer.py

# Quick benchmark (smaller sample size)
./benchmark_duckdb_pointer.py --quick

# Custom sample size
./benchmark_duckdb_pointer.py --sample-size 50

# Custom database location
./benchmark_duckdb_pointer.py --db /custom/path/domain_pointer.duckdb
```

### Benchmark Tests

1. **Index Statistics** - Shows overall index metrics
2. **Pointer Lookup Performance** - Tests lookup speed for small/medium/large domains
3. **Full URL Retrieval** - Measures end-to-end retrieval performance
4. **Concurrent Access** - Tests many concurrent lookups

### Example Output

```
======================================================================
DUCKDB POINTER INDEX BENCHMARK
======================================================================

1. Index Statistics
----------------------------------------------------------------------
Total domains: 15,234,567
Total pointers: 45,678,901
Total URLs: 9,876,543,210
Parquet files: 300
Average URLs per domain: 648.3
Median URLs per domain: 12.0
URL range: 1 - 1,234,567

2. Pointer Lookup Performance
----------------------------------------------------------------------
  SMALL domains (20 samples):
    Mean: 0.42ms
    Median: 0.38ms
    Range: 0.15ms - 1.23ms

  MEDIUM domains (20 samples):
    Mean: 0.55ms
    Median: 0.51ms
    Range: 0.21ms - 1.45ms

  LARGE domains (20 samples):
    Mean: 0.68ms
    Median: 0.62ms
    Range: 0.28ms - 2.10ms

3. Full URL Retrieval Performance
----------------------------------------------------------------------
  SMALL domains (10 samples):
    Mean time: 5.23ms
    Mean throughput: 1,234 URLs/sec

  MEDIUM domains (10 samples):
    Mean time: 125.45ms
    Mean throughput: 5,678 URLs/sec

  LARGE domains (10 samples):
    Mean time: 2,345.67ms
    Mean throughput: 12,345 URLs/sec

4. Concurrent Access Performance
----------------------------------------------------------------------
Benchmark: 100 concurrent lookups
  Total time: 0.15s
  Average per lookup: 0.52ms
  Throughput: 667 lookups/sec
```

## Database Schema

The DuckDB pointer index uses the following schema:

```sql
CREATE TABLE domain_pointers (
    domain VARCHAR,              -- Domain name (e.g., 'example.com')
    parquet_file VARCHAR,        -- Parquet filename
    row_offset BIGINT,           -- Starting row in parquet file
    row_count BIGINT,            -- Number of rows for this domain
    first_url VARCHAR,           -- First URL in range (for verification)
    last_url VARCHAR,            -- Last URL in range (for verification)
    PRIMARY KEY (domain, parquet_file)
);

CREATE INDEX idx_domain ON domain_pointers(domain);
```

## Why This Design is Optimal

### 1. **Fast Lookups**
- Indexed domain queries: O(log n) lookup time
- No full parquet file scans required
- Results in sub-millisecond lookups

### 2. **Optimal I/O**
- Only reads specific row ranges using offset/count
- Parquet's columnar format allows efficient range reads
- Minimizes disk I/O and memory usage

### 3. **Flexible Searching**
- Can search any domain across all indexes
- Works for domains with 1 URL or 1 million URLs
- Consistent performance regardless of data distribution

### 4. **Space Efficient**
- Pointer index is small compared to data
- No duplication of URL data
- Easy to rebuild if needed

### 5. **Scalable**
- Works with any number of parquet files
- Can handle billions of URLs
- Adding new data is straightforward

## Troubleshooting

### Database not found
```bash
# Check if database exists
ls -lh /storage/ccindex_duckdb/domain_pointer.duckdb

# Check if it's being built
ps aux | grep build_cc_pointer_duckdb
```

### Parquet files not found
```bash
# Verify parquet directory
ls -lh /storage/ccindex_parquet/ | head

# Check if files are sorted
./validate_and_sort_parquet.py --check-only
```

### Slow searches
- Check if database has proper indexes
- Verify parquet files are sorted
- Monitor disk I/O during searches

## Integration with Existing Tools

These tools work alongside existing scripts:

- `build_cc_pointer_duckdb.py` - Builds the pointer index
- `validate_and_sort_parquet.py` - Validates sorted parquet files
- `monitor_cc_pointer_build.py` - Monitors index build progress

## Next Steps

Once the pointer index is built, you can:

1. **Run tests** to verify everything works
2. **Search domains** to find URLs
3. **Benchmark** to validate performance
4. **Integrate** with other tools in your pipeline

## Questions?

If you have concerns about the design, run the test suite first:

```bash
./test_cc_pipeline.py --keep
```

This will create a complete working example demonstrating all phases.
