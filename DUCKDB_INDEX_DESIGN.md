# DuckDB Domain Pointer Index - Design Documentation

## Overview

This is a **domain-first pointer index** for Common Crawl data that provides fast, flexible searching while minimizing storage overhead. The design separates the index layer (DuckDB) from the data layer (Parquet) to optimize for both search speed and storage efficiency.

## Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────────┐
│                     Query Layer                              │
│  search_cc_duckdb_index.py - High-level search interface    │
│  - Domain search, URL lookup, pattern matching              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     Index Layer (DuckDB)                     │
│  Tables:                                                     │
│  - cc_domain_shards:  domain → parquet mappings             │
│  - cc_parquet_rowgroups: row-level range metadata           │
│  Sharding: One DB per year (CC-MAIN-YYYY-*)                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     Data Layer (Parquet)                     │
│  Sorted parquet files containing full URL records           │
│  - Sorted by: host_rev, url, timestamp                      │
│  - Compression: zstd                                         │
│  - Layout: <year>/<collection>/<shard>.parquet              │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Domain-first indexing**: DuckDB stores only domain → parquet mappings, not full URL records
   - Keeps DuckDB compact (~1% of data size)
   - Allows flexible URL-level queries via parquet scans
   - Avoids duplication between index and data layers

2. **Row group range metadata**: Optional optimization for filtered scans
   - Stores min/max host_rev per row group
   - Enables skipping row groups that can't contain target domain
   - Reduces scan time by 50-90% for focused queries

3. **Year-based sharding**: Separate DuckDB file per year
   - Enables parallel processing
   - Simplifies incremental updates
   - Reduces lock contention

4. **Sorted parquet data**: All parquet files sorted by host_rev
   - Maximizes compression (similar data grouped together)
   - Enables efficient range scans
   - Improves row group statistics quality

## Schema

### cc_domain_shards Table

Maps domains to parquet files that contain them.

```sql
CREATE TABLE cc_domain_shards (
    source_path VARCHAR,        -- Original .gz shard path
    collection VARCHAR,         -- CC-MAIN-YYYY-WW
    year INTEGER,               -- YYYY
    shard_file VARCHAR,         -- cdx-NNNNN.gz
    parquet_relpath VARCHAR,    -- Relative path to parquet file
    host VARCHAR,               -- example.gov
    host_rev VARCHAR            -- gov,example (reversed for prefix matching)
);

-- Indexes for fast lookup
CREATE INDEX idx_cc_domain_shards_host_rev ON cc_domain_shards(host_rev);
CREATE INDEX idx_cc_domain_shards_host ON cc_domain_shards(host);
CREATE INDEX idx_cc_domain_shards_collection ON cc_domain_shards(collection);
```

**Example rows:**
```
| host          | host_rev      | parquet_relpath                        | collection      |
|---------------|---------------|----------------------------------------|-----------------|
| whitehouse.gov| gov,whitehouse| 2024/CC-MAIN-2024-10/cdx-00042.parquet | CC-MAIN-2024-10 |
| senate.gov    | gov,senate    | 2024/CC-MAIN-2024-10/cdx-00042.parquet | CC-MAIN-2024-10 |
```

### cc_parquet_rowgroups Table

Row group range metadata for optimization.

```sql
CREATE TABLE cc_parquet_rowgroups (
    source_path VARCHAR,
    collection VARCHAR,
    year INTEGER,
    shard_file VARCHAR,
    parquet_relpath VARCHAR,
    row_group INTEGER,          -- Row group index
    row_start BIGINT,           -- First row index
    row_end BIGINT,             -- Last row index (exclusive)
    host_rev_min VARCHAR,       -- Minimum host_rev in group
    host_rev_max VARCHAR        -- Maximum host_rev in group
);

CREATE INDEX idx_cc_parquet_rowgroups_host_rev_min ON cc_parquet_rowgroups(host_rev_min);
CREATE INDEX idx_cc_parquet_rowgroups_host_rev_max ON cc_parquet_rowgroups(host_rev_max);
```

**Example rows:**
```
| parquet_relpath        | row_group | row_start | row_end | host_rev_min | host_rev_max |
|------------------------|-----------|-----------|---------|--------------|--------------|
| 2024/.../cdx-00042.pq  | 0         | 0         | 100000  | com,aaa      | com,zzz      |
| 2024/.../cdx-00042.pq  | 1         | 100000    | 200000  | gov,aaa      | gov,zzz      |
```

### Parquet File Schema

Full URL records with WARC pointers.

```
collection: string
shard_file: string
surt: string
ts: string              -- Timestamp
url: string
host: string            -- example.gov
host_rev: string        -- gov,example
status: int32
mime: string
digest: string
warc_filename: string   -- WARC file containing record
warc_offset: int64      -- Byte offset in WARC
warc_length: int64      -- Record length in bytes
```

## Search Operations

### 1. Domain Search

**Query**: "Find all URLs for domain example.gov"

**Steps**:
1. Convert domain to host_rev: `gov,example`
2. Query DuckDB for matching parquet shards:
   ```sql
   SELECT DISTINCT parquet_relpath 
   FROM cc_domain_shards 
   WHERE host_rev = 'gov,example' OR host_rev LIKE 'gov,example,%'
   ```
3. Scan identified parquet files:
   ```sql
   SELECT * FROM read_parquet('shard.parquet')
   WHERE host_rev = 'gov,example' OR host_rev LIKE 'gov,example,%'
   ```

**Performance**:
- DuckDB lookup: <10ms (indexed)
- Parquet scan: 100-500ms per shard (depends on size)
- Total: 100-1000ms for typical domain

### 2. URL Batch Search

**Query**: "Find WARC pointers for 1000 specific URLs"

**Steps**:
1. Extract unique domains from URLs
2. Query DuckDB for all relevant parquet shards
3. Create temp table with search URLs
4. Join parquet data against search URLs:
   ```sql
   SELECT p.* FROM read_parquet('shard.parquet') p
   INNER JOIN search_urls s ON p.url = s.url
   ```

**Performance**:
- DuckDB lookup: <50ms for 100 domains
- Parquet join: 50-200ms per shard
- Total: 200-1000ms for 1000 URLs across 10 shards

### 3. Pattern Matching

**Query**: "Find all .gov domains"

**Steps**:
1. Query DuckDB with LIKE pattern:
   ```sql
   SELECT DISTINCT host, host_rev, parquet_relpath
   FROM cc_domain_shards
   WHERE host_rev LIKE 'gov,%'
   ```
2. Optionally scan parquet for full records

**Performance**:
- Pattern scan: 100-500ms (full index scan)
- Results streaming: immediate

### 4. Row Group Optimized Search

**Query**: "Find all URLs for domain example.gov (optimized)"

**Steps**:
1. Query DuckDB for matching row groups:
   ```sql
   SELECT parquet_relpath, row_group, row_start, row_end
   FROM cc_parquet_rowgroups
   WHERE host_rev_min <= 'gov,example' AND host_rev_max >= 'gov,example'
   ```
2. Read only relevant row groups from parquet:
   ```python
   pf = pq.ParquetFile('shard.parquet')
   for rg_idx in relevant_row_groups:
       table = pf.read_row_group(rg_idx)
       # Filter in memory
   ```

**Performance**:
- 50-90% reduction in data scanned
- Especially effective for rare domains

## Usage Examples

### Building the Index

```bash
# Full build for 2024 collections
./overnight_build_duckdb_index.sh --collections-regex 'CC-MAIN-2024-.*'

# Quick test build (100 files)
./overnight_build_duckdb_index.sh --quick

# Rebuild index for specific year
python build_cc_pointer_duckdb.py \
  --input-root /storage/ccindex \
  --db /storage/ccindex_duckdb/cc_domain_by_year \
  --shard-by-year \
  --collections-regex 'CC-MAIN-2024-.*' \
  --duckdb-index-mode domain \
  --domain-index-action rebuild \
  --domain-range-index \
  --parquet-out /storage/ccindex_parquet/cc_pointers_by_year \
  --create-indexes
```

### Searching

```bash
# Search for a domain
python search_cc_duckdb_index.py \
  --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
  --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
  --domain whitehouse.gov \
  --count-urls \
  --verbose

# Search for URLs from file
python search_cc_duckdb_index.py \
  --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
  --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
  --url-file my_urls.txt \
  --output results.jsonl

# List all domains in index
python search_cc_duckdb_index.py \
  --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
  --list-domains \
  --limit 1000

# Search with row group optimization
python search_cc_duckdb_index.py \
  --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
  --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
  --domain example.gov \
  --use-rowgroup-ranges
```

### Benchmarking

```bash
# Full benchmark suite
python benchmarks/ccindex/benchmark_cc_duckdb_search.py \
  --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
  --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
  --sample-domains 200 \
  --sample-urls 1000

# Quick benchmark
python benchmarks/ccindex/benchmark_cc_duckdb_search.py \
  --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
  --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
  --quick
```

### Direct DuckDB Access

```bash
# Interactive SQL
duckdb /storage/ccindex_duckdb/cc_domain_by_year/cc_pointers_2024.duckdb

# Example queries
duckdb cc_pointers_2024.duckdb << EOF
-- Count domains per collection
SELECT collection, count(DISTINCT host_rev) as domains
FROM cc_domain_shards
GROUP BY collection
ORDER BY collection;

-- Find all .gov domains
SELECT DISTINCT host
FROM cc_domain_shards
WHERE host_rev LIKE 'gov,%'
ORDER BY host
LIMIT 100;

-- Get parquet shards for a domain
SELECT DISTINCT parquet_relpath, collection
FROM cc_domain_shards
WHERE host_rev = 'gov,whitehouse'
ORDER BY collection;
EOF
```

## Performance Characteristics

### Storage

| Component | Size | Ratio |
|-----------|------|-------|
| Original CDXJ (.gz) | 100 GB | 100% |
| Parquet (zstd) | 40 GB | 40% |
| DuckDB index | 400 MB | 0.4% |
| **Total** | **40.4 GB** | **40.4%** |

### Search Speed

| Operation | Typical Time | Notes |
|-----------|-------------|-------|
| Domain lookup (index) | <10 ms | DuckDB indexed query |
| Domain lookup (full) | 100-500 ms | Including parquet scan |
| URL batch (100 URLs) | 200-1000 ms | Depends on shard count |
| Pattern scan (.gov) | 100-500 ms | Full index table scan |
| Row group optimized | 50-200 ms | 50-90% reduction |

### Scalability

- **Collections tested**: 10-50 collections per year
- **Domains indexed**: 1M-10M unique domains per year
- **Parquet shards**: 100-300 per collection
- **DuckDB size**: ~10-50 MB per year
- **Query concurrency**: Excellent (read-only access)

## Design Rationale

### Why Not Store URLs in DuckDB?

**Considered**: Full URL table in DuckDB

**Rejected because**:
- Storage: 100x larger (40 GB vs 400 MB)
- Flexibility: Can't easily modify URL records without rebuilding entire DB
- Updates: Difficult to incrementally update large DuckDB tables
- Duplication: Same data stored twice (DuckDB + Parquet)

**Domain index is better**:
- Compact: ~1% storage overhead
- Flexible: URL data stays in easily updated parquet
- Fast: Index lookups are <10ms, parquet scans are still fast
- Scalable: Can handle billions of URLs

### Why Row Group Metadata?

**Problem**: Scanning entire parquet files wastes I/O on irrelevant data

**Solution**: Store min/max host_rev per row group in DuckDB
- Enables early elimination of row groups
- Reduces scan time by 50-90% for focused queries
- Adds <5% overhead to index size
- Especially valuable for rare domains

**Trade-off**: Only beneficial when:
- Parquet files are sorted by host_rev
- Queries target specific domains (not full scans)
- Row group size is reasonable (50K-200K rows)

### Why Year-Based Sharding?

**Alternative**: Single monolithic DuckDB

**Benefits of sharding**:
- Parallel processing: Each year can be queried independently
- Incremental updates: Rebuild only affected years
- Smaller files: Easier to manage, backup, transfer
- Reduced contention: No single file bottleneck

**Trade-off**: Must query multiple DBs for multi-year searches
- Mitigated: Query script handles this transparently
- Benefit: Can prioritize recent years first

## Extending the Design

### Adding New Collections

1. Run build script with new collections:
   ```bash
   python build_cc_pointer_duckdb.py \
     --collections-regex 'CC-MAIN-2025-.*' \
     --domain-index-action append
   ```

2. Existing data is preserved, new domains are added

### Custom Queries

Access DuckDB directly for advanced queries:

```python
import duckdb

con = duckdb.connect('cc_pointers_2024.duckdb', read_only=True)

# Find domains with most URLs
result = con.execute("""
    SELECT host, count(DISTINCT parquet_relpath) as shard_count
    FROM cc_domain_shards
    GROUP BY host
    ORDER BY shard_count DESC
    LIMIT 100
""").fetchall()

# Join with parquet for full analysis
result = con.execute("""
    SELECT 
        d.host,
        count(DISTINCT p.url) as url_count,
        count(DISTINCT p.warc_filename) as warc_count
    FROM cc_domain_shards d
    JOIN read_parquet(d.parquet_relpath) p
        ON p.host_rev = d.host_rev
    WHERE d.host LIKE '%.gov'
    GROUP BY d.host
    ORDER BY url_count DESC
    LIMIT 100
""").fetchall()
```

### Performance Tuning

**If domain lookups are slow:**
- Ensure indexes are created (`--create-indexes`)
- Check index statistics: `ANALYZE cc_domain_shards`
- Consider partitioning by collection

**If parquet scans are slow:**
- Enable row group optimization (`--domain-range-index`)
- Reduce row group size in parquet files
- Ensure parquet files are sorted
- Use SSD storage for hot data

**If memory is constrained:**
- Reduce DuckDB threads (`--threads`)
- Set memory limit (`--memory-limit-gib`)
- Process collections sequentially

## Troubleshooting

### "No DuckDB files found"
- Check `--duckdb-dir` path
- Ensure build completed successfully
- Look for `.duckdb` files in directory

### "Parquet file not found"
- Check `--parquet-root` path
- Ensure parquet files were created (`--parquet-out`)
- Verify file permissions

### Slow searches
- Run benchmark to identify bottleneck
- Check if indexes exist
- Consider enabling row group optimization
- Verify parquet files are sorted

### Out of memory
- Reduce `--threads`
- Set `--memory-limit-gib`
- Process smaller batches
- Use `--cdx-shard-mod` for partitioning

## Summary

This design provides a **fast, flexible, and storage-efficient** solution for searching Common Crawl data:

✅ **Fast**: <10ms index lookups, 100-500ms full searches  
✅ **Flexible**: Supports domain, URL, and pattern searches  
✅ **Efficient**: ~1% storage overhead for index  
✅ **Scalable**: Handles billions of URLs across years  
✅ **Maintainable**: Clear separation of index and data layers  

The key insight is that **domain-level indexing is sufficient** for most use cases, allowing the full URL data to remain in easily-managed parquet files while providing fast search capabilities through a compact DuckDB index.
