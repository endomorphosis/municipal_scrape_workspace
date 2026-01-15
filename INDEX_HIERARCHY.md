# Common Crawl Index Hierarchy

## Overview

The DuckDB pointer index system uses a **three-tier hierarchical architecture** for optimal search performance, scalability, and flexibility:

1. **Tier 1: Per-Collection Indexes** - Individual DuckDB files for each CC collection
2. **Tier 2: Year-Level Meta-Indexes** - Aggregated indexes referencing all collections in a year
3. **Tier 3: Master Index** - Top-level index referencing all years

## Architecture Diagram

```
cc_master_index.duckdb                    ← Tier 3: Master (all years)
    ↓
cc_domain_by_year/
    ├── cc_pointers_2024.duckdb           ← Tier 2: Year (all 2024 collections)
    ├── cc_pointers_2025.duckdb           ← Tier 2: Year (all 2025 collections)
    └── ...
        ↓
cc_domain_by_collection/
    ├── cc_pointers_CC-MAIN-2024-10.duckdb    ← Tier 1: Collection (individual)
    ├── cc_pointers_CC-MAIN-2024-18.duckdb    ← Tier 1: Collection (individual)
    └── ...
        ↓
ccindex_parquet/
    ├── CC-MAIN-2024-10/
    │   ├── cdx-00000.gz.parquet.sorted
    │   ├── cdx-00001.gz.parquet.sorted
    │   └── ...
    └── ...
```

## Directory Structure

```
/storage/ccindex_duckdb/
├── cc_master_index.duckdb                 # Tier 3: Master index
│
├── cc_domain_by_year/                     # Tier 2: Year-level indexes
│   ├── cc_pointers_2024.duckdb
│   ├── cc_pointers_2025.duckdb
│   └── ...
│
└── cc_domain_by_collection/               # Tier 1: Collection indexes
    ├── cc_pointers_CC-MAIN-2024-10.duckdb
    ├── cc_pointers_CC-MAIN-2024-10.duckdb.sorted  # Marker file
    ├── cc_pointers_CC-MAIN-2024-18.duckdb
    └── ...
```

## Tier 1: Per-Collection Indexes

### Purpose
- Direct access to individual collections
- Optimized for domain-based queries
- Fast lookups with offset/range support

### Schema

**Table: `cc_domain_shards`**
```sql
CREATE TABLE cc_domain_shards (
    source_path TEXT,        -- Original parquet file path
    collection TEXT,         -- CC-MAIN-YYYY-WW identifier
    year INTEGER,           -- Year
    shard_file TEXT,        -- Shard filename
    parquet_relpath TEXT,   -- Relative path to parquet file
    host TEXT,              -- Domain name
    host_rev TEXT           -- Reversed domain for efficient sorting
);
```

### Usage
```python
import duckdb

conn = duckdb.connect(
    '/storage/ccindex_duckdb/cc_domain_by_collection/cc_pointers_CC-MAIN-2024-10.duckdb',
    read_only=True
)

# Find all entries for a domain
results = conn.execute("""
    SELECT * FROM cc_domain_shards 
    WHERE host = 'example.com'
    ORDER BY host
""").fetchall()

conn.close()
```

## Tier 2: Year-Level Meta-Indexes

### Purpose
- Aggregate view of all collections in a year
- Registry of available collections
- Year-wide statistics and metadata

### Schema

**Table: `collection_registry`**
```sql
CREATE TABLE collection_registry (
    collection TEXT PRIMARY KEY,
    db_path TEXT NOT NULL,
    domain_count INTEGER,
    file_count INTEGER,
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Table: `meta_info`**
```sql
CREATE TABLE meta_info (
    year TEXT PRIMARY KEY,
    collection_count INTEGER,
    total_domains INTEGER,
    total_files INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Usage
```python
import duckdb

conn = duckdb.connect(
    '/storage/ccindex_duckdb/cc_domain_by_year/cc_pointers_2024.duckdb',
    read_only=True
)

# Get metadata for 2024
meta = conn.execute("SELECT * FROM meta_info WHERE year = '2024'").fetchone()
print(f"2024: {meta[1]} collections, {meta[2]:,} domains")

# List all collections in 2024
collections = conn.execute("""
    SELECT collection, domain_count, file_count 
    FROM collection_registry 
    ORDER BY collection
""").fetchall()

for coll, domains, files in collections:
    print(f"  {coll}: {domains:,} domains in {files:,} files")

conn.close()
```

## Tier 3: Master Index

### Purpose
- Unified view of entire Common Crawl corpus
- Cross-year queries and analysis
- High-level statistics and monitoring

### Schema

**Table: `year_registry`**
```sql
CREATE TABLE year_registry (
    year TEXT PRIMARY KEY,
    db_path TEXT NOT NULL,
    collection_count INTEGER,
    total_domains INTEGER,
    total_files INTEGER,
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Table: `collection_summary`**
```sql
CREATE TABLE collection_summary (
    collection TEXT PRIMARY KEY,
    year TEXT NOT NULL,
    year_db_path TEXT NOT NULL,
    collection_db_path TEXT NOT NULL,
    domain_count INTEGER,
    file_count INTEGER,
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Table: `master_info`**
```sql
CREATE TABLE master_info (
    id INTEGER PRIMARY KEY,
    year_count INTEGER,
    collection_count INTEGER,
    total_domains INTEGER,
    total_files INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Views:**
- `year_summary` - Summary statistics by year
- `collections_by_year` - All collections grouped by year

### Usage
```python
import duckdb

conn = duckdb.connect(
    '/storage/ccindex_duckdb/cc_master_index.duckdb',
    read_only=True
)

# Get master statistics
master = conn.execute("SELECT * FROM master_info").fetchone()
print(f"Master Index: {master[1]} years, {master[2]} collections, {master[3]:,} domains")

# Year breakdown
years = conn.execute("SELECT * FROM year_summary").fetchall()
for year, coll_count, domains, files, _ in years:
    print(f"  {year}: {coll_count} collections, {domains:,} domains, {files:,} files")

# Find which collections contain a specific domain (conceptual)
# Note: Actual domain queries should go to Tier 1 or Tier 2 indexes
collections = conn.execute("""
    SELECT collection, year, collection_db_path 
    FROM collection_summary 
    WHERE year = '2024'
    ORDER BY collection
""").fetchall()

conn.close()
```

## Building the Index Hierarchy

### Automated (Recommended)

The orchestrator handles everything:

```bash
# Build all 2024 collections and meta-indexes
python cc_pipeline_orchestrator.py --filter 2024 --workers 8

# When all collections are complete, it automatically:
# 1. Builds per-collection indexes (Tier 1)
# 2. Builds year-level indexes (Tier 2)
# 3. Builds master index (Tier 3)
```

### Manual

#### Step 1: Build Per-Collection Indexes (Tier 1)

```bash
python build_cc_pointer_duckdb.py \
    --input-root /storage/ccindex_parquet \
    --db /storage/ccindex_duckdb/cc_domain_by_collection/cc_pointers_CC-MAIN-2024-10.duckdb \
    --collections CC-MAIN-2024-10 \
    --threads 8 \
    --duckdb-index-mode domain \
    --domain-range-index
```

#### Step 2: Build Year-Level Indexes (Tier 2)

```bash
# Build all year indexes
python build_year_meta_indexes.py

# Or specific year
python build_year_meta_indexes.py --year 2024
```

#### Step 3: Build Master Index (Tier 3)

```bash
# Build master index
python build_master_index.py

# View statistics
python build_master_index.py --stats
```

## Query Strategies

### For Single Domain Lookup
**Use Tier 1** - Direct collection access is fastest

```python
# If you know the collection
conn = duckdb.connect('cc_domain_by_collection/cc_pointers_CC-MAIN-2024-10.duckdb', read_only=True)
results = conn.execute("SELECT * FROM cc_domain_shards WHERE host = 'example.com'").fetchall()
```

### For Year-Wide Search
**Use Tier 2** - Year index with collection federation

```python
# Query all 2024 collections
conn = duckdb.connect('cc_domain_by_year/cc_pointers_2024.duckdb', read_only=True)

# Get list of collections to query
collections = conn.execute("SELECT collection, db_path FROM collection_registry").fetchall()

# Query each collection (can be parallelized)
for coll, db_path in collections:
    coll_conn = duckdb.connect(db_path, read_only=True)
    results = coll_conn.execute("SELECT * FROM cc_domain_shards WHERE host = 'example.com'").fetchall()
    # Process results...
    coll_conn.close()
```

### For Corpus-Wide Analysis
**Use Tier 3** - Master index for metadata

```python
# Analyze entire corpus
conn = duckdb.connect('cc_master_index.duckdb', read_only=True)

# Find which years might contain data
years = conn.execute("""
    SELECT year, collection_count, total_domains 
    FROM year_registry 
    ORDER BY year DESC
""").fetchall()

# Get all collection paths for detailed queries
collections = conn.execute("""
    SELECT collection, collection_db_path 
    FROM collection_summary 
    ORDER BY year DESC, collection
""").fetchall()
```

## Monitoring and Validation

### Check Index Status

```bash
# Overall pipeline status
python cc_pipeline_watch.py

# Collection completeness
python validate_collection_completeness.py

# Master index statistics
python build_master_index.py --stats
```

### Verify Index Integrity

```bash
# Check if all expected indexes exist
ls -lh /storage/ccindex_duckdb/cc_domain_by_collection/*.duckdb
ls -lh /storage/ccindex_duckdb/cc_domain_by_year/*.duckdb
ls -lh /storage/ccindex_duckdb/cc_master_index.duckdb
```

## Benefits of This Architecture

1. **Scalability**: Collections built independently in parallel
2. **Flexibility**: Query at any level (collection, year, or corpus)
3. **Performance**: Optimized indexes with domain-based ranges
4. **Maintainability**: Rebuild individual components without affecting others
5. **Efficiency**: No data duplication (meta-indexes reference base indexes)
6. **Monitoring**: Clear hierarchy makes progress tracking simple

## Disk Space Requirements

- **Tier 1** (Per-Collection): ~1-2GB per collection (depends on domain count)
- **Tier 2** (Year-Level): ~10-50MB per year (metadata only)
- **Tier 3** (Master): ~1-10MB (metadata only)

**Example for 100 collections:**
- Tier 1: 100-200 GB
- Tier 2: 50-500 MB
- Tier 3: 1-10 MB
- **Total**: ~100-200 GB (dominated by Tier 1)

## Troubleshooting

### Index Not Found
- Check directory structure matches expected paths
- Verify marker files (`.sorted`) exist for completed indexes

### Query Performance Issues
- Ensure indexes are sorted (check for `.sorted` marker)
- Use appropriate tier (don't query Tier 3 for domain lookups)
- Check DuckDB connection isn't locked by another process

### Incomplete Indexes
- Run `validate_collection_completeness.py` to identify gaps
- Rebuild specific collections with orchestrator
- Check logs in `pipeline_run.log`

## See Also

- `INDEX_ARCHITECTURE.md` - Original design document
- `CC_ORCHESTRATOR_README.md` - Orchestrator documentation
- `DUCKDB_INDEX_QUICKREF.md` - Quick reference guide
