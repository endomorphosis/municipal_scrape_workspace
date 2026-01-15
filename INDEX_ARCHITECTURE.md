# DuckDB Index Architecture

## Overview
The DuckDB pointer index system uses a **two-tier architecture** for optimal search performance and flexibility:

1. **Per-Collection Indexes** (Tier 1): Individual DuckDB files for each Common Crawl collection
2. **Year-Level Meta-Indexes** (Tier 2): Aggregated indexes that reference all collections in a year

## Directory Structure

```
/storage/ccindex_duckdb/
├── cc_domain_by_collection/          # Tier 1: Per-collection indexes
│   ├── cc_pointers_CC-MAIN-2024-10.duckdb
│   ├── cc_pointers_CC-MAIN-2024-10.duckdb.sorted    # Marker file
│   ├── cc_pointers_CC-MAIN-2024-18.duckdb
│   └── ... (one per collection)
│
└── cc_domain_by_year/                # Tier 2: Year-level meta-indexes
    ├── cc_pointers_2024.duckdb       # References all 2024 collections
    ├── cc_pointers_2025.duckdb       # References all 2025 collections
    └── ...
```

## Tier 1: Per-Collection Indexes

Each collection gets its own DuckDB file with schema:

**Table: `cc_domain_shards`**
- `source_path` - Original parquet file path
- `collection` - CC-MAIN-YYYY-WW identifier
- `year` - Year (integer)
- `shard_file` - Shard filename
- `parquet_relpath` - Relative path to parquet file
- `host` - Domain name
- `host_rev` - Reversed domain for sorting

**Properties:**
- Fast lookups for specific collections
- Optimized for domain-based queries with offsets/ranges
- Can be built/rebuilt independently
- ~1-2GB per collection with ~5M domain entries

## Tier 2: Year-Level Meta-Indexes

Year-level databases aggregate all collections for a given year:

**Table: `collection_registry`**
- `collection` - Collection identifier
- `db_path` - Path to collection database
- `domain_count` - Number of domains in collection
- `file_count` - Number of parquet files
- `indexed_at` - Timestamp

**Table: `meta_info`**
- `year` - Year
- `collection_count` - Number of collections
- `total_domains` - Total domains across all collections
- `total_files` - Total parquet files
- `created_at` - Timestamp

**Properties:**
- Efficient year-wide queries
- Registry of available collections
- Statistics and metadata
- Uses DuckDB's ATTACH DATABASE for federation

## Building the Indexes

### Step 1: Build Per-Collection Indexes

Use the orchestrator:
```bash
# Build all 2024 collections
python cc_pipeline_orchestrator.py --filter 2024 --workers 8

# Build specific collection
python cc_pipeline_orchestrator.py --filter 2024-10 --workers 8
```

Or manually:
```bash
python build_cc_pointer_duckdb.py \
    --input-root /storage/ccindex_parquet \
    --db /storage/ccindex_duckdb/cc_domain_by_collection/cc_pointers_CC-MAIN-2024-10.duckdb \
    --collections CC-MAIN-2024-10 \
    --threads 8 \
    --duckdb-index-mode domain \
    --domain-range-index
```

### Step 2: Build Year-Level Meta-Indexes

After collection indexes are built:
```bash
# Build all year indexes
python build_year_meta_indexes.py

# Build specific year
python build_year_meta_indexes.py --year 2024
```

## Querying the Indexes

### Query Single Collection
```python
import duckdb

conn = duckdb.connect('/storage/ccindex_duckdb/cc_domain_by_collection/cc_pointers_CC-MAIN-2024-10.duckdb', read_only=True)

# Find all entries for a domain
results = conn.execute("""
    SELECT * FROM cc_domain_shards 
    WHERE host = 'example.com'
    ORDER BY host
""").fetchall()

conn.close()
```

### Query Entire Year
```python
import duckdb

conn = duckdb.connect('/storage/ccindex_duckdb/cc_domain_by_year/cc_pointers_2024.duckdb', read_only=True)

# Get list of collections
collections = conn.execute("SELECT * FROM collection_registry").fetchall()

# Query across all collections (requires attaching databases)
for row in conn.execute("SELECT * FROM collection_registry").fetchall():
    collection, db_path, domain_count, file_count, _ = row
    print(f"{collection}: {domain_count:,} domains in {file_count:,} files")

conn.close()
```

## Benefits

1. **Scalability**: Each collection is independent, can be built in parallel
2. **Flexibility**: Query at collection or year level
3. **Performance**: Optimized indexes with domain-based ranges
4. **Maintainability**: Can rebuild individual collections without affecting others
5. **Federation**: Year indexes provide unified view without data duplication

## Validation

Check completeness:
```bash
python validate_collection_completeness.py
```

Check if indexes are sorted:
```bash
python validate_collection_completeness.py | grep "index.*sorted"
```

## Migration from Old Structure

If you have indexes in `/storage/ccindex_duckdb/cc_pointers_by_collection/` or other locations:
1. They will work but won't be found by the orchestrator
2. Move them to `cc_domain_by_collection/` and rename to `cc_pointers_<collection>.duckdb`
3. Rebuild year indexes with `build_year_meta_indexes.py`
