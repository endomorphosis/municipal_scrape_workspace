# DuckDB Pointer Index - Quick Reference

## Search for a Domain
```bash
python3 search_parallel_duckdb_indexes.py <domain>
```

Examples:
```bash
# Search across all collections
python3 search_parallel_duckdb_indexes.py example.com

# Limit results
python3 search_parallel_duckdb_indexes.py example.com --limit 100

# Search specific collections
python3 search_parallel_duckdb_indexes.py example.com --collections CC-MAIN-2024-33,CC-MAIN-2024-38

# Get JSON output
python3 search_parallel_duckdb_indexes.py example.com --json
```

## Build/Rebuild Index
```bash
# Build all collection indexes
python3 build_parallel_duckdb_indexes.py

# Monitor progress
tail -f overnight_parallel_index_build.sh.log
tail -f logs/build_CC-MAIN-*_*.log
```

## Benchmark Performance
```bash
python3 benchmark_parallel_duckdb_indexes.py
```

## Check Status
```bash
# List all indexes
ls -lh /storage/ccindex_duckdb/cc_pointers_by_collection/

# Check index contents
python3 -c "
import duckdb
con = duckdb.connect('/storage/ccindex_duckdb/cc_pointers_by_collection/CC-MAIN-2024-33.duckdb', read_only=True)
print(con.execute('SELECT COUNT(*) FROM domain_pointers').fetchone())
print(con.execute('SELECT COUNT(DISTINCT domain) FROM domain_pointers').fetchone())
con.close()
"

# View build logs
grep "COMPLETED" logs/build_CC-MAIN-*.log
```

## Architecture Summary
- **Index**: `domain → (parquet_file, row_offset, row_count)`
- **Search**: O(log n) domain lookup + direct parquet access at offset
- **Parallel**: Independent per-collection indexes
- **Flexible**: Can query any combination of collections

## Performance
- **Search time**: ~640ms across 3 collections (761K domains)
- **Build time**: ~12-22 min per large collection (250K domains)
- **Memory**: Efficient (no full parquet loading needed)
