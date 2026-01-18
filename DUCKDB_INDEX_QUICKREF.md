# DuckDB Pointer Index - Quick Reference

## Search for a Domain
```bash
python3 search_parallel_duckdb_indexes.py <domain>
```

## Search via Meta Indexes (Master → Year → Collection → Parquet → WARC)

This is the recommended verification path once meta-indexes exist.

```bash
# Master meta-index (all registered years)
python3 search_cc_via_meta_indexes.py --domain 18f.gov \
	--master-db /storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb \
	--parquet-root /storage/ccindex_parquet \
	--max-matches 25

# Restrict to a year (still starting from master)
python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 25

# Start from a year meta-index directly
python3 search_cc_via_meta_indexes.py --domain 18f.gov \
	--year-db /storage/ccindex_duckdb/cc_pointers_by_year/cc_pointers_2024.duckdb \
	--parquet-root /storage/ccindex_parquet \
	--max-matches 25
```

## Turn Pointer Results into Candidate WARC Files

```bash
# Unique list of WARC files to fetch
python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 5000 \
	| python3 warc_candidates_from_jsonl.py --format list \
	> warc_candidates.txt

# Unique list of full download URLs
python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 5000 \
	| python3 warc_candidates_from_jsonl.py --format list --prefix https://data.commoncrawl.org/ \
	> warc_candidate_urls.txt

# Summarize by WARC (counts/bytes), keep top 50
python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 5000 \
	| python3 warc_candidates_from_jsonl.py --format json --sort bytes --max-warcs 50 \
	> warc_candidates_top50.json
```

## Verify WARC URLs Are Fetchable

```bash
# Check a few candidates with HEAD and a tiny Range GET
python3 search_cc_via_meta_indexes.py --domain 18f.gov --year 2024 --max-matches 5000 \
  | python3 warc_candidates_from_jsonl.py --format list --prefix https://data.commoncrawl.org/ --max-warcs 5 \
  | python3 verify_warc_retrieval.py --range 0:63
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
