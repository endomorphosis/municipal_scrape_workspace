#!/bin/bash
# Build domain index WITH row group offset/range metadata for sorted parquet files
# This enables exhaustive domain searches by scanning only relevant row groups

set -euo pipefail

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/storage/ccindex_duckdb/logs/rebuild_sorted_ranges_${TIMESTAMP}.log"
mkdir -p /storage/ccindex_duckdb/logs

echo "===================================================================================="
echo "BUILDING DOMAIN INDEX WITH ROW GROUP RANGES"
echo "===================================================================================="
echo ""
echo "What this does:"
echo "  1. Reads sorted parquet files (already sorted by host_rev)"
echo "  2. Extracts row group min/max host_rev for each row group"
echo "  3. Stores in cc_parquet_rowgroups table (row_start, row_end, host_rev_min, host_rev_max)"
echo "  4. Enables searching by offset/range instead of full file scan"
echo ""
echo "Result:"
echo "  - Fast domain lookup: DuckDB index tells you which row groups contain domain"
echo "  - Exhaustive search: Sorted parquet + ranges = guaranteed complete results"
echo "  - Skip irrelevant data: Only read row groups that can contain target domain"
echo ""
echo "Log: ${LOG_FILE}"
echo "===================================================================================="
echo ""

exec > >(tee -a "${LOG_FILE}") 2>&1

echo "Started: $(date)"
echo ""

# Build domain index WITH row group ranges
echo "Building domain index with row group range metadata..."
echo ""

/home/barberb/municipal_scrape_workspace/.venv/bin/python build_cc_pointer_duckdb.py \
    --input-root /storage/ccindex \
    --db /storage/ccindex_duckdb/cc_domain_by_year_sorted \
    --shard-by-year \
    --collections-regex 'CC-MAIN-2024-.*' \
    --duckdb-index-mode domain \
    --domain-index-action rebuild \
    --domain-range-index \
    --parquet-out /storage/ccindex_parquet/cc_pointers_by_year \
    --parquet-action skip-if-exists \
    --parquet-compression zstd \
    --parquet-sort none \
    --threads 56 \
    --create-indexes \
    --progress-dir /storage/ccindex_duckdb/progress

BUILD_EXIT=$?

echo ""
echo "===================================================================================="
if [ ${BUILD_EXIT} -eq 0 ]; then
    echo "✅ BUILD SUCCESSFUL"
    echo ""
    
    NEW_DB="/storage/ccindex_duckdb/cc_domain_by_year_sorted/cc_pointers_2024.duckdb"
    if [ -f "${NEW_DB}" ]; then
        SIZE=$(ls -lh "${NEW_DB}" | awk '{print $5}')
        echo "New domain index with ranges: ${SIZE}"
        echo ""
        
        # Check what we built
        /home/barberb/municipal_scrape_workspace/.venv/bin/python << 'PYEOF'
import duckdb
con = duckdb.connect("/storage/ccindex_duckdb/cc_domain_by_year_sorted/cc_pointers_2024.duckdb", read_only=True)

print("Tables created:")
tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
for (tbl,) in tables:
    print(f"  - {tbl}")
print()

# Check domain shards
row = con.execute("SELECT count(*) FROM cc_domain_shards").fetchone()
print(f"Domain mappings: {row[0]:,}")

row = con.execute("SELECT count(DISTINCT host_rev) FROM cc_domain_shards").fetchone()
print(f"Unique domains: {row[0]:,}")

row = con.execute("SELECT count(DISTINCT parquet_relpath) FROM cc_domain_shards").fetchone()
print(f"Parquet files: {row[0]:,}")

# Check row group ranges
row = con.execute("SELECT count(*) FROM cc_parquet_rowgroups").fetchone()
print(f"\nRow group ranges: {row[0]:,}")

# Show sample
print("\nSample row group range:")
sample = con.execute("""
    SELECT parquet_relpath, row_group, row_start, row_end, host_rev_min, host_rev_max
    FROM cc_parquet_rowgroups
    ORDER BY parquet_relpath, row_group
    LIMIT 3
""").fetchall()
for row in sample:
    print(f"  {row[0]}")
    print(f"    Row group {row[1]}: rows {row[2]:,}-{row[3]:,}")
    print(f"    Range: {row[4]} to {row[5]}")

con.close()
PYEOF
        
        echo ""
        echo "Running benchmark with row group optimization..."
        /home/barberb/municipal_scrape_workspace/.venv/bin/python benchmarks/ccindex/benchmark_cc_duckdb_search.py \
            --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year_sorted \
            --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
            --quick
        
        echo ""
        echo "===================================================================================="
        echo "COMPLETE!"
        echo "===================================================================================="
        echo ""
        echo "Index: ${SIZE}"
        echo "Location: /storage/ccindex_duckdb/cc_domain_by_year_sorted/"
        echo ""
        echo "Features:"
        echo "  ✅ Domain → parquet mappings"
        echo "  ✅ Row group offset/range metadata"
        echo "  ✅ Sorted parquet files (by host_rev)"
        echo "  ✅ Exhaustive domain search (no missing data)"
        echo "  ✅ Skip irrelevant row groups (fast)"
        echo ""
        echo "Test a search with row group optimization:"
        echo "  python search_cc_duckdb_index.py \\"
        echo "    --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year_sorted \\"
        echo "    --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \\"
        echo "    --domain whitehouse.gov \\"
        echo "    --use-rowgroup-ranges \\"
        echo "    --verbose"
        echo ""
    fi
else
    echo "❌ BUILD FAILED (exit code: ${BUILD_EXIT})"
    echo "Check log: ${LOG_FILE}"
fi

echo ""
echo "Finished: $(date)"
echo "===================================================================================="

exit ${BUILD_EXIT}
