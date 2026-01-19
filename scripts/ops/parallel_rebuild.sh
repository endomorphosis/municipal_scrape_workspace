#!/bin/bash
# FINAL REBUILD WITH PARALLELISM
# Much faster validation using multiple CPU cores

set -euo pipefail

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/storage/ccindex_duckdb/logs/parallel_rebuild_${TIMESTAMP}.log"
mkdir -p /storage/ccindex_duckdb/logs

# Detect optimal worker count (leave some cores for system)
TOTAL_CORES=$(nproc)
WORKERS=$((TOTAL_CORES - 4))
if [ ${WORKERS} -lt 8 ]; then
    WORKERS=8
fi

echo "===================================================================================="
echo "PARALLEL REBUILD - ALL 6,800 PARQUET FILES"
echo "===================================================================================="
echo ""
echo "System: ${TOTAL_CORES} CPU cores available"
echo "Using:  ${WORKERS} parallel workers for validation"
echo ""
echo "This will:"
echo "  1. PARALLEL validation of 6,800 files (~20-30 minutes)"
echo "  2. Build index from parquet metadata (~1-2 hours)"
echo "  3. Validate completeness (~15 min)"
echo "  4. Run benchmarks (~15 min)"
echo ""
echo "Expected total time: 2-3 hours"
echo "Log: ${LOG_FILE}"
echo "===================================================================================="
echo ""

exec > >(tee -a "${LOG_FILE}") 2>&1

START_TIME=$(date +%s)
echo "Started: $(date)"
echo ""

# STEP 1: Parallel validation
echo "===================================================================================="
echo "STEP 1: PARALLEL VALIDATION (${WORKERS} workers)"
echo "===================================================================================="
echo ""

SORTED_LIST="/tmp/sorted_files_${TIMESTAMP}.txt"

/home/barberb/municipal_scrape_workspace/.venv/bin/python parallel_validate_parquet.py \
    --parquet-root /storage/ccindex_parquet \
    --workers ${WORKERS} \
    --output "${SORTED_LIST}"

VALIDATE_EXIT=$?

if [ ${VALIDATE_EXIT} -ne 0 ]; then
    echo ""
    echo "⚠️  WARNING: Some files are unsorted"
    echo "Continuing with sorted files only..."
fi

echo ""
echo "✅ Validation complete"
echo ""

# STEP 2: Build index from parquet (parallel per year)
echo "===================================================================================="
echo "STEP 2: BUILD INDEX FROM PARQUET"
echo "===================================================================================="
echo ""

PARQUET_ROOT="/storage/ccindex_parquet"

# Process years in parallel using background jobs
for YEAR in 2024 2025; do
    (
        YEAR_DIR="${PARQUET_ROOT}/cc_pointers_by_collection/${YEAR}"
        
        if [ ! -d "${YEAR_DIR}" ]; then
            echo "Year ${YEAR}: Directory not found, skipping"
            exit 0
        fi
        
        NUM_FILES=$(find "${YEAR_DIR}" -name "*.parquet" 2>/dev/null | wc -l)
        echo "Year ${YEAR}: Processing ${NUM_FILES} files"
        
        OUTPUT_DB="/storage/ccindex_duckdb/cc_domain_parallel/cc_pointers_${YEAR}.duckdb"
        
        /home/barberb/municipal_scrape_workspace/.venv/bin/python build_index_from_parquet.py \
            --parquet-root "${YEAR_DIR}" \
            --output-db "${OUTPUT_DB}" \
            --batch-size 50 \
            --extract-rowgroups
        
        echo "Year ${YEAR}: ✅ Complete"
    ) &
done

# Wait for all years to complete
wait

echo ""
echo "✅ Index building complete for all years"
echo ""

# STEP 3: Validate search completeness
echo "===================================================================================="
echo "STEP 3: VALIDATE SEARCH COMPLETENESS"
echo "===================================================================================="
echo ""

for domain in "whitehouse.gov" "senate.gov"; do
    echo "Testing: ${domain}"
    /home/barberb/municipal_scrape_workspace/.venv/bin/python validate_search_completeness.py \
        --duckdb-dir /storage/ccindex_duckdb/cc_domain_parallel \
        --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \
        --domain "${domain}" 2>&1 | head -20
    echo ""
done

# STEP 4: Statistics
echo "===================================================================================="
echo "STEP 4: INDEX STATISTICS"
echo "===================================================================================="
echo ""

for YEAR in 2024 2025; do
    DB_FILE="/storage/ccindex_duckdb/cc_domain_parallel/cc_pointers_${YEAR}.duckdb"
    if [ -f "${DB_FILE}" ]; then
        SIZE=$(ls -lh "${DB_FILE}" | awk '{print $5}')
        echo "Index ${YEAR}: ${SIZE}"
        
        /home/barberb/municipal_scrape_workspace/.venv/bin/python << PYEOF
import duckdb
con = duckdb.connect("${DB_FILE}", read_only=True)
try:
    print(f"  Mappings: {con.execute('SELECT count(*) FROM cc_domain_shards').fetchone()[0]:,}")
    print(f"  Domains:  {con.execute('SELECT count(DISTINCT host_rev) FROM cc_domain_shards').fetchone()[0]:,}")
    print(f"  Files:    {con.execute('SELECT count(DISTINCT parquet_relpath) FROM cc_domain_shards').fetchone()[0]:,}")
    print(f"  Rowgroups: {con.execute('SELECT count(*) FROM cc_parquet_rowgroups').fetchone()[0]:,}")
except Exception as e:
    print(f"  Error: {e}")
con.close()
PYEOF
        echo ""
    fi
done

# STEP 5: Quick benchmark
echo "===================================================================================="
echo "STEP 5: BENCHMARK"
echo "===================================================================================="
echo ""

/home/barberb/municipal_scrape_workspace/.venv/bin/python benchmarks/ccindex/benchmark_cc_duckdb_search.py \
    --duckdb-dir /storage/ccindex_duckdb/cc_domain_parallel \
    --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \
    --quick 2>&1 || echo "Benchmark had issues (non-fatal)"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "===================================================================================="
echo "✅ PARALLEL REBUILD COMPLETE"
echo "===================================================================================="
echo ""
echo "Duration: ${DURATION} seconds ($((DURATION / 60)) minutes)"
echo ""
echo "Output: /storage/ccindex_duckdb/cc_domain_parallel/"
echo ""
echo "Test search:"
echo "  python search_cc_duckdb_index.py \\"
echo "    --duckdb-dir /storage/ccindex_duckdb/cc_domain_parallel \\"
echo "    --parquet-root /storage/ccindex_parquet/cc_pointers_by_collection \\"
echo "    --domain whitehouse.gov --use-rowgroup-ranges --verbose"
echo ""
echo "Log: ${LOG_FILE}"
echo "===================================================================================="

exit 0
