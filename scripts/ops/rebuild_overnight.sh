#!/bin/bash
# Rebuild as compact domain index (instead of 166GB URL index)

set -euo pipefail

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/storage/ccindex_duckdb/logs/rebuild_domain_index_${TIMESTAMP}.log"
mkdir -p /storage/ccindex_duckdb/logs

echo "===================================================================================="
echo "REBUILDING AS DOMAIN INDEX"
echo "===================================================================================="
echo ""
echo "Current: 166 GB URL-mode index (old design)"
echo "Target:  ~1-2 GB domain-mode index (new design)"
echo ""
echo "This will:"
echo "  1. Read existing 3,081 parquet files"
echo "  2. Extract domain -> parquet mappings"
echo "  3. Build compact DuckDB index"
echo "  4. Add row group optimization"
echo "  5. Run benchmarks"
echo ""
echo "Log: ${LOG_FILE}"
echo "===================================================================================="
echo ""

exec > >(tee -a "${LOG_FILE}") 2>&1

echo "Started: $(date)"
echo ""

# Move old DB out of the way
OLD_DB="/storage/ccindex_duckdb/cc_pointers_by_year/cc_pointers_2024.duckdb"
if [ -f "${OLD_DB}" ]; then
    BACKUP="${OLD_DB}.url_mode_backup_${TIMESTAMP}"
    echo "Backing up old index: ${BACKUP}"
    mv "${OLD_DB}" "${BACKUP}"
    echo "  ✅ Backed up (can delete later to save 166 GB)"
    echo ""
fi

# Build new domain index from existing parquet files
echo "Building domain index from parquet files..."
echo ""

/home/barberb/municipal_scrape_workspace/.venv/bin/python build_cc_pointer_duckdb.py \
    --input-root /storage/ccindex \
    --db /storage/ccindex_duckdb/cc_domain_by_year \
    --shard-by-year \
    --collections-regex 'CC-MAIN-2024-.*' \
    --duckdb-index-mode domain \
    --domain-index-action rebuild \
    --domain-range-index \
    --parquet-out /storage/ccindex_parquet/cc_pointers_by_year \
    --parquet-action skip-if-exists \
    --parquet-compression zstd \
    --threads 56 \
    --create-indexes \
    --progress-dir /storage/ccindex_duckdb/progress

BUILD_EXIT=$?

echo ""
echo "===================================================================================="
if [ ${BUILD_EXIT} -eq 0 ]; then
    echo "✅ BUILD SUCCESSFUL"
    echo ""
    
    # Show new DB size
    NEW_DB="/storage/ccindex_duckdb/cc_domain_by_year/cc_pointers_2024.duckdb"
    if [ -f "${NEW_DB}" ]; then
        SIZE=$(ls -lh "${NEW_DB}" | awk '{print $5}')
        echo "New domain index: ${SIZE}"
        echo ""
        
        # Run quick benchmark
        echo "Running benchmark..."
        /home/barberb/municipal_scrape_workspace/.venv/bin/python benchmarks/ccindex/benchmark_cc_duckdb_search.py \
            --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \
            --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \
            --quick
        
        echo ""
        echo "===================================================================================="
        echo "COMPLETE!"
        echo "===================================================================================="
        echo ""
        echo "Old index: 166 GB (backed up)"
        echo "New index: ${SIZE}"
        echo "Savings: ~165 GB"
        echo ""
        echo "Test a search:"
        echo "  python search_cc_duckdb_index.py \\"
        echo "    --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \\"
        echo "    --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \\"
        echo "    --domain whitehouse.gov --verbose"
        echo ""
        echo "Delete old backup when ready:"
        echo "  rm ${BACKUP}"
        echo "  # (saves 166 GB)"
    fi
else
    echo "❌ BUILD FAILED (exit code: ${BUILD_EXIT})"
    echo "Check log: ${LOG_FILE}"
fi

echo ""
echo "Finished: $(date)"
echo "===================================================================================="

exit ${BUILD_EXIT}
