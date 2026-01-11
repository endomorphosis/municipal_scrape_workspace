#!/bin/bash
#
# Overnight job to build parallel DuckDB pointer indexes
# This script:
# 1. Cleans up old index directory
# 2. Verifies all parquet files are sorted
# 3. Builds one DuckDB index per collection in parallel
# 4. Creates master index
# 5. Runs validation searches
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/overnight_parallel_index_$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "========================================================================"
log "OVERNIGHT PARALLEL DUCKDB INDEX BUILD"
log "========================================================================"

# Step 1: Clean up old index directory
log ""
log "Step 1: Cleaning up old index directory..."
if [ -d "/storage/ccindex_duckdb/cc_pointers_by_year" ]; then
    log "  Removing old cc_pointers_by_year directory..."
    rm -rf /storage/ccindex_duckdb/cc_pointers_by_year
    log "  ✓ Cleaned up"
fi

# Step 2: Verify parquet files are sorted
log ""
log "Step 2: Verifying parquet files are sorted..."
UNSORTED_COUNT=$(python3 "$SCRIPT_DIR/parallel_validate_parquet.py" 2>&1 | grep -c "NOT SORTED" || true)

if [ "$UNSORTED_COUNT" -gt 0 ]; then
    log "  WARNING: Found $UNSORTED_COUNT unsorted files"
    log "  Please sort all files before building index"
    exit 1
else
    log "  ✓ All parquet files are sorted"
fi

# Step 3: Check disk space
log ""
log "Step 3: Checking disk space..."
AVAILABLE_GB=$(df -BG /storage/ccindex_duckdb | tail -1 | awk '{print $4}' | sed 's/G//')
log "  Available space: ${AVAILABLE_GB}GB"

if [ "$AVAILABLE_GB" -lt 100 ]; then
    log "  WARNING: Low disk space (< 100GB)"
    log "  Attempting to free space by removing ZFS snapshots..."
    
    # Remove auto snapshots
    SNAPSHOTS=$(zfs list -t snapshot -o name | grep ccindex_duckdb | grep auto || true)
    if [ -n "$SNAPSHOTS" ]; then
        echo "$SNAPSHOTS" | while read snap; do
            log "    Removing snapshot: $snap"
            sudo zfs destroy "$snap" || true
        done
        
        AVAILABLE_GB=$(df -BG /storage/ccindex_duckdb | tail -1 | awk '{print $4}' | sed 's/G//')
        log "  Available space after cleanup: ${AVAILABLE_GB}GB"
    fi
fi

# Step 4: Build parallel indexes
log ""
log "Step 4: Building parallel DuckDB indexes..."
log "  Starting parallel index build..."

python3 "$SCRIPT_DIR/build_parallel_duckdb_indexes.py" 2>&1 | tee -a "$LOG_FILE"

BUILD_STATUS=$?
if [ $BUILD_STATUS -ne 0 ]; then
    log "  ✗ Index build failed with status $BUILD_STATUS"
    exit 1
fi

log "  ✓ Index build completed"

# Step 5: Verify indexes were created
log ""
log "Step 5: Verifying indexes..."
INDEX_COUNT=$(find /storage/ccindex_duckdb/cc_pointers_by_collection -name "*.duckdb" -type f | wc -l)
log "  Found $INDEX_COUNT index files"

if [ "$INDEX_COUNT" -eq 0 ]; then
    log "  ✗ No indexes were created!"
    exit 1
fi

# Step 6: Test searches
log ""
log "Step 6: Testing searches..."

TEST_DOMAINS=("example.com" "wikipedia.org" "github.com")

for domain in "${TEST_DOMAINS[@]}"; do
    log "  Testing search for: $domain"
    
    SEARCH_OUTPUT=$(python3 "$SCRIPT_DIR/search_parallel_duckdb_indexes.py" "$domain" --limit 10 2>&1 || true)
    RESULT_COUNT=$(echo "$SEARCH_OUTPUT" | grep "Found" | awk '{print $2}' || echo "0")
    
    if [ "$RESULT_COUNT" != "0" ]; then
        log "    ✓ Found $RESULT_COUNT results"
    else
        log "    ⚠ No results found (may be expected)"
    fi
done

# Step 7: Run benchmark
log ""
log "Step 7: Running benchmark..."
python3 "$SCRIPT_DIR/benchmark_parallel_duckdb_indexes.py" 2>&1 | tee -a "$LOG_FILE"

# Final summary
log ""
log "========================================================================"
log "OVERNIGHT BUILD COMPLETE"
log "========================================================================"
log ""
log "Summary:"
log "  - Index directory: /storage/ccindex_duckdb/cc_pointers_by_collection"
log "  - Number of indexes: $INDEX_COUNT"
log "  - Log file: $LOG_FILE"
log ""
log "Usage:"
log "  Search: python3 search_parallel_duckdb_indexes.py <domain>"
log "  Benchmark: python3 benchmark_parallel_duckdb_indexes.py"
log ""
log "Done at $(date)"
