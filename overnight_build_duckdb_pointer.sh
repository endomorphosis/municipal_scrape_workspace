#!/bin/bash
# Overnight job to build DuckDB domain pointer index from sorted parquet files

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${SCRIPT_DIR}/overnight_duckdb_pointer_${TIMESTAMP}.log"

echo "Starting DuckDB Pointer Index Build - $(date)" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"

# Configuration
PARQUET_ROOT="/storage/ccindex_parquet/cc_pointers_by_year"
DB_PATH="/storage/ccindex_duckdb/domain_pointer.duckdb"
THREADS=8
BATCH_ROWS=1000000

# Ensure database directory exists
mkdir -p "$(dirname "$DB_PATH")"

# Remove old database to start fresh
if [ -f "$DB_PATH" ]; then
    echo "Removing old database..." | tee -a "$LOG_FILE"
    rm -f "$DB_PATH" "$DB_PATH.wal"
fi

# Build the pointer index from existing sorted parquet files
echo "Building pointer index from sorted parquet files..." | tee -a "$LOG_FILE"
python3 "${SCRIPT_DIR}/build_duckdb_pointer_from_parquet.py" \
    --db "$DB_PATH" \
    --parquet-root "$PARQUET_ROOT" \
    --verbose \
    2>&1 | tee -a "$LOG_FILE"

BUILD_EXIT=$?

if [ $BUILD_EXIT -eq 0 ]; then
    echo "Build completed successfully - $(date)" | tee -a "$LOG_FILE"
    
    # Run validation benchmark
    echo -e "\nRunning validation benchmark..." | tee -a "$LOG_FILE"
    python3 "${SCRIPT_DIR}/benchmark_duckdb_pointer_domain.py" \
        --db "$DB_PATH" \
        --parquet-root "$PARQUET_ROOT" \
        --domains 10 \
        --validate \
        2>&1 | tee -a "$LOG_FILE"
    
    echo -e "\n✓ Overnight build complete! - $(date)" | tee -a "$LOG_FILE"
    echo "Database: $DB_PATH" | tee -a "$LOG_FILE"
    echo "Log: $LOG_FILE" | tee -a "$LOG_FILE"
else
    echo "Build failed with exit code $BUILD_EXIT - $(date)" | tee -a "$LOG_FILE"
    exit $BUILD_EXIT
fi
