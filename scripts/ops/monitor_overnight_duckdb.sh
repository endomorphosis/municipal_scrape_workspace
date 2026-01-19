#!/bin/bash
# Monitor overnight DuckDB pointer index build

echo "========================================================================"
echo "DUCKDB POINTER INDEX - OVERNIGHT BUILD MONITOR"
echo "========================================================================"
echo ""

# Check if build is running
BUILD_PID=$(ps aux | grep "build_duckdb_from_sorted_parquet.py" | grep -v grep | awk '{print $2}')

if [ -z "$BUILD_PID" ]; then
    echo "❌ Build process NOT RUNNING"
    echo ""
    echo "Check if it completed or failed:"
    echo "  tail -50 /storage/ccindex_duckdb/logs/*.log"
    exit 1
fi

echo "✅ Build process RUNNING"
echo "   PID: $BUILD_PID"
echo ""

# Get process info
echo "Process Information:"
echo "---"
ps aux | grep "$BUILD_PID" | grep -v grep | awk '{printf "  CPU: %s%%\n  Memory: %s MB\n  Runtime: %s\n", $3, int($6/1024), $10}'
echo ""

# Check database
echo "Database Status:"
echo "---"
if [ -f /storage/ccindex_duckdb/domain_pointer.duckdb ]; then
    DB_SIZE=$(du -h /storage/ccindex_duckdb/domain_pointer.duckdb | cut -f1)
    DB_FILE_SIZE=$(ls -lh /storage/ccindex_duckdb/domain_pointer.duckdb | awk '{print $5}')
    echo "  Database file: $DB_FILE_SIZE (actual: $DB_SIZE)"
else
    echo "  ❌ Database file not found"
fi

if [ -f /storage/ccindex_duckdb/domain_pointer.duckdb.wal ]; then
    WAL_SIZE=$(ls -lh /storage/ccindex_duckdb/domain_pointer.duckdb.wal | awk '{print $5}')
    echo "  WAL file: $WAL_SIZE (active writes)"
else
    echo "  No WAL file (may have checkpointed)"
fi

DB_MODIFIED=$(stat -c %y /storage/ccindex_duckdb/domain_pointer.duckdb 2>/dev/null | cut -d. -f1)
echo "  Last modified: $DB_MODIFIED"
echo ""

# Parquet files status
echo "Parquet Files:"
echo "---"
TOTAL_PARQUET=$(find /storage/ccindex_parquet -name "*.parquet" | wc -l)
echo "  Total parquet files: $TOTAL_PARQUET"
echo ""

# Try to query database (read-only)
echo "Index Progress (if accessible):"
echo "---"
duckdb /storage/ccindex_duckdb/domain_pointer.duckdb -readonly <<EOF 2>/dev/null
SELECT 
    COUNT(DISTINCT domain) as domains,
    COUNT(*) as pointers,
    COUNT(DISTINCT parquet_file) as files_processed,
    SUM(row_count) as total_urls
FROM domain_pointers;
EOF

if [ $? -ne 0 ]; then
    echo "  (Database locked during write, can't query yet)"
fi

echo ""

# System resources
echo "System Resources:"
echo "---"
FREE_MEM=$(free -h | grep Mem | awk '{print $4}')
FREE_DISK=$(df -h /storage | tail -1 | awk '{print $4}')
echo "  Free memory: $FREE_MEM"
echo "  Free disk space: $FREE_DISK"
echo ""

# IO stats for the process
if [ -f /proc/$BUILD_PID/io ]; then
    echo "I/O Statistics:"
    echo "---"
    grep -E "^(read|write)_bytes" /proc/$BUILD_PID/io | while read line; do
        key=$(echo $line | cut -d: -f1)
        value=$(echo $line | cut -d: -f2 | xargs)
        value_gb=$(echo "scale=2; $value / 1024 / 1024 / 1024" | bc)
        echo "  $key: ${value_gb} GB"
    done
    echo ""
fi

# Estimate completion time
echo "Estimated Progress:"
echo "---"

# Try to get progress from database
PROGRESS=$(duckdb /storage/ccindex_duckdb/domain_pointer.duckdb -readonly -c "SELECT COUNT(DISTINCT parquet_file) FROM domain_pointers" 2>/dev/null)

if [ ! -z "$PROGRESS" ] && [ "$PROGRESS" != "" ]; then
    PERCENT=$(echo "scale=1; $PROGRESS * 100 / $TOTAL_PARQUET" | bc)
    echo "  Files processed: $PROGRESS / $TOTAL_PARQUET ($PERCENT%)"
    
    if [ "$PROGRESS" -gt 0 ]; then
        # Estimate time remaining based on current rate
        RUNTIME_SEC=$(ps -p $BUILD_PID -o etimes= | xargs)
        SEC_PER_FILE=$(echo "scale=2; $RUNTIME_SEC / $PROGRESS" | bc)
        REMAINING_FILES=$(echo "$TOTAL_PARQUET - $PROGRESS" | bc)
        REMAINING_SEC=$(echo "$SEC_PER_FILE * $REMAINING_FILES" | bc | cut -d. -f1)
        REMAINING_HOURS=$(echo "scale=1; $REMAINING_SEC / 3600" | bc)
        
        echo "  Average time per file: ${SEC_PER_FILE}s"
        echo "  Estimated remaining: ${REMAINING_HOURS} hours"
    fi
else
    echo "  (Cannot estimate - database locked)"
fi

echo ""
echo "========================================================================"
echo "Monitor commands:"
echo "  watch -n 60 '$0'          # Auto-refresh every minute"
echo "  tail -f logs/*.log        # Watch build logs"
echo "  htop -p $BUILD_PID        # Monitor process resources"
echo "========================================================================"
