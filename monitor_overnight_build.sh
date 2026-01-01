#!/bin/bash
# Monitor progress of overnight DuckDB build

echo "================================================"
echo "Overnight DuckDB Build - Progress Monitor"
echo "================================================"
echo

# Check if conversion is running
if pgrep -f "parallel_convert_missing.py" > /dev/null; then
    echo "Status: ⏳ CONVERTING .gz → .parquet"
    echo
    echo "Worker Processes:"
    ps aux | grep parallel_convert_missing | grep -v grep | head -9 | awk '{printf "  PID %s: CPU %s%%, MEM %s%%\n", $2, $3, $4}'
    echo
    echo "Latest Progress:"
    tail -10 conversion_progress.log 2>/dev/null | grep -E '\[|✓|✗|Progress'
    echo
    echo "Parquet Files Created:"
    find /storage/ccindex_parquet/cc_pointers_by_year -name "*.parquet" 2>/dev/null | wc -l
else
    echo "Status: ✓ Conversion complete"
    echo
    if [ -f conversion_progress.log ]; then
        echo "Final Stats:"
        tail -15 conversion_progress.log | grep -E 'Success|Failed|complete'
    fi
fi

echo
echo "================================================"

# Check if overnight job is running  
if pgrep -f "overnight_duckdb_complete.sh" > /dev/null; then
    echo "Orchestration: ⏳ RUNNING"
    echo
    echo "Current Step:"
    tail -5 overnight_duckdb_*.log 2>/dev/null | tail -5
else
    echo "Orchestration: ⏸ Not started or completed"
    if ls overnight_duckdb_*.log 2>/dev/null; then
        echo
        echo "Last log entry:"
        tail -3 $(ls -t overnight_duckdb_*.log | head -1) 2>/dev/null
    fi
fi

echo
echo "================================================"
echo "System Resources"
echo "================================================"
echo "Memory: $(free -h | awk '/^Mem:/ {print $7}') available"
echo "Disk:   $(df -h /storage | tail -1 | awk '{print $4}') available on /storage"
echo "Load:   $(uptime | awk -F'load average:' '{print $2}')"
echo
echo "================================================"
