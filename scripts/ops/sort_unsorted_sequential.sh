#!/bin/bash
# Sort files ONE AT A TIME (sequential) to guarantee no OOM
# Slower but 100% safe

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-${REPO_ROOT}/.venv/bin/python}"

if [[ -x "${VENV_PYTHON}" ]]; then
    :
elif command -v "${VENV_PYTHON}" >/dev/null 2>&1; then
    :
else
    echo "ERROR: Python interpreter not found: ${VENV_PYTHON}" >&2
    exit 1
fi

UNSORTED_LIST="$1"
PARQUET_ROOT="/storage/ccindex_parquet"
TEMP_DIR="/tmp/sort_sequential"

mkdir -p "${TEMP_DIR}"

echo "===================================================================================="
echo "SEQUENTIAL SORTING (One at a time - Memory Safe)"
echo "===================================================================================="
echo ""

TOTAL=$(wc -l < "${UNSORTED_LIST}")
echo "Files to sort: ${TOTAL}"
echo "Method: Sequential (one at a time)"
echo "Memory per sort: 4GB max"
echo "Estimated time: $((TOTAL * 2)) minutes (~2 min per file)"
echo ""

SUCCESS=0
FAILED=0
CURRENT=0

while IFS= read -r RELPATH; do
    CURRENT=$((CURRENT + 1))
    FULL_PATH="${PARQUET_ROOT}/${RELPATH}"
    
    if [ ! -f "${FULL_PATH}" ]; then
        echo "[$CURRENT/$TOTAL] ❌ File not found: ${RELPATH}"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    echo "[$CURRENT/$TOTAL] Sorting: ${RELPATH}"
    
    SORTED_TMP="${TEMP_DIR}/$(basename ${FULL_PATH}).sorted.tmp"
    
    # Sort with DuckDB
    "${VENV_PYTHON}" << PYEOF
import duckdb
import sys

try:
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='2GB'")
    con.execute("SET temp_directory='${TEMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=1")  # Single thread to reduce memory
    con.execute("""
        COPY (
            SELECT * FROM read_parquet('${FULL_PATH}')
            ORDER BY host_rev, url, ts
        )
        TO '${SORTED_TMP}' (FORMAT 'parquet', COMPRESSION 'zstd')
    """)
    con.close()
    sys.exit(0)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
    
    if [ $? -eq 0 ]; then
        # Verify and replace
        if [ -f "${SORTED_TMP}" ]; then
            mv "${SORTED_TMP}" "${FULL_PATH}"
            echo "[$CURRENT/$TOTAL] ✅ Sorted: ${RELPATH}"
            SUCCESS=$((SUCCESS + 1))
        else
            echo "[$CURRENT/$TOTAL] ❌ Sort output missing: ${RELPATH}"
            FAILED=$((FAILED + 1))
        fi
    else
        echo "[$CURRENT/$TOTAL] ❌ Sort failed: ${RELPATH}"
        FAILED=$((FAILED + 1))
    fi
    
    # Clean up temp files after each sort
    rm -f "${TEMP_DIR}"/*.tmp 2>/dev/null
    rm -f "${TEMP_DIR}"/duckdb_temp_* 2>/dev/null
    
    if [ $((CURRENT % 10)) -eq 0 ]; then
        echo "Progress: $CURRENT/$TOTAL - Success: $SUCCESS, Failed: $FAILED"
        echo "Memory status:"
        free -h | grep Mem:
        echo ""
    fi
    
done < "${UNSORTED_LIST}"

echo ""
echo "===================================================================================="
echo "SORTING COMPLETE"
echo "===================================================================================="
echo "Total:   ${TOTAL}"
echo "Success: ${SUCCESS}"
echo "Failed:  ${FAILED}"
echo ""

# Cleanup
rm -rf "${TEMP_DIR}"

if [ ${FAILED} -gt 0 ]; then
    exit 1
fi

exit 0
