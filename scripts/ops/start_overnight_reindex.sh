#!/bin/bash
# Start overnight sorting and reindexing job
# Created: 2025-12-31 02:51 UTC

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="/storage/ccindex_duckdb/logs"
mkdir -p "${LOG_DIR}"

MAIN_LOG="${LOG_DIR}/overnight_reindex_${TIMESTAMP}.log"

echo "================================================================================"
echo "Starting Overnight Sort and Reindex Job"
echo "================================================================================"
echo "Started at: $(date)"
echo "Log file: ${MAIN_LOG}"
echo ""

# Redirect all output to log file
exec > >(tee -a "${MAIN_LOG}") 2>&1

echo "Configuration:"
echo "  Collections: CC-MAIN-2024-* (all 2024)"
echo "  Threads: $(nproc)"
echo "  Working directory: $(pwd)"
echo ""

# Run the overnight build script
"${SCRIPT_DIR}/overnight_build_duckdb_index.sh" \
  --collections-regex 'CC-MAIN-2024-.*'

EXIT_CODE=$?

echo ""
echo "================================================================================"
echo "Overnight Job Completed"
echo "================================================================================"
echo "Exit code: ${EXIT_CODE}"
echo "Finished at: $(date)"
echo "Log file: ${MAIN_LOG}"
echo ""

if [ ${EXIT_CODE} -eq 0 ]; then
    echo "✅ SUCCESS: Job completed successfully"
    echo ""
    echo "Next steps:"
    echo "  1. Review the report:"
    echo "     cat /storage/ccindex_duckdb/reports/overnight_report_*.txt | tail -100"
    echo ""
    echo "  2. Test a search:"
    echo "     python3 ${REPO_ROOT}/search_cc_duckdb_index.py \\"
    echo "       --duckdb-dir /storage/ccindex_duckdb/cc_domain_by_year \\"
    echo "       --parquet-root /storage/ccindex_parquet/cc_pointers_by_year \\"
    echo "       --domain whitehouse.gov --verbose"
    echo ""
    echo "  3. Check DuckDB files:"
    echo "     ls -lh /storage/ccindex_duckdb/cc_domain_by_year/"
else
    echo "❌ FAILED: Job exited with code ${EXIT_CODE}"
    echo "Check log for errors: ${MAIN_LOG}"
fi

exit ${EXIT_CODE}
