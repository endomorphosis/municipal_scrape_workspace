#!/bin/bash
# Quick Start: Test the DuckDB index with a small sample
#
# This script builds a small test index and runs basic searches
# to validate the design before running the full overnight job.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-${REPO_ROOT}/.venv/bin/python}"

# Test configuration
TEST_DIR="/tmp/ccindex_test_$$"
CCINDEX_ROOT="${CCINDEX_ROOT:-/storage/ccindex}"
TEST_MAX_FILES=10

echo "============================================================================"
echo "DuckDB Index Quick Start Test"
echo "============================================================================"
echo ""
echo "This will:"
echo "  1. Build a small test index (10 files)"
echo "  2. Run search examples"
echo "  3. Run quick benchmark"
echo "  4. Clean up test data"
echo ""
echo "Test directory: ${TEST_DIR}"
echo "Source data:    ${CCINDEX_ROOT}"
echo ""
read -p "Continue? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted"
    exit 1
fi

# Setup test directories
mkdir -p "${TEST_DIR}"/{duckdb,parquet,logs}

cleanup() {
    echo ""
    echo "Cleaning up test directory..."
    rm -rf "${TEST_DIR}"
}
trap cleanup EXIT

# Step 1: Build test index
echo ""
echo "============================================================================"
echo "Step 1: Building test index (${TEST_MAX_FILES} files)..."
echo "============================================================================"
echo ""

"${VENV_PYTHON}" "${REPO_ROOT}/build_cc_pointer_duckdb.py" \
    --input-root "${CCINDEX_ROOT}" \
    --db "${TEST_DIR}/duckdb" \
    --shard-by-year \
    --collections-regex 'CC-MAIN-2024-.*' \
    --max-files ${TEST_MAX_FILES} \
    --duckdb-index-mode domain \
    --domain-index-action rebuild \
    --domain-range-index \
    --parquet-out "${TEST_DIR}/parquet" \
    --parquet-compression zstd \
    --threads 4 \
    --create-indexes \
    --progress-dir "${TEST_DIR}/logs"

echo ""
echo "Index built successfully!"
echo ""
echo "DuckDB files:"
ls -lh "${TEST_DIR}/duckdb"/*.duckdb 2>/dev/null || echo "  No files yet"
echo ""
echo "Parquet files:"
find "${TEST_DIR}/parquet" -name "*.parquet" | head -5
echo "  ..."
echo ""

# Step 2: Interactive query example
echo "============================================================================"
echo "Step 2: Sample queries"
echo "============================================================================"
echo ""

DB_FILE=$(ls "${TEST_DIR}/duckdb"/*.duckdb | head -1)

if [[ -f "${DB_FILE}" ]]; then
    echo "Using: ${DB_FILE}"
    echo ""
    
    echo "Query 1: Count total domains"
    duckdb "${DB_FILE}" "SELECT count(DISTINCT host_rev) as total_domains FROM cc_domain_shards"
    echo ""
    
    echo "Query 2: Top 10 domains by shard count"
    duckdb "${DB_FILE}" "SELECT host, count(*) as shards FROM cc_domain_shards GROUP BY host ORDER BY shards DESC LIMIT 10"
    echo ""
    
    echo "Query 3: Collections indexed"
    duckdb "${DB_FILE}" "SELECT DISTINCT collection FROM cc_domain_shards ORDER BY collection"
    echo ""
    
    # Get a sample domain
    SAMPLE_DOMAIN=$(duckdb "${DB_FILE}" "SELECT host FROM cc_domain_shards LIMIT 1" | tail -1)
    
    if [[ -n "${SAMPLE_DOMAIN}" ]] && [[ "${SAMPLE_DOMAIN}" != "host" ]]; then
        echo "Query 4: Sample domain search (${SAMPLE_DOMAIN})"
        duckdb "${DB_FILE}" "SELECT * FROM cc_domain_shards WHERE host = '${SAMPLE_DOMAIN}' LIMIT 3"
        echo ""
    fi
else
    echo "WARNING: No DuckDB file found"
fi

# Step 3: Test search script
echo "============================================================================"
echo "Step 3: Testing search script"
echo "============================================================================"
echo ""

if [[ -f "${DB_FILE}" ]] && [[ -n "${SAMPLE_DOMAIN}" ]] && [[ "${SAMPLE_DOMAIN}" != "host" ]]; then
    echo "Searching for domain: ${SAMPLE_DOMAIN}"
    echo ""
    
    "${VENV_PYTHON}" "${REPO_ROOT}/search_cc_duckdb_index.py" \
        --duckdb-dir "${TEST_DIR}/duckdb" \
        --parquet-root "${TEST_DIR}/parquet" \
        --domain "${SAMPLE_DOMAIN}" \
        --use-rowgroup-ranges \
        --verbose
    echo ""
else
    echo "SKIP: No sample domain available"
fi

# Step 4: Quick benchmark
echo "============================================================================"
echo "Step 4: Running quick benchmark"
echo "============================================================================"
echo ""

"${VENV_PYTHON}" "${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_duckdb_search.py" \
    --duckdb-dir "${TEST_DIR}/duckdb" \
    --parquet-root "${TEST_DIR}/parquet" \
    --quick

echo ""
echo "============================================================================"
echo "Quick Start Test Complete!"
echo "============================================================================"
echo ""
echo "The index design has been validated with a small sample."
echo ""
echo "Next steps:"
echo ""
echo "1. Review the design documentation:"
echo "   less ${REPO_ROOT}/DUCKDB_INDEX_DESIGN.md"
echo ""
echo "2. Run the full overnight build:"
echo "   ${REPO_ROOT}/overnight_build_duckdb_index.sh"
echo ""
echo "3. Or start with a larger test:"
echo "   ${REPO_ROOT}/overnight_build_duckdb_index.sh --max-files 1000"
echo ""
echo "Test directory will be cleaned up on exit."
echo ""

read -p "Press Enter to clean up and exit..."

exit 0
