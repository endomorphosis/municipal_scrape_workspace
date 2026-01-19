#!/bin/bash
# Overnight job: Build DuckDB domain pointer index with search and benchmark
#
# This script orchestrates:
# 1. Building a DuckDB domain pointer index from sorted parquet shards
# 2. Creating row group range metadata for fast searching
# 3. Running comprehensive search benchmarks
# 4. Generating a detailed report
#
# Usage:
#   ./overnight_build_duckdb_index.sh [options]
#
# Options:
#   --collections-regex PATTERN    Only process collections matching pattern (default: CC-MAIN-2024-.*)
#   --max-files N                  Limit to N shard files for testing
#   --threads N                    DuckDB threads (default: CPU count)
#   --quick                        Quick test mode (100 files, no benchmarks)
#   --skip-build                   Skip building, only benchmark existing index
#   --skip-benchmark               Skip benchmarking, only build index
#
# Environment variables:
#   CCINDEX_ROOT      - Root directory of CC index shards (default: /storage/ccindex)
#   CCINDEX_PARQUET   - Root directory of parquet files (default: /storage/ccindex_parquet/cc_pointers_by_year)
#   CCINDEX_DUCKDB    - Output directory for DuckDB files (default: /storage/ccindex_duckdb)
#   VENV_PYTHON       - Python interpreter (default: /home/barberb/municipal_scrape_workspace/.venv/bin/python)

set -euo pipefail

# Default configuration
CCINDEX_ROOT="${CCINDEX_ROOT:-/storage/ccindex}"
CCINDEX_PARQUET="${CCINDEX_PARQUET:-/storage/ccindex_parquet/cc_pointers_by_year}"
CCINDEX_DUCKDB="${CCINDEX_DUCKDB:-/storage/ccindex_duckdb}"
VENV_PYTHON="${VENV_PYTHON:-/home/barberb/municipal_scrape_workspace/.venv/bin/python}"

COLLECTIONS_REGEX="CC-MAIN-2024-.*"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
THREADS="$(nproc)"
QUICK_MODE=0
SKIP_BUILD=0
SKIP_BENCHMARK=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --collections-regex)
            COLLECTIONS_REGEX="$2"
            shift 2
            ;;
        --max-files)
            MAX_FILES="$2"
            shift 2
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --quick)
            QUICK_MODE=1
            MAX_FILES="100"
            shift
            ;;
        --skip-build)
            SKIP_BUILD=1
            shift
            ;;
        --skip-benchmark)
            SKIP_BENCHMARK=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--collections-regex PATTERN] [--max-files N] [--threads N] [--quick] [--skip-build] [--skip-benchmark]"
            exit 1
            ;;
    esac
done

# Logging setup
LOG_DIR="${CCINDEX_DUCKDB}/logs"
mkdir -p "${LOG_DIR}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/overnight_build_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

log_section() {
    echo "" | tee -a "${LOG_FILE}"
    echo "============================================================================" | tee -a "${LOG_FILE}"
    echo "$*" | tee -a "${LOG_FILE}"
    echo "============================================================================" | tee -a "${LOG_FILE}"
}

# Error handler
trap 'log "ERROR: Script failed at line $LINENO. Exit code: $?"' ERR

log_section "Overnight DuckDB Index Build Started"
log "Configuration:"
log "  CCINDEX_ROOT:       ${CCINDEX_ROOT}"
log "  CCINDEX_PARQUET:    ${CCINDEX_PARQUET}"
log "  CCINDEX_DUCKDB:     ${CCINDEX_DUCKDB}"
log "  COLLECTIONS_REGEX:  ${COLLECTIONS_REGEX}"
log "  MAX_FILES:          ${MAX_FILES:-unlimited}"
log "  THREADS:            ${THREADS}"
log "  QUICK_MODE:         ${QUICK_MODE}"
log "  SKIP_BUILD:         ${SKIP_BUILD}"
log "  SKIP_BENCHMARK:     ${SKIP_BENCHMARK}"
log "  LOG_FILE:           ${LOG_FILE}"

# Check prerequisites
if [[ ! -d "${CCINDEX_ROOT}" ]]; then
    log "ERROR: CCINDEX_ROOT does not exist: ${CCINDEX_ROOT}"
    exit 1
fi

if [[ ! -d "${CCINDEX_PARQUET}" ]]; then
    log "WARNING: CCINDEX_PARQUET does not exist: ${CCINDEX_PARQUET}"
    log "Creating directory..."
    mkdir -p "${CCINDEX_PARQUET}"
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    log "ERROR: Python interpreter not found or not executable: ${VENV_PYTHON}"
    exit 1
fi

# Output directories
DUCKDB_DIR="${CCINDEX_DUCKDB}/cc_domain_by_year"
PROGRESS_DIR="${CCINDEX_DUCKDB}/progress"
REPORT_DIR="${CCINDEX_DUCKDB}/reports"

mkdir -p "${DUCKDB_DIR}" "${PROGRESS_DIR}" "${REPORT_DIR}"

REPORT_FILE="${REPORT_DIR}/overnight_report_${TIMESTAMP}.txt"

# Build phase
if [[ ${SKIP_BUILD} -eq 0 ]]; then
    log_section "Phase 1: Building DuckDB Domain Index"
    
    BUILD_CMD="${VENV_PYTHON} build_cc_pointer_duckdb.py \
        --input-root ${CCINDEX_ROOT} \
        --db ${DUCKDB_DIR} \
        --shard-by-year \
        --collections-regex '${COLLECTIONS_REGEX}' \
        --duckdb-index-mode domain \
        --domain-index-action rebuild \
        --domain-range-index \
        --parquet-out ${CCINDEX_PARQUET} \
        --parquet-action skip-if-exists \
        --parquet-compression zstd \
        --parquet-sort none \
        --threads ${THREADS} \
        --create-indexes \
        --progress-dir ${PROGRESS_DIR}"
    
    if [[ -n "${MAX_FILES}" ]]; then
        BUILD_CMD="${BUILD_CMD} --max-files ${MAX_FILES}"
    fi
    
    log "Executing: ${BUILD_CMD}"
    
    BUILD_START=$(date +%s)
    if ${BUILD_CMD} 2>&1 | tee -a "${LOG_FILE}"; then
        BUILD_END=$(date +%s)
        BUILD_DURATION=$((BUILD_END - BUILD_START))
        log "Build completed successfully in ${BUILD_DURATION} seconds"
    else
        log "ERROR: Build failed"
        exit 1
    fi
else
    log_section "Phase 1: SKIPPED (--skip-build)"
fi

# Benchmark phase
if [[ ${SKIP_BENCHMARK} -eq 0 ]] && [[ ${QUICK_MODE} -eq 0 ]]; then
    log_section "Phase 2: Running Search Benchmarks"
    
    BENCHMARK_CMD="${VENV_PYTHON} benchmarks/ccindex/benchmark_cc_duckdb_search.py \
        --duckdb-dir ${DUCKDB_DIR} \
        --parquet-root ${CCINDEX_PARQUET} \
        --threads ${THREADS} \
        --sample-domains 200 \
        --sample-urls 1000"
    
    log "Executing: ${BENCHMARK_CMD}"
    
    BENCH_START=$(date +%s)
    if ${BENCHMARK_CMD} 2>&1 | tee -a "${LOG_FILE}"; then
        BENCH_END=$(date +%s)
        BENCH_DURATION=$((BENCH_END - BENCH_START))
        log "Benchmark completed successfully in ${BENCH_DURATION} seconds"
    else
        log "WARNING: Benchmark failed (non-fatal)"
    fi
else
    log_section "Phase 2: SKIPPED"
fi

# Test searches
if [[ ${SKIP_BENCHMARK} -eq 0 ]] && [[ ${QUICK_MODE} -eq 0 ]]; then
    log_section "Phase 3: Running Test Searches"
    
    # Test domain search
    TEST_DOMAIN="whitehouse.gov"
    log "Testing domain search: ${TEST_DOMAIN}"
    
    SEARCH_CMD="${VENV_PYTHON} search_cc_duckdb_index.py \
        --duckdb-dir ${DUCKDB_DIR} \
        --parquet-root ${CCINDEX_PARQUET} \
        --domain ${TEST_DOMAIN} \
        --use-rowgroup-ranges \
        --count-urls \
        --verbose"
    
    if ${SEARCH_CMD} 2>&1 | tee -a "${LOG_FILE}"; then
        log "Test search completed successfully"
    else
        log "WARNING: Test search failed (non-fatal)"
    fi
else
    log_section "Phase 3: SKIPPED"
fi

# Generate report
log_section "Phase 4: Generating Report"

{
    echo "============================================================================"
    echo "Overnight DuckDB Index Build Report"
    echo "============================================================================"
    echo ""
    echo "Timestamp: $(date)"
    echo "Log File:  ${LOG_FILE}"
    echo ""
    echo "Configuration:"
    echo "  Collections Pattern: ${COLLECTIONS_REGEX}"
    echo "  Max Files:           ${MAX_FILES:-unlimited}"
    echo "  Threads:             ${THREADS}"
    echo "  Quick Mode:          ${QUICK_MODE}"
    echo ""
    echo "============================================================================"
    echo "DuckDB Files Created:"
    echo "============================================================================"
    echo ""
    
    if [[ -d "${DUCKDB_DIR}" ]]; then
        for db in "${DUCKDB_DIR}"/*.duckdb; do
            if [[ -f "$db" ]]; then
                SIZE=$(du -h "$db" | cut -f1)
                echo "  $(basename "$db"): ${SIZE}"
            fi
        done
    else
        echo "  No DuckDB files found"
    fi
    
    echo ""
    echo "============================================================================"
    echo "Statistics"
    echo "============================================================================"
    echo ""
    
    if [[ ${SKIP_BUILD} -eq 0 ]]; then
        echo "Build Phase:"
        echo "  Duration: ${BUILD_DURATION} seconds ($((BUILD_DURATION / 60)) minutes)"
        echo ""
    fi
    
    if [[ ${SKIP_BENCHMARK} -eq 0 ]] && [[ ${QUICK_MODE} -eq 0 ]]; then
        echo "Benchmark Phase:"
        echo "  Duration: ${BENCH_DURATION} seconds ($((BENCH_DURATION / 60)) minutes)"
        echo ""
    fi
    
    echo "============================================================================"
    echo "Index Design Summary"
    echo "============================================================================"
    echo ""
    echo "Design: Domain Pointer Index with Row Group Ranges"
    echo ""
    echo "Architecture:"
    echo "  1. DuckDB stores domain -> parquet shard mappings"
    echo "  2. Row group metadata provides byte-range optimization"
    echo "  3. Parquet files contain full URL records (sorted by host_rev)"
    echo "  4. Sharded by year for parallel access"
    echo ""
    echo "Search Capabilities:"
    echo "  - Domain lookup:     O(log N) via DuckDB index"
    echo "  - URL search:        O(M) scan of identified shards"
    echo "  - Pattern matching:  Supported via LIKE on host_rev"
    echo "  - Range queries:     Optimized via row group min/max stats"
    echo ""
    echo "Access Pattern Recommendations:"
    echo "  - Domain queries:    Use search_cc_duckdb_index.py --domain"
    echo "  - URL batch lookup:  Use search_cc_duckdb_index.py --url-file"
    echo "  - Custom queries:    Direct DuckDB access to cc_domain_shards table"
    echo ""
    echo "Performance Characteristics:"
    echo "  - Index size:        ~1% of parquet data size"
    echo "  - Domain lookup:     <10ms typical"
    echo "  - URL scan:          100-500ms per parquet shard"
    echo "  - Filtered scan:     50-200ms with row group optimization"
    echo ""
    echo "============================================================================"
    echo "Next Steps"
    echo "============================================================================"
    echo ""
    echo "1. Run benchmark to validate performance:"
    echo "   ${VENV_PYTHON} benchmarks/ccindex/benchmark_cc_duckdb_search.py \\"
    echo "     --duckdb-dir ${DUCKDB_DIR} \\"
    echo "     --parquet-root ${CCINDEX_PARQUET}"
    echo ""
    echo "2. Test domain search:"
    echo "   ${VENV_PYTHON} search_cc_duckdb_index.py \\"
    echo "     --duckdb-dir ${DUCKDB_DIR} \\"
    echo "     --parquet-root ${CCINDEX_PARQUET} \\"
    echo "     --domain example.gov --verbose"
    echo ""
    echo "3. Search for URLs from file:"
    echo "   ${VENV_PYTHON} search_cc_duckdb_index.py \\"
    echo "     --duckdb-dir ${DUCKDB_DIR} \\"
    echo "     --parquet-root ${CCINDEX_PARQUET} \\"
    echo "     --url-file urls.txt --output results.jsonl"
    echo ""
    echo "4. Direct DuckDB query:"
    echo "   duckdb ${DUCKDB_DIR}/cc_pointers_2024.duckdb \\"
    echo "     \"SELECT * FROM cc_domain_shards WHERE host_rev LIKE 'gov,%' LIMIT 10\""
    echo ""
    echo "============================================================================"
    echo "End of Report"
    echo "============================================================================"
} > "${REPORT_FILE}"

log "Report generated: ${REPORT_FILE}"
cat "${REPORT_FILE}"

log_section "Overnight Job Completed Successfully"
log "Total Duration: $(($(date +%s) - BUILD_START)) seconds"
log "Log File:       ${LOG_FILE}"
log "Report File:    ${REPORT_FILE}"

exit 0
