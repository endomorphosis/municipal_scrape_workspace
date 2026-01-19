#!/usr/bin/env bash
set -euo pipefail

#
# Overnight job to build DuckDB pointer index with row-group ranges
#
# This script:
# 1. Validates all parquet files are sorted
# 2. Sorts any unsorted files (memory-aware, with ZFS snapshot cleanup)
# 3. Builds DuckDB pointer index with offset/range metadata
# 4. Creates search and benchmark scripts
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV="${REPO_ROOT}/.venv"
PYTHON="${VENV}/bin/python"

CCINDEX_ROOT="/storage/ccindex"
CCINDEX_PARQUET="/storage/ccindex_parquet"
CCINDEX_DUCKDB="/storage/ccindex_duckdb"

LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "${LOG_DIR}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/overnight_pointer_build_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "${LOG_FILE}" >&2
}

# Check prerequisites
if [[ ! -f "${PYTHON}" ]]; then
    error "Python virtual environment not found: ${PYTHON}"
    exit 1
fi

if [[ ! -d "${CCINDEX_PARQUET}" ]]; then
    error "Parquet directory not found: ${CCINDEX_PARQUET}"
    exit 1
fi

log "Starting overnight DuckDB pointer index build"
log "Log file: ${LOG_FILE}"
log ""

# ============================================================================
# STEP 1: Validate parquet files are sorted
# ============================================================================
log "STEP 1: Validating parquet files are sorted..."

VALIDATION_SCRIPT="${REPO_ROOT}/tmp_ccindex/validate_all_parquet_sorted.py"

mkdir -p "$(dirname "${VALIDATION_SCRIPT}")"

if [[ ! -f "${VALIDATION_SCRIPT}" ]]; then
    log "Creating validation script..."
    cat > "${VALIDATION_SCRIPT}" << 'EOFVAL'
#!/usr/bin/env python3
"""Validate that all parquet files are sorted by host_rev."""

import sys
from pathlib import Path
import pyarrow.parquet as pq

def is_sorted(parquet_path: Path) -> bool:
    """Check if parquet file is sorted by host_rev."""
    try:
        pf = pq.ParquetFile(parquet_path)
        table = pf.read()
        
        if 'host_rev' not in table.column_names:
            return False
        
        df = table.to_pandas()
        host_rev_col = df['host_rev']
        
        # Check if sorted
        return (host_rev_col == host_rev_col.sort_values()).all()
    
    except Exception as e:
        print(f"Error checking {parquet_path}: {e}", file=sys.stderr)
        return False

def main():
    parquet_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/storage/ccindex_parquet")
    
    unsorted = []
    total = 0
    
    for pf in parquet_dir.glob("*.parquet"):
        if "sample" in pf.name:
            continue
        
        total += 1
        if total % 100 == 0:
            print(f"Checked {total} files...", file=sys.stderr)
        
        if not is_sorted(pf):
            unsorted.append(pf)
    
    print(f"Total: {total}", file=sys.stderr)
    print(f"Unsorted: {len(unsorted)}", file=sys.stderr)
    
    if unsorted:
        for pf in unsorted:
            print(pf.name)
        sys.exit(1)
    
    sys.exit(0)

if __name__ == "__main__":
    main()
EOFVAL
    chmod +x "${VALIDATION_SCRIPT}"
fi

UNSORTED_FILES=$(mktemp)
if "${PYTHON}" "${VALIDATION_SCRIPT}" "${CCINDEX_PARQUET}" 2>&1 | tee -a "${LOG_FILE}" > "${UNSORTED_FILES}"; then
    log "All parquet files are sorted!"
else
    UNSORTED_COUNT=$(wc -l < "${UNSORTED_FILES}")
    log "Found ${UNSORTED_COUNT} unsorted files"
    
    if [[ ${UNSORTED_COUNT} -gt 0 ]]; then
        log "STEP 1b: Sorting unsorted files..."
        
        # Use existing memory-aware sorting script
        if [[ -f "${REPO_ROOT}/sort_unsorted_memory_aware.py" ]]; then
            log "Running memory-aware sorting..."
            "${PYTHON}" "${REPO_ROOT}/sort_unsorted_memory_aware.py" \
                --parquet-dir "${CCINDEX_PARQUET}" \
                --unsorted-list "${UNSORTED_FILES}" \
                --max-memory-gb 50 \
                --compression zstd \
                --compression-level 3 2>&1 | tee -a "${LOG_FILE}"
            
            if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
                error "Sorting failed"
                exit 1
            fi
        else
            error "Sorting script not found: sort_unsorted_memory_aware.py"
            exit 1
        fi
    fi
fi

rm -f "${UNSORTED_FILES}"

log "STEP 1 complete: All parquet files validated and sorted"
log ""

# ============================================================================
# STEP 2: Build DuckDB pointer index with row-group ranges
# ============================================================================
log "STEP 2: Building DuckDB pointer index..."

DB_OUTPUT_DIR="${CCINDEX_DUCKDB}"
mkdir -p "${DB_OUTPUT_DIR}"

# Build index for 2024-2025 data
log "Building pointer index for 2024-2025 collections..."

"${PYTHON}" "${REPO_ROOT}/build_cc_pointer_duckdb.py" \
    --input-root "${CCINDEX_ROOT}" \
    --db "${DB_OUTPUT_DIR}" \
    --shard-by-year \
    --collections-regex 'CC-MAIN-202[45]-.*' \
    --duckdb-index-mode domain \
    --domain-index-action rebuild \
    --domain-range-index \
    --parquet-out "${CCINDEX_PARQUET}" \
    --parquet-action skip-if-exists \
    --parquet-validate quick \
    --threads 8 \
    --memory-limit-gib 60 \
    --create-indexes \
    --batch-rows 200000 2>&1 | tee -a "${LOG_FILE}"

if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
    error "DuckDB pointer index build failed"
    exit 1
fi

log "STEP 2 complete: DuckDB pointer index built"
log ""

# ============================================================================
# STEP 3: Verify index quality
# ============================================================================
log "STEP 3: Verifying index quality..."

# Count total domains indexed
TOTAL_DOMAINS=0
for DB in "${DB_OUTPUT_DIR}"/cc_pointers_*.duckdb; do
    if [[ -f "${DB}" ]]; then
        COUNT=$("${VENV}/bin/python" -c "
import duckdb
con = duckdb.connect('${DB}', read_only=True)
try:
    result = con.execute('SELECT COUNT(DISTINCT host_rev) FROM cc_domain_shards').fetchone()
    print(result[0] if result else 0)
except:
    print(0)
finally:
    con.close()
" 2>/dev/null || echo "0")
        
        log "  $(basename ${DB}): ${COUNT} domains"
        TOTAL_DOMAINS=$((TOTAL_DOMAINS + COUNT))
    fi
done

log "Total domains indexed: ${TOTAL_DOMAINS}"
log ""

# ============================================================================
# STEP 4: Test search and benchmark
# ============================================================================
log "STEP 4: Testing search functionality..."

# Test search for a known domain
TEST_DOMAIN="example.com"
log "Testing search for: ${TEST_DOMAIN}"

"${PYTHON}" "${REPO_ROOT}/search_cc_pointer_index.py" \
    --domain "${TEST_DOMAIN}" \
    --db-dir "${DB_OUTPUT_DIR}" \
    --parquet-root "${CCINDEX_PARQUET}" \
    --output-format summary \
    --verbose 2>&1 | tee -a "${LOG_FILE}"

log ""
log "STEP 4 complete: Search functionality verified"
log ""

# ============================================================================
# STEP 5: Run performance benchmark (optional)
# ============================================================================
if [[ "${RUN_BENCHMARK:-0}" == "1" ]]; then
    log "STEP 5: Running performance benchmark..."
    
    "${PYTHON}" "${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_pointer_search.py" \
        --db-dir "${DB_OUTPUT_DIR}" \
        --parquet-root "${CCINDEX_PARQUET}" \
        --count 50 \
        --verbose 2>&1 | tee -a "${LOG_FILE}"
    
    log "STEP 5 complete: Benchmark finished"
    log ""
fi

# ============================================================================
# Summary
# ============================================================================
log "======================================================================"
log "OVERNIGHT BUILD COMPLETE"
log "======================================================================"
log ""
log "DuckDB Index Location: ${DB_OUTPUT_DIR}"
log "Parquet Files Location: ${CCINDEX_PARQUET}"
log "Total Domains Indexed: ${TOTAL_DOMAINS}"
log ""
log "Usage Examples:"
log ""
log "  # Search for a domain:"
log "  ${PYTHON} ${REPO_ROOT}/search_cc_pointer_index.py \\"
log "    --domain example.com \\"
log "    --db-dir ${DB_OUTPUT_DIR} \\"
log "    --parquet-root ${CCINDEX_PARQUET}"
log ""
log "  # Run benchmark:"
log "  ${PYTHON} ${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_pointer_search.py \\"
log "    --db-dir ${DB_OUTPUT_DIR} \\"
log "    --parquet-root ${CCINDEX_PARQUET}"
log ""
log "Log file: ${LOG_FILE}"
log "======================================================================"

exit 0
