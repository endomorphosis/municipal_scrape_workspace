#!/bin/bash
# Overnight job to complete the full DuckDB pointer index build
# 
# This script:
# 1. Monitors parquet conversion completion
# 2. Validates all parquet files are sorted by domain
# 3. Builds DuckDB pointer index with domain->parquet file mappings
# 4. Runs search tests and benchmarks
# 5. Generates completion report

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BENCHMARK_OUT_DIR="${REPO_ROOT}/benchmarks/ccindex"

LOG_FILE="${REPO_ROOT}/overnight_duckdb_build_$(date +%Y%m%d_%H%M%S).log"
PARQUET_ROOT="/storage/ccindex_parquet/cc_pointers_by_year"
DUCKDB_PATH="/storage/ccindex_duckdb/cc_pointers.duckdb"
CONVERSION_LOG="${REPO_ROOT}/conversion_progress.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

check_memory() {
    local available_gb=$(free -g | awk '/^Mem:/ {print $7}')
    echo "$available_gb"
}

wait_for_conversion() {
    log "Step 1: Waiting for parquet conversion to complete..."
    
    while true; do
        if pgrep -f "parallel_convert_missing.py" > /dev/null; then
            local mem_gb=$(check_memory)
            local progress=$(tail -5 "$CONVERSION_LOG" 2>/dev/null | grep -oP '\[\d+/\d+\]' | tail -1 || echo "starting")
            log "  Conversion running: $progress (Memory: ${mem_gb}GB available)"
            sleep 60
        else
            log "  ✓ Conversion process completed"
            break
        fi
    done
    
    # Show final conversion stats
    if [ -f "$CONVERSION_LOG" ]; then
        log "  Final conversion stats:"
        tail -20 "$CONVERSION_LOG" | tee -a "$LOG_FILE"
    fi
}

validate_sorted_parquet() {
    log "Step 2: Validating all parquet files are sorted by domain..."
    
    python3 << 'PYEOF' | tee -a "$LOG_FILE"
import sys
from pathlib import Path
import pyarrow.parquet as pq

parquet_root = Path("/storage/ccindex_parquet/cc_pointers_by_year")
files = sorted(parquet_root.rglob("*.parquet"))
print(f"Found {len(files)} parquet files to validate")

unsorted = []
for i, pq_file in enumerate(files, 1):
    if i % 100 == 0:
        print(f"  Checked {i}/{len(files)}...")
    
    try:
        table = pq.read_table(pq_file, columns=['domain'])
        domains = table.column('domain').to_pylist()
        
        # Check if sorted
        if domains != sorted(domains):
            unsorted.append(str(pq_file))
    except Exception as e:
        print(f"  Error reading {pq_file}: {e}")
        unsorted.append(str(pq_file))

if unsorted:
    print(f"\n✗ Found {len(unsorted)} unsorted files:")
    for f in unsorted[:10]:
        print(f"  - {f}")
    if len(unsorted) > 10:
        print(f"  ... and {len(unsorted) - 10} more")
    sys.exit(1)
else:
    print(f"\n✓ All {len(files)} parquet files are sorted by domain")
    sys.exit(0)
PYEOF
    
    if [ $? -ne 0 ]; then
        log "  ✗ Validation failed - some files are not sorted"
        return 1
    fi
    
    log "  ✓ All files validated as sorted"
    return 0
}

build_duckdb_index() {
    log "Step 3: Building DuckDB pointer index..."
    
    # Check available space
    local disk_avail=$(df -BG /storage | tail -1 | awk '{print $4}' | sed 's/G//')
    log "  Available disk space: ${disk_avail}GB"
    
    if [ "$disk_avail" -lt 100 ]; then
        log "  ⚠ Low disk space - cleaning ZFS snapshots..."
        sudo zfs list -t snapshot -o name -s creation | grep ccindex_duckdb | head -n -5 | xargs -r -n 1 sudo zfs destroy || true
    fi
    
    # Build the index
    log "  Building index from all 2024-2025 parquet files..."
    python3 "${REPO_ROOT}/build_cc_pointer_duckdb.py" \
        --input-root "$PARQUET_ROOT" \
        --db "$DUCKDB_PATH" \
        --collections-regex 'CC-MAIN-202[45]-.*' \
        --duckdb-index-mode domain \
        2>&1 | tee -a "$LOG_FILE"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log "  ✓ DuckDB index built successfully"
        
        # Show index stats
        local db_size_mb=$(du -sm "$DUCKDB_PATH" | cut -f1)
        log "  Index size: ${db_size_mb}MB"
        
        return 0
    else
        log "  ✗ DuckDB index build failed"
        return 1
    fi
}

run_search_tests() {
    log "Step 4: Running search tests..."
    
    local test_domains=("example.com" "google.com" "github.com" "wikipedia.org")
    
    for domain in "${test_domains[@]}"; do
        log "  Testing domain: $domain"
        python3 "${REPO_ROOT}/search_cc_domain.py" "$domain" --limit 10 2>&1 | tee -a "$LOG_FILE"
    done
    
    log "  ✓ Search tests completed"
}

run_benchmark() {
    log "Step 5: Running performance benchmark..."

    mkdir -p "${BENCHMARK_OUT_DIR}"
    
    python3 "${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_domain_search.py" \
        --db "$DUCKDB_PATH" \
        --domains example.com google.com github.com wikipedia.org archive.org \
        --output "${BENCHMARK_OUT_DIR}/benchmark_results_$(date +%Y%m%d_%H%M%S).json" \
        2>&1 | tee -a "$LOG_FILE"
    
    log "  ✓ Benchmark completed"
}

generate_report() {
    log "Step 6: Generating completion report..."
    
    cat << EOF | tee -a "$LOG_FILE"

${'='*60}
OVERNIGHT BUILD COMPLETION REPORT
${'='*60}

Parquet Files:
  Total files: $(find "$PARQUET_ROOT" -name "*.parquet" | wc -l)
  Total size: $(du -sh "$PARQUET_ROOT" | cut -f1)

DuckDB Index:
  Database: $DUCKDB_PATH
  Size: $(du -sh "$DUCKDB_PATH" | cut -f1)
  Status: $([ -f "$DUCKDB_PATH" ] && echo "✓ Created" || echo "✗ Missing")

Disk Usage:
$(df -h /storage | tail -1)

Memory Status:
  Available: $(check_memory)GB

Completion Time: $(date)
Log File: $LOG_FILE

EOF
}

main() {
    log "=========================================="
    log "Overnight DuckDB Index Build - Starting"
    log "=========================================="
    log "Memory available: $(check_memory)GB"
    
    # Step 1: Wait for conversion
    wait_for_conversion
    
    # Step 2: Validate sorting
    if ! validate_sorted_parquet; then
        log "ERROR: Parquet validation failed"
        exit 1
    fi
    
    # Step 3: Build index
    if ! build_duckdb_index; then
        log "ERROR: DuckDB index build failed"
        exit 1
    fi
    
    # Step 4: Test searches
    run_search_tests
    
    # Step 5: Benchmark
    run_benchmark
    
    # Step 6: Report
    generate_report
    
    log "=========================================="
    log "Overnight DuckDB Index Build - Complete"
    log "=========================================="
}

main "$@"
