#!/bin/bash
# Quick reference for DuckDB pointer index operations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-${REPO_ROOT}/.venv/bin/python}"

if [[ -x "${VENV_PYTHON}" ]]; then
  :
elif command -v "${VENV_PYTHON}" >/dev/null 2>&1; then
  :
else
  VENV_PYTHON="python3"
fi

cat << EOF
╔══════════════════════════════════════════════════════════════╗
║         DuckDB Pointer Index - Quick Reference               ║
╚══════════════════════════════════════════════════════════════╝

📊 CHECK STATUS
  ${REPO_ROOT}/monitor_overnight_build.sh
  
📋 VIEW LOGS
  tail -f conversion_progress.log         # Conversion progress
  tail -f overnight_duckdb_*.log          # Orchestration log
  
🔍 SEARCH DOMAINS (after build completes)
  ${VENV_PYTHON} ${REPO_ROOT}/search_cc_domain.py example.com --limit 100
  ${VENV_PYTHON} ${REPO_ROOT}/search_cc_domain.py example.com --mode both --show
  
⚡ RUN BENCHMARKS (after build completes)
  ${VENV_PYTHON} ${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_domain_search.py
  ${VENV_PYTHON} ${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_domain_search.py --clear-cache
  
📁 FILE LOCATIONS
  Parquet:  /storage/ccindex_parquet/cc_pointers_by_year/
  DuckDB:   /storage/ccindex_duckdb/cc_pointers.duckdb
  Source:   /storage/ccindex/CC-MAIN-202[45]-*/
  
🎯 EXPECTED PERFORMANCE
  Index Size:      <1GB
  Query Time:      <100ms (warm cache)
  Result Fetching: <1s for typical domains
  
📈 CURRENT PROGRESS
  Conversion: $(find /storage/ccindex_parquet/cc_pointers_by_year -name "*.parquet" 2>/dev/null | wc -l) / 6396 files
  Memory:     $(free -h | awk '/^Mem:/ {print $7}') available
  Disk:       $(df -h /storage | tail -1 | awk '{print $4}') available
  
🛑 STOP EVERYTHING
  pkill -f parallel_convert_missing.py
  pkill -f overnight_duckdb_complete.sh
  
🔄 RESTART FROM SCRATCH
  ${REPO_ROOT}/overnight_duckdb_complete.sh
  
📖 FULL DOCUMENTATION
  See: OVERNIGHT_BUILD_STATUS.md

╚══════════════════════════════════════════════════════════════╝
EOF
