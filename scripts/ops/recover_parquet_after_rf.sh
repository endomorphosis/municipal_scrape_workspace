#!/usr/bin/env bash
set -euo pipefail

# Recover pipeline outputs after an accidental deletion by rerunning the orchestrator
# for specific collections. This will re-download missing shards as needed.
#
# Defaults are conservative for memory:
# - sort-workers=1
# - sort-memory-per-worker-gb=12.0

COLLECTIONS=(
  "2023-06"
  "2023-40"
  "2023-50"
  "2024-22"
)

TS=${TS:-$(date +%Y%m%d_%H%M%S)}
WORKERS=${WORKERS:-8}
SORT_WORKERS=${SORT_WORKERS:-1}
SORT_MEM_GB=${SORT_MEM_GB:-12.0}
HB_SECS=${HB_SECS:-30}

mkdir -p logs \
  /storage/ccindex_parquet/tmp/duckdb_sort/2023 \
  /storage/ccindex_parquet/tmp/duckdb_sort/2024

is_orchestrator_running() {
  ps -ef | grep -E "python3? -u -m common_crawl_search_engine\\.ccindex\\.cc_pipeline_orchestrator" | grep -v grep >/dev/null
}

run_one() {
  local filter="$1"
  local year="$2"
  local logf="logs/orchestrator_recover_${filter}_${TS}.log"

  echo "==== $(date -Is) START recover ${filter} ====" | tee -a "$logf"
  PYTHONPATH=src python3 -u -m common_crawl_search_engine.ccindex.cc_pipeline_orchestrator \
    --config pipeline_config.json \
    --filter "$filter" \
    --workers "$WORKERS" \
    --heartbeat-seconds "$HB_SECS" \
    --no-cleanup-source-archives \
    --yes \
    --force-reindex \
    --sort-workers "$SORT_WORKERS" \
    --sort-memory-per-worker-gb "$SORT_MEM_GB" \
    --sort-temp-dir "/storage/ccindex_parquet/tmp/duckdb_sort/${year}" \
    --build-domain-rowgroup-index \
    --domain-rowgroup-index-workers "$WORKERS" \
    2>&1 | tee -a "$logf"
  echo "==== $(date -Is) DONE  recover ${filter} (exit=$?) ====" | tee -a "$logf"
}

while is_orchestrator_running; do
  echo "[$(date -Is)] orchestrator already running; waiting..."
  sleep 60
done

for f in "${COLLECTIONS[@]}"; do
  year=$(echo "$f" | cut -d- -f1)
  run_one "$f" "$year"

done
