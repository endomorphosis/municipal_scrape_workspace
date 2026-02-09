#!/usr/bin/env bash
set -euo pipefail

# Runs forced reindex for a fixed set of impacted collections, sequentially.
# Safe defaults:
# - waits for any currently-running orchestrator to finish (avoids DB locks)
# - uses --existing-parquet-only (no new downloads)
# - keeps source archives (no cleanup of .gz/.tar.gz)

COLLECTIONS=(
  "2023-50"
  "2023-06"
  "2022-05"
  "2022-21"
  "2022-27"
  "2022-33"
  "2022-40"
  "2022-49"
)

TS=${TS:-$(date +%Y%m%d_%H%M%S)}
WORKERS=${WORKERS:-8}
SORT_WORKERS=${SORT_WORKERS:-1}
SORT_MEM_GB=${SORT_MEM_GB:-12.0}
HB_SECS=${HB_SECS:-30}

mkdir -p logs \
  /storage/ccindex_parquet/tmp/duckdb_sort/2022 \
  /storage/ccindex_parquet/tmp/duckdb_sort/2023

is_orchestrator_running() {
  ps -ef | grep -E "python3? -u -m common_crawl_search_engine\\.ccindex\\.cc_pipeline_orchestrator" | grep -v grep >/dev/null
}

while is_orchestrator_running; do
  echo "[$(date -Is)] orchestrator already running; waiting..."
  sleep 60
done

run_one() {
  local filter="$1"
  local year="$2"
  local logf="logs/orchestrator_force_reindex_${filter}_${TS}.log"

  echo "==== $(date -Is) START ${filter} ====" | tee -a "$logf"
  PYTHONPATH=src python3 -u -m common_crawl_search_engine.ccindex.cc_pipeline_orchestrator \
    --config pipeline_config.json \
    --filter "$filter" \
    --workers "$WORKERS" \
    --heartbeat-seconds "$HB_SECS" \
    --existing-parquet-only \
    --no-cleanup-source-archives \
    --yes \
    --force-reindex \
    --sort-workers "$SORT_WORKERS" \
    --sort-memory-per-worker-gb "$SORT_MEM_GB" \
    --sort-temp-dir "/storage/ccindex_parquet/tmp/duckdb_sort/${year}" \
    --build-domain-rowgroup-index \
    --domain-rowgroup-index-workers "$WORKERS" \
    2>&1 | tee -a "$logf"
  echo "==== $(date -Is) DONE  ${filter} (exit=$?) ====" | tee -a "$logf"
}

# 2023-40 was already running separately; we start with the remainder.
for f in "${COLLECTIONS[@]}"; do
  year=$(echo "$f" | cut -d- -f1)
  run_one "$f" "$year"

done
