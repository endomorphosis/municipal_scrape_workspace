#!/usr/bin/env bash
set -euo pipefail

# Recover pipeline outputs after an accidental deletion by rerunning the orchestrator
# for specific collections. This will re-download missing shards as needed.
#
# Defaults:
# - Start by *requesting* sort workers equal to WORKERS (typically 8)
# - Auto-downshift based on currently available RAM so we don't OOM
# - Keep sort-memory-per-worker-gb configurable (default 12.0)

COLLECTIONS=(
  "2023-06"
  "2023-14"
  "2023-23"
  "2023-40"
  "2023-50"
  "2024-10"
  "2024-22"
)

TS=${TS:-$(date +%Y%m%d_%H%M%S)}
WORKERS=${WORKERS:-8}
# Requested sort concurrency. Effective concurrency is auto-computed at runtime.
SORT_WORKERS=${SORT_WORKERS:-$WORKERS}
SORT_MEM_GB=${SORT_MEM_GB:-12.0}
SORT_RESERVE_GB=${SORT_RESERVE_GB:-6.0}
ARC_FRACTION=${ARC_FRACTION:-0.50}
SWAP_FREE_MIN_GB=${SWAP_FREE_MIN_GB:-1.0}
SWAP_LOW_SORT_WORKERS_CAP=${SWAP_LOW_SORT_WORKERS_CAP:-2}
HB_SECS=${HB_SECS:-30}

mem_available_gb() {
  awk '/MemAvailable:/ {printf "%.3f\n", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo "0"
}

swap_free_gb() {
  awk '/SwapFree:/ {printf "%.3f\n", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo "0"
}

zfs_arc_reclaimable_gb() {
  if [[ -r /proc/spl/kstat/zfs/arcstats ]]; then
    awk 'BEGIN{size=0;cmin=0}
         $1=="size"{size=$3}
         $1=="c_min"{cmin=$3}
         END{reclaim=size-cmin; if (reclaim<0) reclaim=0; printf "%.3f\n", reclaim/1024/1024/1024}' \
      /proc/spl/kstat/zfs/arcstats 2>/dev/null || echo "0"
  else
    echo "0"
  fi
}

effective_available_gb() {
  # On ZFS hosts, MemAvailable can be pessimistic because ARC isn't always counted.
  # Use MemAvailable + ARC_FRACTION * (ARC reclaimable above c_min).
  python3 - <<'PY' "$(mem_available_gb)" "$(zfs_arc_reclaimable_gb)" "${ARC_FRACTION}"
import sys
mem_avail = float(sys.argv[1])
arc_reclaim = float(sys.argv[2])
arc_frac = float(sys.argv[3])
if arc_frac < 0:
    arc_frac = 0.0
if arc_frac > 1:
    arc_frac = 1.0
print(f"{mem_avail + (arc_reclaim * arc_frac):.3f}")
PY
}

compute_sort_workers() {
  # Compute a safe sort worker count based on current MemAvailable.
  # effective = min(requested, floor((effective_avail - reserve) / mem_per_worker))
  # On ZFS, we optionally count a fraction of ARC reclaimable memory.
  # Always at least 1.
  local requested="$1"
  local mem_per_worker_gb="$2"
  local reserve_gb="$3"

  python3 - <<'PY' \
  "$requested" "$mem_per_worker_gb" "$reserve_gb" \
  "$(effective_available_gb)" "$(swap_free_gb)" "${SWAP_FREE_MIN_GB}" "${SWAP_LOW_SORT_WORKERS_CAP}"
import math, sys
requested = int(float(sys.argv[1]))
mem_per = float(sys.argv[2])
reserve = float(sys.argv[3])
avail = float(sys.argv[4])
swap_free = float(sys.argv[5])
swap_min = float(sys.argv[6])
swap_cap = int(float(sys.argv[7]))
if requested < 1:
    requested = 1
if mem_per <= 0:
    mem_per = 1.0

# If swap is basically exhausted, cap sort concurrency.
if swap_free < swap_min:
  requested = min(requested, max(1, swap_cap))

safe = int(math.floor((avail - reserve) / mem_per))
if safe < 1:
    safe = 1
eff = min(requested, safe)
print(eff)
PY
}

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

  local attempt=1
  local max_attempts=4
  local requested_sort_workers="$SORT_WORKERS"

  while true; do
    local effective_sort_workers
    effective_sort_workers=$(compute_sort_workers "$requested_sort_workers" "$SORT_MEM_GB" "$SORT_RESERVE_GB")

    echo "==== $(date -Is) START recover ${filter} (attempt ${attempt}/${max_attempts}) ====" | tee -a "$logf"
    echo "[$(date -Is)] sort_workers requested=${requested_sort_workers} effective=${effective_sort_workers} mem_per_worker_gb=${SORT_MEM_GB} reserve_gb=${SORT_RESERVE_GB} mem_available_gb=$(mem_available_gb) arc_reclaimable_gb=$(zfs_arc_reclaimable_gb) arc_fraction=${ARC_FRACTION} effective_avail_gb=$(effective_available_gb) swap_free_gb=$(swap_free_gb)" | tee -a "$logf"

    set +e
    PYTHONPATH=src python3 -u -m common_crawl_search_engine.ccindex.cc_pipeline_orchestrator \
      --config pipeline_config.json \
      --filter "$filter" \
      --workers "$WORKERS" \
      --heartbeat-seconds "$HB_SECS" \
      --no-cleanup-source-archives \
      --yes \
      --force-reindex \
      --sort-workers "$effective_sort_workers" \
      --sort-memory-per-worker-gb "$SORT_MEM_GB" \
      --sort-temp-dir "/storage/ccindex_parquet/tmp/duckdb_sort/${year}" \
      --build-domain-rowgroup-index \
      --domain-rowgroup-index-workers "$WORKERS" \
      2>&1 | tee -a "$logf"
    rc=${PIPESTATUS[0]}
    set -e

    echo "==== $(date -Is) DONE  recover ${filter} (exit=${rc}) ====" | tee -a "$logf"

    if [[ "$rc" -eq 0 ]]; then
      return 0
    fi

    # Backoff if we likely hit memory pressure.
    if [[ "$attempt" -ge "$max_attempts" ]]; then
      return "$rc"
    fi

    # Heuristic: if OOM-like failure, reduce requested_sort_workers.
    if tail -n 200 "$logf" | grep -Eqi 'out of memory|oom|killed process|std::bad_alloc|MemoryError|SIGKILL'; then
      if [[ "$requested_sort_workers" -le 1 ]]; then
        return "$rc"
      fi
      requested_sort_workers=$(( (requested_sort_workers + 1) / 2 ))
      if [[ "$requested_sort_workers" -lt 1 ]]; then requested_sort_workers=1; fi
      echo "[$(date -Is)] Detected OOM-like failure; backing off requested sort workers to ${requested_sort_workers} and retrying..." | tee -a "$logf"
      attempt=$((attempt + 1))
      sleep 5
      continue
    fi

    # Non-OOM failure: don't loop.
    return "$rc"
  done
}

while is_orchestrator_running; do
  echo "[$(date -Is)] orchestrator already running; waiting..."
  sleep 60
done

for f in "${COLLECTIONS[@]}"; do
  year=$(echo "$f" | cut -d- -f1)
  run_one "$f" "$year"

done
