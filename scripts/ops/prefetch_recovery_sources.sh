#!/usr/bin/env bash
set -euo pipefail

# Prefetch Common Crawl index shard sources (cdx-*.gz) for the recovery collections.
# This is safe to run while the orchestrator is sorting, since it only downloads
# sources into /storage/ccindex/<collection>/.

COLLECTIONS=(
  "CC-MAIN-2023-40"
  "CC-MAIN-2023-50"
  "CC-MAIN-2024-10"
  "CC-MAIN-2024-22"
)

JOBS=${JOBS:-8}
SKIP_COLLECTION=${SKIP_COLLECTION:-""}

for coll in "${COLLECTIONS[@]}"; do
  if [[ -n "$SKIP_COLLECTION" && "$coll" == "$SKIP_COLLECTION" ]]; then
    echo "[$(date -Is)] SKIP $coll (SKIP_COLLECTION)"
    continue
  fi

  out_dir="/storage/ccindex/${coll}"
  mkdir -p "$out_dir"

  echo "[$(date -Is)] START download $coll -> $out_dir (jobs=$JOBS)"
  bash scripts/ops/download_cc_indexes.sh "$coll" "$out_dir" "$JOBS"
  echo "[$(date -Is)] DONE  download $coll"
  echo
done

echo "[$(date -Is)] ALL DONE"
