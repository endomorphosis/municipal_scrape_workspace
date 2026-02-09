#!/usr/bin/env bash
set -euo pipefail

# Rebuild year-level meta indexes (2021..2025) + master meta index.
# Uses the pointer-index layout configured in pipeline_config.json.
#
# Safe behavior:
# - waits for any orchestrator process to finish (avoids reading DBs mid-write)
# - rebuilds year DBs using per-collection DBs as inputs
# - rebuilds master DB from the year DB directory

START_YEAR=${START_YEAR:-2021}
END_YEAR=${END_YEAR:-2025}
TS=${TS:-$(date +%Y%m%d_%H%M%S)}

COLL_DIR=${COLL_DIR:-/storage/ccindex_duckdb/cc_pointers_by_collection}
YEAR_DIR=${YEAR_DIR:-/storage/ccindex_duckdb/cc_pointers_by_year}
MASTER_DIR=${MASTER_DIR:-/storage/ccindex_duckdb/cc_pointers_master}
MASTER_DB=${MASTER_DB:-${MASTER_DIR}/cc_master_index.duckdb}

mkdir -p logs "$YEAR_DIR" "$MASTER_DIR"

is_orchestrator_running() {
  ps -ef | grep -E "python3? -u -m common_crawl_search_engine\\.ccindex\\.cc_pipeline_orchestrator" | grep -v grep >/dev/null
}

while is_orchestrator_running; do
  echo "[$(date -Is)] orchestrator running; waiting before meta-index rebuild..."
  sleep 60
done

logf="logs/metaindex_rebuild_${START_YEAR}_${END_YEAR}_${TS}.log"
{
  echo "==== $(date -Is) METAINDEX REBUILD START (${START_YEAR}-${END_YEAR}) ===="
  echo "collection_dir=${COLL_DIR}"
  echo "year_dir=${YEAR_DIR}"
  echo "master_db=${MASTER_DB}"
  echo

  for y in $(seq "$START_YEAR" "$END_YEAR"); do
    echo "---- $(date -Is) year=${y} (build_year_meta_indexes) ----"
    # If there are no collection DBs for a year, skip with a clear message.
    if ! ls "${COLL_DIR}/CC-MAIN-${y}-"*.duckdb >/dev/null 2>&1; then
      echo "skip year=${y}: no collection DBs matched ${COLL_DIR}/CC-MAIN-${y}-*.duckdb"
      continue
    fi

    PYTHONPATH=src python3 -u src/common_crawl_search_engine/ccindex/build_year_meta_indexes.py \
      --collection-dir "$COLL_DIR" \
      --output-dir "$YEAR_DIR" \
      --year "$y"
  done

  echo
  echo "---- $(date -Is) build master meta-index ----"
  PYTHONPATH=src python3 -u src/common_crawl_search_engine/ccindex/build_master_index.py \
    --year-dir "$YEAR_DIR" \
    --output "$MASTER_DB"

  echo
  echo "---- $(date -Is) master stats ----"
  PYTHONPATH=src python3 -u src/common_crawl_search_engine/ccindex/build_master_index.py \
    --stats \
    --output "$MASTER_DB"

  echo "==== $(date -Is) METAINDEX REBUILD DONE ===="
} 2>&1 | tee "$logf"
