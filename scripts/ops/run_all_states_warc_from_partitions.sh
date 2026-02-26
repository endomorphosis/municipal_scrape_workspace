#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-/home/barberb/municipal_scrape_workspace}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/.venv/bin/python}"
PYTHONPATH_DIR="${PYTHONPATH_DIR:-$REPO_ROOT/ipfs_datasets_py}"
PARTITIONED_PARQUET_DIR="${PARTITIONED_PARQUET_DIR:-$REPO_ROOT/artifacts/jurisdiction_pointer_inventory/pointers_by_jurisdiction}"
ARCHIVE_DIR="${ARCHIVE_DIR:-$REPO_ROOT/archive}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs/state_warc_full}"

MAX_PARALLEL="${MAX_PARALLEL:-6}"
MAX_RETRIES="${MAX_RETRIES:-8}"
RETRY_BACKOFF_SECONDS="${RETRY_BACKOFF_SECONDS:-2.0}"
RANGE_GAP_BYTES="${RANGE_GAP_BYTES:-1024}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
OVERWRITE_HTML="${OVERWRITE_HTML:-0}"
OVERWRITE_RANGES="${OVERWRITE_RANGES:-0}"
SKIP_DOWNLOAD="${SKIP_DOWNLOAD:-0}"
MAX_WARC_FILES="${MAX_WARC_FILES:-}"

STATES=(
  AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD
  MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC
  SD TN TX UT VT VA WA WV WI WY
)

mkdir -p "$LOG_DIR"

launch_state() {
  local state="$1"
  local log_file="$LOG_DIR/${state}.log"
  local pid_file="$LOG_DIR/${state}.pid"

  local -a cmd=(
    env "PYTHONPATH=$PYTHONPATH_DIR" "$PYTHON_BIN"
    -m ipfs_datasets_py.processors.legal_scrapers.state_scrapers.state_warc_batch_downloader_from_partitions
    --partitioned-parquet-dir "$PARTITIONED_PARQUET_DIR"
    --repo-root "$REPO_ROOT"
    --archive-dir "$ARCHIVE_DIR"
    --state "$state"
    --max-retries "$MAX_RETRIES"
    --retry-backoff-seconds "$RETRY_BACKOFF_SECONDS"
    --range-gap-bytes "$RANGE_GAP_BYTES"
    --timeout-seconds "$TIMEOUT_SECONDS"
    --log-level "$LOG_LEVEL"
  )

  if [[ -n "$MAX_WARC_FILES" ]]; then
    cmd+=(--max-warc-files "$MAX_WARC_FILES")
  fi
  if [[ "$OVERWRITE_HTML" == "1" ]]; then
    cmd+=(--overwrite-html)
  fi
  if [[ "$OVERWRITE_RANGES" == "1" ]]; then
    cmd+=(--overwrite-ranges)
  fi
  if [[ "$SKIP_DOWNLOAD" == "1" ]]; then
    cmd+=(--skip-download)
  fi

  nohup "${cmd[@]}" >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" > "$pid_file"
  echo "launched state=$state pid=$pid log=$log_file"
}

running_jobs() {
  jobs -rp | wc -l | tr -d ' '
}

echo "Starting all-state WARC batch run"
echo "repo_root=$REPO_ROOT"
echo "partitioned_parquet_dir=$PARTITIONED_PARQUET_DIR"
echo "max_parallel=$MAX_PARALLEL"

for state in "${STATES[@]}"; do
  while [[ "$(running_jobs)" -ge "$MAX_PARALLEL" ]]; do
    wait -n || true
  done
  launch_state "$state"
done

wait || true

echo "All state jobs launched and finished."
echo "Logs: $LOG_DIR"
