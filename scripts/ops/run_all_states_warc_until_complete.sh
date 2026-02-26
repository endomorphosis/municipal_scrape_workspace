#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-/home/barberb/municipal_scrape_workspace}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/.venv/bin/python}"
PYTHONPATH_DIR="${PYTHONPATH_DIR:-$REPO_ROOT/ipfs_datasets_py}"
PARTITIONED_PARQUET_DIR="${PARTITIONED_PARQUET_DIR:-$REPO_ROOT/artifacts/jurisdiction_pointer_inventory/pointers_by_jurisdiction}"
ARCHIVE_DIR="${ARCHIVE_DIR:-$REPO_ROOT/archive}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs/state_warc_full_until_complete}"

MAX_PARALLEL="${MAX_PARALLEL:-6}"
MAX_RETRIES="${MAX_RETRIES:-8}"
RETRY_BACKOFF_SECONDS="${RETRY_BACKOFF_SECONDS:-2.0}"
RANGE_GAP_BYTES="${RANGE_GAP_BYTES:-1024}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
OVERWRITE_HTML="${OVERWRITE_HTML:-0}"
OVERWRITE_RANGES="${OVERWRITE_RANGES:-0}"
SKIP_DOWNLOAD="${SKIP_DOWNLOAD:-0}"
MAX_PASSES="${MAX_PASSES:-10}"

STATES=(
  AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD
  MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC
  SD TN TX UT VT VA WA WV WI WY
)

mkdir -p "$LOG_DIR"

launch_state() {
  local state="$1"
  local pass="$2"
  local log_file="$LOG_DIR/${state}.pass${pass}.log"
  local pid_file="$LOG_DIR/${state}.pass${pass}.pid"

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
  echo "launched pass=$pass state=$state pid=$pid log=$log_file"
}

running_jobs() {
  jobs -rp | wc -l | tr -d ' '
}

is_state_complete() {
  local state="$1"
  local summary
  summary=$(python - <<'PY' "$REPO_ROOT" "$state"
import json
import sys
from pathlib import Path
root=Path(sys.argv[1])
state=sys.argv[2]
mdir=root/'data'/'state_laws'/state/'manifests'
files=sorted(mdir.glob(f'warc_batch_summary_{state.lower()}_*.json')) if mdir.exists() else []
if not files:
    print('INCOMPLETE:no_summary')
    raise SystemExit(0)
obj=json.loads(files[-1].read_text(encoding='utf-8'))
if int(obj.get('warc_files_processed',0)) != int(obj.get('warc_files_total',-1)):
    print('INCOMPLETE:processed_mismatch')
    raise SystemExit(0)
if int(obj.get('warc_files_failed',0)) > 0:
    print('INCOMPLETE:failed_warc_files')
    raise SystemExit(0)
print('COMPLETE')
PY
)
  [[ "$summary" == "COMPLETE" ]]
}

remaining_states=(${STATES[@]})

for pass in $(seq 1 "$MAX_PASSES"); do
  if [[ ${#remaining_states[@]} -eq 0 ]]; then
    break
  fi

  echo "===== PASS $pass ====="
  echo "states_to_run=${#remaining_states[@]}"

  for state in "${remaining_states[@]}"; do
    while [[ "$(running_jobs)" -ge "$MAX_PARALLEL" ]]; do
      wait -n || true
    done
    launch_state "$state" "$pass"
  done

  wait || true

  next_remaining=()
  for state in "${remaining_states[@]}"; do
    if is_state_complete "$state"; then
      echo "pass=$pass state=$state complete"
    else
      echo "pass=$pass state=$state incomplete"
      next_remaining+=("$state")
    fi
  done

  remaining_states=(${next_remaining[@]})
  echo "pass=$pass remaining_states=${#remaining_states[@]}"
done

if [[ ${#remaining_states[@]} -gt 0 ]]; then
  echo "Run ended with incomplete states: ${remaining_states[*]}"
  exit 2
fi

echo "All 50 states completed with zero failed WARC files."
