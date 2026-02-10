#!/usr/bin/env bash
set -euo pipefail

# Upload /storage/ccindex_parquet/cc_pointers_by_collection to the HF dataset:
#   endomorphosis/common_crawl_pointers_by_collection
#
# This mode does NOT clone the repo or rsync/copy files locally.
# It streams uploads via the Hugging Face Hub HTTP API.
#
# Requirements:
#   - huggingface-cli (huggingface_hub)
#   - You must have run: huggingface-cli login

SRC_DIR=${SRC_DIR:-/storage/ccindex_parquet/cc_pointers_by_collection}
HF_DATASET=${HF_DATASET:-endomorphosis/common_crawl_pointers_by_collection}

# Optional: space-separated years to upload (e.g. "2023 2024 2025").
# If empty, the uploader auto-detects year directories under SRC_DIR.
YEARS=${YEARS:-"2023 2024 2025"}

# Set to 1 to skip creating the remote dataset repo (assumes it already exists)
SKIP_CREATE=${SKIP_CREATE:-0}

# Optional upgrades / speedups:
# - ENSURE_HF_XET=1 upgrades huggingface_hub to a Xet-aware version (>=0.32.0)
#   which installs `hf_xet` automatically.
# - HF_XET_HIGH_PERFORMANCE=1 tells hf_xet to use more CPU/network.
ENSURE_HF_XET=${ENSURE_HF_XET:-0}

# Python environment handling (recommended on Debian/Ubuntu with PEP 668):
# - USE_VENV=1 will run everything from the repo-local .venv
# - VENV_DIR sets the venv path
USE_VENV=${USE_VENV:-1}
VENV_DIR=${VENV_DIR:-.venv}
# Default to higher parallelism; uploader will still reduce concurrency on retries/429.
NUM_WORKERS=${NUM_WORKERS:-8}
PRINT_REPORT_EVERY=${PRINT_REPORT_EVERY:-60}
HEARTBEAT_SECONDS=${HEARTBEAT_SECONDS:-60}
VERBOSE=${VERBOSE:-1}

# Prevent tqdm-based progress bars from crashing the process with BrokenPipeError
# when output is piped (e.g. through tee) or the terminal consumer is interrupted.
# We already provide structured logging + a heartbeat.
export HF_HUB_DISABLE_PROGRESS_BARS=${HF_HUB_DISABLE_PROGRESS_BARS:-1}

MAX_RETRIES=${MAX_RETRIES:-5}
RETRY_SLEEP_SECONDS=${RETRY_SLEEP_SECONDS:-10}
FALLBACK_TO_SINGLE_WORKER=${FALLBACK_TO_SINGLE_WORKER:-1}

# Throttle API-heavy stages inside huggingface_hub.upload_large_folder.
# Keep low to avoid 1000-requests/5min rate limit bursts.
MAX_GET_UPLOAD_MODE_WORKERS=${MAX_GET_UPLOAD_MODE_WORKERS:-1}
MAX_PREUPLOAD_WORKERS=${MAX_PREUPLOAD_WORKERS:-1}

# Upload granularity. Default to collection to reduce Hub API request bursts
# and avoid 429 rate limiting on large folder uploads.
CHUNK_BY=${CHUNK_BY:-collection}

# Keep HF cache/locks out of the dataset directory tree.
# IMPORTANT: /storage may not be writable; default under /storage/ccindex_parquet which exists.
HF_CACHE_DIR=${HF_CACHE_DIR:-/storage/ccindex_parquet/hf_cache}

# Optional: upload only these collections (space-separated), e.g.
#   COLLECTIONS="CC-MAIN-2023-06 CC-MAIN-2023-14"
COLLECTIONS=${COLLECTIONS:-""}

# Handle local upload_large_folder resume cache issues.
# - invalid: delete only corrupted *.metadata entries (recommended)
# - all: delete all resume state under SRC_DIR/.cache/huggingface/upload for selected scope
PURGE_UPLOAD_CACHE=${PURGE_UPLOAD_CACHE:-invalid}

# Save computed sha256 hashes outside the dataset tree to speed up restarts.
# Values: off | ro | rw
SHA256_CACHE=${SHA256_CACHE:-rw}
# Optional override for where to store the SQLite DB. Default is $HF_HOME/sha256_cache.sqlite.
SHA256_CACHE_DB=${SHA256_CACHE_DB:-""}

# LFS is deprecated in this environment (DNS often fails for lfs.* hostnames).
# By default, require Xet to be enabled on the destination repo.
REQUIRE_XET=${REQUIRE_XET:-1}

TS=$(date +%Y%m%d_%H%M%S)
LOG_FILE=${LOG_FILE:-logs/hf_upload_cc_pointers_by_collection_${TS}.log}

mkdir -p logs
mkdir -p "$HF_CACHE_DIR" || true

echo "Starting HF upload"
echo "  SRC_DIR=$SRC_DIR"
echo "  HF_DATASET=$HF_DATASET"
echo "  YEARS=$YEARS"
echo "  CHUNK_BY=$CHUNK_BY"
echo "  NUM_WORKERS=$NUM_WORKERS"
echo "  LOG_FILE=$LOG_FILE"
echo "  HF_HOME/HF_CACHE_DIR=$HF_CACHE_DIR"
echo "  HF_HUB_DISABLE_PROGRESS_BARS=$HF_HUB_DISABLE_PROGRESS_BARS"
echo "  REQUIRE_XET=$REQUIRE_XET"
echo "  MAX_GET_UPLOAD_MODE_WORKERS=$MAX_GET_UPLOAD_MODE_WORKERS"
echo "  MAX_PREUPLOAD_WORKERS=$MAX_PREUPLOAD_WORKERS"
echo "  SHA256_CACHE=$SHA256_CACHE"
if [ -n "$SHA256_CACHE_DB" ]; then
  echo "  SHA256_CACHE_DB=$SHA256_CACHE_DB"
fi
if [ -n "${HF_ENDPOINT:-}" ]; then
  echo "  HF_ENDPOINT=$HF_ENDPOINT"
fi

if [ "${HF_XET_HIGH_PERFORMANCE:-0}" = "1" ]; then
  export HF_XET_HIGH_PERFORMANCE=1
  echo "  HF_XET_HIGH_PERFORMANCE=1"
fi

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 2
  }
}

check_dns() {
  # Python-based DNS check (avoids dependency on `dig`/`nslookup`).
  local host="$1"
  "$PY_BIN" - "$host" <<'PY'
import socket
import sys

host = sys.argv[1]
try:
    socket.getaddrinfo(host, 443)
except Exception as e:
    print(f"DNS FAIL: {host}: {e}")
    raise SystemExit(1)
print(f"DNS OK:   {host}")
PY
}

need_cmd huggingface-cli

venv_python() {
  echo "${VENV_DIR}/bin/python"
}

ensure_venv() {
  if [ ! -x "$(venv_python)" ]; then
    echo "Creating venv at ${VENV_DIR}"
    python3 -m venv "${VENV_DIR}"
  fi
  "$(venv_python)" -m pip install -U pip setuptools wheel >/dev/null
}

PY_BIN=python3
if [ "$USE_VENV" = "1" ]; then
  ensure_venv
  PY_BIN="$(venv_python)"
fi

echo "huggingface_hub / hf_xet preflight:"
"$PY_BIN" - <<'PY'
import os
try:
    import huggingface_hub
    print('  huggingface_hub version:', huggingface_hub.__version__)
except Exception as e:
    print('  huggingface_hub import: FAIL', e)

disabled = os.environ.get('HF_HUB_DISABLE_XET')
if disabled:
    print('  HF_HUB_DISABLE_XET is set -> Xet is disabled')

try:
    import hf_xet  # noqa: F401
    print('  hf_xet import: OK')
except Exception as e:
    print('  hf_xet import: FAIL', e)
PY

echo "DNS preflight (Hub API):"
check_dns huggingface.co || true

echo "Xet preflight (deprecating LFS):"
"$PY_BIN" - "$HF_DATASET" "$REQUIRE_XET" <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request

repo_id = sys.argv[1]
require_xet = sys.argv[2] == '1'

endpoint = os.environ.get('HF_ENDPOINT', 'https://huggingface.co').rstrip('/')
url = f"{endpoint}/api/datasets/{repo_id}/revision/main?expand=xetEnabled"

headers = {"User-Agent": "municipal-scrape-workspace-hf-uploader"}
token = os.environ.get('HF_TOKEN') or os.environ.get('HUGGING_FACE_HUB_TOKEN')
if token:
  headers["Authorization"] = f"Bearer {token}"

req = urllib.request.Request(url, headers=headers)
try:
  with urllib.request.urlopen(req, timeout=10) as resp:
    data = json.loads(resp.read().decode('utf-8'))
except Exception as e:
  print(f"  xetEnabled: UNKNOWN (failed to query {url}: {e})")
  if require_xet:
    print("  ERROR: REQUIRE_XET=1 but cannot determine xetEnabled; refusing to proceed.")
    sys.exit(2)
  sys.exit(0)

xet = data.get('xetEnabled')
print(f"  xetEnabled: {xet}")
if require_xet and xet is not True:
  print("  ERROR: REQUIRE_XET=1 but repo is not Xet-enabled. Enable Xet on the dataset repo.")
  sys.exit(2)
PY

if [ ! -d "$SRC_DIR" ]; then
  echo "Source directory not found: $SRC_DIR" >&2
  exit 2
fi

# Verify auth (will fail fast if not logged in)
if ! huggingface-cli whoami >/dev/null 2>&1; then
  echo "Hugging Face auth check failed (huggingface-cli whoami)." >&2
  echo "Try: huggingface-cli login" >&2
  if [ -n "${HF_ENDPOINT:-}" ]; then
    echo "Note: HF_ENDPOINT is set to '$HF_ENDPOINT'" >&2
  fi
  # Print the error details to stderr for visibility.
  huggingface-cli whoami >&2 || true
  exit 2
fi

if [ "$ENSURE_HF_XET" = "1" ]; then
  echo "Installing/upgrading requirements in venv"
  "$PY_BIN" -m pip install -U -r requirements.txt
fi

create_flag=()
if [ "$SKIP_CREATE" = "0" ]; then
  create_flag=(--create-repo)
fi

years_args=()
if [ -n "$YEARS" ]; then
  # shellcheck disable=SC2206
  years_args=(--years $YEARS)
fi

collections_args=()
if [ -n "$COLLECTIONS" ]; then
  # shellcheck disable=SC2206
  collections_args=(--collections $COLLECTIONS)
fi

PYTHONPATH=src "$PY_BIN" -u scripts/ops/hf_upload_cc_pointers_by_collection.py \
  --repo-id "$HF_DATASET" \
  --src "$SRC_DIR" \
  $( [ "$REQUIRE_XET" = "1" ] && echo --require-xet || echo --allow-lfs ) \
  --purge-upload-cache "$PURGE_UPLOAD_CACHE" \
  --chunk-by "$CHUNK_BY" \
  --num-workers "$NUM_WORKERS" \
  --max-get-upload-mode-workers "$MAX_GET_UPLOAD_MODE_WORKERS" \
  --max-preupload-workers "$MAX_PREUPLOAD_WORKERS" \
  --sha256-cache "$SHA256_CACHE" \
  $( [ -n "$SHA256_CACHE_DB" ] && echo --sha256-cache-db "$SHA256_CACHE_DB" ) \
  --print-report-every "$PRINT_REPORT_EVERY" \
  --heartbeat-seconds "$HEARTBEAT_SECONDS" \
  --max-retries "$MAX_RETRIES" \
  --retry-sleep-seconds "$RETRY_SLEEP_SECONDS" \
  --hf-cache-dir "$HF_CACHE_DIR" \
  $( [ "$FALLBACK_TO_SINGLE_WORKER" = "1" ] && echo --fallback-to-single-worker ) \
  --log-file "$LOG_FILE" \
  $( [ "$VERBOSE" = "1" ] && echo --verbose ) \
  "${create_flag[@]}" \
  "${years_args[@]}" \
  "${collections_args[@]}" \
  2>&1 | tee -a "$LOG_FILE"

echo "Done. Dataset: https://huggingface.co/datasets/${HF_DATASET}"
echo "Log: $LOG_FILE"
