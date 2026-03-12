#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
PKG_DIR="$ROOT_DIR/ipfs_datasets_py"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

CORPUS=${LEGAL_DAEMON_PENDING_RETRY_CORPUS:-${LEGAL_DAEMON_CORPUS:-state_laws}}
DAEMON_OUTPUT_DIR=${LEGAL_DAEMON_PENDING_RETRY_OUTPUT_DIR:-}
WATCH=${LEGAL_DAEMON_PENDING_RETRY_WATCH:-1}
INTERVAL_SECONDS=${LEGAL_DAEMON_PENDING_RETRY_INTERVAL_SECONDS:-10}
MAX_REPORTS=${LEGAL_DAEMON_PENDING_RETRY_MAX_REPORTS:-0}

ARGS=(
  "./scripts/ops/legal_data/report_agentic_daemon_pending_retry.py"
  --corpus "$CORPUS"
)

if [[ -n "$DAEMON_OUTPUT_DIR" ]]; then
  ARGS+=(--daemon-output-dir "$DAEMON_OUTPUT_DIR")
fi

if [[ "$WATCH" == "1" ]]; then
  ARGS+=(--watch --interval-seconds "$INTERVAL_SECONDS")
  if [[ -n "$MAX_REPORTS" && "$MAX_REPORTS" != "0" ]]; then
    ARGS+=(--max-reports "$MAX_REPORTS")
  fi
fi

cd "$ROOT_DIR"
exec python3 "${ARGS[@]}" "$@"