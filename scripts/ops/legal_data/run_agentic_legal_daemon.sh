#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
PKG_DIR="$ROOT_DIR/ipfs_datasets_py"
ROOT_VENV_PYTHON="$ROOT_DIR/.venv/bin/python"
PKG_VENV_PYTHON="$PKG_DIR/.venv/bin/python"

_PRESERVE_ENV_VARS=(
  LEGAL_DAEMON_PYTHON_BIN
  LEGAL_DAEMON_CORPUS
  LEGAL_DAEMON_STATES
  LEGAL_DAEMON_MAX_CYCLES
  LEGAL_DAEMON_MAX_STATUTES
  LEGAL_DAEMON_CYCLE_INTERVAL_SECONDS
  LEGAL_DAEMON_ARCHIVE_WARMUP_URLS
  LEGAL_DAEMON_PER_STATE_TIMEOUT_SECONDS
  LEGAL_DAEMON_SCRAPE_TIMEOUT_SECONDS
  LEGAL_DAEMON_ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE
  LEGAL_DAEMON_ADMIN_AGENTIC_MAX_FETCH_PER_STATE
  LEGAL_DAEMON_ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN
  LEGAL_DAEMON_ADMIN_AGENTIC_MAX_HOPS
  LEGAL_DAEMON_ADMIN_AGENTIC_MAX_PAGES
  LEGAL_DAEMON_ADMIN_AGENTIC_FETCH_CONCURRENCY
  LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_ENABLED
  LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_STATE_LIMIT
  LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_MAX_URLS_PER_DOMAIN
  LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS
  CLOUDFLARE_ACCOUNT_ID
  CLOUDFLARE_API_TOKEN
  IPFS_DATASETS_CLOUDFLARE_ACCOUNT_ID
  IPFS_DATASETS_CLOUDFLARE_API_TOKEN
  LEGAL_SCRAPER_CLOUDFLARE_ACCOUNT_ID
  LEGAL_SCRAPER_CLOUDFLARE_API_TOKEN
  IPFS_DATASETS_CLOUDFLARE_CRAWL_TIMEOUT_SECONDS
  IPFS_DATASETS_CLOUDFLARE_CRAWL_POLL_INTERVAL_SECONDS
  IPFS_DATASETS_CLOUDFLARE_CRAWL_MAX_RATE_LIMIT_WAIT_SECONDS
  IPFS_DATASETS_CLOUDFLARE_CRAWL_LIMIT
  IPFS_DATASETS_CLOUDFLARE_CRAWL_DEPTH
  IPFS_DATASETS_CLOUDFLARE_CRAWL_RENDER
  IPFS_DATASETS_CLOUDFLARE_CRAWL_SOURCE
  IPFS_DATASETS_CLOUDFLARE_CRAWL_FORMATS
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_TIMEOUT_SECONDS
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_POLL_INTERVAL_SECONDS
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_MAX_RATE_LIMIT_WAIT_SECONDS
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_LIMIT
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_DEPTH
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_RENDER
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_SOURCE
  LEGAL_SCRAPER_CLOUDFLARE_CRAWL_FORMATS
  LEGAL_DAEMON_ROUTER_LLM_TIMEOUT_SECONDS
  LEGAL_DAEMON_ROUTER_EMBEDDINGS_TIMEOUT_SECONDS
  LEGAL_DAEMON_ROUTER_IPFS_TIMEOUT_SECONDS
  LEGAL_DAEMON_MIN_DOCUMENT_RECOVERY_RATIO
  LEGAL_DAEMON_TARGET_SCORE
  LEGAL_DAEMON_RANDOM_SEED
  LEGAL_DAEMON_OUTPUT_DIR
  LEGAL_DAEMON_STOP_ON_TARGET_SCORE
  LEGAL_DAEMON_PRINT_RELEASE_PLAN
  LEGAL_DAEMON_POST_CYCLE_RELEASE
  LEGAL_DAEMON_POST_CYCLE_RELEASE_DRY_RUN
  LEGAL_DAEMON_POST_CYCLE_RELEASE_MIN_SCORE
  LEGAL_DAEMON_POST_CYCLE_RELEASE_IGNORE_PASS
  LEGAL_DAEMON_POST_CYCLE_RELEASE_TIMEOUT_SECONDS
  LEGAL_DAEMON_POST_CYCLE_RELEASE_WORKSPACE_ROOT
  LEGAL_DAEMON_POST_CYCLE_RELEASE_PYTHON_BIN
  LEGAL_DAEMON_POST_CYCLE_RELEASE_PUBLISH_COMMAND
  LEGAL_DAEMON_POST_CYCLE_RELEASE_PREVIEW_SCORE
  LEGAL_DAEMON_POST_CYCLE_RELEASE_PREVIEW_CYCLE
  LEGAL_DAEMON_SUMMARIZE_TACTIC_SELECTION
)

for _var in "${_PRESERVE_ENV_VARS[@]}"; do
  if [[ -v "$_var" ]]; then
    _preserve_name="_PRESERVE_${_var}"
    _preserve_flag="_PRESERVE_SET_${_var}"
    printf -v "$_preserve_name" '%s' "${!_var}"
    printf -v "$_preserve_flag" '%s' '1'
  fi
done

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

for _var in "${_PRESERVE_ENV_VARS[@]}"; do
  _flag="_PRESERVE_SET_${_var}"
  if [[ -n "${!_flag:-}" ]]; then
    _value_var="_PRESERVE_${_var}"
    export "$_var=${!_value_var}"
  fi
done

PYTHON_BIN=${LEGAL_DAEMON_PYTHON_BIN:-}
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$ROOT_VENV_PYTHON" ]]; then
    PYTHON_BIN="$ROOT_VENV_PYTHON"
  elif [[ -x "$PKG_VENV_PYTHON" ]]; then
    PYTHON_BIN="$PKG_VENV_PYTHON"
  else
    PYTHON_BIN=$(command -v python3)
  fi
fi

CORPUS=${LEGAL_DAEMON_CORPUS:-state_laws}
STATES=${LEGAL_DAEMON_STATES:-all}
MAX_CYCLES=${LEGAL_DAEMON_MAX_CYCLES:-1}
MAX_STATUTES=${LEGAL_DAEMON_MAX_STATUTES:-0}
CYCLE_INTERVAL_SECONDS=${LEGAL_DAEMON_CYCLE_INTERVAL_SECONDS:-900}
ARCHIVE_WARMUP_URLS=${LEGAL_DAEMON_ARCHIVE_WARMUP_URLS:-25}
PER_STATE_TIMEOUT_SECONDS=${LEGAL_DAEMON_PER_STATE_TIMEOUT_SECONDS:-86400}
ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE=${LEGAL_DAEMON_ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE:-1000}
ADMIN_AGENTIC_MAX_FETCH_PER_STATE=${LEGAL_DAEMON_ADMIN_AGENTIC_MAX_FETCH_PER_STATE:-1000}
ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN=${LEGAL_DAEMON_ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN:-1000}
ADMIN_AGENTIC_MAX_HOPS=${LEGAL_DAEMON_ADMIN_AGENTIC_MAX_HOPS:-4}
ADMIN_AGENTIC_MAX_PAGES=${LEGAL_DAEMON_ADMIN_AGENTIC_MAX_PAGES:-1000}
ADMIN_AGENTIC_FETCH_CONCURRENCY=${LEGAL_DAEMON_ADMIN_AGENTIC_FETCH_CONCURRENCY:-}
ADMIN_PARALLEL_ASSIST_ENABLED=${LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_ENABLED:-}
ADMIN_PARALLEL_ASSIST_STATE_LIMIT=${LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_STATE_LIMIT:-}
ADMIN_PARALLEL_ASSIST_MAX_URLS_PER_DOMAIN=${LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_MAX_URLS_PER_DOMAIN:-}
ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS=${LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS:-}
ROUTER_LLM_TIMEOUT_SECONDS=${LEGAL_DAEMON_ROUTER_LLM_TIMEOUT_SECONDS:-20}
ROUTER_EMBEDDINGS_TIMEOUT_SECONDS=${LEGAL_DAEMON_ROUTER_EMBEDDINGS_TIMEOUT_SECONDS:-10}
ROUTER_IPFS_TIMEOUT_SECONDS=${LEGAL_DAEMON_ROUTER_IPFS_TIMEOUT_SECONDS:-10}
MIN_DOCUMENT_RECOVERY_RATIO=${LEGAL_DAEMON_MIN_DOCUMENT_RECOVERY_RATIO:-0}
SCRAPE_TIMEOUT_SECONDS=${LEGAL_DAEMON_SCRAPE_TIMEOUT_SECONDS:-}
TARGET_SCORE=${LEGAL_DAEMON_TARGET_SCORE:-0.92}
RANDOM_SEED=${LEGAL_DAEMON_RANDOM_SEED:-}
OUTPUT_DIR=${LEGAL_DAEMON_OUTPUT_DIR:-}
STOP_ON_TARGET_SCORE=${LEGAL_DAEMON_STOP_ON_TARGET_SCORE:-0}
PRINT_RELEASE_PLAN=${LEGAL_DAEMON_PRINT_RELEASE_PLAN:-0}
POST_CYCLE_RELEASE=${LEGAL_DAEMON_POST_CYCLE_RELEASE:-0}
POST_CYCLE_RELEASE_DRY_RUN=${LEGAL_DAEMON_POST_CYCLE_RELEASE_DRY_RUN:-0}
POST_CYCLE_RELEASE_MIN_SCORE=${LEGAL_DAEMON_POST_CYCLE_RELEASE_MIN_SCORE:-}
POST_CYCLE_RELEASE_IGNORE_PASS=${LEGAL_DAEMON_POST_CYCLE_RELEASE_IGNORE_PASS:-0}
POST_CYCLE_RELEASE_TIMEOUT_SECONDS=${LEGAL_DAEMON_POST_CYCLE_RELEASE_TIMEOUT_SECONDS:-7200}
POST_CYCLE_RELEASE_WORKSPACE_ROOT=${LEGAL_DAEMON_POST_CYCLE_RELEASE_WORKSPACE_ROOT:-$ROOT_DIR}
POST_CYCLE_RELEASE_PYTHON_BIN=${LEGAL_DAEMON_POST_CYCLE_RELEASE_PYTHON_BIN:-$PYTHON_BIN}
POST_CYCLE_RELEASE_PUBLISH_COMMAND=${LEGAL_DAEMON_POST_CYCLE_RELEASE_PUBLISH_COMMAND:-}
POST_CYCLE_RELEASE_PREVIEW_SCORE=${LEGAL_DAEMON_POST_CYCLE_RELEASE_PREVIEW_SCORE:-}
POST_CYCLE_RELEASE_PREVIEW_CYCLE=${LEGAL_DAEMON_POST_CYCLE_RELEASE_PREVIEW_CYCLE:-1}
SUMMARIZE_PENDING_RETRY=${LEGAL_DAEMON_SUMMARIZE_PENDING_RETRY:-1}
SUMMARIZE_TACTIC_SELECTION=${LEGAL_DAEMON_SUMMARIZE_TACTIC_SELECTION:-1}

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_PER_STATE_TIMEOUT_SECONDS:-}" ]] && [[ "$PER_STATE_TIMEOUT_SECONDS" == "480" || "$PER_STATE_TIMEOUT_SECONDS" == "480.0" ]]; then
  PER_STATE_TIMEOUT_SECONDS=86400
fi

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE:-}" ]] && [[ "$ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE" == "12" ]]; then
  ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE=1000
fi

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_ADMIN_AGENTIC_MAX_FETCH_PER_STATE:-}" ]] && [[ "$ADMIN_AGENTIC_MAX_FETCH_PER_STATE" == "5" ]]; then
  ADMIN_AGENTIC_MAX_FETCH_PER_STATE=1000
fi

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN:-}" ]] && [[ "$ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN" == "20" ]]; then
  ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN=1000
fi

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_ADMIN_AGENTIC_MAX_HOPS:-}" ]] && [[ "$ADMIN_AGENTIC_MAX_HOPS" == "1" ]]; then
  ADMIN_AGENTIC_MAX_HOPS=4
fi

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_ADMIN_AGENTIC_MAX_PAGES:-}" ]] && [[ "$ADMIN_AGENTIC_MAX_PAGES" == "8" ]]; then
  ADMIN_AGENTIC_MAX_PAGES=1000
fi

if [[ -z "${_PRESERVE_SET_LEGAL_DAEMON_ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS:-}" ]] && [[ "$ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS" == "180" || "$ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS" == "180.0" ]]; then
  ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS=86400
fi

ARGS=(
  -m ipfs_datasets_py.processors.legal_scrapers.state_laws_agentic_daemon
  --corpus "$CORPUS"
  --states "$STATES"
  --max-cycles "$MAX_CYCLES"
  --max-statutes "$MAX_STATUTES"
  --cycle-interval-seconds "$CYCLE_INTERVAL_SECONDS"
  --archive-warmup-urls "$ARCHIVE_WARMUP_URLS"
  --per-state-timeout-seconds "$PER_STATE_TIMEOUT_SECONDS"
  --router-llm-timeout-seconds "$ROUTER_LLM_TIMEOUT_SECONDS"
  --router-embeddings-timeout-seconds "$ROUTER_EMBEDDINGS_TIMEOUT_SECONDS"
  --router-ipfs-timeout-seconds "$ROUTER_IPFS_TIMEOUT_SECONDS"
  --min-document-recovery-ratio "$MIN_DOCUMENT_RECOVERY_RATIO"
  --target-score "$TARGET_SCORE"
  --post-cycle-release-timeout-seconds "$POST_CYCLE_RELEASE_TIMEOUT_SECONDS"
  --post-cycle-release-workspace-root "$POST_CYCLE_RELEASE_WORKSPACE_ROOT"
  --post-cycle-release-python-bin "$POST_CYCLE_RELEASE_PYTHON_BIN"
  --post-cycle-release-preview-cycle "$POST_CYCLE_RELEASE_PREVIEW_CYCLE"
)

if [[ -n "$SCRAPE_TIMEOUT_SECONDS" ]]; then
  ARGS+=(--scrape-timeout-seconds "$SCRAPE_TIMEOUT_SECONDS")
fi

if [[ -n "$ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE" ]]; then
  ARGS+=(--admin-agentic-max-candidates-per-state "$ADMIN_AGENTIC_MAX_CANDIDATES_PER_STATE")
fi

if [[ -n "$ADMIN_AGENTIC_MAX_FETCH_PER_STATE" ]]; then
  ARGS+=(--admin-agentic-max-fetch-per-state "$ADMIN_AGENTIC_MAX_FETCH_PER_STATE")
fi

if [[ -n "$ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN" ]]; then
  ARGS+=(--admin-agentic-max-results-per-domain "$ADMIN_AGENTIC_MAX_RESULTS_PER_DOMAIN")
fi

if [[ -n "$ADMIN_AGENTIC_MAX_HOPS" ]]; then
  ARGS+=(--admin-agentic-max-hops "$ADMIN_AGENTIC_MAX_HOPS")
fi

if [[ -n "$ADMIN_AGENTIC_MAX_PAGES" ]]; then
  ARGS+=(--admin-agentic-max-pages "$ADMIN_AGENTIC_MAX_PAGES")
fi

if [[ -n "$ADMIN_AGENTIC_FETCH_CONCURRENCY" ]]; then
  ARGS+=(--admin-agentic-fetch-concurrency "$ADMIN_AGENTIC_FETCH_CONCURRENCY")
fi

if [[ -n "$ADMIN_PARALLEL_ASSIST_ENABLED" ]]; then
  if [[ "$ADMIN_PARALLEL_ASSIST_ENABLED" == "0" ]]; then
    ARGS+=(--no-admin-parallel-assist-enabled)
  else
    ARGS+=(--admin-parallel-assist-enabled)
  fi
fi

if [[ -n "$ADMIN_PARALLEL_ASSIST_STATE_LIMIT" ]]; then
  ARGS+=(--admin-parallel-assist-state-limit "$ADMIN_PARALLEL_ASSIST_STATE_LIMIT")
fi

if [[ -n "$ADMIN_PARALLEL_ASSIST_MAX_URLS_PER_DOMAIN" ]]; then
  ARGS+=(--admin-parallel-assist-max-urls-per-domain "$ADMIN_PARALLEL_ASSIST_MAX_URLS_PER_DOMAIN")
fi

if [[ -n "$ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS" ]]; then
  ARGS+=(--admin-parallel-assist-timeout-seconds "$ADMIN_PARALLEL_ASSIST_TIMEOUT_SECONDS")
fi

if [[ -n "$RANDOM_SEED" ]]; then
  ARGS+=(--random-seed "$RANDOM_SEED")
fi

if [[ -n "$OUTPUT_DIR" ]]; then
  ARGS+=(--output-dir "$OUTPUT_DIR")
fi

if [[ "$STOP_ON_TARGET_SCORE" == "1" ]]; then
  ARGS+=(--stop-on-target-score)
fi

if [[ "$PRINT_RELEASE_PLAN" == "1" ]]; then
  ARGS+=(--print-post-cycle-release-plan)
fi

if [[ "$POST_CYCLE_RELEASE" == "1" ]]; then
  ARGS+=(--post-cycle-release)
fi

if [[ "$POST_CYCLE_RELEASE_DRY_RUN" == "1" ]]; then
  ARGS+=(--post-cycle-release-dry-run)
fi

if [[ -n "$POST_CYCLE_RELEASE_MIN_SCORE" ]]; then
  ARGS+=(--post-cycle-release-min-score "$POST_CYCLE_RELEASE_MIN_SCORE")
fi

if [[ "$POST_CYCLE_RELEASE_IGNORE_PASS" == "1" ]]; then
  ARGS+=(--post-cycle-release-ignore-pass)
fi

if [[ -n "$POST_CYCLE_RELEASE_PUBLISH_COMMAND" ]]; then
  ARGS+=(--post-cycle-release-publish-command "$POST_CYCLE_RELEASE_PUBLISH_COMMAND")
fi

if [[ -n "$POST_CYCLE_RELEASE_PREVIEW_SCORE" ]]; then
  ARGS+=(--post-cycle-release-preview-score "$POST_CYCLE_RELEASE_PREVIEW_SCORE")
fi

cd "$PKG_DIR"

_stdout_capture=$(mktemp)
cleanup() {
  rm -f "$_stdout_capture"
}
trap cleanup EXIT

set +e
PYTHONPATH=src "$PYTHON_BIN" "${ARGS[@]}" "$@" | tee "$_stdout_capture"
_daemon_status=${PIPESTATUS[0]}
set -e

if [[ "$_daemon_status" -eq 0 && "$SUMMARIZE_PENDING_RETRY" == "1" ]]; then
  if command -v jq >/dev/null 2>&1; then
    _pending_provider=$(jq -r '.pending_retry.provider // empty' "$_stdout_capture" 2>/dev/null || true)
    if [[ -n "$_pending_provider" ]]; then
      _pending_retry_after=$(jq -r '.pending_retry.retry_after_seconds // empty' "$_stdout_capture" 2>/dev/null || true)
      _pending_retry_at=$(jq -r '.pending_retry.retry_at_utc // empty' "$_stdout_capture" 2>/dev/null || true)
      _pending_reason=$(jq -r '.pending_retry.reason // empty' "$_stdout_capture" 2>/dev/null || true)
      printf 'pending_retry scheduled: provider=%s retry_after_seconds=%s retry_at_utc=%s reason=%s\n' \
        "$_pending_provider" \
        "${_pending_retry_after:-unknown}" \
        "${_pending_retry_at:-unknown}" \
        "${_pending_reason:-unknown}" \
        >&2
    fi
  fi
fi

if [[ "$_daemon_status" -eq 0 && "$SUMMARIZE_TACTIC_SELECTION" == "1" ]]; then
  if command -v jq >/dev/null 2>&1; then
    _selected_tactic=$(jq -r '.latest_cycle.tactic_selection.selected_tactic // .tactic_selection.selected_tactic // empty' "$_stdout_capture" 2>/dev/null || true)
    if [[ -n "$_selected_tactic" ]]; then
      _selection_mode=$(jq -r '.latest_cycle.tactic_selection.mode // .tactic_selection.mode // empty' "$_stdout_capture" 2>/dev/null || true)
      _selection_priority_states=$(jq -r '(.latest_cycle.tactic_selection.priority_states // .tactic_selection.priority_states // []) | join(",")' "$_stdout_capture" 2>/dev/null || true)
      _selection_state_order=$(jq -r '(.latest_cycle.cycle_state_order // .cycle_state_order // []) | join(",")' "$_stdout_capture" 2>/dev/null || true)
      _stalled_document_recovery_states=$(jq -r '((.latest_cycle.critic.issues // .critic.issues // []) | map(select(type == "string" and startswith("document-recovery-stalled:"))) | map(split(":")[1] // "") | map(split(",")[]) | map(select(length > 0)) | unique | join(","))' "$_stdout_capture" 2>/dev/null || true)
      printf 'tactic_selection: selected=%s mode=%s priority_states=%s cycle_state_order=%s stalled_document_recovery_states=%s
' \
        "$_selected_tactic" \
        "${_selection_mode:-unknown}" \
        "${_selection_priority_states:-none}" \
        "${_selection_state_order:-unknown}" \
        "${_stalled_document_recovery_states:-none}" \
        >&2
    fi
  fi
fi

exit "$_daemon_status"