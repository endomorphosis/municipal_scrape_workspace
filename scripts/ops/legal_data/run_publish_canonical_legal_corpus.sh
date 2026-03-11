#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
ROOT_VENV_PYTHON="$ROOT_DIR/.venv/bin/python"

_PRESERVE_ENV_VARS=(
  LEGAL_PUBLISH_CORPUS
  LEGAL_PUBLISH_LOCAL_DIR
  LEGAL_PUBLISH_REPO_ID
  LEGAL_PUBLISH_PATH_IN_REPO
  LEGAL_PUBLISH_TOKEN
  LEGAL_PUBLISH_COMMIT_MESSAGE
  LEGAL_PUBLISH_CID_COLUMN
  LEGAL_PUBLISH_CREATE_REPO
  LEGAL_PUBLISH_VERIFY
  LEGAL_PUBLISH_DRY_RUN
  LEGAL_PUBLISH_PYTHON_BIN
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

PYTHON_BIN=${LEGAL_PUBLISH_PYTHON_BIN:-}
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$ROOT_VENV_PYTHON" ]]; then
    PYTHON_BIN="$ROOT_VENV_PYTHON"
  else
    PYTHON_BIN=$(command -v python3)
  fi
fi

CORPUS=${LEGAL_PUBLISH_CORPUS:-state_laws}
LOCAL_DIR=${LEGAL_PUBLISH_LOCAL_DIR:-}
REPO_ID=${LEGAL_PUBLISH_REPO_ID:-}
PATH_IN_REPO=${LEGAL_PUBLISH_PATH_IN_REPO:-}
TOKEN=${LEGAL_PUBLISH_TOKEN:-}
COMMIT_MESSAGE=${LEGAL_PUBLISH_COMMIT_MESSAGE:-}
CID_COLUMN=${LEGAL_PUBLISH_CID_COLUMN:-}
CREATE_REPO=${LEGAL_PUBLISH_CREATE_REPO:-0}
VERIFY=${LEGAL_PUBLISH_VERIFY:-0}
DRY_RUN=${LEGAL_PUBLISH_DRY_RUN:-1}

ARGS=(
  "$ROOT_DIR/scripts/ops/legal_data/publish_canonical_legal_corpus_to_hf.py"
  --corpus "$CORPUS"
)

if [[ -n "$LOCAL_DIR" ]]; then
  ARGS+=(--local-dir "$LOCAL_DIR")
fi

if [[ -n "$REPO_ID" ]]; then
  ARGS+=(--repo-id "$REPO_ID")
fi

if [[ -n "$PATH_IN_REPO" ]]; then
  ARGS+=(--path-in-repo "$PATH_IN_REPO")
fi

if [[ -n "$TOKEN" ]]; then
  ARGS+=(--token "$TOKEN")
fi

if [[ -n "$COMMIT_MESSAGE" ]]; then
  ARGS+=(--commit-message "$COMMIT_MESSAGE")
fi

if [[ -n "$CID_COLUMN" ]]; then
  ARGS+=(--cid-column "$CID_COLUMN")
fi

if [[ "$CREATE_REPO" == "1" ]]; then
  ARGS+=(--create-repo)
fi

if [[ "$VERIFY" == "1" ]]; then
  ARGS+=(--verify)
fi

if [[ "$DRY_RUN" == "1" ]]; then
  ARGS+=(--dry-run)
fi

cd "$ROOT_DIR"
exec "$PYTHON_BIN" "${ARGS[@]}" "$@"