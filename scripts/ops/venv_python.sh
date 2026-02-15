#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "ERROR: venv python not found at: $VENV_PY" >&2
  echo "Run ./bootstrap.sh first." >&2
  exit 1
fi

# Prefer the ipfs_datasets_py submodule implementation (not site-packages).
# This keeps dependencies in the venv, but code loaded from the submodule.
if [[ -d "$ROOT_DIR/ipfs_datasets_py/ipfs_datasets_py" ]]; then
  export PYTHONPATH="$ROOT_DIR/ipfs_datasets_py:${PYTHONPATH:-}"
fi

exec "$VENV_PY" "$@"
