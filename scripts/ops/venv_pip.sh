#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_PIP="$ROOT_DIR/.venv/bin/pip"

if [[ ! -x "$VENV_PIP" ]]; then
  echo "ERROR: venv pip not found at: $VENV_PIP" >&2
  echo "Run ./bootstrap.sh first." >&2
  exit 1
fi

exec "$VENV_PIP" "$@"
