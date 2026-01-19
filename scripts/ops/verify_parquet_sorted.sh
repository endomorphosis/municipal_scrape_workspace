#!/bin/bash
# Quick verification script to check if parquet files are sorted

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-${REPO_ROOT}/.venv/bin/python}"

PARQUET_ROOT="${1:-/storage/ccindex_parquet}"

echo "Verifying sort status of parquet files in: $PARQUET_ROOT"
echo ""

if [[ -x "${VENV_PYTHON}" ]]; then
	:
elif command -v "${VENV_PYTHON}" >/dev/null 2>&1; then
	:
else
	echo "ERROR: Python interpreter not found: ${VENV_PYTHON}" >&2
	exit 1
fi

"${VENV_PYTHON}" "${REPO_ROOT}/validate_urlindex_sorted.py" --parquet-root "$PARQUET_ROOT" --verify-only
