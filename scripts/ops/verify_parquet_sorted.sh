#!/bin/bash
# Quick verification script to check if parquet files are sorted

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PARQUET_ROOT="${1:-/storage/ccindex_parquet}"

echo "Verifying sort status of parquet files in: $PARQUET_ROOT"
echo ""

python3 "${REPO_ROOT}/validate_urlindex_sorted.py" --parquet-root "$PARQUET_ROOT" --verify-only
