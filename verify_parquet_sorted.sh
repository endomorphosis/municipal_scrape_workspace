#!/bin/bash
# Quick verification script to check if parquet files are sorted

PARQUET_ROOT="${1:-/storage/ccindex_parquet}"

echo "Verifying sort status of parquet files in: $PARQUET_ROOT"
echo ""

python validate_urlindex_sorted.py --parquet-root "$PARQUET_ROOT" --verify-only
