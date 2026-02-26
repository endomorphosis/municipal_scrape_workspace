#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

echo "==================================================================="
echo "Validating CC-MAIN-2025-43 and CC-MAIN-2025-47"
echo "==================================================================="
echo ""

for COLL in CC-MAIN-2025-43 CC-MAIN-2025-47; do
    echo "--- $COLL ---"
    ./.venv/bin/python -m common_crawl_search_engine.ccindex.validate_collection_completeness \
      --collection "$COLL" \
      --ccindex-dir /home/barberb/ccindex_storage/ccindex \
      --parquet-dir /home/barberb/ccindex_storage/parquet \
      --pointer-dir /home/barberb/ccindex_storage/duckdb \
      --json
    echo ""
done

echo "==================================================================="
echo "Index files:"
echo "==================================================================="
ls -lah /home/barberb/ccindex_storage/duckdb/cc_pointers_by_collection/CC-MAIN-2025-{43,47}.duckdb* 2>/dev/null || true
echo ""
ls -lah /home/barberb/ccindex_storage/duckdb/cc_domain_rowgroups_by_collection/CC-MAIN-2025-{43,47}.domain_rowgroups.duckdb 2>/dev/null || true
