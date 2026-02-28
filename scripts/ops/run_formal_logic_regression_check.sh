#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

REPORT_PATH="artifacts/formal_logic_tmp_verify/federal/report.json"
BASELINE_PATH="artifacts/formal_logic_tmp_verify/federal/report.pre_phase1_cleanup.json"
RECORDS_PATH="artifacts/formal_logic_tmp_verify/federal/records.jsonl"
LOGIC_PATH="artifacts/formal_logic_tmp_verify/federal/logic.jsonld"

if [[ $# -ge 1 ]]; then
  BASELINE_PATH="$1"
fi

echo "[1/2] Running formal logic conversion benchmark..."
PYTHONPATH=src:ipfs_datasets_py .venv/bin/python scripts/ops/convert_legal_corpus_to_formal_logic.py \
  --input data/federal_laws/us_constitution.jsonld \
  --limit-segments 50 \
  --enable-clause-decomposition \
  --enable-tdfol \
  --enable-cec \
  --enable-semantic-roundtrip \
  --embedding-backend sentence-transformers \
  --strict-embedding-backend \
  --enable-focused-retry-optimizer \
  --enable-encoder-quality-retry \
  --enable-fragment-merging \
  --enable-llm-kg-enrichment \
  --llm-kg-enrichment-max-records 5 \
  --enable-llm-decoder-pass \
  --llm-decoder-pass-max-records 8 \
  --llm-decoder-pass-min-semantic-gain -0.2 \
  --llm-decoder-pass-min-semantic-floor 0.25 \
  --llm-decoder-pass-min-overlap 0.45 \
  --exclude-heading-segments-from-semantic-metrics \
  --output-json "$REPORT_PATH" \
  --output-jsonl "$RECORDS_PATH" \
  --output-logic-jsonld "$LOGIC_PATH"

echo "[2/2] Running low-tail delta analysis..."
if [[ -f "$BASELINE_PATH" ]]; then
  .venv/bin/python scripts/ops/analyze_formal_logic_low_tail.py \
    --report "$REPORT_PATH" \
    --baseline "$BASELINE_PATH" \
    --top-k 12 \
    --show-worst 8
else
  echo "Baseline not found at: $BASELINE_PATH"
  .venv/bin/python scripts/ops/analyze_formal_logic_low_tail.py \
    --report "$REPORT_PATH" \
    --top-k 12 \
    --show-worst 8
fi
