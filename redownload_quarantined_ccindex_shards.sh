#!/usr/bin/env bash
set -euo pipefail

COLLECTION=${1:-CC-MAIN-2024-10}

BASE="https://data.commoncrawl.org/cc-index/collections/${COLLECTION}/indexes"
INDIR="/storage/ccindex/${COLLECTION}"
QDIR="${INDIR}/_corrupt_quarantine"

if [[ ! -d "$QDIR" ]]; then
  echo "Quarantine dir not found: $QDIR" >&2
  exit 1
fi

mapfile -t FILES < <(ls -1 "${QDIR}"/cdx-*.gz 2>/dev/null | sed 's#.*/##' | sort)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No quarantined shards found in: $QDIR"
  exit 0
fi

echo "collection=${COLLECTION}"
echo "quarantined_count=${#FILES[@]}"

download_one() {
  local f="$1"
  local url="${BASE}/${f}"
  local tmp="${INDIR}/${f}.part"
  local out="${INDIR}/${f}"

  echo "==> ${f}"

  # If a previous attempt left a partial .part, resume it.
  if [[ -f "$tmp" ]]; then
    echo "resuming partial: ${tmp}"
  else
    rm -f "$tmp"
  fi

  # Download into .part so we never leave a corrupt final file.
  curl -fL --silent --show-error \
    --retry 12 --retry-all-errors --retry-delay 2 \
    --connect-timeout 20 \
    -C - \
    -o "$tmp" \
    "$url"

  mv -f "$tmp" "$out"
  gzip -t "$out"
}

failures=0
for f in "${FILES[@]}"; do
  if ! download_one "$f"; then
    echo "FAILED: ${f}" >&2
    failures=$((failures+1))
  fi
done

echo "done failures=${failures}"
if [[ $failures -ne 0 ]]; then
  exit 2
fi
