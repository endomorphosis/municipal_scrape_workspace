#!/bin/bash
# Download Common Crawl indexes for past 1 year (2025)
# Stores in /storage/ccindex/<collection>/
# Estimated: 12 collections, up to ~3600 index files, ~10-12 TB total

set -euo pipefail

BASE_URL="https://data.commoncrawl.org/cc-index/collections"
STORAGE="/storage/ccindex"
LOG_FILE="/tmp/cc_1year_download.log"
PARALLEL_JOBS=${1:-8}

# Optional integrity check (reads entire .gz): set VERIFY_GZIP=1 to enable
VERIFY_GZIP=${VERIFY_GZIP:-0}

# Network retry knobs
RETRIES=${RETRIES:-8}
SLEEP_BASE_SECONDS=${SLEEP_BASE_SECONDS:-2}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

# Returns:
#   0 = downloaded OK
#   2 = 404 (file does not exist)
#   1 = other failure
download_one() {
  local url="$1"
  local dest="$2"
  local part="${dest}.part"
  local attempt=1

  rm -f "$part" 2>/dev/null || true

  while [ "$attempt" -le "$RETRIES" ]; do
    if have_cmd curl; then
      local code
      code=$(curl -L -sS -w '%{http_code}' -o "$part" "$url" || echo "000")
      if [ "$code" = "200" ]; then
        mv -f "$part" "$dest"
        if [ "$VERIFY_GZIP" = "1" ] && have_cmd gzip; then
          if ! gzip -t "$dest" >/dev/null 2>&1; then
            rm -f "$dest"
            attempt=$((attempt + 1))
            sleep $((SLEEP_BASE_SECONDS * attempt))
            continue
          fi
        fi
        return 0
      fi
      rm -f "$part" 2>/dev/null || true
      if [ "$code" = "404" ]; then
        return 2
      fi
    else
      if wget -q --timeout=120 --tries=1 "$url" -O "$part" 2>/dev/null; then
        mv -f "$part" "$dest"
        if [ "$VERIFY_GZIP" = "1" ] && have_cmd gzip; then
          if ! gzip -t "$dest" >/dev/null 2>&1; then
            rm -f "$dest"
            attempt=$((attempt + 1))
            sleep $((SLEEP_BASE_SECONDS * attempt))
            continue
          fi
        fi
        return 0
      fi
      rm -f "$part" 2>/dev/null || true
      if wget --spider -S "$url" 2>&1 | grep -q " 404 Not Found"; then
        return 2
      fi
    fi

    attempt=$((attempt + 1))
    sleep $((SLEEP_BASE_SECONDS * attempt))
  done

  rm -f "$part" 2>/dev/null || true
  return 1
}

{
  echo "=== Common Crawl 1-Year Index Download (2025) ==="
  echo "Start: $(date)"
  echo "Target: /storage/ccindex/"
  echo "Parallel jobs: $PARALLEL_JOBS"
  echo ""
  
  # 12 collections for 1-year coverage (Dec 2024 - Dec 2025, 2025 only)
  # Generated from official collinfo.json
  COLLECTIONS=(
    "CC-MAIN-2025-51" "CC-MAIN-2025-47" "CC-MAIN-2025-43" "CC-MAIN-2025-38"
    "CC-MAIN-2025-33" "CC-MAIN-2025-30" "CC-MAIN-2025-26" "CC-MAIN-2025-21"
    "CC-MAIN-2025-18" "CC-MAIN-2025-13" "CC-MAIN-2025-08" "CC-MAIN-2025-05"
  )
  
  echo "Collections to download: ${#COLLECTIONS[@]}"
  echo "Expected size: ~10-12 TB total"
  echo ""
  
  download_collection() {
    local collection=$1
    local collection_dir="$STORAGE/$collection"
    
    mkdir -p "$collection_dir"
    
    echo "[$(date +%H:%M:%S)] ⬇ $collection: resuming/starting download"
    
    echo "[$(date +%H:%M:%S)] ⬇ $collection: starting download"
    
    # Download all indexes for this collection (up to 300)
    for i in $(seq 0 299); do
      local file="cdx-$(printf "%05d" $i).gz"
      local filepath="$collection_dir/$file"
      local url="$BASE_URL/$collection/indexes/$file"

      if [ -f "$filepath" ]; then
        if [ "$VERIFY_GZIP" = "1" ] && have_cmd gzip; then
          if ! gzip -t "$filepath" >/dev/null 2>&1; then
            rm -f "$filepath"
          else
            continue
          fi
        else
          continue
        fi
      fi

      if download_one "$url" "$filepath"; then
        :
      else
        rc=$?
        if [ "$rc" -eq 2 ]; then
          break
        fi
        echo "[$(date +%H:%M:%S)] ! $collection: failed $file after retries (will retry on next run)" >&2
        continue
      fi
    done
    
    local count=$(ls "$collection_dir"/*.gz 2>/dev/null | wc -l)
    local size=$(du -sh "$collection_dir" | cut -f1)
    echo "[$(date +%H:%M:%S)] ✓ $collection: downloaded $count files ($size)"
  }
  
  export -f download_collection
  export -f download_one have_cmd
  export STORAGE BASE_URL VERIFY_GZIP RETRIES SLEEP_BASE_SECONDS
  
  # Download all collections in parallel
  printf '%s\n' "${COLLECTIONS[@]}" | xargs -P "$PARALLEL_JOBS" -I {} bash -c 'download_collection "$@"' _ {}
  
  echo ""
  echo "=== Summary ==="
  for collection in "${COLLECTIONS[@]}"; do
    count=$(ls "$STORAGE/$collection"/*.gz 2>/dev/null | wc -l)
    if [ "$count" -gt 0 ]; then
      size=$(du -sh "$STORAGE/$collection" | cut -f1)
      echo "✓ $collection: $count files ($size)"
    fi
  done
  
  total_size=$(du -sh "$STORAGE" | cut -f1)
  echo ""
  echo "Total storage used: $total_size"
  echo "End: $(date)"
  
} 2>&1 | tee "$LOG_FILE"

echo ""
echo "Log saved to: $LOG_FILE"
