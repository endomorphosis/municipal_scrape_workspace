#!/bin/bash
# Download Common Crawl indexes for past 5 years
# Stores in /storage/ccindex/<collection>/

set -euo pipefail

BASE_URL="https://data.commoncrawl.org/cc-index/collections"
STORAGE="/storage/ccindex"
LOG_FILE="/tmp/cc_5year_download.log"
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
  echo "=== Common Crawl 5-Year Index Download ==="
  echo "Start: $(date)"
  echo "Target: /storage/ccindex/"
  echo "Parallel jobs: $PARALLEL_JOBS"
  echo ""
  
  # Generate list of collections for past 5 years (roughly 2021-01 to 2025-12)
  # Generated from official collinfo.json from index.commoncrawl.org/collinfo.json
  # Total: 42 collections with ~6000+ index files covering 5 years
  COLLECTIONS=(
    "CC-MAIN-2025-51" "CC-MAIN-2025-47" "CC-MAIN-2025-43" "CC-MAIN-2025-38"
    "CC-MAIN-2025-33" "CC-MAIN-2025-30" "CC-MAIN-2025-26" "CC-MAIN-2025-21"
    "CC-MAIN-2025-18" "CC-MAIN-2025-13" "CC-MAIN-2025-08" "CC-MAIN-2025-05"
    "CC-MAIN-2024-51" "CC-MAIN-2024-46" "CC-MAIN-2024-42" "CC-MAIN-2024-38"
    "CC-MAIN-2024-33" "CC-MAIN-2024-30" "CC-MAIN-2024-26" "CC-MAIN-2024-22"
    "CC-MAIN-2024-18" "CC-MAIN-2024-10"
    "CC-MAIN-2023-50" "CC-MAIN-2023-40" "CC-MAIN-2023-23" "CC-MAIN-2023-14"
    "CC-MAIN-2023-06"
    "CC-MAIN-2022-49" "CC-MAIN-2022-40" "CC-MAIN-2022-33" "CC-MAIN-2022-27"
    "CC-MAIN-2022-21" "CC-MAIN-2022-05"
    "CC-MAIN-2021-49" "CC-MAIN-2021-43" "CC-MAIN-2021-39" "CC-MAIN-2021-31"
    "CC-MAIN-2021-25" "CC-MAIN-2021-21" "CC-MAIN-2021-17" "CC-MAIN-2021-10"
    "CC-MAIN-2021-04"
  )
  
  download_collection() {
    local collection=$1
    local collection_dir="$STORAGE/$collection"
    
    mkdir -p "$collection_dir"
    
    echo "[$(date +%H:%M:%S)] ⬇ $collection: resuming/starting download"
    
    # Try to fetch the index file list
    local index_url="$BASE_URL/$collection/indexes/cdx-00000.gz"
    
    # Test if collection exists
    if ! wget --spider -q "$index_url" 2>/dev/null; then
      echo "[$(date +%H:%M:%S)] ✗ $collection: not found or inaccessible (skipping)"
      return 1
    fi
    
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
  echo "Total storage: $total_size"
  echo "Complete: $(date)"
  
} 2>&1 | tee -a "$LOG_FILE"

echo ""
echo "Log saved to: $LOG_FILE"
