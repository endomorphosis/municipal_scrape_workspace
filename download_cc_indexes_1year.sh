#!/bin/bash
# Download Common Crawl indexes for past 1 year (2025)
# Stores in /storage/ccindex/<collection>/
# Estimated: 12 collections, ~1500+ index files, ~10-12 TB total

set -e

BASE_URL="https://data.commoncrawl.org/cc-index/collections"
STORAGE="/storage/ccindex"
LOG_FILE="/tmp/cc_1year_download.log"
PARALLEL_JOBS=${1:-8}

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
      
      if [ ! -f "$filepath" ]; then
        wget --tries=2 --timeout=120 -q "$BASE_URL/$collection/indexes/$file" -O "$filepath" 2>/dev/null || {
          rm -f "$filepath"  # Remove partial downloads
          break  # Stop at first 404
        }
      fi
    done
    
    local count=$(ls "$collection_dir"/*.gz 2>/dev/null | wc -l)
    local size=$(du -sh "$collection_dir" | cut -f1)
    echo "[$(date +%H:%M:%S)] ✓ $collection: downloaded $count files ($size)"
  }
  
  export -f download_collection
  export STORAGE BASE_URL
  
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
