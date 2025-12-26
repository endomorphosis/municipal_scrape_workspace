#!/bin/bash
# Download Common Crawl indexes for past 2 years in background
# Stores in /storage/ccindex/<collection>/

set -e

BASE_URL="https://data.commoncrawl.org/cc-index/collections"
STORAGE="/storage/ccindex"
LOG_FILE="/tmp/cc_2year_download.log"
PARALLEL_JOBS=${1:-8}

{
  echo "=== Common Crawl 2-Year Index Download ==="
  echo "Start: $(date)"
  echo "Target: /storage/ccindex/"
  echo "Parallel jobs: $PARALLEL_JOBS"
  echo ""
  
  # Generate list of collections for past 5 years (Dec 2020 - Dec 2025)
  COLLECTIONS=(
    "CC-MAIN-2020-12"
    "CC-MAIN-2021-01" "CC-MAIN-2021-02" "CC-MAIN-2021-03" "CC-MAIN-2021-04"
    "CC-MAIN-2021-05" "CC-MAIN-2021-06" "CC-MAIN-2021-07" "CC-MAIN-2021-08"
    "CC-MAIN-2021-09" "CC-MAIN-2021-10" "CC-MAIN-2021-11" "CC-MAIN-2021-12"
    "CC-MAIN-2022-01" "CC-MAIN-2022-02" "CC-MAIN-2022-03" "CC-MAIN-2022-04"
    "CC-MAIN-2022-05" "CC-MAIN-2022-06" "CC-MAIN-2022-07" "CC-MAIN-2022-08"
    "CC-MAIN-2022-09" "CC-MAIN-2022-10" "CC-MAIN-2022-11" "CC-MAIN-2022-12"
    "CC-MAIN-2023-01" "CC-MAIN-2023-02" "CC-MAIN-2023-03" "CC-MAIN-2023-04"
    "CC-MAIN-2023-05" "CC-MAIN-2023-06" "CC-MAIN-2023-07" "CC-MAIN-2023-08"
    "CC-MAIN-2023-09" "CC-MAIN-2023-10" "CC-MAIN-2023-11" "CC-MAIN-2023-12"
    "CC-MAIN-2024-01" "CC-MAIN-2024-02" "CC-MAIN-2024-03" "CC-MAIN-2024-04"
    "CC-MAIN-2024-05" "CC-MAIN-2024-06" "CC-MAIN-2024-07" "CC-MAIN-2024-08"
    "CC-MAIN-2024-09" "CC-MAIN-2024-10" "CC-MAIN-2024-11" "CC-MAIN-2024-12"
    "CC-MAIN-2025-01" "CC-MAIN-2025-02" "CC-MAIN-2025-03" "CC-MAIN-2025-04"
    "CC-MAIN-2025-05" "CC-MAIN-2025-06" "CC-MAIN-2025-07" "CC-MAIN-2025-08"
    "CC-MAIN-2025-09" "CC-MAIN-2025-10" "CC-MAIN-2025-11" "CC-MAIN-2025-12"
  )
  
  download_collection() {
    local collection=$1
    local collection_dir="$STORAGE/$collection"
    
    mkdir -p "$collection_dir"
    
    # Skip if already exists and has files
    if [ "$(ls "$collection_dir"/*.gz 2>/dev/null | wc -l)" -gt 0 ]; then
      local count=$(ls "$collection_dir"/*.gz 2>/dev/null | wc -l)
      echo "[$(date +%H:%M:%S)] ✓ $collection: $count files already present"
      return 0
    fi
    
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
  echo "Total storage: $total_size"
  echo "Complete: $(date)"
  
} | tee -a "$LOG_FILE"
