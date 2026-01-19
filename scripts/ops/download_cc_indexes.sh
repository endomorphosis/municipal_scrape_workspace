#!/bin/bash
# Download all Common Crawl index segments for a collection
# Usage: ./download_cc_indexes.sh [collection] [output_dir] [parallel_jobs]

set -e

COLLECTION="${1:-CC-MAIN-2024-10}"
OUTPUT_DIR="${2:-/tmp/cc_indexes/$COLLECTION}"
PARALLEL_JOBS="${3:-8}"

echo "=== Common Crawl Index Downloader ==="
echo "Collection: $COLLECTION"
echo "Output: $OUTPUT_DIR"
echo "Parallel: $PARALLEL_JOBS jobs"
echo

mkdir -p "$OUTPUT_DIR"

# Fetch the list of all index files for this collection
INDEX_LIST_URL="https://data.commoncrawl.org/crawl-data/$COLLECTION/cc-index.paths.gz"
echo "Fetching index file list from: $INDEX_LIST_URL"

# Download and parse the list
curl -sS "$INDEX_LIST_URL" | gunzip | grep "\.gz$" > "$OUTPUT_DIR/index_files.txt"

TOTAL_FILES=$(wc -l < "$OUTPUT_DIR/index_files.txt")
echo "Found $TOTAL_FILES index files"
echo

# Function to download one file
download_one() {
    local path="$1"
    local output_dir="$2"
    local filename=$(basename "$path")
    local url="https://data.commoncrawl.org/$path"
    
    if [ -f "$output_dir/$filename" ]; then
        echo "[SKIP] $filename (already exists)"
        return 0
    fi
    
    echo "[DOWN] $filename"
    wget -q -O "$output_dir/$filename" "$url" 2>&1 | grep -v "^$" || true
    
    if [ $? -eq 0 ]; then
        echo "[DONE] $filename ($(du -h "$output_dir/$filename" | cut -f1))"
    else
        echo "[FAIL] $filename"
        rm -f "$output_dir/$filename"
    fi
}

export -f download_one

# Download in parallel using xargs
echo "Starting parallel download with $PARALLEL_JOBS jobs..."
echo "This will take a while (each file is ~30-100MB, total ~10-50GB)"
echo

cat "$OUTPUT_DIR/index_files.txt" | \
    xargs -I {} -P "$PARALLEL_JOBS" bash -c "download_one '{}' '$OUTPUT_DIR'"

echo
echo "=== Download Complete ==="
DOWNLOADED=$(find "$OUTPUT_DIR" -name "*.gz" -type f | wc -l)
echo "Downloaded: $DOWNLOADED / $TOTAL_FILES files"
du -sh "$OUTPUT_DIR"
echo
echo "To use with the scraper:"
echo "  --cc-local-index-file $OUTPUT_DIR"
