#!/bin/bash
# Quick status check for Common Crawl 5-year index download
# Usage: bash check_cc_download_status.sh

echo "════════════════════════════════════════════════════════════════"
echo "COMMON CRAWL 5-YEAR INDEX DOWNLOAD STATUS"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Current time: $(date)"
echo ""

# Expected 42 collections
EXPECTED_COLLECTIONS=42

# Count what we have
TOTAL_FILES=$(find /storage/ccindex -name "*.gz" 2>/dev/null | wc -l)
TOTAL_SIZE=$(du -sh /storage/ccindex 2>/dev/null | cut -f1)
COLLECTIONS_WITH_FILES=$(find /storage/ccindex -mindepth 2 -name "*.gz" 2>/dev/null | cut -d'/' -f5 | sort -u | wc -l)

echo "Overall Progress:"
echo "─────────────────────────────────────────────────────────────────"
echo "Total files downloaded: $TOTAL_FILES"
echo "Total storage used: $TOTAL_SIZE"
echo "Collections with content: $COLLECTIONS_WITH_FILES / $EXPECTED_COLLECTIONS"
echo ""

# Show top collections by size
echo "Top 10 Collections by Size:"
echo "─────────────────────────────────────────────────────────────────"
for dir in /storage/ccindex/CC-MAIN-*/; do
    count=$(ls "$dir"*.gz 2>/dev/null | wc -l)
    if [ "$count" -gt 0 ]; then
        size=$(du -sh "$dir" | cut -f1)
        printf "  %-25s: %3d files | %8s\n" "$(basename $dir)" "$count" "$size"
    fi
done | sort -t'|' -k3 -hr | head -10

echo ""
echo "Status by Year:"
echo "─────────────────────────────────────────────────────────────────"

for year in 2025 2024 2023 2022 2021; do
    count=$(find /storage/ccindex/CC-MAIN-${year}-* -name "*.gz" 2>/dev/null | wc -l)
    collections=$(ls -d /storage/ccindex/CC-MAIN-${year}-*/ 2>/dev/null | wc -l)
    
    if [ "$collections" -gt 0 ]; then
        size=$(du -sh /storage/ccindex/CC-MAIN-${year}-*/ 2>/dev/null | awk '{s+=$1} END {print s}' || echo "0")
        printf "  %d: %3d files across %2d collections\n" "$year" "$count" "$collections"
    fi
done

echo ""
echo "Download Process Status:"
echo "─────────────────────────────────────────────────────────────────"

if pgrep -f "download_cc_indexes_5years" > /dev/null 2>&1; then
    pid=$(pgrep -f "download_cc_indexes_5years" | head -1)
    echo "  Status: ✅ ACTIVE (PID: $pid)"
    echo "  Log: /tmp/cc_5year_download.log"
else
    echo "  Status: ⏸️  INACTIVE"
    echo "  To resume: bash download_cc_indexes_5years.sh 12"
fi

echo ""
echo "════════════════════════════════════════════════════════════════"

# Quick estimate
echo ""
echo "Completion Estimate:"
echo "─────────────────────────────────────────────────────────────────"
downloaded_gb=$(du -sh /storage/ccindex 2>/dev/null | cut -f1 | sed 's/G//' | sed 's/T/0/')
echo "  Downloaded: ~$downloaded_gb GB / ~50-60 TB expected"
echo "  Progress: ~$(echo "scale=1; $downloaded_gb / 500" | bc)%"
echo ""
echo "Note: Download continues in background automatically."
echo "════════════════════════════════════════════════════════════════"
