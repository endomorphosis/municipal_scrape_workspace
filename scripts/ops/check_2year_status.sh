#!/bin/bash
# Quick status check for 2-year Common Crawl download

echo "════════════════════════════════════════════════════════════"
echo "  2-YEAR COMMON CRAWL INDEX DOWNLOAD STATUS"
echo "  $(date)"
echo "════════════════════════════════════════════════════════════"
echo ""

# 22 collections for 2-year range
declare -a COLLECTIONS=(
  "CC-MAIN-2025-51" "CC-MAIN-2025-47" "CC-MAIN-2025-43" "CC-MAIN-2025-38"
  "CC-MAIN-2025-33" "CC-MAIN-2025-30" "CC-MAIN-2025-26" "CC-MAIN-2025-21"
  "CC-MAIN-2025-18" "CC-MAIN-2025-13" "CC-MAIN-2025-08" "CC-MAIN-2025-05"
  "CC-MAIN-2024-51" "CC-MAIN-2024-46" "CC-MAIN-2024-42" "CC-MAIN-2024-38"
  "CC-MAIN-2024-33" "CC-MAIN-2024-30" "CC-MAIN-2024-26" "CC-MAIN-2024-22"
  "CC-MAIN-2024-18" "CC-MAIN-2024-10"
)

echo "Collections Status:"
echo "─────────────────────────────────────────────────────────────"

downloaded=0
for coll in "${COLLECTIONS[@]}"; do
  count=$(ls /storage/ccindex/$coll/*.gz 2>/dev/null | wc -l)
  if [ "$count" -gt 0 ]; then
    size=$(du -sh /storage/ccindex/$coll 2>/dev/null | cut -f1)
    printf "  ✓ %-25s: %3d files | %8s\n" "$coll" "$count" "$size"
    ((downloaded++))
  else
    printf "  ⏳ %-25s: downloading...\n" "$coll"
  fi
done

echo ""
echo "Summary:"
echo "─────────────────────────────────────────────────────────────"
echo "  Collections complete: $downloaded/22"
echo "  Total storage: $(du -sh /storage/ccindex 2>/dev/null | cut -f1)"
echo ""

# Check both the script process and wget processes downloading 2024-2025 collections
if pgrep -f "download_cc_indexes_2years" > /dev/null || pgrep -f "wget.*CC-MAIN-202[45]" > /dev/null; then
  echo "  Status: ✅ DOWNLOAD ACTIVE"
  echo "  Log: tail -f /tmp/cc_2year_download.log"
  ACTIVE_WGET=$(ps aux | grep -c "wget.*CC-MAIN-202[45].*indexes" | grep -v grep || echo 0)
  echo "  Active wget processes: $ACTIVE_WGET"
else
  echo "  Status: ⏸️  DOWNLOAD INACTIVE"
fi

echo ""
echo "Disk Space:"
df -h /storage | tail -1 | awk '{printf "  Used: %s / %s (%.1f%%)\n", $3, $2, ($3/$2)*100}'

echo ""
echo "════════════════════════════════════════════════════════════"
