#!/bin/bash
# Common Crawl 2-Year Index Management
# Focus: Manage 2-year rolling window (Dec 2023 - Dec 2025)
# Status: Current network to CC appears down - managing existing downloads

STORAGE="/storage/ccindex"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║  Common Crawl 2-Year Index Management                      ║"
echo "║  Status as of $(date +%Y-%m-%d)                           ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Current 2-year required collections (Dec 2023 - Dec 2025)
declare -a TWO_YEAR_COLLECTIONS=(
  "CC-MAIN-2025-51" "CC-MAIN-2025-47" "CC-MAIN-2025-43" "CC-MAIN-2025-38"
  "CC-MAIN-2025-33" "CC-MAIN-2025-30" "CC-MAIN-2025-26" "CC-MAIN-2025-21"
  "CC-MAIN-2025-18" "CC-MAIN-2025-13" "CC-MAIN-2025-08" "CC-MAIN-2025-05"
  "CC-MAIN-2024-51" "CC-MAIN-2024-46" "CC-MAIN-2024-42" "CC-MAIN-2024-38"
  "CC-MAIN-2024-33" "CC-MAIN-2024-30" "CC-MAIN-2024-26" "CC-MAIN-2024-22"
  "CC-MAIN-2024-18" "CC-MAIN-2024-10"
)

echo "EXPECTED 2-YEAR COLLECTIONS (22 total):"
echo "─────────────────────────────────────────────────────────────"
echo ""
echo "Current Status:"
echo ""

have_2year=0
for coll in "${TWO_YEAR_COLLECTIONS[@]}"; do
  if [ -d "$STORAGE/$coll" ] && [ "$(ls $STORAGE/$coll/*.gz 2>/dev/null | wc -l)" -gt 0 ]; then
    count=$(ls "$STORAGE/$coll"/*.gz 2>/dev/null | wc -l)
    size=$(du -sh "$STORAGE/$coll" 2>/dev/null | cut -f1)
    printf "  ✓ %-25s: %3d files | %8s\n" "$coll" "$count" "$size"
    ((have_2year++))
  else
    printf "  ✗ %-25s: MISSING\n" "$coll"
  fi
done

echo ""
echo "2-Year Coverage: $have_2year/22 collections"
echo ""

# What we actually have
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ACTUAL CURRENT INVENTORY:"
echo "─────────────────────────────────────────────────────────────"
echo ""

declare -A year_stats

for dir in "$STORAGE"/CC-MAIN-*/; do
  coll=$(basename "$dir")
  files=$(ls "$dir"*.gz 2>/dev/null | wc -l)
  if [ "$files" -gt 0 ]; then
    size=$(du -sh "$dir" | cut -f1)
    year="${coll##CC-MAIN-}"
    year="${year%-*}"
    
    if [ -z "${year_stats[$year]}" ]; then
      year_stats[$year]="$files:$size"
    else
      year_stats[$year]="${year_stats[$year]},$files:$size"
    fi
    
    printf "  %-30s: %3d files | %8s\n" "$coll" "$files" "$size"
  fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "SUMMARY BY YEAR:"
echo "─────────────────────────────────────────────────────────────"

total_files=$(find "$STORAGE" -name "*.gz" 2>/dev/null | wc -l)
total_size=$(du -sh "$STORAGE" 2>/dev/null | cut -f1)

for year in 2025 2024 2023 2022 2021 2020; do
  files=$(find "$STORAGE"/CC-MAIN-${year}-* -name "*.gz" 2>/dev/null | wc -l)
  if [ "$files" -gt 0 ]; then
    size=$(du -sh "$STORAGE"/CC-MAIN-${year}-*/ 2>/dev/null | tail -1 | cut -f1)
    colls=$(ls -d "$STORAGE"/CC-MAIN-${year}-*/ 2>/dev/null | wc -l)
    printf "  %d: %4d files across %2d collections (%8s)\n" "$year" "$files" "$colls" "$size"
  fi
done

echo ""
echo "TOTAL: $total_files files, $total_size"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DISK SPACE:"
echo "─────────────────────────────────────────────────────────────"
df -h /storage | tail -1 | awk '{printf "  Used: %s / %s (%.1f%% full)\n", $3, $2, ($3/$2)*100}'
echo ""

# Analysis
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ANALYSIS & RECOMMENDATIONS:"
echo "─────────────────────────────────────────────────────────━━━━━"
echo ""

if [ $have_2year -ge 20 ]; then
  echo "✓ EXCELLENT: You have most 2-year collections!"
  echo "  Focus: Ensure missing recent 2025 collections are downloaded"
elif [ $have_2year -ge 15 ]; then
  echo "✓ GOOD: You have substantial 2-year coverage"
  echo "  Focus: Fill in missing recent months"
else
  echo "⚠ PARTIAL: Only partial 2-year coverage"
  echo "  Recommendation: Prioritize recent collections (2025, 2024)"
fi

echo ""
echo "Storage: $total_size used of 12TB available"
used_pct=$(du -sh "$STORAGE" 2>/dev/null | cut -f1 | sed 's/G//' | awk '{printf "%.0f", ($1/12000)*100}')
echo "Percentage: ~${used_pct}% of total storage"

if [ $used_pct -lt 30 ]; then
  echo "Status: ✓ SAFE - Plenty of room for additional downloads"
elif [ $used_pct -lt 60 ]; then
  echo "Status: ⚠ CAUTION - Monitor disk usage"
else
  echo "Status: ⚠ WARNING - Getting full, prioritize cleanup"
fi

echo ""
echo "Network Status:"
if curl -s --connect-timeout 5 "https://index.commoncrawl.org/" > /dev/null 2>&1; then
  echo "  ✓ Common Crawl online - downloads can resume"
else
  echo "  ✗ Common Crawl offline - use cached data only"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Script completed: $(date)"
