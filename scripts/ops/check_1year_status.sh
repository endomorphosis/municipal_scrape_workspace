#!/bin/bash
# Monitor 1-year Common Crawl index download progress

COLLECTIONS=(
  "CC-MAIN-2025-51" "CC-MAIN-2025-47" "CC-MAIN-2025-43" "CC-MAIN-2025-38"
  "CC-MAIN-2025-33" "CC-MAIN-2025-30" "CC-MAIN-2025-26" "CC-MAIN-2025-21"
  "CC-MAIN-2025-18" "CC-MAIN-2025-13" "CC-MAIN-2025-08" "CC-MAIN-2025-05"
)

echo "════════════════════════════════════════════════════════════"
echo "  1-YEAR (2025) COMMON CRAWL INDEX DOWNLOAD STATUS"
echo "  $(date)"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Collections Status:"
echo "─────────────────────────────────────────────────────────────"

completed=0
total_files=0
total_size=0

for coll in "${COLLECTIONS[@]}"; do
  count=$(find /storage/ccindex/$coll -name "*.gz" 2>/dev/null | wc -l)
  size=$(du -sh /storage/ccindex/$coll 2>/dev/null | awk '{print $1}')
  remaining=$((300-count))
  
  # Calculate percentage
  percent=$((count * 100 / 300))
  
  # Create progress bar
  bar=""
  for ((i=0; i<percent; i+=10)); do
    bar="$bar█"
  done
  for ((i=percent; i<100; i+=10)); do
    bar="$bar░"
  done
  
  if [ "$count" -eq 300 ]; then
    echo "  ✓ $coll : $count files | $size | [$bar] 100% ✅"
    ((completed++))
  else
    printf "  ⬇ %-20s : %3d files | %8s | [%-10s] %3d%%\n" "$coll" "$count" "${size:-0}" "$bar" "$percent"
  fi
  
  total_files=$((total_files + count))
done

echo ""
echo "Summary:"
echo "─────────────────────────────────────────────────────────────"
echo "  Collections complete: $completed/12"
echo "  Total files downloaded: $total_files / 3600"
echo "  Completion: $(( total_files * 100 / 3600 ))%"
echo ""

# Check active download processes
active=$(ps aux | grep -c "wget.*CC-MAIN-2025" || true)
if [ "$active" -gt 0 ]; then
  echo "  Status: 🔄  DOWNLOAD ACTIVE ($((active-1)) wget processes)"
  echo "  Log: tail -f /tmp/cc_1year_download.log"
else
  echo "  Status: ⏸️  DOWNLOAD INACTIVE"
fi

echo ""
echo "Disk Space:"
echo "─────────────────────────────────────────────────────────────"
df -h /storage | tail -1 | awk '{printf "  Used: %s / %s (%s)\n", $3, $2, $5}'

echo ""
echo "════════════════════════════════════════════════════════════"
