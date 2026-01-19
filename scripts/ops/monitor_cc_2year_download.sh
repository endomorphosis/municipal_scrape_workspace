#!/bin/bash
# Monitor 2-year CC index download progress

echo "=== Common Crawl 2-Year Index Download Monitor ==="
echo "Start time: $(date)"
echo ""

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')]"
  
  # Show active downloads
  has_downloads=0
  for dir in /storage/ccindex/*/; do
    count=$(ls "$dir"*.gz 2>/dev/null | wc -l)
    if [ "$count" -gt 0 ]; then
      collection=$(basename "$dir")
      size=$(du -sh "$dir" | cut -f1)
      printf "  %-25s: %3d files (%8s)\n" "$collection" "$count" "$size"
      has_downloads=1
    fi
  done
  
  if [ "$has_downloads" -eq 0 ]; then
    echo "  No active downloads found"
  fi
  
  total=$(du -sh /storage/ccindex/ 2>/dev/null | cut -f1)
  printf "  %-25s: %s (total)\n" "TOTAL" "$total"
  
  # Check if main process is still running
  if ! ps aux | grep -q "download_cc_indexes_2years" | grep -v grep; then
    echo ""
    echo "Download process completed!"
    break
  fi
  
  sleep 30
  echo ""
done
