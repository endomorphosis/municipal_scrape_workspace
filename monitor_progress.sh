#!/bin/bash
# Monitor scraping progress for both option sets

echo "=== Scraping Progress Monitor ==="
date

echo -e "\n=== Running Workers ==="
ps aux | grep orchestrate_municipal_scrape | grep -v grep | wc -l
echo "workers running"

echo -e "\n=== Option 1: No Domain Expansion ==="
echo "Databases:"
du -sh out_no_expansion/state/*.duckdb 2>&1 | tail -1
echo "Content blobs:"
find out_no_expansion/content_blobs/ -type f 2>/dev/null | wc -l
echo "blobs"
echo "Latest log activity:"
tail -n 3 out_no_expansion/logs/worker_0.log | grep -E "INFO|Scraping|completed" || echo "Still initializing..."

echo -e "\n=== Option 2: Local Index Domain Expansion ==="
echo "Databases:"
du -sh out_option2_local/state/*.duckdb 2>&1 | tail -1
echo "Content blobs:"
find out_option2_local/content_blobs/ -type f 2>/dev/null | wc -l
echo "blobs"
echo "Latest log activity:"
tail -n 3 out_option2_local/logs/worker_0.log | grep -E "INFO|Scraping|completed|Searching" || echo "Still initializing..."

echo -e "\n=== Local CC Index Files ==="
echo "Index files available: $(ls /tmp/cc_indexes/CC-MAIN-2024-10/*.gz 2>/dev/null | wc -l)"
echo "Total size: $(du -sh /tmp/cc_indexes/CC-MAIN-2024-10/ 2>&1 | cut -f1)"
