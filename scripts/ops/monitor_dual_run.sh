#!/bin/bash
# Monitor both scraping runs

echo "=========================================="
echo "Municipal Scrape - Dual Run Monitor"
echo "=========================================="
echo

echo "=== OPTION 1: No Expansion (Full Fallback Methods) ==="
echo "Workers: $(ps aux | grep 'out_no_expansion' | grep -v grep | wc -l)"
echo "Output: out_no_expansion/"
echo "Limit: 500 URLs per worker (2000 total)"
echo "Methods: Playwright → Wayback → Archive.is → Common Crawl → BeautifulSoup → Requests"
echo
echo "Recent activity:"
tail -20 out_no_expansion/logs/worker_0.log 2>/dev/null | grep -E "Assigned|scraping|SUCCESS|ERROR" | tail -5
echo

echo "=== OPTION 2: Local Index Expansion ==="
echo "Workers: $(ps aux | grep 'out_local_index' | grep -v grep | wc -l)"
echo "Output: out_local_index/"
echo "Limit: 200 URLs per worker (800 total)"
echo "Index: /tmp/cc_index/cdx-00000.gz ($(du -h /tmp/cc_index/cdx-00000.gz 2>/dev/null | cut -f1))"
echo "Mode: Full domain crawl using local CC index"
echo
echo "Recent activity:"
tail -20 out_local_index/logs/worker_0.log 2>/dev/null | grep -E "Assigned|Expanding|Reading local|yielded|SUCCESS|ERROR" | tail -5
echo

echo "=== Blob Counts ==="
echo "No expansion: $(ls -1 out_no_expansion/content_blobs/ 2>/dev/null | wc -l) blobs"
echo "Local index: $(ls -1 out_local_index/content_blobs/ 2>/dev/null | wc -l) blobs"
echo

echo "=== Monitoring Commands ==="
echo "tail -f out_no_expansion/logs/worker_0.log"
echo "tail -f out_local_index/logs/worker_0.log"
echo "watch -n 10 'ls -1 out_*/content_blobs/ | wc -l'"
