# Common Crawl Integration Guide

This document explains the multiple methods for querying Common Crawl indexes to expand domain coverage during municipal website scraping.

## Overview

When `--full-domain-crawl` is enabled, the orchestrator can discover all archived URLs for each domain using several methods (tried in order):

1. **Local Index File** (fastest, no network)
2. **cdx_toolkit** (polite Python API wrapper)
3. **index.commoncrawl.org** (official recommended API)
4. **cdx.commoncrawl.org** (legacy fallback)

## Methods

### 1. Local Index Files (Recommended for Large-Scale)

Download index segments from `data.commoncrawl.org` and filter locally. This is the fastest method and avoids rate limits.

#### Download a Recent Index

```bash
# List available crawls
curl -sS https://data.commoncrawl.org/crawl-data/index.html

# Example: Download the January 2024 index (cluster.idx file or cdx files)
# CC-MAIN-2024-10 index segments are at:
# https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2024-10/indexes/

# Download a small segment (these are huge; start with one file)
wget https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2024-10/indexes/cdx-00000.gz
```

#### Use Local Index

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
  --csv us_towns_and_counties_urls.csv \
  --out out_local_index \
  --num-workers 4 --worker-id 0 \
  --full-domain-crawl \
  --cc-local-index-file /path/to/cdx-00000.gz \
  --limit 50
```

**Benefits:**
- No network calls to Common Crawl API
- No rate limits
- Very fast (local gzip decompression + grep-like filtering)
- Good for repeated runs or large-scale processing

**Drawbacks:**
- Requires downloading large files (10s of GB per segment)
- Index may be slightly stale (monthly snapshots)

---

### 2. cdx_toolkit (Recommended for Moderate Use)

Python library that wraps the Common Crawl and Wayback Machine APIs with built-in politeness and retry logic.

#### Install

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/pip install cdx_toolkit
```

#### Use cdx_toolkit

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
  --csv us_towns_and_counties_urls.csv \
  --out out_cdx_toolkit \
  --num-workers 4 --worker-id 0 \
  --full-domain-crawl \
  --use-cdx-toolkit \
  --cc-index-max-lines 5000 \
  --limit 50
```

**Benefits:**
- Well-tested API wrapper
- Automatic retry and backoff logic
- Works with both Common Crawl and Wayback Machine

**Drawbacks:**
- Still makes network requests (subject to rate limits)
- Additional dependency

---

### 3. index.commoncrawl.org (Default)

Official recommended API. Queries the latest N collections via `collinfo.json` and parses NDJSON responses.

#### Use index.commoncrawl.org

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
  --csv us_towns_and_counties_urls.csv \
  --out out_index_api \
  --num-workers 4 --worker-id 0 \
  --full-domain-crawl \
  --cc-api-host index.commoncrawl.org \
  --cc-collections-latest 2 \
  --cc-user-agent "municipal-scrape/2025-12 (+YOUR_EMAIL)" \
  --cc-request-delay-ms 500 \
  --cc-max-retries 3 \
  --limit 50
```

**Configuration Flags:**
- `--cc-api-host index.commoncrawl.org` — Use official index API
- `--cc-collections-latest N` — Query N most recent crawls
- `--cc-user-agent "..."` — Identify your scraper (be polite!)
- `--cc-request-delay-ms 500` — Delay between requests (+ jitter)
- `--cc-max-retries 3` — Retry count with exponential backoff
- `--cc-index-max-lines 5000` — Cap lines parsed per collection

**Benefits:**
- Official recommended endpoint
- More stable than legacy CDX
- Returns NDJSON (one record per line)

**Drawbacks:**
- Network-dependent
- Can hit rate limits if delay is too short

---

### 4. cdx.commoncrawl.org (Legacy Fallback)

Wayback Machine-style CDX server. Used only if index.commoncrawl.org fails.

#### Use CDX Fallback

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
  --csv us_towns_and_counties_urls.csv \
  --out out_cdx_fallback \
  --num-workers 4 --worker-id 0 \
  --full-domain-crawl \
  --cc-api-host cdx.commoncrawl.org \
  --cc-request-delay-ms 1000 \
  --limit 50
```

**Benefits:**
- Simple JSON array response
- Works when index API is unavailable

**Drawbacks:**
- More prone to rate limits and abuse blocks
- Returns large JSON arrays (not streaming)
- HTTP is unsupported (must use HTTPS)

---

## Priority Order

When `--full-domain-crawl` is enabled, the orchestrator tries methods in this order for each domain:

1. **Check cache** (JSONL files in `out/state/cc_index_cache/`)
2. **Local index file** (if `--cc-local-index-file` is set)
3. **cdx_toolkit** (if `--use-cdx-toolkit` is set and installed)
4. **index.commoncrawl.org** (if `--cc-api-host` is `index.commoncrawl.org`)
5. **cdx.commoncrawl.org** (legacy fallback)

Results are cached locally (TTL controlled by `--cc-cache-ttl-days`).

---

## Rate Limit Handling

If you receive HTTP 503 or 429 errors:

1. **Increase delay:** `--cc-request-delay-ms 2000` (2 seconds)
2. **Reduce collections:** `--cc-collections-latest 1` (only latest crawl)
3. **Use local index:** Download a segment and use `--cc-local-index-file`
4. **Wait 24 hours** if blocked by abuse prevention

---

## Advanced: AWS Athena / Spark

For very large-scale analytics (millions of domains), consider using Common Crawl's columnar index via:

- **Amazon Athena** (SQL queries on S3-hosted Parquet files)
- **Apache Spark** (distributed processing)

See: https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format

---

## Examples

### Example 1: Small test with index API (default)

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
  --csv us_towns_and_counties_urls.csv \
  --out out_test_index \
  --worker-id 0 --num-workers 1 \
  --full-domain-crawl \
  --limit 10
```

### Example 2: Use local index for 4 workers

```bash
# Download index segment first
wget -O /tmp/cc-index.gz https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2024-10/indexes/cdx-00000.gz

# Launch 4 workers
for i in {0..3}; do
  /home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
    --csv us_towns_and_counties_urls.csv \
    --out out_local_4w \
    --num-workers 4 --worker-id $i \
    --full-domain-crawl \
    --cc-local-index-file /tmp/cc-index.gz \
    --limit 100 &
done
```

### Example 3: Use cdx_toolkit with conservative delay

```bash
/home/barberb/municipal_scrape_workspace/.venv/bin/pip install cdx_toolkit

/home/barberb/municipal_scrape_workspace/.venv/bin/python orchestrate_municipal_scrape.py \
  --csv us_towns_and_counties_urls.csv \
  --out out_cdx_toolkit \
  --num-workers 2 --worker-id 0 \
  --full-domain-crawl \
  --use-cdx-toolkit \
  --cc-index-max-lines 3000 \
  --limit 50
```

---

## Troubleshooting

### "Common Crawl CDX unreachable"

- Check network connectivity: `curl -I https://index.commoncrawl.org`
- Try using local index: `--cc-local-index-file`
- Increase delay: `--cc-request-delay-ms 2000`
- Use cdx_toolkit: `--use-cdx-toolkit`

### Rate Limited (HTTP 503/429)

- Wait 24 hours
- Use local index file
- Reduce request rate: `--cc-request-delay-ms 3000`
- Query fewer collections: `--cc-collections-latest 1`

### Empty Results

- Domain may not be archived in Common Crawl
- Try Wayback Machine or Archive.is (disable `--common-crawl-only`)
- Check cache TTL: older cache may have stale data

---

## See Also

- [Common Crawl Index Documentation](https://commoncrawl.org/the-data/get-started/)
- [cdx_toolkit GitHub](https://github.com/cocrawler/cdx_toolkit)
- [AWS Athena Index Guide](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format)
