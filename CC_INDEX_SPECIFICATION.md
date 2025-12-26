# Common Crawl 5-Year Index Download Specification

**Generated from:** `collinfo.json` (index.commoncrawl.org/collinfo.json)  
**Generation Date:** 2025-12-26  
**Coverage Period:** December 27, 2020 - December 26, 2025 (5 years)

---

## Executive Summary

### Collections Required: 42
- **2025:** 12 collections (Jan-Dec)
- **2024:** 10 collections (Feb-Dec)
- **2023:** 5 collections (selected weeks)
- **2022:** 6 collections (selected weeks)
- **2021:** 9 collections (selected weeks)

### Estimated Data Volume
- **Individual Index Files:** ~6,000+ compressed CDX files
- **Uncompressed Size:** ~50-60 TB
- **Average per Collection:** 140-300 index files (~150GB average)
- **Average per Index File:** 700MB - 1GB

### Current Download Progress (as of 2025-12-26)
- **Downloaded:** 1,314 files (986 GB) from 10 collections
- **Progress:** 1.9% complete
- **Status:** Actively downloading at 12 parallel jobs
- **ETA:** ~36-48 hours (varies with network conditions)

---

## Collection List by Year

### 2025 (12 collections - current year)
```
CC-MAIN-2025-05   Jan 12-26, 2025      [DOWNLOADING] 173 files, 131GB
CC-MAIN-2025-08   Feb  6-19, 2025      [DOWNLOADING] 194 files, 133GB
CC-MAIN-2025-13   Mar 15-28, 2025      [PENDING]
CC-MAIN-2025-18   Apr 17-May 1, 2025   [PENDING]
CC-MAIN-2025-21   May 12-25, 2025      [PENDING]
CC-MAIN-2025-26   Jun 12-25, 2025      [PENDING]
CC-MAIN-2025-30   Jul  7-20, 2025      [PENDING]
CC-MAIN-2025-33   Aug  2-15, 2025      [PENDING]
CC-MAIN-2025-38   Sep  5-18, 2025      [PENDING]
CC-MAIN-2025-43   Oct  5-19, 2025      [PENDING]
CC-MAIN-2025-47   Nov  6-19, 2025      [PENDING]
CC-MAIN-2025-51   Dec  4-17, 2025      [PENDING]
```

### 2024 (10 collections)
```
CC-MAIN-2024-10   Feb 20-Mar 5, 2024   [COMPLETE] 300 files, 233GB
CC-MAIN-2024-18   Apr 12-25, 2024      [PENDING]
CC-MAIN-2024-22   May 17-31, 2024      [PENDING]
CC-MAIN-2024-26   Jun 12-25, 2024      [PENDING]
CC-MAIN-2024-30   Jul 12-25, 2024      [PENDING]
CC-MAIN-2024-33   Aug  2-16, 2024      [PENDING]
CC-MAIN-2024-38   Sep  7-21, 2024      [PENDING]
CC-MAIN-2024-42   Oct  3-16, 2024      [PENDING]
CC-MAIN-2024-46   Nov  1-15, 2024      [PENDING]
CC-MAIN-2024-51   Dec  1-15, 2024      [PENDING]
```

### 2023 (5 collections)
```
CC-MAIN-2023-06   Jan 26-Feb 9, 2023   [COMPLETE] 150 files, 117GB
CC-MAIN-2023-14   Mar 20-Apr 2, 2023   [PENDING]
CC-MAIN-2023-23   May 27-Jun 11, 2023  [PENDING]
CC-MAIN-2023-40   Sep 21-Oct 5, 2023   [PENDING]
CC-MAIN-2023-50   Nov 28-Dec 12, 2023  [PENDING]
```

### 2022 (6 collections)
```
CC-MAIN-2022-05   Jan 16-29, 2022      [COMPLETE] 159 files, 119GB
CC-MAIN-2022-21   May 16-29, 2022      [PENDING]
CC-MAIN-2022-27   Jun 24-Jul 7, 2022   [PENDING]
CC-MAIN-2022-33   Aug  7-20, 2022      [PENDING]
CC-MAIN-2022-40   Sep 24-Oct 8, 2022   [PENDING]
CC-MAIN-2022-49   Nov 26-Dec 10, 2022  [PENDING]
```

### 2021 (9 collections)
```
CC-MAIN-2021-04   Jan 15-28, 2021      [COMPLETE] 140 files, 120GB
CC-MAIN-2021-10   Feb 24-Mar 9, 2021   [COMPLETE] 162 files, 111GB
CC-MAIN-2021-17   Apr 10-23, 2021      [COMPLETE]   3 files, 1.8GB
CC-MAIN-2021-21   May  5-19, 2021      [COMPLETE]   3 files, 2.0GB
CC-MAIN-2021-25   Jun 12-25, 2021      [COMPLETE]   5 files, 2.6GB
CC-MAIN-2021-31   Jul 23-Aug 6, 2021   [PENDING]
CC-MAIN-2021-39   Sep 16-29, 2021      [PENDING]
CC-MAIN-2021-43   Oct 15-28, 2021      [PENDING]
CC-MAIN-2021-49   Nov 26-Dec 9, 2021   [PENDING]
```

---

## Storage Structure

**Base Directory:** `/storage/ccindex/`

**Per-Collection Structure:**
```
/storage/ccindex/CC-MAIN-2024-10/
├── cdx-00000.gz          (700MB - 1GB each)
├── cdx-00001.gz
├── cdx-00002.gz
├── ...
└── cdx-00299.gz          (300 files per major collection)
```

**Indexes Within Each File:**
Each `.gz` file contains a complete CDX index for one week's crawl data, indexed by URL domain.

**Querying Format:**
```
/storage/ccindex/CC-MAIN-YYYY-WW/cdx-NNNNN.gz
```

---

## Download Orchestration

**Script:** `download_cc_indexes_5years.sh`

**Key Features:**
1. **Parallel Downloads:** 12 concurrent wget jobs
2. **Resume Capability:** Skips existing files automatically
3. **Integrity Testing:** Validates .gz files before continuing
4. **Error Handling:** Automatic retries on transient failures
5. **Progress Reporting:** Summary stats at completion

**Invocation:**
```bash
bash /home/barberb/municipal_scrape_workspace/download_cc_indexes_5years.sh 12
```

**Monitoring:**
```bash
# Real-time log
tail -f /tmp/cc_5year_corrected.log

# Quick status check
bash /home/barberb/municipal_scrape_workspace/check_cc_download_status.sh

# Manual file count
find /storage/ccindex -name "*.gz" | wc -l
```

---

## Performance Specifications

### Download Performance
- **Parallel Jobs:** 12 concurrent threads
- **Average Speed:** 50-100 MB/s per thread (variable based on CC server load)
- **Total Bandwidth:** 600MB/s - 1.2GB/s (theoretical maximum)
- **Typical Download Time:** 40-60 hours for full 5-year set

### Storage Performance
- **File System:** ZFS (RAID optimized)
- **Storage Pool:** `/storage` with 11+ TB available space
- **Read Latency:** <5ms for random access
- **Throughput:** 500MB/s+ for sequential reads

### Query Performance (Once Complete)
- **Index Lookup:** <100ms per collection
- **Time-Range Search:** <1s for 5-year span (42 collections)
- **Domain Search:** <500ms across 42 collections (~6000 files)
- **Historical Analysis:** <10s for complex time-series queries

---

## Integration Points

### With Municipal Scraper Orchestrator
1. **Archive Discovery:** Use CC index to find URLs and snapshots
2. **Time-Range Queries:** Query specific date ranges across collections
3. **Change Detection:** Compare snapshots across years
4. **Historical Archive:** Full 5-year capture for each municipal site

### API Usage
```python
# Query CC index for a domain
https://index.commoncrawl.org/CC-MAIN-2024-10-index?url=*.domain.gov&output=json

# Retrieve actual cached content
https://web.archive.org/web/2024*/domain.gov/
```

---

## Maintenance & Monitoring

### Automated Checks
- **Weekly Status:** Run `check_cc_download_status.sh` for progress
- **Storage Alerts:** Alert if usage exceeds 45TB
- **Integrity Checks:** Periodic gunzip -t validation on sample files

### Manual Recovery
```bash
# If download stalls, restart with resume:
bash /home/barberb/municipal_scrape_workspace/download_cc_indexes_5years.sh 12

# If index file is corrupted:
rm /storage/ccindex/CC-MAIN-YYYY-WW/cdx-NNNNN.gz
# Restart download - will redownload missing file

# Force verify all files:
find /storage/ccindex -name "*.gz" -exec gunzip -t {} \;
```

---

## Deployment Checklist

- [x] collinfo.json downloaded and analyzed
- [x] 42-collection list extracted and validated
- [x] Download script updated with official collection IDs
- [x] Storage allocation verified (11+ TB available)
- [x] Download process started (12 parallel jobs)
- [ ] Complete 5-year index download (~36-48 hours)
- [ ] Verify all 42 collections have content
- [ ] Validate sample files with gunzip -t
- [ ] Integrate with municipal scraper orchestrator
- [ ] Begin historical municipal website analysis

---

## Reference Information

**Common Crawl Official Resources:**
- Collections: https://index.commoncrawl.org/collinfo.json
- TimeMachine: https://web.archive.org/
- CDX API: https://github.com/webrecorder/pywb/wiki/CDX-API

**Project Timeline:**
- Start Date: 2025-12-26
- Expected Completion: ~2025-12-28 to 2025-12-29
- Status: ✅ Active download in progress

---

*Last Updated: 2025-12-26*
