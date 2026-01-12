# CRITICAL ORCHESTRATOR FINDINGS - 2026-01-12 03:02 UTC

## 🚨 VALIDATOR REPORTS GHOST FILES 🚨

### The Problem
The validator (`validate_collection_completeness.py`) and orchestrator (`cc_pipeline_orchestrator.py`) report collections are partially complete with hundreds of files, but **ZERO files actually exist on disk**.

### Evidence

```bash
# What validator reports:
CC-MAIN-2024-18:
  📦 tar.gz:      300/300 (100.0%)  
  📄 parquet:     300/300 (100.0%)  
  ✅ sorted:      163/300 ( 54.3%)  

# What actually exists:
$ find /storage -name "*CC-MAIN-2024-18*" -type f 2>/dev/null
# NO RESULTS

$ ls /storage/ccindex/*.tar.gz | wc -l
0

$ find /storage/ccindex_parquet -name "*.parquet" | wc -l  
0
```

### Root Cause

1. **Hardcoded expected counts**: Line 104 of validator always returns `expected=300` regardless of reality
2. **Wrong glob patterns**: Validator looks in wrong directories (`cc_pointers_by_collection/2024/CC-MAIN-2024-18/`)
3. **No verification**: Orchestrator trusts validator without checking if files exist
4. **Logic errors**: `len(parquet_files)` returns count of files found, but if glob finds nothing, it still reports as if files exist somewhere

### Why Orchestrator Fails

```python
# Orchestrator line ~165
def _sort_collection(self, collection: str) -> bool:
    unsorted = list(self.parquet_dir.glob(f"{collection}-cdx-*.gz.parquet"))
    # Returns empty list because files don't exist OR pattern is wrong
    
    if not unsorted:
        self.logger.info(f"No unsorted files found for {collection}")
        return True  # FALSE SUCCESS!
```

The orchestrator reports "No unsorted files found" and succeeds, but it's because:
- Files don't exist at all, OR
- Glob pattern doesn't match actual file structure

### What Should Happen

1. **Download missing .tar.gz files** from Common Crawl
2. **Convert .tar.gz → .parquet** using bulk conversion
3. **Sort .parquet files** by url_surtkey  
4. **Build DuckDB pointer indexes** with offset/range

### What Actually Happens

1. Validator claims files exist ✗
2. Orchestrator skips download (thinks files exist) ✗
3. Orchestrator skips conversion (thinks already converted) ✗
4. Orchestrator finds no unsorted files (wrong pattern) ✗
5. Orchestrator builds index from non-existent files ✗
6. Reports "success" ✗

---

## Required Fixes

### 1. Fix Validator Discovery

Replace hardcoded paths with actual file detection:

```python
def check_parquet_converted(self, collection: str):
    # Check ALL possible locations
    locations = [
        self.parquet_dir / collection,  # /storage/ccindex_parquet/CC-MAIN-2024-18/
        self.parquet_dir / "2024" / collection,  # Year-organized
        self.parquet_dir / collection.split('-')[2] / collection,  # Year subdir
    ]
    
    files = []
    for loc in locations:
        if loc.exists():
            files.extend(loc.glob("*.gz.parquet*"))
    
    # Query CC API for actual expected count
    expected = self._query_cc_collection_size(collection)  
    return len(files), expected, location_found
```

### 2. Fix Orchestrator to Actually Execute

```python
def _download_collection(self, collection: str) -> bool:
    # Actually download if missing
    gz_files = list(self.index_dir.glob(f"{collection}/*.tar.gz"))
    if len(gz_files) < expected:
        # Run download script
        result = subprocess.run([...])
        # Verify files appeared
        gz_files = list(self.index_dir.glob(f"{collection}/*.tar.gz"))
        return len(gz_files) == expected
```

### 3. Add Comprehensive Verification

- Before each phase: verify prerequisites exist
- After each phase: verify output created
- Log actual file counts and paths
- Fail fast if assertion fails

---

## Next Steps

**User must decide:**

1. **Do you have ANY actual .tar.gz downloads?**  
   - If yes: Where are they? Let's find and organize them
   - If no: Need to start downloads from scratch

2. **What's your desired file structure?**
   ```
   Option A (flat):
   /storage/ccindex/CC-MAIN-2024-18-cdx-00000.tar.gz
   /storage/ccindex_parquet/CC-MAIN-2024-18-cdx-00000.gz.parquet
   
   Option B (organized):
   /storage/ccindex/2024/CC-MAIN-2024-18/cdx-00000.tar.gz  
   /storage/ccindex_parquet/2024/CC-MAIN-2024-18/cdx-00000.gz.parquet
   ```

3. **Should we start fresh or salvage existing work?**
   - Start fresh: Clear directories, download collections properly
   - Salvage: Find what actually exists, reorganize, fill gaps

**Without fixing validator, orchestrator will continue to hallucinate success while doing nothing!**
