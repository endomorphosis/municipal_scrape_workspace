# Repository Refactoring Roadmap

**Status**: Analysis Complete  
**Last Updated**: 2026-01-19  
**Purpose**: Document the complete file migration plan, import refactoring requirements, and dependency gaps

---

## Table of Contents

1. [Current State Analysis](#current-state-analysis)
2. [Final Repository Structure](#final-repository-structure)
3. [File Migration Status](#file-migration-status)
4. [Files Requiring Action](#files-requiring-action)
5. [Import Refactoring Guidelines](#import-refactoring-guidelines)
6. [Dependency Gaps](#dependency-gaps)
7. [Running Tools After Migration](#running-tools-after-migration)

---

## Current State Analysis

### Repository Overview

This repository contains two major components:

1. **Municipal Scraping Workflow** - An installable Python package for scraping municipal websites
2. **Common Crawl (CC) Index Pipeline** - Tools for building and querying DuckDB/Parquet indexes of Common Crawl data

### Current File Distribution

```
Root directory:        52 Python files
  - Wrappers:          19 files (already migrated)
  - Full impl:         33 files (need migration or archival)
  
src/ccindex:          23 canonical implementations
benchmarks/ccindex:   10 benchmark scripts
scripts/ops:          Multiple shell scripts
archive/ccindex:      5 archived converters
```

### Files Already Migrated to `src/municipal_scrape_workspace/ccindex/`

These files have been successfully migrated with root-level wrappers in place:

1. `build_cc_pointer_duckdb.py` ✓
2. `build_index_from_parquet.py` ✓
3. `build_master_index.py` ✓
4. `build_parallel_duckdb_indexes.py` ✓
5. `build_year_meta_indexes.py` ✓
6. `cc_domain_parquet_locator.py` ✓
7. `download_warc_records.py` ✓
8. `parallel_validate_parquet.py` ✓
9. `search_cc_domain.py` ✓
10. `search_cc_duckdb_index.py` ✓
11. `search_cc_pointer_index.py` ✓
12. `search_cc_via_meta_indexes.py` ✓
13. `search_parallel_duckdb_indexes.py` ✓
14. `sort_cc_parquet_shards.py` ✓
15. `validate_and_sort_parquet.py` ✓
16. `validate_collection_completeness.py` ✓
17. `validate_warc_record_blobs.py` ✓
18. `verify_warc_retrieval.py` ✓
19. `warc_candidates_from_jsonl.py` ✓

---

## Final Repository Structure

```
.
├── src/municipal_scrape_workspace/          # Installable Python package
│   ├── __init__.py
│   ├── cli.py                               # Main CLI entrypoint
│   ├── orchestrate_municipal_scrape.py      # Municipal scrape orchestrator
│   ├── check_archive_callbacks.py           # Archive integration checks
│   │
│   └── ccindex/                             # Common Crawl tooling
│       ├── __init__.py
│       │
│       ├── # Core pipeline orchestration
│       ├── cc_pipeline_orchestrator.py
│       ├── cc_pipeline_watch.py
│       ├── cc_pipeline_hud.py
│       ├── monitor_progress.py
│       │
│       ├── # Conversion tools
│       ├── bulk_convert_gz_to_parquet.py
│       ├── parallel_convert_missing.py
│       ├── regenerate_parquet_from_gz.py
│       ├── sample_ccindex_to_parquet.py
│       ├── extract_cc_index_tarballs.py
│       │
│       ├── # Sorting tools (keep canonical only)
│       ├── sort_cc_parquet_shards.py        [DONE]
│       ├── sort_unsorted_memory_aware.py    [CANONICAL]
│       │
│       ├── # Validation tools
│       ├── validate_and_sort_parquet.py     [DONE]
│       ├── parallel_validate_parquet.py     [DONE]
│       ├── validate_urlindex_sorted.py
│       ├── validate_search_completeness.py
│       ├── validate_collection_completeness.py [DONE]
│       │
│       ├── # Index builders
│       ├── build_cc_pointer_duckdb.py       [DONE]
│       ├── build_index_from_parquet.py      [DONE]
│       ├── build_parallel_duckdb_indexes.py [DONE]
│       ├── build_duckdb_pointer_from_parquet.py
│       ├── build_cc_parquet_rowgroup_index.py
│       │
│       ├── # Meta-index tools
│       ├── build_year_meta_indexes.py       [DONE]
│       ├── build_master_index.py            [DONE]
│       │
│       ├── # Search tools
│       ├── search_cc_domain.py              [DONE]
│       ├── search_cc_duckdb_index.py        [DONE]
│       ├── search_cc_pointer_index.py       [DONE]
│       ├── search_cc_via_meta_indexes.py    [DONE]
│       ├── search_parallel_duckdb_indexes.py [DONE]
│       ├── cc_domain_parquet_locator.py     [DONE]
│       │
│       ├── # WARC retrieval tools
│       ├── download_warc_records.py         [DONE]
│       ├── verify_warc_retrieval.py         [DONE]
│       ├── validate_warc_record_blobs.py    [DONE]
│       ├── warc_candidates_from_jsonl.py    [DONE]
│       │
│       └── # Monitoring/queue tools
│           ├── queue_cc_pointer_build.py
│           ├── launch_cc_pointer_build.py
│           ├── monitor_cc_pointer_build.py
│           ├── watchdog_cc_pointer_build.py
│           ├── watchdog_monitor.py
│           └── cc_pointer_status.py
│
├── scripts/
│   └── ops/                                 # Operational shell scripts
│       ├── download_cc_indexes*.sh
│       ├── overnight_*.sh
│       ├── monitor_*.sh
│       ├── rebuild_*.sh
│       └── ...
│
├── benchmarks/
│   └── ccindex/                             # Performance benchmarks
│       ├── benchmark_*.py
│       └── README.md
│
├── archive/
│   └── ccindex/
│       ├── converters/                      # One-off conversion scripts
│       │   ├── convert_final_three*.py
│       │   └── convert_missing*.py
│       └── superseded/                      # Deprecated tools
│           ├── cc_pipeline_manager.py       [TO BE ARCHIVED]
│           ├── consolidate_parquet_files.py [TO BE ARCHIVED]
│           ├── sort_unsorted_files.py       [TO BE ARCHIVED]
│           ├── sort_parquet_external_merge.py [TO BE ARCHIVED]
│           ├── validate_and_mark_sorted.py  [TO BE ARCHIVED]
│           ├── build_duckdb_from_sorted_parquet.py [TO BE ARCHIVED]
│           └── compare_crawl_results.py     [TO BE ARCHIVED]
│
├── docs/                                    # Documentation
│   ├── REPO_LAYOUT_PLAN.md
│   ├── COMMON_CRAWL_USAGE.md
│   └── ...
│
└── <root-level wrappers>                    # Backwards compatibility
    ├── search_cc_domain.py                  # Thin wrapper imports from src/
    ├── build_cc_pointer_duckdb.py           # Thin wrapper imports from src/
    └── ...
```

---

## File Migration Status

### Category 1: Already Migrated (19 files)

These files exist in `src/municipal_scrape_workspace/ccindex/` with thin wrappers at root:

- ✅ build_cc_pointer_duckdb.py
- ✅ build_index_from_parquet.py
- ✅ build_master_index.py
- ✅ build_parallel_duckdb_indexes.py
- ✅ build_year_meta_indexes.py
- ✅ cc_domain_parquet_locator.py
- ✅ download_warc_records.py
- ✅ parallel_validate_parquet.py
- ✅ search_cc_domain.py
- ✅ search_cc_duckdb_index.py
- ✅ search_cc_pointer_index.py
- ✅ search_cc_via_meta_indexes.py
- ✅ search_parallel_duckdb_indexes.py
- ✅ sort_cc_parquet_shards.py
- ✅ validate_and_sort_parquet.py
- ✅ validate_collection_completeness.py
- ✅ validate_warc_record_blobs.py
- ✅ verify_warc_retrieval.py
- ✅ warc_candidates_from_jsonl.py

### Category 2: Migrated to src/ But Missing Wrapper (4 files)

These exist in both locations but root file is not a wrapper:

- ⚠️ `build_cc_parquet_rowgroup_index.py` - Root file needs to become wrapper
- ⚠️ `bulk_convert_gz_to_parquet.py` - Root file needs to become wrapper
- ⚠️ `validate_search_completeness.py` - Root file needs to become wrapper  
- ⚠️ `validate_urlindex_sorted.py` - Root file needs to become wrapper

**Action Required**: Convert root files to wrappers that import from src/

### Category 3: Need Migration to src/ (17 files)

These are full implementations that should be moved to `src/municipal_scrape_workspace/ccindex/`:

#### Orchestration/Monitoring (6 files)
- 📦 `cc_pipeline_orchestrator.py` → `src/.../ccindex/cc_pipeline_orchestrator.py`
- 📦 `cc_pipeline_watch.py` → `src/.../ccindex/cc_pipeline_watch.py`
- 📦 `cc_pipeline_hud.py` → `src/.../ccindex/cc_pipeline_hud.py`
- 📦 `monitor_progress.py` → `src/.../ccindex/monitor_progress.py`
- 📦 `monitor_cc_pointer_build.py` → `src/.../ccindex/monitor_cc_pointer_build.py`
- 📦 `cc_pointer_status.py` → `src/.../ccindex/cc_pointer_status.py`

#### Pointer Build Queue/Watchdog (3 files)
- 📦 `queue_cc_pointer_build.py` → `src/.../ccindex/queue_cc_pointer_build.py`
- 📦 `launch_cc_pointer_build.py` → `src/.../ccindex/launch_cc_pointer_build.py`
- 📦 `watchdog_cc_pointer_build.py` → `src/.../ccindex/watchdog_cc_pointer_build.py`
- 📦 `watchdog_monitor.py` → `src/.../ccindex/watchdog_monitor.py`

#### Conversion Tools (4 files)
- 📦 `parallel_convert_missing.py` → `src/.../ccindex/parallel_convert_missing.py`
- 📦 `regenerate_parquet_from_gz.py` → `src/.../ccindex/regenerate_parquet_from_gz.py`
- 📦 `sample_ccindex_to_parquet.py` → `src/.../ccindex/sample_ccindex_to_parquet.py`
- 📦 `extract_cc_index_tarballs.py` → `src/.../ccindex/extract_cc_index_tarballs.py`

#### Index Builders (1 file)
- 📦 `build_duckdb_pointer_from_parquet.py` → `src/.../ccindex/build_duckdb_pointer_from_parquet.py`

#### Municipal Scrape (2 files)
- 📦 `orchestrate_municipal_scrape.py` → `src/municipal_scrape_workspace/orchestrate_municipal_scrape.py`
- 📦 `check_archive_callbacks.py` → `src/municipal_scrape_workspace/check_archive_callbacks.py`

### Category 4: Archive as Duplicate/Superseded (7 files)

These should be moved to `archive/ccindex/superseded/`:

#### Superseded by Orchestrator
- 🗄️ `cc_pipeline_manager.py` → `archive/ccindex/superseded/`  
  *Reason: Superseded by cc_pipeline_orchestrator.py*

#### Duplicate/Redundant Search Tools (4 files)
- 🗄️ `search_domain_duckdb_pointer.py` → `archive/ccindex/superseded/`  
  *Reason: Duplicate of search_parallel_duckdb_indexes.py*
- 🗄️ `search_domain_pointer_index.py` → `archive/ccindex/superseded/`  
  *Reason: Duplicate functionality*
- 🗄️ `search_duckdb_domain.py` → `archive/ccindex/superseded/`  
  *Reason: Covered by canonical search tools*
- 🗄️ `search_duckdb_pointer_domain.py` → `archive/ccindex/superseded/`  
  *Reason: Covered by canonical search tools*

#### Superseded Sort/Validate Tools (2 files)
- 🗄️ `sort_unsorted_files.py` → `archive/ccindex/superseded/`  
  *Reason: Keep sort_unsorted_memory_aware.py as canonical*
- 🗄️ `sort_parquet_external_merge.py` → `archive/ccindex/superseded/`  
  *Reason: Functionality covered by canonical sorters*

### Category 5: Evaluate and Decide (5 files)

These need case-by-case evaluation:

- ❓ `consolidate_parquet_files.py` - Keep if actively used, else archive
- ❓ `compare_crawl_results.py` - Archive if one-off, keep if reusable utility
- ❓ `validate_and_mark_sorted.py` - Evaluate vs validate_and_sort_parquet.py
- ❓ `build_duckdb_from_sorted_parquet.py` - Evaluate vs build_duckdb_pointer_from_parquet.py
- ❓ `sort_unsorted_memory_aware.py` - Keep as canonical memory-aware sorter

---

## Files Requiring Action

### Immediate Actions

#### 1. Fix Missing Wrappers (4 files)

These files are in `src/` but root version is not a wrapper:

```bash
# For each file, convert root version to thin wrapper:

# build_cc_parquet_rowgroup_index.py
# bulk_convert_gz_to_parquet.py
# validate_search_completeness.py
# validate_urlindex_sorted.py
```

**Wrapper Template**:
```python
#!/usr/bin/env python3
"""Backwards-compatible wrapper for <tool name>.

Moved to:
  municipal_scrape_workspace.ccindex.<module_name>
"""

from municipal_scrape_workspace.ccindex.<module_name> import main

if __name__ == "__main__":
    raise SystemExit(main())
```

#### 2. Migrate High-Priority Files (10 files)

Move these core tools to `src/municipal_scrape_workspace/ccindex/`:

**Orchestration** (Priority 1):
- cc_pipeline_orchestrator.py
- cc_pipeline_watch.py  
- cc_pipeline_hud.py
- monitor_progress.py

**Queue/Watchdog** (Priority 2):
- queue_cc_pointer_build.py
- launch_cc_pointer_build.py
- monitor_cc_pointer_build.py
- watchdog_cc_pointer_build.py
- watchdog_monitor.py
- cc_pointer_status.py

**Municipal Scrape** (Priority 3):
- orchestrate_municipal_scrape.py
- check_archive_callbacks.py

#### 3. Archive Superseded Files (7 files)

Move to `archive/ccindex/superseded/`:
- cc_pipeline_manager.py
- search_domain_duckdb_pointer.py
- search_domain_pointer_index.py
- search_duckdb_domain.py
- search_duckdb_pointer_domain.py
- sort_unsorted_files.py
- sort_parquet_external_merge.py

---

## Import Refactoring Guidelines

### When Moving a File to src/

1. **Add/Preserve main() Function**
   ```python
   def main(argv=None) -> int:
       """Main entry point."""
       parser = argparse.ArgumentParser(...)
       args = parser.parse_args(argv)
       # ... implementation
       return 0  # or exit code
   ```

2. **Replace Intra-repo Imports**
   
   ❌ **Before** (root-level import):
   ```python
   import validate_collection_completeness
   from cc_domain_parquet_locator import find_domain_files
   ```
   
   ✅ **After** (package import):
   ```python
   from municipal_scrape_workspace.ccindex.validate_collection_completeness import CollectionValidator
   from municipal_scrape_workspace.ccindex.cc_domain_parquet_locator import find_domain_files
   ```

3. **Remove sys.path Hacks**
   
   ❌ **Remove**:
   ```python
   sys.path.insert(0, str(Path(__file__).parent))
   sys.path.insert(0, "/home/barberb/ipfs_datasets_py")
   ```

4. **Lazy Import Heavy Dependencies**
   
   For optional dependencies (allows `--help` without installing ccindex extras):
   ```python
   def main(argv=None) -> int:
       import duckdb  # Import here, not at module level
       import pyarrow.parquet as pq
       # ... use dependencies
   ```

5. **Update Documentation References**
   
   Update any docstrings, comments, or docs that reference file locations:
   ```python
   """
   Search CC indexes via meta-indexes.
   
   Canonical location:
     src/municipal_scrape_workspace/ccindex/search_cc_via_meta_indexes.py
   
   Run via wrapper:
     ./search_cc_via_meta_indexes.py --help
   
   Run via module:
     python -m municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes --help
   """
   ```

### Creating Backwards-Compatible Wrappers

For every file moved to `src/`, create a thin wrapper at the root:

```python
#!/usr/bin/env python3
"""Backwards-compatible wrapper for <Tool Name>.

Moved to:
  municipal_scrape_workspace.ccindex.<module_name>
"""

from municipal_scrape_workspace.ccindex.<module_name> import main

if __name__ == "__main__":
    raise SystemExit(main())
```

**Key Points**:
- Keep the wrapper minimal (no business logic)
- Preserve the original filename at root
- Use `raise SystemExit(main())` to propagate exit codes correctly

### Import Pattern Examples

#### Example 1: Orchestrator Imports Validator

**File**: `cc_pipeline_orchestrator.py`

❌ **Before**:
```python
import validate_collection_completeness
validator = validate_collection_completeness.CollectionValidator(...)
```

✅ **After**:
```python
from municipal_scrape_workspace.ccindex.validate_collection_completeness import CollectionValidator
validator = CollectionValidator(...)
```

#### Example 2: Municipal Scrape Calls ipfs_datasets_py

**File**: `orchestrate_municipal_scrape.py`

❌ **Before** (hardcoded path):
```python
sys.path.insert(0, "/home/barberb/ipfs_datasets_py")
from ipfs_datasets.unified_scraper import UnifiedScraper
```

✅ **After** (installed dependency):
```python
# Just import - ipfs_datasets_py is in pyproject.toml dependencies
from ipfs_datasets.unified_scraper import UnifiedScraper
```

Or with dev override:
```python
import os
import sys

# Support local dev checkout via environment variable
ipfs_root = os.environ.get("IPFS_DATASETS_PY_ROOT")
if ipfs_root:
    sys.path.insert(0, ipfs_root)

from ipfs_datasets.unified_scraper import UnifiedScraper
```

---

## Dependency Gaps

### 1. ipfs_datasets_py Portability Issue

**Current Problem**:
```toml
dependencies = [
    "ipfs_datasets_py @ file:///home/barberb/ipfs_datasets_py",
]
```

This hardcoded local path is not portable across development environments.

**Solutions**:

**Option A: Git URL Dependency** (Recommended if upstream is stable)
```toml
dependencies = [
    "ipfs_datasets_py @ git+https://github.com/endomorphosis/ipfs_datasets_py.git@main",
]
```

**Option B: Make it Optional with Dev Override**
```toml
[project.optional-dependencies]
ipfs = [
    "ipfs_datasets_py @ git+https://github.com/endomorphosis/ipfs_datasets_py.git@main",
]
```

Then document the dev override pattern:
```bash
# For local development
export IPFS_DATASETS_PY_ROOT="/path/to/local/ipfs_datasets_py"
pip install -e .

# For production
pip install -e '.[ipfs]'
```

**Option C: Published Package** (Best long-term)
```toml
dependencies = [
    "ipfs-datasets-py>=0.1.0",  # If/when published to PyPI
]
```

### 2. Optional CCIndex Dependencies

**Current State**: ✅ Already handled correctly

The `[ccindex]` optional extra properly declares heavy dependencies:

```toml
[project.optional-dependencies]
ccindex = [
  "duckdb>=0.10.0",
  "pyarrow>=14.0.0",
  "psutil>=5.9.0",
  "requests>=2.31.0",
]
```

**Usage**:
```bash
# Install with CC tooling
pip install -e '.[ccindex]'

# Install without CC tooling (lighter)
pip install -e .
```

### 3. Development Dependencies

**Missing**: No dev/test extras currently defined

**Recommendation**: Add development dependencies
```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
    "black>=23.0",
    "ruff>=0.1.0",
    "mypy>=1.0",
]
```

---

## Running Tools After Migration

### Via Root Wrapper (Backwards Compatible)

```bash
# Activate virtual environment
source .venv/bin/activate

# Run via wrapper (old way still works)
./search_cc_domain.py --domain example.com
./build_cc_pointer_duckdb.py --help
```

### Via Python Module (New Way)

```bash
# Run as module
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com
python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --help
```

### Via Console Script Entry Points (Future Enhancement)

Add to `pyproject.toml`:
```toml
[project.scripts]
municipal-scrape = "municipal_scrape_workspace.cli:main"

# Optional: Add ccindex tool entry points
ccindex-search = "municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes:main"
ccindex-build-pointer = "municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb:main"
ccindex-orchestrate = "municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator:main"
```

Then run directly:
```bash
ccindex-search --domain example.com
ccindex-build-pointer --help
```

### Shell Scripts

```bash
# Via ops scripts (canonical location)
./scripts/ops/overnight_build_duckdb_index.sh

# Via root wrapper (backwards compatible)
./overnight_build_duckdb_index.sh
```

---

## Migration Checklist

### For Each File Being Migrated:

- [ ] 1. Move file to appropriate location in `src/municipal_scrape_workspace/ccindex/`
- [ ] 2. Add/verify `main(argv=None) -> int` function exists
- [ ] 3. Update all intra-repo imports to use package imports
- [ ] 4. Remove any `sys.path.insert()` hacks
- [ ] 5. Add lazy imports for optional dependencies if needed
- [ ] 6. Update docstring with canonical location
- [ ] 7. Create thin wrapper at root with original filename
- [ ] 8. Test wrapper works: `./tool.py --help`
- [ ] 9. Test module import works: `python -m municipal_scrape_workspace.ccindex.tool --help`
- [ ] 10. Update any documentation referencing the old location
- [ ] 11. Check for any scripts/docs that reference this file and update them

### For Files Being Archived:

- [ ] 1. Move to `archive/ccindex/superseded/`
- [ ] 2. Add README in archive explaining why archived
- [ ] 3. Update any docs that reference the archived file
- [ ] 4. Note canonical replacement tool (if applicable)

### Final Validation:

- [ ] 1. All root `.py` files are either wrappers or archived
- [ ] 2. All imports use package imports (no relative/sys.path hacks)
- [ ] 3. `pip install -e .` works without ccindex dependencies
- [ ] 4. `pip install -e '.[ccindex]'` enables all CC tools
- [ ] 5. All wrappers execute correctly
- [ ] 6. Documentation reflects new structure
- [ ] 7. `.gitignore` excludes generated files (build artifacts, etc.)

---

## Summary of Changes Required

### Immediate (4 files)
- Convert 4 root files to wrappers (already in src/)

### High Priority (17 files)  
- Migrate 17 core tools to src/municipal_scrape_workspace/ccindex/
- Create wrappers for each

### Medium Priority (7 files)
- Archive 7 superseded/duplicate files

### Low Priority (5 files)
- Evaluate and decide on 5 ambiguous files

### Documentation
- ✅ REFACTORING_ROADMAP.md created (this document)
- Update REPO_LAYOUT_README.md with final state
- Update README.md with new structure

### Total Files to Process: 33 root Python files
