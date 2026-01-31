# Final Root Directory Reorganization Guide

**Date**: 2026-01-20  
**Status**: 📋 PLANNING - Awaiting Execution  
**Purpose**: Document the final file locations and refactoring requirements for completing root directory cleanup

---

## 🎯 Executive Summary

The repository has successfully completed **Phase 1** of reorganization (code refactoring). This document outlines **Phase 2** (optional root directory cleanup) where we can choose to:

**Option A (Current)**: Keep 41 Python wrapper files for backward compatibility  
**Option B (Cleaner)**: Remove wrapper files, direct users to use proper package imports

---

## 📊 Current Root Directory Analysis

### Current State (60 items total)

#### Python Files (41 files)
All are thin wrappers (10-14 lines) that forward to `src/municipal_scrape_workspace/ccindex/`:

**Build Tools (7 files):**
- `build_cc_parquet_rowgroup_index.py` → `src/.../ccindex/build_cc_parquet_rowgroup_index.py`
- `build_cc_pointer_duckdb.py` → `src/.../ccindex/build_cc_pointer_duckdb.py`
- `build_duckdb_pointer_from_parquet.py` → `src/.../ccindex/build_duckdb_pointer_from_parquet.py`
- `build_index_from_parquet.py` → `src/.../ccindex/build_index_from_parquet.py`
- `build_master_index.py` → `src/.../ccindex/build_master_index.py`
- `build_parallel_duckdb_indexes.py` → `src/.../ccindex/build_parallel_duckdb_indexes.py`
- `build_year_meta_indexes.py` → `src/.../ccindex/build_year_meta_indexes.py`

**Conversion Tools (5 files):**
- `bulk_convert_gz_to_parquet.py` → `src/.../ccindex/bulk_convert_gz_to_parquet.py`
- `parallel_convert_missing.py` → `src/.../ccindex/parallel_convert_missing.py`
- `regenerate_parquet_from_gz.py` → `src/.../ccindex/regenerate_parquet_from_gz.py`
- `sample_ccindex_to_parquet.py` → `src/.../ccindex/sample_ccindex_to_parquet.py`
- `extract_cc_index_tarballs.py` → `src/.../ccindex/extract_cc_index_tarballs.py`

**Search Tools (6 files):**
- `search_cc_domain.py` → `src/.../ccindex/search_cc_domain.py`
- `search_cc_duckdb_index.py` → `src/.../ccindex/search_cc_duckdb_index.py`
- `search_cc_pointer_index.py` → `src/.../ccindex/search_cc_pointer_index.py`
- `search_cc_via_meta_indexes.py` → `src/.../ccindex/search_cc_via_meta_indexes.py`
- `search_parallel_duckdb_indexes.py` → `src/.../ccindex/search_parallel_duckdb_indexes.py`
- `cc_domain_parquet_locator.py` → `src/.../ccindex/cc_domain_parquet_locator.py`

**Validation Tools (7 files):**
- `validate_and_sort_parquet.py` → `src/.../ccindex/validate_and_sort_parquet.py`
- `parallel_validate_parquet.py` → `src/.../ccindex/parallel_validate_parquet.py`
- `validate_urlindex_sorted.py` → `src/.../ccindex/validate_urlindex_sorted.py`
- `validate_search_completeness.py` → `src/.../ccindex/validate_search_completeness.py`
- `validate_collection_completeness.py` → `src/.../ccindex/validate_collection_completeness.py`
- `validate_warc_record_blobs.py` → `src/.../ccindex/validate_warc_record_blobs.py`
- `verify_warc_retrieval.py` → `src/.../ccindex/verify_warc_retrieval.py`

**Sorting Tools (2 files):**
- `sort_cc_parquet_shards.py` → `src/.../ccindex/sort_cc_parquet_shards.py`
- `sort_unsorted_memory_aware.py` → `src/.../ccindex/sort_unsorted_memory_aware.py`

**Monitoring & Orchestration (10 files):**
- `cc_pipeline_orchestrator.py` → `src/.../ccindex/cc_pipeline_orchestrator.py`
- `cc_pipeline_watch.py` → `src/.../ccindex/cc_pipeline_watch.py`
- `cc_pipeline_hud.py` → `src/.../ccindex/cc_pipeline_hud.py`
- `monitor_progress.py` → `src/.../ccindex/monitor_progress.py`
- `monitor_cc_pointer_build.py` → `src/.../ccindex/monitor_cc_pointer_build.py`
- `cc_pointer_status.py` → `src/.../ccindex/cc_pointer_status.py`
- `queue_cc_pointer_build.py` → `src/.../ccindex/queue_cc_pointer_build.py`
- `launch_cc_pointer_build.py` → `src/.../ccindex/launch_cc_pointer_build.py`
- `watchdog_cc_pointer_build.py` → `src/.../ccindex/watchdog_cc_pointer_build.py`
- `watchdog_monitor.py` → `src/.../ccindex/watchdog_monitor.py`

**WARC Tools (2 files):**
- `download_warc_records.py` → `src/.../ccindex/download_warc_records.py`
- `warc_candidates_from_jsonl.py` → `src/.../ccindex/warc_candidates_from_jsonl.py`

**Municipal Scraping (2 files):**
- `orchestrate_municipal_scrape.py` → `src/municipal_scrape_workspace/orchestrate_municipal_scrape.py`
- `check_archive_callbacks.py` → `src/municipal_scrape_workspace/check_archive_callbacks.py`

#### Shell Scripts (1 file)
- `bootstrap.sh` - ✅ **KEEP** (unique setup script)

#### Configuration Files (4 files)
- `pyproject.toml` - ✅ **KEEP** (package configuration)
- `pytest.ini` - ✅ **KEEP** (test configuration)
- `constraints.txt` - ✅ **KEEP** (dependency constraints)
- `collinfo.json` - ✅ **KEEP** (Common Crawl runtime config)
- `pipeline_config.json` - ✅ **KEEP** (pipeline settings)

#### Documentation Files (7 files)
- `README.md` - ✅ **KEEP** (main entry point)
- `QUICKSTART.md` - ✅ **KEEP** (quick start guide)
- `REFACTORED_STRUCTURE.md` - ✅ **KEEP** (structure guide)
- `REFACTORING_PROJECT_SUMMARY.md` - ✅ **KEEP** (refactoring summary)
- `REFACTORING_VALIDATION.md` - ✅ **KEEP** (validation report)
- `REORGANIZATION_PLAN.md` - ✅ **KEEP** (reorganization plan)
- `REORGANIZATION_COMPLETE.md` - ✅ **KEEP** (completion summary)

---

## 🔄 Two Reorganization Options

### Option A: Keep Wrappers (Current State - Recommended)

**Pros:**
- ✅ Full backward compatibility for existing users
- ✅ Shorter commands: `./search_cc_domain.py --domain example.com`
- ✅ No migration needed for existing scripts
- ✅ Familiar to users who have been using the repo

**Cons:**
- ⚠️ 41 extra files in root directory
- ⚠️ Two ways to do everything (confusing for new users)
- ⚠️ Must maintain wrappers alongside canonical implementations

**Root directory after:** ~60 items (current state)

---

### Option B: Remove Wrappers (Cleaner Structure)

**Pros:**
- ✅ Much cleaner root directory (19 items vs 60)
- ✅ Single source of truth (canonical implementations only)
- ✅ Forces proper Python package usage
- ✅ More professional/standard Python project structure

**Cons:**
- ⚠️ Breaking change for existing users
- ⚠️ Longer commands required
- ⚠️ Migration guide required
- ⚠️ Existing scripts/workflows will break

**Root directory after:** ~19 items

**What stays:**
```
municipal_scrape_workspace/
├── bootstrap.sh                    # Setup script
├── pyproject.toml                  # Package config
├── pytest.ini                      # Test config
├── constraints.txt                 # Dependencies
├── collinfo.json                   # CC config
├── pipeline_config.json            # Pipeline config
├── README.md                       # Main docs
├── QUICKSTART.md                   # Quick start
├── REFACTORED_STRUCTURE.md         # Structure guide
├── REFACTORING_PROJECT_SUMMARY.md  # Summary
├── REFACTORING_VALIDATION.md       # Validation
├── REORGANIZATION_PLAN.md          # Plan
├── REORGANIZATION_COMPLETE.md      # Completion
├── .gitignore                      # Git ignore
├── data/                           # Data files
├── src/                            # Source code
├── scripts/                        # Shell scripts
├── tests/                          # Tests
└── docs/                           # Documentation
```

---

## 📋 File Location Map (Final State)

### All Python Tools Location

| Tool Category | Canonical Location | Console Script | Python Module |
|---------------|-------------------|----------------|---------------|
| **Search Tools** | | | |
| Domain search | `src/.../ccindex/search_cc_domain.py` | `ccindex-search-domain` | `python -m municipal_scrape_workspace.ccindex.search_cc_domain` |
| DuckDB search | `src/.../ccindex/search_cc_duckdb_index.py` | `ccindex-search-duckdb` | `python -m municipal_scrape_workspace.ccindex.search_cc_duckdb_index` |
| Pointer search | `src/.../ccindex/search_cc_pointer_index.py` | `ccindex-search-pointer` | `python -m municipal_scrape_workspace.ccindex.search_cc_pointer_index` |
| Meta-index search | `src/.../ccindex/search_cc_via_meta_indexes.py` | `ccindex-search` | `python -m municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes` |
| Parallel search | `src/.../ccindex/search_parallel_duckdb_indexes.py` | `ccindex-search-parallel` | `python -m municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes` |
| Domain locator | `src/.../ccindex/cc_domain_parquet_locator.py` | N/A | `python -m municipal_scrape_workspace.ccindex.cc_domain_parquet_locator` |
| **Build Tools** | | | |
| Pointer index | `src/.../ccindex/build_cc_pointer_duckdb.py` | `ccindex-build-pointer` | `python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb` |
| Generic index | `src/.../ccindex/build_index_from_parquet.py` | N/A | `python -m municipal_scrape_workspace.ccindex.build_index_from_parquet` |
| Parallel indexes | `src/.../ccindex/build_parallel_duckdb_indexes.py` | `ccindex-build-parallel` | `python -m municipal_scrape_workspace.ccindex.build_parallel_duckdb_indexes` |
| Rowgroup index | `src/.../ccindex/build_cc_parquet_rowgroup_index.py` | N/A | `python -m municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index` |
| Master index | `src/.../ccindex/build_master_index.py` | N/A | `python -m municipal_scrape_workspace.ccindex.build_master_index` |
| Year meta-indexes | `src/.../ccindex/build_year_meta_indexes.py` | `ccindex-build-meta` | `python -m municipal_scrape_workspace.ccindex.build_year_meta_indexes` |
| **Orchestration** | | | |
| Pipeline orchestrator | `src/.../ccindex/cc_pipeline_orchestrator.py` | `ccindex-orchestrate` | `python -m municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator` |
| Pipeline watch | `src/.../ccindex/cc_pipeline_watch.py` | `ccindex-watch` | `python -m municipal_scrape_workspace.ccindex.cc_pipeline_watch` |
| Pipeline HUD | `src/.../ccindex/cc_pipeline_hud.py` | `ccindex-hud` | `python -m municipal_scrape_workspace.ccindex.cc_pipeline_hud` |
| **Monitoring** | | | |
| Progress monitor | `src/.../ccindex/monitor_progress.py` | N/A | `python -m municipal_scrape_workspace.ccindex.monitor_progress` |
| Pointer build monitor | `src/.../ccindex/monitor_cc_pointer_build.py` | N/A | `python -m municipal_scrape_workspace.ccindex.monitor_cc_pointer_build` |
| Pointer status | `src/.../ccindex/cc_pointer_status.py` | N/A | `python -m municipal_scrape_workspace.ccindex.cc_pointer_status` |
| **Validation** | | | |
| Validate & sort | `src/.../ccindex/validate_and_sort_parquet.py` | `ccindex-validate-parquet` | `python -m municipal_scrape_workspace.ccindex.validate_and_sort_parquet` |
| Parallel validate | `src/.../ccindex/parallel_validate_parquet.py` | N/A | `python -m municipal_scrape_workspace.ccindex.parallel_validate_parquet` |
| Sort validation | `src/.../ccindex/validate_urlindex_sorted.py` | N/A | `python -m municipal_scrape_workspace.ccindex.validate_urlindex_sorted` |
| Collection complete | `src/.../ccindex/validate_collection_completeness.py` | `ccindex-validate` | `python -m municipal_scrape_workspace.ccindex.validate_collection_completeness` |
| Search complete | `src/.../ccindex/validate_search_completeness.py` | N/A | `python -m municipal_scrape_workspace.ccindex.validate_search_completeness` |
| WARC validate | `src/.../ccindex/validate_warc_record_blobs.py` | N/A | `python -m municipal_scrape_workspace.ccindex.validate_warc_record_blobs` |

---

## 🔧 Import Refactoring Requirements

### Files Already Refactored ✅

All 41 Python files in root are already thin wrappers with proper imports:

```python
#!/usr/bin/env python3
"""Backwards-compatible wrapper."""

from municipal_scrape_workspace.ccindex.MODULE_NAME import main

if __name__ == "__main__":
    raise SystemExit(main())
```

### Canonical Implementations ✅

All canonical files in `src/municipal_scrape_workspace/ccindex/` use proper package imports:

```python
from municipal_scrape_workspace.ccindex.other_module import helper_function
from municipal_scrape_workspace.ccindex.another_module import SomeClass
```

**No sys.path hacks** ✅ - All imports are clean and proper

---

## 🚨 Gaps After Reorganization

### 1. Documentation Updates

**Current Status:** ⚠️ **PARTIAL**

Files that reference wrapper usage patterns:
- `README.md` - Shows both wrapper and module usage
- `QUICKSTART.md` - Shows wrapper usage
- `REFACTORED_STRUCTURE.md` - Documents both approaches
- `docs/COMMON_CRAWL_USAGE.md` - May have wrapper examples

**If removing wrappers (Option B):**
- [ ] Update all documentation to remove wrapper examples
- [ ] Update all code examples to use Python module format
- [ ] Update shell script examples to use console scripts
- [ ] Create migration guide for existing users

### 2. Testing Coverage

**Current Status:** ⚠️ **PARTIAL**

```
tests/
├── conftest.py
├── test_ccindex/
│   ├── test_cli.py
│   ├── test_wrappers.py      # Tests wrapper functionality
│   └── test_imports.py
└── test_municipal_scrape/
```

**If removing wrappers (Option B):**
- [ ] Remove or update `test_wrappers.py`
- [ ] Ensure all tools have direct module tests
- [ ] Add console script tests

### 3. CI/CD Pipeline

**Current Status:** ⚠️ **NOT CONFIGURED**

**Needed:**
- [ ] GitHub Actions workflow for testing
- [ ] Automated check that no temporary files committed
- [ ] Automated import validation
- [ ] Package installation test

### 4. Migration Guide

**Current Status:** ❌ **NOT CREATED**

**If removing wrappers (Option B), need to document:**

#### For Shell Scripts
```bash
# OLD (will break after wrapper removal)
./search_cc_domain.py --domain example.com

# NEW Option 1: Python module
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com

# NEW Option 2: Console script (after pip install)
ccindex-search-domain --domain example.com
```

#### For Python Code
```python
# OLD (will break)
import sys
sys.path.insert(0, '/path/to/repo')
import search_cc_domain

# NEW Option 1: Direct import (after pip install -e .)
from municipal_scrape_workspace.ccindex import search_cc_domain

# NEW Option 2: Submodule import
from municipal_scrape_workspace.ccindex.search_cc_domain import main
```

### 5. Package Installation

**Current Status:** ✅ **WORKING**

Package installs cleanly:
```bash
pip install -e .                # Basic
pip install -e '.[ccindex]'     # With CC index tools
pip install -e '.[dev]'         # With dev tools
```

Console scripts are configured in `pyproject.toml`.

### 6. Data File References

**Current Status:** ✅ **RESOLVED**

CSV file already moved to `data/us_towns_and_counties_urls.csv`

Any code referencing it needs to use:
```python
csv_path = "data/us_towns_and_counties_urls.csv"
# or relative from repo root
import os
repo_root = os.path.dirname(os.path.dirname(__file__))
csv_path = os.path.join(repo_root, "data", "us_towns_and_counties_urls.csv")
```

### 7. Shell Scripts in scripts/ops/

**Current Status:** ✅ **ORGANIZED**

All 32 operational shell scripts are in `scripts/ops/` directory.

Users should call them as:
```bash
scripts/ops/download_cc_indexes.sh
scripts/ops/overnight_build_duckdb_index.sh
```

**Optional Enhancement:** Add to PATH or create aliases in a convenience script.

---

## 🎯 Recommended Action Plan

### Recommendation: **Keep Option A (Current State)**

**Reasoning:**
1. ✅ Backward compatibility is valuable for existing users
2. ✅ Wrappers are thin (10-14 lines each) - minimal maintenance burden
3. ✅ No breaking changes required
4. ✅ Provides convenience without sacrificing clean package structure
5. ✅ Package can still be properly installed and used as a library

### Alternative: If choosing Option B (Remove Wrappers)

**Execute in this order:**

#### Phase 1: Preparation (Low Risk)
- [ ] Create comprehensive migration guide
- [ ] Update all documentation to remove wrapper examples
- [ ] Add deprecation warnings to wrappers (if doing gradual migration)
- [ ] Communicate changes to users

#### Phase 2: Testing (Medium Risk)
- [ ] Verify all console scripts work correctly
- [ ] Test Python module invocation for all tools
- [ ] Update test suite to reflect new structure
- [ ] Run full test suite

#### Phase 3: Execution (High Risk - Breaking Change)
- [ ] Create backup branch
- [ ] Remove all 41 wrapper files from root
- [ ] Update .gitignore if needed
- [ ] Commit changes
- [ ] Test installation from fresh clone

#### Phase 4: Validation (Critical)
- [ ] Verify package installs cleanly
- [ ] Test all console scripts
- [ ] Test all Python module invocations
- [ ] Verify documentation accuracy
- [ ] Check that no broken links exist

---

## 📊 Final Directory Structures

### Option A (Current - Keep Wrappers)

```
municipal_scrape_workspace/
├── bootstrap.sh                          # Setup script
├── pyproject.toml                        # Package config
├── pytest.ini                            # Test config  
├── constraints.txt                       # Dependencies
├── collinfo.json                         # CC config
├── pipeline_config.json                  # Pipeline config
│
├── README.md                             # Main docs
├── QUICKSTART.md
├── REFACTORED_STRUCTURE.md
├── REFACTORING_PROJECT_SUMMARY.md
├── REFACTORING_VALIDATION.md
├── REORGANIZATION_PLAN.md
├── REORGANIZATION_COMPLETE.md
├── FINAL_REORGANIZATION_README.md        # This file
│
├── search_cc_domain.py                   # 41 wrapper files
├── build_cc_pointer_duckdb.py            # (10-14 lines each)
├── ... (39 more)
│
├── data/
│   └── us_towns_and_counties_urls.csv
│
├── src/
│   └── municipal_scrape_workspace/
│       ├── __init__.py
│       ├── cli.py
│       ├── orchestrate_municipal_scrape.py
│       ├── check_archive_callbacks.py
│       └── ccindex/                      # 39 canonical modules
│           ├── search_cc_domain.py
│           ├── build_cc_pointer_duckdb.py
│           └── ... (37 more)
│
├── scripts/
│   └── ops/                              # 32 shell scripts
│
├── tests/
├── docs/
├── benchmarks/
├── archive/
└── logs/
```

**Item count:** ~60 items in root

---

### Option B (Remove Wrappers - Cleaner)

```
municipal_scrape_workspace/
├── bootstrap.sh                          # Setup script
├── pyproject.toml                        # Package config
├── pytest.ini                            # Test config
├── constraints.txt                       # Dependencies
├── collinfo.json                         # CC config
├── pipeline_config.json                  # Pipeline config
│
├── README.md                             # Main docs
├── QUICKSTART.md
├── REFACTORED_STRUCTURE.md
├── REFACTORING_PROJECT_SUMMARY.md
├── REFACTORING_VALIDATION.md
├── REORGANIZATION_PLAN.md
├── REORGANIZATION_COMPLETE.md
├── FINAL_REORGANIZATION_README.md        # This file
├── MIGRATION_GUIDE.md                    # New - for users
│
├── data/
│   └── us_towns_and_counties_urls.csv
│
├── src/
│   └── municipal_scrape_workspace/
│       ├── __init__.py
│       ├── cli.py
│       ├── orchestrate_municipal_scrape.py
│       ├── check_archive_callbacks.py
│       └── ccindex/                      # 39 canonical modules
│           ├── search_cc_domain.py
│           ├── build_cc_pointer_duckdb.py
│           └── ... (37 more)
│
├── scripts/
│   └── ops/                              # 32 shell scripts
│
├── tests/
├── docs/
├── benchmarks/
├── archive/
└── logs/
```

**Item count:** ~19 items in root (68% reduction from 60)

---

## ✅ Decision Matrix

| Criteria | Option A (Keep) | Option B (Remove) | Winner |
|----------|----------------|-------------------|--------|
| **Backward Compatibility** | ✅ Perfect | ❌ Breaking change | A |
| **Root Cleanliness** | ⚠️ 60 items | ✅ 19 items | B |
| **User Convenience** | ✅ Short commands | ⚠️ Longer commands | A |
| **Maintenance Burden** | ⚠️ 41 extra files | ✅ Minimal | B |
| **Professional Structure** | ⚠️ Non-standard | ✅ Standard Python | B |
| **Migration Effort** | ✅ None | ❌ Significant | A |
| **Risk Level** | ✅ Zero risk | ⚠️ High risk | A |
| **Documentation Update** | ✅ Minimal | ❌ Extensive | A |

**Score:** Option A = 6, Option B = 4

**Recommendation:** **Option A (Keep wrappers)** unless there's a compelling reason for the breaking change.

---

## 🚀 Execution Checklist

### If Keeping Wrappers (Option A - Recommended)

- [x] Verify all wrappers function correctly
- [x] Ensure canonical implementations exist
- [x] Confirm package installs correctly
- [x] Documentation reflects dual usage patterns
- [x] No action needed - current state is acceptable

### If Removing Wrappers (Option B - Cleaner)

- [ ] Create `MIGRATION_GUIDE.md`
- [ ] Update `README.md` to remove wrapper examples
- [ ] Update `QUICKSTART.md` to use module/console script patterns
- [ ] Update `REFACTORED_STRUCTURE.md` to reflect new state
- [ ] Update any code examples in `docs/`
- [ ] Remove `tests/test_ccindex/test_wrappers.py` or update it
- [ ] Create backup branch: `git checkout -b backup/before-wrapper-removal`
- [ ] Remove all 41 wrapper .py files from root
- [ ] Test package installation: `pip install -e .`
- [ ] Test console scripts: `ccindex-search-domain --help`
- [ ] Test Python modules: `python -m municipal_scrape_workspace.ccindex.search_cc_domain --help`
- [ ] Commit changes
- [ ] Update this file's status to COMPLETE

---

## 📞 Questions & Considerations

### Q: Will removing wrappers break existing workflows?
**A:** Yes, if users have scripts that call `./search_cc_domain.py` directly. They'll need to migrate to either:
- Python module: `python -m municipal_scrape_workspace.ccindex.search_cc_domain`
- Console script: `ccindex-search-domain` (after pip install)

### Q: Can we do a gradual deprecation?
**A:** Yes! Add deprecation warnings to wrappers:
```python
import warnings
warnings.warn(
    "Direct wrapper usage is deprecated. "
    "Use 'python -m municipal_scrape_workspace.ccindex.MODULE' instead.",
    DeprecationWarning,
    stacklevel=2
)
```

### Q: What about symlinks instead of removal?
**A:** Symlinks could work but:
- Still clutter root directory
- Don't work well on Windows
- Don't teach users proper Python package usage

### Q: Can we make console scripts match wrapper names?
**A:** Yes! Update `pyproject.toml`:
```toml
[project.scripts]
search_cc_domain = "municipal_scrape_workspace.ccindex.search_cc_domain:main"
# Keeps the same command name, no .py extension needed
```

Then users can just type: `search_cc_domain --domain example.com`

---

## 🎉 Conclusion

The repository structure is **already well-organized** after Phase 1 refactoring. The decision now is whether to:

1. **Keep the current state (Option A)** - Pragmatic, user-friendly, backward compatible
2. **Remove wrappers (Option B)** - Cleaner structure, more professional, but breaking change

**My Recommendation:** **Keep Option A** unless there's a strong reason for the breaking change. The wrappers provide valuable convenience with minimal cost.

If you do proceed with Option B, follow the execution checklist carefully and communicate changes to users well in advance.

---

**Status**: 📋 PLANNING - Awaiting Decision  
**Next Step**: Choose Option A or Option B, then execute corresponding checklist  
**Risk Level**: Option A = 🟢 LOW | Option B = 🟠 MEDIUM
