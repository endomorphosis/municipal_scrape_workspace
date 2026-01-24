# Root Folder Reorganization Plan

**Date**: 2026-01-20  
**Status**: 📋 PLANNING → READY FOR EXECUTION  
**Goal**: Clean up root directory by moving files to their permanent locations

---

## 📊 Executive Summary

The repository has **successfully refactored Python code** but still has **cleanup needed** in the root directory:

**Current State:**
- ✅ **41 Python wrappers** - Properly implemented (10-14 lines each)
- ⚠️ **27 shell script wrappers** - Redundant duplicates of scripts/ops/
- ⚠️ **5 configuration files** - Some need relocation
- ⚠️ **1 temporary file** - Should not be in repo
- ✅ **5 markdown docs** - Already organized

**Target State:**
- Keep only essential files in root (bootstrap, configs, docs)
- Remove duplicate shell script wrappers
- Move data files to data/ directory
- Archive temporary/obsolete files
- Update any hardcoded paths

---

## 📁 Complete Root Directory Analysis

### Current Root Directory Contents (93 items)

#### ✅ Python Files (41 files) - KEEP AS-IS
All are thin wrappers (10-14 lines) forwarding to `src/municipal_scrape_workspace/ccindex/`:
```
build_*.py (7 files)
search_*.py (6 files)
validate_*.py (6 files)
monitor_*.py (4 files)
cc_*.py (7 files)
orchestrate_*.py (1 file)
check_*.py (1 file)
download_*.py (1 file)
extract_*.py (1 file)
launch_*.py (1 file)
parallel_*.py (2 files)
queue_*.py (1 file)
regenerate_*.py (1 file)
sample_*.py (1 file)
sort_*.py (2 files)
verify_*.py (1 file)
warc_*.py (1 file)
watchdog_*.py (2 files)
```

**Status**: ✅ **NO ACTION NEEDED** - Already properly refactored
**Reason**: Provides backwards compatibility for users' existing scripts

---

#### ⚠️ Shell Scripts (27 files) - CONSOLIDATE/REMOVE

All 27 shell scripts in root are **wrapper scripts** that simply delegate to `scripts/ops/`:

**Pattern (26 files):**
```bash
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/scripts/ops/<script_name>.sh" "$@"
```

**Exception (1 file):**
- `bootstrap.sh` - **UNIQUE** setup script (no counterpart in scripts/ops/)

**Complete List:**
1. `bootstrap.sh` ← **UNIQUE - KEEP**
2. `check_1year_status.sh` ← wrapper
3. `check_2year_status.sh` ← wrapper
4. `check_cc_download_status.sh` ← wrapper
5. `cleanup_space.sh` ← wrapper
6. `comprehensive_rebuild.sh` ← wrapper
7. `download_cc_indexes.sh` ← wrapper
8. `download_cc_indexes_1year.sh` ← wrapper
9. `download_cc_indexes_2years.sh` ← wrapper
10. `download_cc_indexes_5years.sh` ← wrapper
11. `final_rebuild.sh` ← wrapper
12. `manage_cc_2year.sh` ← wrapper
13. `monitor_cc_2year_download.sh` ← wrapper
14. `monitor_dual_run.sh` ← wrapper
15. `monitor_overnight_build.sh` ← wrapper
16. `monitor_overnight_duckdb.sh` ← wrapper
17. `monitor_progress.sh` ← wrapper
18. `overnight_build_duckdb_index.sh` ← wrapper
19. `overnight_build_duckdb_pointer.sh` ← wrapper
20. `overnight_build_pointer_index.sh` ← wrapper
21. `overnight_duckdb_complete.sh` ← wrapper
22. `overnight_parallel_index_build.sh` ← wrapper
23. `overnight_sort_and_index.sh` ← wrapper
24. `parallel_rebuild.sh` ← wrapper
25. `prune_ccindex_zfs_autosnapshots.sh` ← wrapper
26. `quickref_duckdb.sh` ← wrapper
27. `quickstart_duckdb_index.sh` ← wrapper
28. `rebuild_overnight.sh` ← wrapper
29. `rebuild_with_sorted_ranges.sh` ← wrapper
30. `redownload_quarantined_ccindex_shards.sh` ← wrapper
31. `sort_unsorted_sequential.sh` ← wrapper
32. `start_overnight_reindex.sh` ← wrapper
33. `verify_parquet_sorted.sh` ← wrapper

**Status**: ⚠️ **ACTION REQUIRED**  
**Decision**: **REMOVE WRAPPERS** - Users should call `scripts/ops/<name>.sh` directly  
**Reason**: Reduces clutter, eliminates maintenance burden, makes structure clearer

**Migration Path:**
- Keep `bootstrap.sh` (unique functionality)
- Remove 26 wrapper scripts from root
- Update documentation to reference `scripts/ops/` directly
- Consider creating a `Makefile` with convenient targets

---

#### 📋 Configuration Files (5 files)

| File | Type | Purpose | Decision |
|------|------|---------|----------|
| `collinfo.json` | JSON | Common Crawl collection metadata (2009-2025) | ✅ **KEEP in root** |
| `pipeline_config.json` | JSON | Pipeline storage paths, DuckDB settings | ✅ **KEEP in root** |
| `constraints.txt` | TXT | Python pip dependency constraints | ✅ **KEEP in root** |
| `us_towns_and_counties_urls.csv` | CSV | Municipal scraping target list | ⚠️ **MOVE to data/** |
| `watchdog.pid` | PID | Watchdog process ID | ❌ **DELETE** |

**Details:**

**KEEP in root:**
- `collinfo.json` - Runtime configuration for Common Crawl data access
- `pipeline_config.json` - Core pipeline settings (paths, memory limits)
- `constraints.txt` - Dependency constraints for pip install

**MOVE to data/ directory:**
- `us_towns_and_counties_urls.csv` - Reference data, not configuration
  - Create `data/` directory if needed
  - This is input data for municipal scraping

**DELETE:**
- `watchdog.pid` - Temporary runtime artifact
  - Should not be in version control
  - Add `*.pid` to `.gitignore`

---

#### 📚 Documentation Files - KEEP AS-IS

```
README.md                          ← Main entry point
QUICKSTART.md                      ← Quick start guide
MIGRATION_GUIDE.md                 ← Root wrapper removal + command migration map
docs/README.md                     ← Docs index (by component)
docs/REPO_STRUCTURE.md             ← Current package/layout guide

docs/municipal_scrape_workspace/refactoring/          ← Refactoring/migration docs
docs/municipal_scrape_workspace/reorganization/       ← Historical reorganization plans/notes
```

**Status**: ✅ **NO ACTION NEEDED**  
**Reason**: Already well-organized and essential

---

#### ⚙️ Build/Package Files (3 files) - KEEP AS-IS

```
pyproject.toml    ← Package configuration
pytest.ini        ← Test configuration
bootstrap.sh      ← Environment setup
```

**Status**: ✅ **NO ACTION NEEDED**  
**Reason**: Standard Python project files, belong in root

---

## 🎯 Reorganization Actions

### Phase 1: Safety Preparations
- [x] Create this plan document
- [x] Verify all shell scripts in root are wrappers (check content)
- [x] Confirm scripts/ops/ has all actual implementations

### Phase 2: File Moves
- [x] Create `data/` directory
- [x] Move `us_towns_and_counties_urls.csv` to `data/`
- [x] Update documentation references to the CSV file location

### Phase 3: File Deletions
- [x] Delete `watchdog.pid`
- [x] Delete 32 shell script wrappers (keep only `bootstrap.sh`)

### Phase 4: .gitignore Updates
- [x] Add `*.pid` to `.gitignore`
- [x] Clean up `.gitignore` formatting

### Phase 5: Documentation Updates
- [ ] Update README.md with new structure
- [ ] Update REFACTORED_STRUCTURE.md with cleanup details
- [ ] Update QUICKSTART.md with correct script paths
- [ ] Add note about `scripts/ops/` being the source of truth

### Phase 6: Validation
- [ ] Verify `bootstrap.sh` still works
- [ ] Test sample scripts from `scripts/ops/` directly
- [ ] Check that git status is clean
- [ ] Run basic functionality tests

---

## 📋 Detailed File Migration Map

### Files to DELETE

| File | Reason | Replacement |
|------|--------|-------------|
| `watchdog.pid` | Temporary runtime artifact | Add to .gitignore |
| `check_1year_status.sh` | Wrapper | Use `scripts/ops/check_1year_status.sh` |
| `check_2year_status.sh` | Wrapper | Use `scripts/ops/check_2year_status.sh` |
| `check_cc_download_status.sh` | Wrapper | Use `scripts/ops/check_cc_download_status.sh` |
| `cleanup_space.sh` | Wrapper | Use `scripts/ops/cleanup_space.sh` |
| `comprehensive_rebuild.sh` | Wrapper | Use `scripts/ops/comprehensive_rebuild.sh` |
| `download_cc_indexes.sh` | Wrapper | Use `scripts/ops/download_cc_indexes.sh` |
| `download_cc_indexes_1year.sh` | Wrapper | Use `scripts/ops/download_cc_indexes_1year.sh` |
| `download_cc_indexes_2years.sh` | Wrapper | Use `scripts/ops/download_cc_indexes_2years.sh` |
| `download_cc_indexes_5years.sh` | Wrapper | Use `scripts/ops/download_cc_indexes_5years.sh` |
| `final_rebuild.sh` | Wrapper | Use `scripts/ops/final_rebuild.sh` |
| `manage_cc_2year.sh` | Wrapper | Use `scripts/ops/manage_cc_2year.sh` |
| `monitor_cc_2year_download.sh` | Wrapper | Use `scripts/ops/monitor_cc_2year_download.sh` |
| `monitor_dual_run.sh` | Wrapper | Use `scripts/ops/monitor_dual_run.sh` |
| `monitor_overnight_build.sh` | Wrapper | Use `scripts/ops/monitor_overnight_build.sh` |
| `monitor_overnight_duckdb.sh` | Wrapper | Use `scripts/ops/monitor_overnight_duckdb.sh` |
| `monitor_progress.sh` | Wrapper | Use `scripts/ops/monitor_progress.sh` |
| `overnight_build_duckdb_index.sh` | Wrapper | Use `scripts/ops/overnight_build_duckdb_index.sh` |
| `overnight_build_duckdb_pointer.sh` | Wrapper | Use `scripts/ops/overnight_build_duckdb_pointer.sh` |
| `overnight_build_pointer_index.sh` | Wrapper | Use `scripts/ops/overnight_build_pointer_index.sh` |
| `overnight_duckdb_complete.sh` | Wrapper | Use `scripts/ops/overnight_duckdb_complete.sh` |
| `overnight_parallel_index_build.sh` | Wrapper | Use `scripts/ops/overnight_parallel_index_build.sh` |
| `overnight_sort_and_index.sh` | Wrapper | Use `scripts/ops/overnight_sort_and_index.sh` |
| `parallel_rebuild.sh` | Wrapper | Use `scripts/ops/parallel_rebuild.sh` |
| `prune_ccindex_zfs_autosnapshots.sh` | Wrapper | Use `scripts/ops/prune_ccindex_zfs_autosnapshots.sh` |
| `quickref_duckdb.sh` | Wrapper | Use `scripts/ops/quickref_duckdb.sh` |
| `quickstart_duckdb_index.sh` | Wrapper | Use `scripts/ops/quickstart_duckdb_index.sh` |
| `rebuild_overnight.sh` | Wrapper | Use `scripts/ops/rebuild_overnight.sh` |
| `rebuild_with_sorted_ranges.sh` | Wrapper | Use `scripts/ops/rebuild_with_sorted_ranges.sh` |
| `redownload_quarantined_ccindex_shards.sh` | Wrapper | Use `scripts/ops/redownload_quarantined_ccindex_shards.sh` |
| `sort_unsorted_sequential.sh` | Wrapper | Use `scripts/ops/sort_unsorted_sequential.sh` |
| `start_overnight_reindex.sh` | Wrapper | Use `scripts/ops/start_overnight_reindex.sh` |
| `verify_parquet_sorted.sh` | Wrapper | Use `scripts/ops/verify_parquet_sorted.sh` |

### Files to MOVE

| From | To | Reason |
|------|-----|--------|
| `us_towns_and_counties_urls.csv` | `data/us_towns_and_counties_urls.csv` | Reference data, not config |

### Files to KEEP in Root

| File | Type | Reason |
|------|------|--------|
| `bootstrap.sh` | Shell | Unique setup script |
| `pyproject.toml` | Config | Package definition |
| `pytest.ini` | Config | Test configuration |
| `constraints.txt` | Config | Dependency constraints |
| `collinfo.json` | Config | CC runtime config |
| `pipeline_config.json` | Config | Pipeline settings |
| `README.md` | Doc | Main entry point |
| `QUICKSTART.md` | Doc | Quick start |
| `REFACTORED_STRUCTURE.md` | Doc | Structure guide |
| `REFACTORING_PROJECT_SUMMARY.md` | Doc | Summary |
| `REFACTORING_VALIDATION.md` | Doc | Validation |
| All 41 Python wrappers | Code | Backwards compatibility |

---

## 🔧 Import/Path Refactoring Requirements

### Files Referencing us_towns_and_counties_urls.csv

Need to search for:
```bash
grep -r "us_towns_and_counties_urls.csv" . --include="*.py" --include="*.sh"
```

**Expected files to update:**
- `src/municipal_scrape_workspace/orchestrate_municipal_scrape.py`
- Any scripts in `scripts/ops/` that reference it
- Documentation mentioning the file location

**Update pattern:**
```python
# OLD
csv_path = "us_towns_and_counties_urls.csv"
csv_path = os.path.join(os.path.dirname(__file__), "../../us_towns_and_counties_urls.csv")

# NEW
csv_path = "data/us_towns_and_counties_urls.csv"
csv_path = os.path.join(os.path.dirname(__file__), "../../data/us_towns_and_counties_urls.csv")
```

### Shell Script References

**Documentation to update:**
- README.md - Change references from `./script.sh` to `scripts/ops/script.sh`
- QUICKSTART.md - Update command examples
- Any scripts/ops/*.sh that call other scripts

**Pattern:**
```bash
# OLD
./download_cc_indexes.sh

# NEW
scripts/ops/download_cc_indexes.sh
```

---

## 🚨 Gaps to Fill After Reorganization

### 1. .gitignore Updates
**Gap**: Runtime artifacts not ignored  
**Action**: Add patterns to .gitignore:
```
*.pid
watchdog.pid
*.log
logs/*.log
```

### 2. Documentation Updates
**Gap**: Shell script paths in documentation  
**Action**: Update all docs to reference `scripts/ops/` paths

### 3. Makefile/Task Runner (Optional)
**Gap**: Convenience wrappers for common tasks  
**Action**: Consider creating a Makefile:
```makefile
.PHONY: bootstrap download-cc build-index

bootstrap:
	./bootstrap.sh

download-cc:
	scripts/ops/download_cc_indexes.sh

build-index:
	scripts/ops/overnight_build_duckdb_index.sh
```

### 4. Migration Guide
**Gap**: Users need to know about path changes  
**Action**: Create MIGRATION_GUIDE.md or update CHANGELOG

### 5. CSV File Discovery
**Gap**: Code needs to find CSV in new location  
**Action**: 
- Use package resources API for installed package
- Use relative path from project root for dev mode
- Document environment variable override (e.g., `MUNICIPAL_CSV_PATH`)

---

## 📊 Final Directory Structure

```
municipal_scrape_workspace/
│
├── 📄 bootstrap.sh                    # ✅ UNIQUE setup script
├── 📄 pyproject.toml                  # ✅ Package config
├── 📄 pytest.ini                      # ✅ Test config
├── 📄 constraints.txt                 # ✅ Dependency constraints
├── 📄 collinfo.json                   # ✅ CC runtime config
├── 📄 pipeline_config.json            # ✅ Pipeline config
│
├── 📚 README.md                       # ✅ Main docs
├── 📚 QUICKSTART.md
├── 📚 REFACTORED_STRUCTURE.md
├── 📚 REFACTORING_PROJECT_SUMMARY.md
├── 📚 REFACTORING_VALIDATION.md
├── 📚 REORGANIZATION_PLAN.md          # 🆕 THIS FILE
│
├── 🐍 <41 Python wrappers>.py         # ✅ Backwards compat
│
├── 📁 data/                           # 🆕 NEW DIRECTORY
│   └── us_towns_and_counties_urls.csv
│
├── 📁 src/
│   └── municipal_scrape_workspace/
│       ├── __init__.py
│       ├── cli.py
│       ├── orchestrate_municipal_scrape.py
│       ├── check_archive_callbacks.py
│       └── ccindex/                   # 40 modules
│
├── 📁 scripts/
│   └── ops/                           # 🎯 ALL shell scripts here
│       ├── download_cc_indexes.sh
│       ├── overnight_build_*.sh
│       └── ... (30+ operational scripts)
│
├── 📁 tests/
├── 📁 docs/
├── 📁 benchmarks/
├── 📁 archive/
└── 📁 logs/
```

**Root directory reduction:**
- Before: 93 items
- After: ~55 items (removed 26 shell wrappers + 1 pid + moved 1 csv)
- Reduction: 41% fewer root items

---

## ✅ Benefits of This Reorganization

1. **Cleaner Root** - Essential files only (configs, bootstrap, docs, Python wrappers)
2. **Single Source of Truth** - All shell scripts in `scripts/ops/`
3. **Better Organization** - Data files in `data/`, not mixed with configs
4. **No Duplicates** - Eliminates 26 wrapper scripts
5. **Clearer Structure** - Obvious where to find operational scripts
6. **Easier Maintenance** - Fewer files to manage in root
7. **Better Gitignore** - Temporary files properly excluded

---

## 🎯 Success Criteria

- [ ] Root directory has ~55 files (down from 93)
- [ ] Only `bootstrap.sh` remains as root shell script
- [ ] `data/` directory created with CSV file
- [ ] No `*.pid` files in git
- [ ] All imports/paths updated for CSV location
- [ ] Documentation reflects new structure
- [ ] Bootstrap still works
- [ ] Sample scripts from `scripts/ops/` work
- [ ] Tests pass (if any)
- [ ] Clean git status

---

## 📝 Notes for Implementation

### Backwards Compatibility Considerations

**Python wrappers**: KEEP ALL 41 - provides backwards compatibility
**Shell wrappers**: REMOVE - users can adapt to `scripts/ops/` paths

**Justification**:
- Python wrappers are imported/called from code (harder to change)
- Shell wrappers are called from command line (easier to change)
- Shell scripts in `scripts/ops/` are more discoverable
- Reduces maintenance burden significantly

### Testing Strategy

1. **Before changes**: Test that `scripts/ops/download_cc_indexes.sh` works
2. **After deletion**: Verify same script still works
3. **CSV move**: Test municipal scrape tool finds data file
4. **Documentation**: Verify all doc links work

### Rollback Plan

If issues arise:
1. Restore from `backup/before-root-cleanup` branch
2. Or restore individual files from git history
3. CSV move is easily reversible

---

## 🚀 Ready to Execute

This plan is comprehensive and ready for execution. All actions are clearly defined with minimal risk and maximum benefit.

**Next Step**: Begin Phase 1 (Safety Preparations)

---

**Document Status**: ✅ COMPLETE  
**Ready for Execution**: ✅ YES  
**Risk Level**: 🟢 LOW (mostly deletions of wrapper scripts)
