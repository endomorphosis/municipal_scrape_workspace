# Refactoring Execution Checklist

**Purpose**: Step-by-step checklist to execute the refactoring plan  
**Estimated Time**: 6-8 hours  
**Status**: Ready for execution

---

## Pre-Flight Checklist

Before starting, ensure:

- [ ] Working in a clean git branch: `git checkout -b refactor-migrations`
- [ ] Virtual environment activated: `source .venv/bin/activate`
- [ ] All current tests passing (if any): `pytest` or relevant test command
- [ ] Clean working directory: `git status` shows no uncommitted changes
- [ ] Have reviewed [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md)

---

## Phase 1: Fix Missing Wrappers ⚠️ (15 min)

**Goal**: Convert 4 root files that are already in src/ to thin wrappers

### File 1: build_cc_parquet_rowgroup_index.py

- [ ] Backup current content: `cp build_cc_parquet_rowgroup_index.py /tmp/backup_build_cc_parquet.py`
- [ ] Verify file exists in src/: `ls -la src/municipal_scrape_workspace/ccindex/build_cc_parquet_rowgroup_index.py`
- [ ] Replace root file with wrapper:
```python
#!/usr/bin/env python3
"""Backwards-compatible wrapper for CC Parquet rowgroup index builder.

Moved to:
  municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index
"""

from municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index import main

if __name__ == "__main__":
    raise SystemExit(main())
```
- [ ] Test wrapper: `./build_cc_parquet_rowgroup_index.py --help`
- [ ] Test module: `python -m municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index --help`
- [ ] Commit: `git add . && git commit -m "Convert build_cc_parquet_rowgroup_index.py to wrapper"`

### File 2: bulk_convert_gz_to_parquet.py

- [ ] Backup: `cp bulk_convert_gz_to_parquet.py /tmp/backup_bulk_convert.py`
- [ ] Verify in src/: `ls -la src/municipal_scrape_workspace/ccindex/bulk_convert_gz_to_parquet.py`
- [ ] Replace with wrapper (change module name appropriately)
- [ ] Test: `./bulk_convert_gz_to_parquet.py --help`
- [ ] Test module: `python -m municipal_scrape_workspace.ccindex.bulk_convert_gz_to_parquet --help`
- [ ] Commit: `git add . && git commit -m "Convert bulk_convert_gz_to_parquet.py to wrapper"`

### File 3: validate_search_completeness.py

- [ ] Backup: `cp validate_search_completeness.py /tmp/backup_validate_search.py`
- [ ] Verify in src/: `ls -la src/municipal_scrape_workspace/ccindex/validate_search_completeness.py`
- [ ] Replace with wrapper
- [ ] Test: `./validate_search_completeness.py --help`
- [ ] Test module: `python -m municipal_scrape_workspace.ccindex.validate_search_completeness --help`
- [ ] Commit: `git add . && git commit -m "Convert validate_search_completeness.py to wrapper"`

### File 4: validate_urlindex_sorted.py

- [ ] Backup: `cp validate_urlindex_sorted.py /tmp/backup_validate_urlindex.py`
- [ ] Verify in src/: `ls -la src/municipal_scrape_workspace/ccindex/validate_urlindex_sorted.py`
- [ ] Replace with wrapper
- [ ] Test: `./validate_urlindex_sorted.py --help`
- [ ] Test module: `python -m municipal_scrape_workspace.ccindex.validate_urlindex_sorted --help`
- [ ] Commit: `git add . && git commit -m "Convert validate_urlindex_sorted.py to wrapper"`

**Phase 1 Complete**: Push changes: `git push origin refactor-migrations`

---

## Phase 2: Migrate Core Orchestration 📦 (2 hours)

**Goal**: Move main pipeline orchestration tools to src/

### File 1: cc_pipeline_orchestrator.py

- [ ] Review file imports: `grep -E "^import |^from " cc_pipeline_orchestrator.py`
- [ ] Note any intra-repo imports that need updating
- [ ] Move file: `git mv cc_pipeline_orchestrator.py src/municipal_scrape_workspace/ccindex/`
- [ ] Edit src/.../ccindex/cc_pipeline_orchestrator.py:
  - [ ] Update imports from intra-repo to package imports
  - [ ] Remove any `sys.path.insert()` lines
  - [ ] Ensure `main(argv=None) -> int` function exists
  - [ ] Update docstring with new location
- [ ] Create root wrapper cc_pipeline_orchestrator.py
- [ ] Test wrapper: `./cc_pipeline_orchestrator.py --help`
- [ ] Test module: `python -m municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator --help`
- [ ] Run quick smoke test if possible
- [ ] Commit: `git add . && git commit -m "Migrate cc_pipeline_orchestrator.py to package"`

### File 2: cc_pipeline_watch.py

- [ ] Review imports: `grep -E "^import |^from " cc_pipeline_watch.py`
- [ ] Move: `git mv cc_pipeline_watch.py src/municipal_scrape_workspace/ccindex/`
- [ ] Update imports in moved file
- [ ] Remove sys.path hacks
- [ ] Ensure main() exists
- [ ] Create wrapper
- [ ] Test both execution methods
- [ ] Commit: `git add . && git commit -m "Migrate cc_pipeline_watch.py to package"`

### File 3: cc_pipeline_hud.py

- [ ] Review imports
- [ ] Move file
- [ ] Update imports
- [ ] Remove sys.path hacks
- [ ] Ensure main() exists
- [ ] Create wrapper
- [ ] Test
- [ ] Commit: `git add . && git commit -m "Migrate cc_pipeline_hud.py to package"`

### File 4: monitor_progress.py

- [ ] Review imports
- [ ] Move file
- [ ] Update imports
- [ ] Remove sys.path hacks
- [ ] Ensure main() exists
- [ ] Create wrapper
- [ ] Test
- [ ] Commit: `git add . && git commit -m "Migrate monitor_progress.py to package"`

**Phase 2 Complete**: Push changes: `git push origin refactor-migrations`

---

## Phase 3: Migrate Queue/Watchdog Tools 📦 (1.5 hours)

### File 1: queue_cc_pointer_build.py

- [ ] Review imports
- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 2: launch_cc_pointer_build.py

- [ ] Review imports
- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 3: monitor_cc_pointer_build.py

- [ ] Review imports
- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 4: watchdog_cc_pointer_build.py

- [ ] Review imports
- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 5: watchdog_monitor.py

- [ ] Review imports
- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 6: cc_pointer_status.py

- [ ] Review imports
- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

**Phase 3 Complete**: Push changes: `git push origin refactor-migrations`

---

## Phase 4: Migrate Conversion Tools 📦 (1 hour)

### File 1: parallel_convert_missing.py

- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 2: regenerate_parquet_from_gz.py

- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 3: sample_ccindex_to_parquet.py

- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 4: extract_cc_index_tarballs.py

- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

### File 5: build_duckdb_pointer_from_parquet.py

- [ ] Move to src/.../ccindex/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

**Phase 4 Complete**: Push changes: `git push origin refactor-migrations`

---

## Phase 5: Migrate Municipal Scrape Tools 📦 (1 hour)

### File 1: orchestrate_municipal_scrape.py

- [ ] Review imports (especially ipfs_datasets_py usage)
- [ ] Move to src/municipal_scrape_workspace/
- [ ] Update imports
- [ ] Fix hardcoded sys.path for ipfs_datasets_py:
  - [ ] Add: `ipfs_root = os.environ.get("IPFS_DATASETS_PY_ROOT")`
  - [ ] Conditional sys.path.insert only if env var set
- [ ] Create wrapper
- [ ] Test
- [ ] Commit: `git add . && git commit -m "Migrate orchestrate_municipal_scrape.py with env var support"`

### File 2: check_archive_callbacks.py

- [ ] Move to src/municipal_scrape_workspace/
- [ ] Update imports
- [ ] Create wrapper
- [ ] Test
- [ ] Commit

**Phase 5 Complete**: Push changes: `git push origin refactor-migrations`

---

## Phase 6: Archive Superseded Files 🗄️ (30 min)

### Setup Archive Directory

- [ ] Create directory: `mkdir -p archive/ccindex/superseded`
- [ ] Create archive/ccindex/superseded/README.md:
```markdown
# Superseded CCIndex Tools

This directory contains tools that have been superseded by newer implementations
or are no longer actively maintained.

## Files

- cc_pipeline_manager.py - Superseded by cc_pipeline_orchestrator.py
- search_domain_duckdb_pointer.py - Duplicate of search_parallel_duckdb_indexes.py
- search_domain_pointer_index.py - Functionality covered by canonical search tools
- search_duckdb_domain.py - Functionality covered by canonical search tools
- search_duckdb_pointer_domain.py - Functionality covered by canonical search tools
- sort_unsorted_files.py - Superseded by sort_unsorted_memory_aware.py
- sort_parquet_external_merge.py - Functionality covered by canonical sorters

These files are kept for historical reference and debugging purposes only.
```

### Archive Files

- [ ] Move cc_pipeline_manager.py: `git mv cc_pipeline_manager.py archive/ccindex/superseded/`
- [ ] Move search_domain_duckdb_pointer.py: `git mv search_domain_duckdb_pointer.py archive/ccindex/superseded/`
- [ ] Move search_domain_pointer_index.py: `git mv search_domain_pointer_index.py archive/ccindex/superseded/`
- [ ] Move search_duckdb_domain.py: `git mv search_duckdb_domain.py archive/ccindex/superseded/`
- [ ] Move search_duckdb_pointer_domain.py: `git mv search_duckdb_pointer_domain.py archive/ccindex/superseded/`
- [ ] Move sort_unsorted_files.py: `git mv sort_unsorted_files.py archive/ccindex/superseded/`
- [ ] Move sort_parquet_external_merge.py: `git mv sort_parquet_external_merge.py archive/ccindex/superseded/`
- [ ] Commit: `git add . && git commit -m "Archive 7 superseded files to archive/ccindex/superseded/"`

**Phase 6 Complete**: Push changes: `git push origin refactor-migrations`

---

## Phase 7: Evaluate Ambiguous Files ❓ (1 hour)

### File 1: consolidate_parquet_files.py

- [ ] Review file purpose and usage
- [ ] Check if referenced in any docs or scripts: `grep -r "consolidate_parquet_files" .`
- [ ] Decision: Keep & migrate OR Archive
- [ ] If keep: Follow migration workflow
- [ ] If archive: Move to archive/ccindex/superseded/
- [ ] Commit decision

### File 2: compare_crawl_results.py

- [ ] Review purpose
- [ ] Check references
- [ ] Decision: Keep & migrate OR Archive
- [ ] Execute decision
- [ ] Commit

### File 3: validate_and_mark_sorted.py

- [ ] Compare with validate_and_sort_parquet.py
- [ ] Determine if duplicate or complementary
- [ ] Decision: Keep & migrate OR Archive
- [ ] Execute decision
- [ ] Commit

### File 4: build_duckdb_from_sorted_parquet.py

- [ ] Compare with build_duckdb_pointer_from_parquet.py
- [ ] Determine relationship
- [ ] Decision: Keep & migrate OR Archive
- [ ] Execute decision
- [ ] Commit

### File 5: sort_unsorted_memory_aware.py

- [ ] Review as potential canonical sorter
- [ ] Decision: Keep & migrate OR Archive
- [ ] Execute decision
- [ ] Commit

**Phase 7 Complete**: Push changes: `git push origin refactor-migrations`

---

## Final Validation ✅

### Structural Validation

- [ ] All root .py files are wrappers (except those archived)
- [ ] No full implementations at root: `for f in *.py; do [ ! -L "$f" ] && head -5 "$f" | grep -q wrapper || echo "FULL: $f"; done`
- [ ] All wrappers are executable: `chmod +x *.py`
- [ ] Archive directory properly structured

### Import Validation

- [ ] No sys.path hacks in src/: `grep -r "sys.path.insert" src/`
- [ ] All imports use package imports: Review manually or with linter
- [ ] No hardcoded paths (except documented env vars)

### Functional Validation

- [ ] Test core tools:
  - [ ] `./search_cc_domain.py --help`
  - [ ] `./build_cc_pointer_duckdb.py --help`
  - [ ] `./cc_pipeline_orchestrator.py --help`
  - [ ] `./orchestrate_municipal_scrape.py --help`
- [ ] Test module imports:
  - [ ] `python -m municipal_scrape_workspace.ccindex.search_cc_domain --help`
  - [ ] `python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --help`
- [ ] Test installation:
  - [ ] `pip install -e .` (should work without ccindex deps)
  - [ ] `pip install -e '.[ccindex]'` (should enable all tools)

### Documentation Validation

- [ ] README.md reflects new structure
- [ ] REFACTORING_ROADMAP.md is up to date
- [ ] All migration docs are accurate
- [ ] Archive README explains archived files

### Test Suite (if exists)

- [ ] Run all tests: `pytest` or relevant command
- [ ] All tests pass
- [ ] No regressions introduced

---

## Post-Migration Cleanup

### Optional: Fix pyproject.toml Dependency

- [ ] Review current ipfs_datasets_py dependency
- [ ] Decide on solution:
  - Option A: Git URL dependency
  - Option B: Optional extra with env var
  - Option C: Wait for PyPI publish
- [ ] Update pyproject.toml
- [ ] Test installation
- [ ] Commit: `git commit -m "Update ipfs_datasets_py dependency"`

### Final Commit and PR

- [ ] Review all changes: `git log --oneline origin/main..HEAD`
- [ ] Ensure all tests pass
- [ ] Push final changes: `git push origin refactor-migrations`
- [ ] Create PR against main branch
- [ ] Add description linking to REFACTORING_ROADMAP.md
- [ ] Request review

---

## Rollback Plan (If Needed)

If something goes wrong:

```bash
# Abort and return to clean state
git checkout main
git branch -D refactor-migrations

# Or revert specific commits
git revert <commit-hash>
```

---

## Success Criteria

✅ **Migration is complete when:**

1. All 52 root Python files are processed
2. All root files are thin wrappers or archived
3. All src/ files use package imports (no sys.path hacks)
4. All tools execute via wrapper: `./tool.py --help`
5. All tools execute via module: `python -m municipal_scrape_workspace.ccindex.tool`
6. Documentation is updated
7. Tests pass (if any)
8. `pip install -e .` works
9. `pip install -e '.[ccindex]'` enables CC tools

---

**Status**: Ready for execution. Good luck! 🚀

**Estimated Time**: 6-8 hours focused work  
**Difficulty**: Medium (mostly mechanical, some judgment needed)  
**Risk**: Low (all changes are additive, wrappers maintain backward compatibility)
