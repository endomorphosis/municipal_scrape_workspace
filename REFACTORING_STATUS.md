# Refactoring Status Dashboard

**Last Updated**: 2026-01-19  
**Purpose**: At-a-glance view of refactoring progress

---

## 📊 Overall Progress

```
┌─────────────────────────────────────────────────────────────┐
│                    File Migration Progress                   │
├─────────────────────────────────────────────────────────────┤
│  ✅ Completed:     ████████████████████░░░░░░  19/52 (37%)  │
│  ⚠️  Needs Fix:    ██░░░░░░░░░░░░░░░░░░░░░░░   4/52 (8%)   │
│  📦 To Migrate:   █████████░░░░░░░░░░░░░░░░░  17/52 (33%)  │
│  🗄️  To Archive:   ████░░░░░░░░░░░░░░░░░░░░░   7/52 (13%)  │
│  ❓ Evaluate:      ███░░░░░░░░░░░░░░░░░░░░░░   5/52 (10%)  │
└─────────────────────────────────────────────────────────────┘
```

**Actionable Items**: 28 files (4 fixes + 17 migrations + 7 archives)  
**Estimated Effort**: 6-8 hours focused work

---

## 🎯 Priority Queue

### 🔥 Critical Path (Must Do First)

#### Week 1: Foundation (2-3 hours)
```
[Phase 1] Fix Missing Wrappers                           ⚠️ PRIORITY 0
├─ build_cc_parquet_rowgroup_index.py          [15 min]
├─ bulk_convert_gz_to_parquet.py               [15 min]
├─ validate_search_completeness.py             [15 min]
└─ validate_urlindex_sorted.py                 [15 min]

[Phase 2] Core Orchestration                             📦 PRIORITY 1
├─ cc_pipeline_orchestrator.py                 [45 min]
├─ cc_pipeline_watch.py                        [30 min]
├─ cc_pipeline_hud.py                          [30 min]
└─ monitor_progress.py                         [30 min]
```

#### Week 2: Infrastructure (3-4 hours)
```
[Phase 3] Queue/Watchdog Tools                           📦 PRIORITY 2
├─ queue_cc_pointer_build.py                   [20 min]
├─ launch_cc_pointer_build.py                  [20 min]
├─ monitor_cc_pointer_build.py                 [20 min]
├─ watchdog_cc_pointer_build.py                [20 min]
├─ watchdog_monitor.py                         [20 min]
└─ cc_pointer_status.py                        [20 min]

[Phase 4] Conversion Tools                               📦 PRIORITY 3
├─ parallel_convert_missing.py                 [20 min]
├─ regenerate_parquet_from_gz.py               [20 min]
├─ sample_ccindex_to_parquet.py                [20 min]
├─ extract_cc_index_tarballs.py                [20 min]
└─ build_duckdb_pointer_from_parquet.py        [20 min]
```

#### Week 3: Cleanup (2 hours)
```
[Phase 5] Municipal Scrape                               📦 PRIORITY 3
├─ orchestrate_municipal_scrape.py             [45 min]
└─ check_archive_callbacks.py                  [30 min]

[Phase 6] Archive Superseded                             🗄️ PRIORITY 4
├─ Move 7 files to archive/                    [20 min]
└─ Create archive README                       [10 min]

[Phase 7] Evaluate & Decide                              ❓ PRIORITY 5
└─ Review 5 ambiguous files                    [60 min]
```

---

## 📁 Directory Structure Before/After

### Before (Current - Cluttered Root)
```
/home/runner/work/municipal_scrape_workspace/
├─ 52 Python files at root ❌ (mix of full impl + wrappers)
├─ src/municipal_scrape_workspace/ccindex/
│  └─ 23 canonical implementations
├─ scripts/ops/ (clean ✅)
├─ benchmarks/ccindex/ (clean ✅)
└─ archive/ccindex/converters/ (partial)
```

### After (Target - Clean Organization)
```
/home/runner/work/municipal_scrape_workspace/
├─ ~40 thin wrappers at root ✅ (backwards compat)
├─ src/municipal_scrape_workspace/
│  ├─ cli.py
│  ├─ orchestrate_municipal_scrape.py
│  ├─ check_archive_callbacks.py
│  └─ ccindex/
│     ├─ ~40 canonical implementations ✅
│     ├─ (orchestration)
│     ├─ (conversion)
│     ├─ (indexing)
│     ├─ (searching)
│     ├─ (validation)
│     ├─ (monitoring)
│     └─ (warc retrieval)
├─ scripts/ops/ (unchanged ✅)
├─ benchmarks/ccindex/ (unchanged ✅)
└─ archive/ccindex/
   ├─ converters/ (existing ✅)
   └─ superseded/ (7 files added)
```

---

## 🔍 File Status by Category

### CCIndex Tools

#### Orchestration & Monitoring
| File | Status | Location |
|------|--------|----------|
| cc_pipeline_orchestrator.py | 📦 To Migrate | → src/.../ccindex/ |
| cc_pipeline_watch.py | 📦 To Migrate | → src/.../ccindex/ |
| cc_pipeline_hud.py | 📦 To Migrate | → src/.../ccindex/ |
| cc_pipeline_manager.py | 🗄️ Archive | → archive/.../superseded/ |
| monitor_progress.py | 📦 To Migrate | → src/.../ccindex/ |
| monitor_cc_pointer_build.py | 📦 To Migrate | → src/.../ccindex/ |
| cc_pointer_status.py | 📦 To Migrate | → src/.../ccindex/ |

#### Queue & Watchdog
| File | Status | Location |
|------|--------|----------|
| queue_cc_pointer_build.py | 📦 To Migrate | → src/.../ccindex/ |
| launch_cc_pointer_build.py | 📦 To Migrate | → src/.../ccindex/ |
| watchdog_cc_pointer_build.py | 📦 To Migrate | → src/.../ccindex/ |
| watchdog_monitor.py | 📦 To Migrate | → src/.../ccindex/ |

#### Conversion
| File | Status | Location |
|------|--------|----------|
| bulk_convert_gz_to_parquet.py | ⚠️ Fix Wrapper | src/.../ccindex/ ✅ |
| parallel_convert_missing.py | 📦 To Migrate | → src/.../ccindex/ |
| regenerate_parquet_from_gz.py | 📦 To Migrate | → src/.../ccindex/ |
| sample_ccindex_to_parquet.py | 📦 To Migrate | → src/.../ccindex/ |
| extract_cc_index_tarballs.py | 📦 To Migrate | → src/.../ccindex/ |

#### Sorting
| File | Status | Location |
|------|--------|----------|
| sort_cc_parquet_shards.py | ✅ Done | src/.../ccindex/ ✅ |
| sort_unsorted_memory_aware.py | ❓ Evaluate | TBD |
| sort_unsorted_files.py | 🗄️ Archive | → archive/.../superseded/ |
| sort_parquet_external_merge.py | 🗄️ Archive | → archive/.../superseded/ |

#### Validation
| File | Status | Location |
|------|--------|----------|
| validate_and_sort_parquet.py | ✅ Done | src/.../ccindex/ ✅ |
| parallel_validate_parquet.py | ✅ Done | src/.../ccindex/ ✅ |
| validate_collection_completeness.py | ✅ Done | src/.../ccindex/ ✅ |
| validate_search_completeness.py | ⚠️ Fix Wrapper | src/.../ccindex/ ✅ |
| validate_urlindex_sorted.py | ⚠️ Fix Wrapper | src/.../ccindex/ ✅ |
| validate_and_mark_sorted.py | ❓ Evaluate | TBD |

#### Index Building
| File | Status | Location |
|------|--------|----------|
| build_cc_pointer_duckdb.py | ✅ Done | src/.../ccindex/ ✅ |
| build_cc_parquet_rowgroup_index.py | ⚠️ Fix Wrapper | src/.../ccindex/ ✅ |
| build_index_from_parquet.py | ✅ Done | src/.../ccindex/ ✅ |
| build_parallel_duckdb_indexes.py | ✅ Done | src/.../ccindex/ ✅ |
| build_duckdb_pointer_from_parquet.py | 📦 To Migrate | → src/.../ccindex/ |
| build_year_meta_indexes.py | ✅ Done | src/.../ccindex/ ✅ |
| build_master_index.py | ✅ Done | src/.../ccindex/ ✅ |
| build_duckdb_from_sorted_parquet.py | ❓ Evaluate | TBD |

#### Searching
| File | Status | Location |
|------|--------|----------|
| search_cc_domain.py | ✅ Done | src/.../ccindex/ ✅ |
| search_cc_duckdb_index.py | ✅ Done | src/.../ccindex/ ✅ |
| search_cc_pointer_index.py | ✅ Done | src/.../ccindex/ ✅ |
| search_cc_via_meta_indexes.py | ✅ Done | src/.../ccindex/ ✅ |
| search_parallel_duckdb_indexes.py | ✅ Done | src/.../ccindex/ ✅ |
| cc_domain_parquet_locator.py | ✅ Done | src/.../ccindex/ ✅ |
| search_domain_duckdb_pointer.py | 🗄️ Archive | → archive/.../superseded/ |
| search_domain_pointer_index.py | 🗄️ Archive | → archive/.../superseded/ |
| search_duckdb_domain.py | 🗄️ Archive | → archive/.../superseded/ |
| search_duckdb_pointer_domain.py | 🗄️ Archive | → archive/.../superseded/ |

#### WARC Retrieval
| File | Status | Location |
|------|--------|----------|
| download_warc_records.py | ✅ Done | src/.../ccindex/ ✅ |
| verify_warc_retrieval.py | ✅ Done | src/.../ccindex/ ✅ |
| validate_warc_record_blobs.py | ✅ Done | src/.../ccindex/ ✅ |
| warc_candidates_from_jsonl.py | ✅ Done | src/.../ccindex/ ✅ |

### Municipal Scrape Tools

| File | Status | Location |
|------|--------|----------|
| orchestrate_municipal_scrape.py | 📦 To Migrate | → src/municipal_scrape_workspace/ |
| check_archive_callbacks.py | 📦 To Migrate | → src/municipal_scrape_workspace/ |

### Utilities

| File | Status | Location |
|------|--------|----------|
| consolidate_parquet_files.py | ❓ Evaluate | TBD |
| compare_crawl_results.py | ❓ Evaluate | TBD |

---

## 🚦 Quality Gates

### ✅ Phase Complete When:

**Phase 1 (Fix Wrappers):**
- [ ] All 4 root files are thin wrappers
- [ ] Each wrapper imports from src/
- [ ] `./tool.py --help` works for each

**Phase 2-5 (Migrations):**
- [ ] File moved to src/
- [ ] Imports updated to package imports
- [ ] No sys.path hacks remain
- [ ] main() function exists
- [ ] Root wrapper created
- [ ] Both wrapper and module import work

**Phase 6 (Archive):**
- [ ] 7 files moved to archive/ccindex/superseded/
- [ ] Archive README created explaining why
- [ ] Documentation updated

**Phase 7 (Evaluate):**
- [ ] Each file reviewed
- [ ] Keep/archive decision made
- [ ] Decision executed

### ✅ Project Complete When:

- [ ] All 52 files processed
- [ ] No full implementations at root (only wrappers)
- [ ] All src/ files use package imports
- [ ] All wrappers execute successfully
- [ ] Documentation reflects new structure
- [ ] Tests pass (if any)
- [ ] `pip install -e .` works
- [ ] `pip install -e '.[ccindex]'` works

---

## 📚 Documentation Hierarchy

```
Quick Reference:
├─ FILE_MIGRATION_MAP.md ........... File-by-file lookup table
├─ REFACTORING_QUICKSTART.md ....... Quick patterns & workflows
└─ REFACTORING_STATUS.md ........... This file (dashboard)

Comprehensive:
├─ REFACTORING_ROADMAP.md .......... Complete guide (683 lines)
├─ REPO_LAYOUT_README.md ........... Layout conventions
└─ docs/REPO_LAYOUT_PLAN.md ........ Detailed plan

Project:
└─ README.md ....................... Main project README
```

---

## 🐛 Known Issues

| Issue | Impact | Fix Complexity |
|-------|--------|----------------|
| ipfs_datasets_py hardcoded path in pyproject.toml | Not portable | Easy - change to git URL |
| orchestrate_municipal_scrape.py sys.path hack | Not portable | Easy - add env var |
| Missing dev/test dependencies | No testing setup | Easy - add [dev] extra |
| 4 files in src/ without wrappers | Confusing | Easy - convert to wrappers |

---

## 📈 Success Metrics

**Completion Rate**: 37% (19/52 files)  
**Remaining Work**: ~6-8 hours focused effort  
**Technical Debt**: Reduced by ~65% after completion  
**Maintainability**: Significantly improved

---

**Status**: Documentation phase complete. Ready for execution.

For detailed instructions, see:
- [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md) - Complete guide
- [REFACTORING_QUICKSTART.md](REFACTORING_QUICKSTART.md) - Quick reference
- [FILE_MIGRATION_MAP.md](FILE_MIGRATION_MAP.md) - File lookup table
