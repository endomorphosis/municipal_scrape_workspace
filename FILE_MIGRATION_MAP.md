# File Migration Map

**Purpose**: Quick lookup table showing where each root Python file will live after refactoring.  
**Status**: As of 2026-01-19  
**Legend**: ✅ Done | ⚠️ Needs Fix | 📦 To Migrate | 🗄️ To Archive | ❓ Needs Decision

---

## Root Python Files (52 total)

| Status | Current Location (Root) | Final Location | Action Required |
|--------|------------------------|----------------|-----------------|
| ✅ | build_cc_pointer_duckdb.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | build_index_from_parquet.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | build_master_index.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | build_parallel_duckdb_indexes.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | build_year_meta_indexes.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | cc_domain_parquet_locator.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | download_warc_records.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | parallel_validate_parquet.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | search_cc_domain.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | search_cc_duckdb_index.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | search_cc_pointer_index.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | search_cc_via_meta_indexes.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | search_parallel_duckdb_indexes.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | sort_cc_parquet_shards.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | validate_and_sort_parquet.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | validate_collection_completeness.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | validate_warc_record_blobs.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | verify_warc_retrieval.py | src/.../ccindex/ | None - wrapper exists |
| ✅ | warc_candidates_from_jsonl.py | src/.../ccindex/ | None - wrapper exists |
| ⚠️ | build_cc_parquet_rowgroup_index.py | src/.../ccindex/ | Convert root to wrapper |
| ⚠️ | bulk_convert_gz_to_parquet.py | src/.../ccindex/ | Convert root to wrapper |
| ⚠️ | validate_search_completeness.py | src/.../ccindex/ | Convert root to wrapper |
| ⚠️ | validate_urlindex_sorted.py | src/.../ccindex/ | Convert root to wrapper |
| 📦 | cc_pipeline_orchestrator.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | cc_pipeline_watch.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | cc_pipeline_hud.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | monitor_progress.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | monitor_cc_pointer_build.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | cc_pointer_status.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | queue_cc_pointer_build.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | launch_cc_pointer_build.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | watchdog_cc_pointer_build.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | watchdog_monitor.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | parallel_convert_missing.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | regenerate_parquet_from_gz.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | sample_ccindex_to_parquet.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | extract_cc_index_tarballs.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | build_duckdb_pointer_from_parquet.py | src/.../ccindex/ | Migrate + create wrapper |
| 📦 | orchestrate_municipal_scrape.py | src/municipal_scrape_workspace/ | Migrate + create wrapper |
| 📦 | check_archive_callbacks.py | src/municipal_scrape_workspace/ | Migrate + create wrapper |
| 🗄️ | cc_pipeline_manager.py | archive/ccindex/superseded/ | Archive (superseded by orchestrator) |
| 🗄️ | search_domain_duckdb_pointer.py | archive/ccindex/superseded/ | Archive (duplicate) |
| 🗄️ | search_domain_pointer_index.py | archive/ccindex/superseded/ | Archive (duplicate) |
| 🗄️ | search_duckdb_domain.py | archive/ccindex/superseded/ | Archive (duplicate) |
| 🗄️ | search_duckdb_pointer_domain.py | archive/ccindex/superseded/ | Archive (duplicate) |
| 🗄️ | sort_unsorted_files.py | archive/ccindex/superseded/ | Archive (superseded) |
| 🗄️ | sort_parquet_external_merge.py | archive/ccindex/superseded/ | Archive (superseded) |
| ❓ | consolidate_parquet_files.py | TBD | Evaluate if actively used |
| ❓ | compare_crawl_results.py | TBD | Evaluate if reusable utility |
| ❓ | validate_and_mark_sorted.py | TBD | Compare vs validate_and_sort_parquet.py |
| ❓ | build_duckdb_from_sorted_parquet.py | TBD | Compare vs build_duckdb_pointer_from_parquet.py |
| ❓ | sort_unsorted_memory_aware.py | src/.../ccindex/ OR archive | Evaluate as canonical sorter |

---

## Import Dependencies After Migration

### Files That Import Other Migrated Files

These files will need import updates when migrated:

| File | Imports (needs update) |
|------|------------------------|
| cc_pipeline_orchestrator.py | validate_collection_completeness |
| cc_pipeline_watch.py | monitor_progress |
| orchestrate_municipal_scrape.py | ipfs_datasets_py (via sys.path) |

---

## Shell Scripts Status

All operational shell scripts already live in `scripts/ops/` with root-level wrappers for backwards compatibility. No action needed.

**Examples**:
- `scripts/ops/overnight_build_duckdb_index.sh` (canonical)
- `./overnight_build_duckdb_index.sh` (wrapper)

---

## Priority Order for Migration

### Phase 1: Fix Wrappers (Quick - ~15 min)
1. build_cc_parquet_rowgroup_index.py
2. bulk_convert_gz_to_parquet.py
3. validate_search_completeness.py
4. validate_urlindex_sorted.py

### Phase 2: Core Orchestration (High Priority - ~2 hours)
1. cc_pipeline_orchestrator.py
2. cc_pipeline_watch.py
3. cc_pipeline_hud.py
4. monitor_progress.py

### Phase 3: Queue/Watchdog Tools (~1.5 hours)
1. queue_cc_pointer_build.py
2. launch_cc_pointer_build.py
3. monitor_cc_pointer_build.py
4. watchdog_cc_pointer_build.py
5. watchdog_monitor.py
6. cc_pointer_status.py

### Phase 4: Conversion Tools (~1 hour)
1. parallel_convert_missing.py
2. regenerate_parquet_from_gz.py
3. sample_ccindex_to_parquet.py
4. extract_cc_index_tarballs.py

### Phase 5: Municipal Scrape (~1 hour)
1. orchestrate_municipal_scrape.py
2. check_archive_callbacks.py

### Phase 6: Archive (~30 min)
1. Move 7 files to archive/ccindex/superseded/
2. Create archive/ccindex/superseded/README.md

### Phase 7: Evaluate & Decide (~1 hour)
1. Review 5 ambiguous files
2. Make keep/archive decisions
3. Execute decisions

---

## Success Criteria

Migration is complete when:

- [ ] All 52 root Python files are either:
  - Thin wrappers importing from src/, OR
  - Archived in archive/ccindex/
- [ ] All src/ files use package imports (no sys.path hacks)
- [ ] All wrappers execute: `./tool.py --help` works
- [ ] Module import works: `python -m municipal_scrape_workspace.ccindex.tool --help`
- [ ] Documentation updated
- [ ] Tests pass (if any)

**Estimated Total Time**: 6-8 hours of focused work

---

## Quick Reference Links

- 📘 [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md) - Comprehensive guide with all details
- 📄 [REFACTORING_QUICKSTART.md](REFACTORING_QUICKSTART.md) - Quick reference and patterns
- 📋 [REPO_LAYOUT_README.md](REPO_LAYOUT_README.md) - Original layout documentation
- 📖 [docs/REPO_LAYOUT_PLAN.md](docs/REPO_LAYOUT_PLAN.md) - Detailed layout plan
