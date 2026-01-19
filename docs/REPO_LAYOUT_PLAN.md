# Repo Layout Plan

Status: mostly implemented. Canonical operational shell scripts live under `scripts/ops/` with root-level wrappers preserved for backwards compatibility.

This repo currently has two large “themes” living side-by-side in the top-level directory:

1) **Municipal scraping workflow** (the installable Python package `municipal_scrape_workspace`).
2) **Common Crawl (CC) pointer-index pipeline** (Parquet + DuckDB + meta-indexes + verification + ops scripts).

The goal of this plan is to:
- reduce top-level clutter
- make “what is production vs experimental” obvious
- eliminate `sys.path` hacks in favor of package imports
- keep backwards-compatible wrappers for operational scripts

---

## Target Top-Level Layout

```text
.
├── src/municipal_scrape_workspace/
│   ├── cli.py
│   ├── ccindex/                         # CC pointer-index tooling (package)
│   │   ├── __init__.py
│   │   ├── validate_collection_completeness.py
│   │   └── … (more modules migrated over time)
│   └── …
├── scripts/
│   ├── ops/                             # shell scripts (download, rebuild, monitor)
│   └── …                                # reserved for other script groupings
├── benchmarks/
│   └── ccindex/                         # benchmark_*.py
├── docs/                                # design docs, quickrefs, runbooks
├── datasets/
├── content_blobs/
├── logs/
└── archive/                             # one-off or superseded scripts
```

Notes:
- `src/…` is the *importable* code.
- `scripts/…` is for operational entrypoints.
- `archive/…` is for scripts kept only for provenance / debugging.

---

## What “permanent” means

A file is “permanent” if it is one of:
- a core pipeline stage (convert/sort/index/validate/build meta-indexes)
- a verifier or retrieval tool used to prove end-to-end functionality
- a monitoring / orchestration tool actively used in runs

A file is “archive” if it is:
- a one-off conversion attempt (e.g. `archive/ccindex/converters/convert_missing_17.py`)
- a dated log-like script variant (e.g. `*_correct.py`, `*_simple.py`) where one canonical tool exists
- an experimental benchmark or comparison that is no longer referenced

---

## Immediate Changes Already Applied

- `validate_collection_completeness.py` has been migrated into the package as:
  - `src/municipal_scrape_workspace/ccindex/validate_collection_completeness.py`
- A backwards-compatible wrapper remains at the old location:
  - `validate_collection_completeness.py`
- `cc_pipeline_orchestrator.py` now imports the validator via the package:
  - `from municipal_scrape_workspace.ccindex.validate_collection_completeness import CollectionValidator`
- Fixed a bug in the validator’s directory scanning that referenced `self.duckdb_dir` (nonexistent). It now uses the configured `pointer_dir`.

Additional migrations completed:
- `search_cc_via_meta_indexes.py` → `src/municipal_scrape_workspace/ccindex/search_cc_via_meta_indexes.py`
- `build_year_meta_indexes.py` → `src/municipal_scrape_workspace/ccindex/build_year_meta_indexes.py`
- `build_master_index.py` → `src/municipal_scrape_workspace/ccindex/build_master_index.py`
- WARC helpers:
  - `warc_candidates_from_jsonl.py` → `src/municipal_scrape_workspace/ccindex/warc_candidates_from_jsonl.py`
  - `verify_warc_retrieval.py` → `src/municipal_scrape_workspace/ccindex/verify_warc_retrieval.py`
  - `download_warc_records.py` → `src/municipal_scrape_workspace/ccindex/download_warc_records.py`
  - `validate_warc_record_blobs.py` → `src/municipal_scrape_workspace/ccindex/validate_warc_record_blobs.py`

Backwards-compatible wrappers remain at the repo root for all of the above.

More migrations completed:
- `build_parallel_duckdb_indexes.py` → `src/municipal_scrape_workspace/ccindex/build_parallel_duckdb_indexes.py`
- `search_parallel_duckdb_indexes.py` → `src/municipal_scrape_workspace/ccindex/search_parallel_duckdb_indexes.py`
- `search_cc_domain.py` → `src/municipal_scrape_workspace/ccindex/search_cc_domain.py`

More migrations completed (core pointer-index build/search):
- `build_index_from_parquet.py` → `src/municipal_scrape_workspace/ccindex/build_index_from_parquet.py`
- `build_cc_pointer_duckdb.py` → `src/municipal_scrape_workspace/ccindex/build_cc_pointer_duckdb.py`
- `search_cc_pointer_index.py` → `src/municipal_scrape_workspace/ccindex/search_cc_pointer_index.py`
- `search_cc_duckdb_index.py` → `src/municipal_scrape_workspace/ccindex/search_cc_duckdb_index.py`
- `cc_domain_parquet_locator.py` → `src/municipal_scrape_workspace/ccindex/cc_domain_parquet_locator.py`

---

## Dependency Gaps to Close

The CC tooling imports heavy dependencies that weren’t declared in `pyproject.toml`.
To make this explicit, an optional extra is added:

- Install with: `pip install -e '.[ccindex]'`

This extra includes `duckdb`, `pyarrow`, `psutil`, and `requests`.

---

## Proposed File Moves (High Value)

### Keep + Move to Package (core library code)
These are good candidates to migrate into `src/municipal_scrape_workspace/ccindex/` over time:
- `cc_pipeline_orchestrator.py` (eventually becomes `municipal_scrape_workspace.ccindex.orchestrator`)
- `build_year_meta_indexes.py` and `build_master_index.py`
- `search_cc_via_meta_indexes.py`
- `download_warc_records.py`, `verify_warc_retrieval.py`, `validate_warc_record_blobs.py`, `warc_candidates_from_jsonl.py`

Pattern for migration:
1) `git mv tool.py src/municipal_scrape_workspace/ccindex/tool.py`
2) leave a top-level wrapper `tool.py` that calls `from municipal_scrape_workspace.ccindex.tool import main`
3) update internal imports to use `municipal_scrape_workspace.ccindex.*`

### Move to Benchmarks
- `benchmark_*.py` → `benchmarks/ccindex/`

### Move to Scripts / Ops
- `download_cc_indexes*.sh`, `overnight_*.sh`, `rebuild_*.sh`, `monitor_*.sh`, `quickstart_*.sh` → `scripts/ops/`

### Likely Archive
These look like one-off iterations or superseded tools (confirm case-by-case):
- `convert_final_three*.py`
- `archive/ccindex/converters/convert_missing_17.py`
- `archive/ccindex/converters/convert_missing_with_chunks.py`

---

## Per-File Destination Map (Operational Code)

This is a pragmatic mapping for the *code entrypoints* (Python + shell) that are currently at repo root.
It’s not moving output folders/logs; those remain where they are.

Legend:
- **KEEP**: should continue to exist, but likely moved/packaged
- **ARCHIVE**: keep for provenance; not part of the primary workflow

### Municipal scrape workflow
- KEEP → `src/municipal_scrape_workspace/orchestrate_municipal_scrape.py` (wrapper keeps old name)
  - current: `orchestrate_municipal_scrape.py`
  - gap: remove `sys.path` dependency on sibling checkout; replace with a documented env var (e.g. `IPFS_DATASETS_PY_ROOT`) or require installed dep

### CC index pipeline (Python)

**Core orchestration / validation**
- KEEP → `src/municipal_scrape_workspace/ccindex/cc_pipeline_orchestrator.py` + root wrapper
  - current: `cc_pipeline_orchestrator.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/validate_collection_completeness.py` + root wrapper
  - current: `validate_collection_completeness.py` (already migrated)

**Meta-index building**
- KEEP → `src/municipal_scrape_workspace/ccindex/meta/build_year_meta_indexes.py` + wrapper
  - current: `build_year_meta_indexes.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/meta/build_master_index.py` + wrapper
  - current: `build_master_index.py`

**Search / query**
- KEEP → `src/municipal_scrape_workspace/ccindex/search/search_cc_via_meta_indexes.py` + wrapper
  - current: `search_cc_via_meta_indexes.py`
- KEEP (pick canon) → `src/municipal_scrape_workspace/ccindex/search/search_cc_domain.py` + wrapper
  - current: `search_cc_domain.py`
- KEEP (canon) → `src/municipal_scrape_workspace/ccindex/search/search_parallel_duckdb_indexes.py` + wrapper
  - current: `search_parallel_duckdb_indexes.py`
- ARCHIVE or consolidate:
  - `search_cc_duckdb_index.py`
  - `search_cc_pointer_index.py`
  - `search_domain_duckdb_pointer.py`
  - `search_domain_pointer_index.py`
  - `search_duckdb_domain.py`
  - `search_duckdb_pointer_domain.py`

**Convert / sort / validate (Parquet)**
- KEEP → `src/municipal_scrape_workspace/ccindex/convert/bulk_convert_gz_to_parquet.py`
  - current: `bulk_convert_gz_to_parquet.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/convert/parallel_convert_missing.py`
  - current: `parallel_convert_missing.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/sort/sort_cc_parquet_shards.py`
  - current: `sort_cc_parquet_shards.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/validate/validate_and_sort_parquet.py`
  - current: `validate_and_sort_parquet.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/validate/parallel_validate_parquet.py`
  - current: `parallel_validate_parquet.py`
- ARCHIVE / consolidate:
  - `sort_unsorted_files.py`
  - `sort_unsorted_memory_aware.py`
  - `sort_parquet_external_merge.py`
  - `validate_and_mark_sorted.py`
  - `validate_urlindex_sorted.py`

**DuckDB index builders**
- KEEP → `src/municipal_scrape_workspace/ccindex/index/build_parallel_duckdb_indexes.py`
  - current: `build_parallel_duckdb_indexes.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/index/build_duckdb_pointer_from_parquet.py`
  - current: `build_duckdb_pointer_from_parquet.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/index/build_cc_pointer_duckdb.py`
  - current: `build_cc_pointer_duckdb.py`
- ARCHIVE / consolidate:
  - `build_duckdb_from_sorted_parquet.py`
  - `build_index_from_parquet.py`
  - `build_cc_parquet_rowgroup_index.py`

**WARC verification / retrieval utilities**
- KEEP → `src/municipal_scrape_workspace/ccindex/warc/verify_warc_retrieval.py`
  - current: `verify_warc_retrieval.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/warc/download_warc_records.py`
  - current: `download_warc_records.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/warc/validate_warc_record_blobs.py`
  - current: `validate_warc_record_blobs.py`
- KEEP → `src/municipal_scrape_workspace/ccindex/warc/warc_candidates_from_jsonl.py`
  - current: `warc_candidates_from_jsonl.py`

**Monitoring / schedulers / queueing**
- KEEP → repo root for now (future: consider `src/municipal_scrape_workspace/ccindex/` or a dedicated `scripts/` subfolder)
  - `monitor_progress.py`, `monitor_cc_pointer_build.py`, `watchdog_cc_pointer_build.py`, `watchdog_monitor.py`
  - `queue_cc_pointer_build.py`, `launch_cc_pointer_build.py`, `cc_pointer_status.py`
- ARCHIVE (likely superseded by orchestrator): `cc_pipeline_manager.py`

### Benchmarks
- KEEP → `benchmarks/ccindex/`
  - `benchmark_cc_domain_search.py`
  - `benchmark_cc_duckdb_search.py`
  - `benchmark_cc_pointer_search.py`
  - `benchmark_ccindex_parquet_vs_duckdb.py`
  - `benchmark_domain_pointer_index.py`
  - `benchmark_duckdb_domain.py`
  - `benchmark_duckdb_pointer.py`
  - `benchmark_duckdb_pointer_domain.py`
  - `benchmark_parallel_duckdb_indexes.py`

### Ops shell scripts
- KEEP → `scripts/ops/` (canonical implementations)
  - Root-level wrappers remain for backwards compatibility (e.g. `./overnight_build_duckdb_index.sh`)
  - `download_cc_indexes*.sh`
  - `monitor_*.sh`
  - `overnight_*.sh`
  - `rebuild_*.sh`, `final_rebuild.sh`, `parallel_rebuild.sh`, `comprehensive_rebuild.sh`
  - `cleanup_space.sh`, `prune_ccindex_zfs_autosnapshots.sh`, `redownload_quarantined_ccindex_shards.sh`
  - `start_overnight_reindex.sh`
  - `verify_parquet_sorted.sh`

### One-off conversion scripts (archive)
- ARCHIVE → `archive/ccindex/converters/`
  - `convert_final_three.py`, `convert_final_three_simple.py`, `convert_final_three_correct.py`
  - `convert_missing_17.py`, `convert_missing_with_chunks.py`


---

## Import Refactor Rules After the Move

- No `sys.path.insert()` for intra-repo imports.
- Operational scripts should either:
  - be thin wrappers that import package modules, or
  - be run as modules: `python -m municipal_scrape_workspace.ccindex.<module>`

---

## Next Steps (Suggested)

1) Consolidate/retire duplicate search scripts (keep one canonical DuckDB + one canonical pointer-index query path).
2) Decide whether monitoring/queueing tools become package modules or live under a dedicated `scripts/` subfolder.
3) Add console-script entry points (optional) for common commands:
   - `ccindex-search-meta`
   - `ccindex-build-year`
   - `ccindex-build-master`
   - `ccindex-orchestrate`

