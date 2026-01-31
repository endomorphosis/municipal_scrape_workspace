# Repo Layout (Final Locations) + Refactor Checklist

**📘 COMPREHENSIVE GUIDE**: See [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md) for complete analysis and migration plan.  
**📄 QUICK START**: See [REFACTORING_QUICKSTART.md](REFACTORING_QUICKSTART.md) for quick reference and action items.

This document is the "source of truth" for where each script/module should live once the repo is fully migrated into its final layout.

The working conventions here are:
- **Canonical implementations live in structured folders** (`src/`, `scripts/ops/`, `benchmarks/`, `archive/`).
- **Repo-root entrypoints are wrappers** that preserve backwards compatibility (`./foo.py`, `./bar.sh`).
- **No `sys.path` hacks for intra-repo imports**. Package imports only.

## Current Migration Status

As of 2026-01-19:
- **19 files** successfully migrated with root wrappers ✅
- **4 files** in src/ but missing wrappers ⚠️
- **17 files** awaiting migration 📦
- **7 files** identified for archival 🗄️
- **5 files** need evaluation ❓

See [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md) for complete file-by-file breakdown.

---

## Final Top-Level Layout

```text
.
├── src/municipal_scrape_workspace/          # installable Python package
│   ├── cli.py
│   ├── ccindex/                            # Common Crawl pointer-index tooling
│   └── …
├── scripts/
│   └── ops/                                # canonical operational shell scripts
├── benchmarks/
│   └── ccindex/                            # benchmark_*.py + benchmark README
├── docs/                                   # design docs, runbooks, quickrefs
├── archive/
│   └── ccindex/                            # one-offs + superseded tools
└── <repo-root wrappers>                    # thin wrappers for compatibility
```

---

## What stays as a repo-root wrapper

Repo-root wrappers are intentionally kept so existing workflows keep working.

Wrapper rules:
- Wrapper must be small and stable.
- Wrapper must import a `main()` from the package and `raise SystemExit(main())`.
- Wrapper must not contain substantial business logic.

Example wrapper pattern:

```python
from municipal_scrape_workspace.ccindex.search_cc_domain import main

if __name__ == "__main__":
    raise SystemExit(main())
```

---

## Classification rules (Permanent vs Archive)

A file is **permanent** if it is one of:
- a core pipeline stage used in real runs (convert/sort/index/validate/build)
- a verifier/retrieval tool used to prove end-to-end correctness
- a monitoring/orchestration tool used operationally

A file is **archive** if it is:
- a one-off experiment or historical attempt
- a superseded tool where a clear canonical replacement exists
- a benchmark/comparison that is no longer referenced

---

## Current status (already migrated)

### CCIndex tooling already packaged

These canonical implementations live under `src/municipal_scrape_workspace/ccindex/` and have repo-root wrappers:

- build/search/meta:
  - `build_cc_pointer_duckdb.py`
  - `build_index_from_parquet.py`
  - `build_master_index.py`
  - `build_parallel_duckdb_indexes.py`
  - `build_year_meta_indexes.py`
  - `search_cc_domain.py`
  - `search_cc_duckdb_index.py`
  - `search_cc_pointer_index.py`
  - `search_cc_via_meta_indexes.py`
  - `search_parallel_duckdb_indexes.py`
- support:
  - `cc_domain_parquet_locator.py`
  - `sort_cc_parquet_shards.py`
  - `validate_and_sort_parquet.py`
  - `parallel_validate_parquet.py`
  - `validate_collection_completeness.py`
  - `warc_candidates_from_jsonl.py`
  - `download_warc_records.py`
  - `verify_warc_retrieval.py`
  - `validate_warc_record_blobs.py`

### Ops shell scripts

Canonical implementations live under `scripts/ops/` and repo-root wrappers exist for all of them.

### Benchmarks

Canonical benchmark scripts live under `benchmarks/ccindex/`.

### Archived converter scripts

One-off converter scripts live under `archive/ccindex/converters/`.

---

## Root-level Python scripts: final location plan

This is a pragmatic mapping of what should happen to each repo-root `*.py`.

### Permanent: move into package (then keep wrapper)

These should become canonical modules under `src/municipal_scrape_workspace/` (preferably under `ccindex/` when CC-related):

- Conversion / ingestion:
  - `bulk_convert_gz_to_parquet.py`  → `src/municipal_scrape_workspace/ccindex/bulk_convert_gz_to_parquet.py`
  - `parallel_convert_missing.py`     → `src/municipal_scrape_workspace/ccindex/parallel_convert_missing.py`
  - `regenerate_parquet_from_gz.py`   → `src/municipal_scrape_workspace/ccindex/regenerate_parquet_from_gz.py`
  - `sample_ccindex_to_parquet.py`    → `src/municipal_scrape_workspace/ccindex/sample_ccindex_to_parquet.py`

- Parquet sort/validate utilities (keep only canonicals):
  - `sort_cc_parquet_shards.py`       → `src/municipal_scrape_workspace/ccindex/sort_cc_parquet_shards.py`
  - `validate_and_sort_parquet.py`    → `src/municipal_scrape_workspace/ccindex/validate_and_sort_parquet.py`
  - `parallel_validate_parquet.py`    → `src/municipal_scrape_workspace/ccindex/parallel_validate_parquet.py`
  - `validate_urlindex_sorted.py`     → `src/municipal_scrape_workspace/ccindex/validate_urlindex_sorted.py`
  - `validate_search_completeness.py` → `src/municipal_scrape_workspace/ccindex/validate_search_completeness.py`

- Index and supporting builders (needs consolidation decisions):
  - `build_cc_parquet_rowgroup_index.py`     → `src/municipal_scrape_workspace/ccindex/build_cc_parquet_rowgroup_index.py`
  - `build_duckdb_pointer_from_parquet.py`   → `src/municipal_scrape_workspace/ccindex/build_duckdb_pointer_from_parquet.py`

- Pipeline orchestration/monitoring (where you want these to live is a choice):
  - `cc_pipeline_orchestrator.py` → `src/municipal_scrape_workspace/ccindex/cc_pipeline_orchestrator.py`
  - `cc_pipeline_watch.py`        → `src/municipal_scrape_workspace/ccindex/cc_pipeline_watch.py`
  - `cc_pipeline_hud.py`          → `src/municipal_scrape_workspace/ccindex/cc_pipeline_hud.py`
  - `monitor_progress.py`         → `src/municipal_scrape_workspace/ccindex/monitor_progress.py`

- Pointer build queue + watchdog tools:
  - `queue_cc_pointer_build.py`     → `src/municipal_scrape_workspace/ccindex/queue_cc_pointer_build.py`
  - `launch_cc_pointer_build.py`    → `src/municipal_scrape_workspace/ccindex/launch_cc_pointer_build.py`
  - `monitor_cc_pointer_build.py`   → `src/municipal_scrape_workspace/ccindex/monitor_cc_pointer_build.py`
  - `watchdog_cc_pointer_build.py`  → `src/municipal_scrape_workspace/ccindex/watchdog_cc_pointer_build.py`
  - `watchdog_monitor.py`           → `src/municipal_scrape_workspace/ccindex/watchdog_monitor.py`
  - `cc_pointer_status.py`          → `src/municipal_scrape_workspace/ccindex/cc_pointer_status.py`

- Municipal scrape workflow:
  - `orchestrate_municipal_scrape.py` → `src/municipal_scrape_workspace/orchestrate_municipal_scrape.py`
  - `check_archive_callbacks.py`      → `src/municipal_scrape_workspace/check_archive_callbacks.py`

After each move, leave a repo-root wrapper (same filename) that imports `main()` from the new module.

### Archive: keep for provenance

Likely archive candidates (confirm case-by-case):
- `cc_pipeline_manager.py` (superseded by orchestrator + queue/watchdog tooling)

Potential archive-or-consolidate candidates (keep ONE canonical implementation):
- `build_duckdb_from_sorted_parquet.py`
- `consolidate_parquet_files.py`
- `sort_unsorted_files.py`
- `sort_unsorted_memory_aware.py`
- `sort_parquet_external_merge.py`
- `validate_and_mark_sorted.py`
- `compare_crawl_results.py`
- `extract_cc_index_tarballs.py`

---

## Import refactor checklist (after each move)

When moving a script into `src/`:

1) **Add/keep `main(argv=None) -> int`** and ensure the module is runnable.
2) Replace any intra-repo imports like `import foo` with package imports:
   - `from municipal_scrape_workspace.ccindex.<module> import ...`
3) Remove `sys.path.insert()` hacks for intra-repo imports.
4) Keep heavy/optional dependencies imported lazily when possible:
   - allows `--help` to work even when optional deps aren’t installed.
5) Update docs to reference canonical locations:
   - `scripts/ops/...`
   - `benchmarks/ccindex/...`
   - `${REPO_ROOT}/...` style for copy/paste portability

---

## Dependency gaps to resolve

### 1) ipfs_datasets_py portability

Current code supports local development checkouts, but for a "final" layout:
- prefer installing `ipfs_datasets_py` as a normal dependency
- optionally support a dev override via `$IPFS_DATASETS_PY_ROOT`

### 2) pyproject.toml hardcoded local dependency

`pyproject.toml` currently pins:

- `ipfs_datasets_py @ file:///home/barberb/ipfs_datasets_py`

This is not portable. Options:
- switch to a git URL dependency (best if upstream build is stable)
- make it an optional extra (e.g. `pip install -e '.[ipfs]'`)
- document `$IPFS_DATASETS_PY_ROOT` + editable install for local dev

### 3) Optional CCIndex dependencies

The `ccindex` optional extra should remain the canonical way to install DuckDB/Parquet tooling:

- `pip install -e '.[ccindex]'`

---

## After the move: how to run

- Ops scripts (shell):
  - `${REPO_ROOT}/overnight_build_duckdb_index.sh` (wrapper)
  - canonical: `${REPO_ROOT}/scripts/ops/overnight_build_duckdb_index.sh`

- Benchmarks:
  - `${VENV_PYTHON} ${REPO_ROOT}/benchmarks/ccindex/benchmark_cc_duckdb_search.py`

- Packaged CCIndex tools:
  - `${VENV_PYTHON} ${REPO_ROOT}/search_cc_domain.py ...` (wrapper)
  - or `python -m municipal_scrape_workspace.ccindex.search_cc_domain ...`
