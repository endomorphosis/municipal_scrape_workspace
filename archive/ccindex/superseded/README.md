# Superseded CCIndex Tools

This directory contains tools that have been superseded by newer implementations or are no longer actively maintained.

## Files and Reasons for Archival

### cc_pipeline_manager.py
**Status**: Superseded  
**Replaced by**: `cc_pipeline_orchestrator.py`  
**Reason**: The orchestrator provides a more unified and feature-complete pipeline management system with better integration, resumability, and monitoring.

### Search Tools (4 files)

#### search_domain_duckdb_pointer.py
**Status**: Duplicate functionality  
**Canonical tool**: `search_parallel_duckdb_indexes.py`  
**Reason**: Duplicate search implementation. The parallel version is the canonical tool.

#### search_domain_pointer_index.py
**Status**: Duplicate functionality  
**Canonical tool**: `search_cc_pointer_index.py` and `search_parallel_duckdb_indexes.py`  
**Reason**: Functionality covered by canonical search tools in the package.

#### search_duckdb_domain.py
**Status**: Duplicate functionality  
**Canonical tool**: `search_cc_domain.py` and `search_parallel_duckdb_indexes.py`  
**Reason**: Functionality covered by canonical search tools in the package.

#### search_duckdb_pointer_domain.py
**Status**: Duplicate functionality  
**Canonical tool**: `search_parallel_duckdb_indexes.py`  
**Reason**: Functionality covered by canonical unified search tool.

### Sort Tools (2 files)

#### sort_unsorted_files.py
**Status**: Superseded  
**Canonical tool**: `sort_unsorted_memory_aware.py` (if kept) or `sort_cc_parquet_shards.py`  
**Reason**: Memory-aware version is more robust for large files.

#### sort_parquet_external_merge.py
**Status**: Superseded  
**Canonical tool**: `sort_cc_parquet_shards.py`  
**Reason**: Functionality covered by canonical sorter in the package.

## Usage

These files are kept for:
- Historical reference
- Debugging purposes
- Understanding the evolution of the tooling

**Do not use these in production workflows.** Use the canonical tools listed above instead.

## Canonical Tool Locations

All canonical tools have been migrated to `src/municipal_scrape_workspace/ccindex/` with backwards-compatible wrappers at the repository root.

To find the canonical version of a tool:
```bash
ls -la src/municipal_scrape_workspace/ccindex/
```

To run a tool:
```bash
# Via root wrapper (backwards compatible)
./search_parallel_duckdb_indexes.py --help

# Via module
python -m municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes --help
```

## Migration Date

**Archived**: 2026-01-19  
**Migration Phase**: Phase 6 of repository refactoring
