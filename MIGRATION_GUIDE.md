# Migration Guide - Root Wrapper Removal

**Date**: 2026-01-20  
**Change**: Root directory Python wrapper files removed  
**Impact**: Breaking change for users calling wrappers directly  
**Severity**: 🟠 MEDIUM - Requires command updates

---

## 🎯 What Changed

### Before (with wrappers)
```bash
# Root directory had 41 wrapper files
./search_cc_domain.py --domain example.com
./build_cc_pointer_duckdb.py --output-dir /path/to/indexes
./validate_collection_completeness.py --collection-dir /data
```

### After (wrappers removed)
```bash
# Use Python module format or console scripts
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com
ccindex-search-domain --domain example.com  # After pip install
```

---

## 🔄 Migration Instructions

### For Shell Scripts / Bash

If you have shell scripts calling the wrappers, update them:

#### Option 1: Use Python Module Format (Works Everywhere)

```bash
# OLD (will break)
./search_cc_domain.py --domain example.com
./build_cc_pointer_duckdb.py --output-dir /indexes

# NEW (always works)
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com
python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --output-dir /indexes
```

**Pros:**
- ✅ Works without pip install (in dev environment)
- ✅ Works from any directory
- ✅ Explicit and clear

**Cons:**
- ⚠️ Longer command line
- ⚠️ Requires package structure

#### Option 2: Use Console Scripts (Shortest)

```bash
# After pip install -e .
ccindex-search-domain --domain example.com
ccindex-build-pointer --output-dir /indexes
ccindex-orchestrate --config pipeline_config.json
ccindex-validate --collection-dir /data
```

**Pros:**
- ✅ Shortest commands
- ✅ Most user-friendly
- ✅ Standard CLI tool experience

**Cons:**
- ⚠️ Requires pip install first
- ⚠️ Different command names

---

### For Python Code

If you import or call the wrappers from Python:

#### Option 1: Use Package Imports

```python
# OLD (will break)
import sys
sys.path.insert(0, '/path/to/repo')
import search_cc_domain

# NEW - Import the module
from municipal_scrape_workspace.ccindex import search_cc_domain

# Call the main function
result = search_cc_domain.main(['--domain', 'example.com'])
```

#### Option 2: Use Submodule Imports

```python
# Direct import of main function
from municipal_scrape_workspace.ccindex.search_cc_domain import main as search_domain

# Call it
result = search_domain(['--domain', 'example.com'])
```

#### Option 3: Use subprocess (for external callers)

```python
import subprocess

# Call via Python module
result = subprocess.run([
    'python', '-m',
    'municipal_scrape_workspace.ccindex.search_cc_domain',
    '--domain', 'example.com'
], capture_output=True)

# Or via console script (after pip install)
result = subprocess.run([
    'ccindex-search-domain',
    '--domain', 'example.com'
], capture_output=True)
```

---

## 📋 Complete Command Migration Map

### Search Tools

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./search_cc_domain.py` | `python -m municipal_scrape_workspace.ccindex.search_cc_domain` | `ccindex-search-domain` |
| `./search_cc_duckdb_index.py` | `python -m municipal_scrape_workspace.ccindex.search_cc_duckdb_index` | `ccindex-search-duckdb` |
| `./search_cc_pointer_index.py` | `python -m municipal_scrape_workspace.ccindex.search_cc_pointer_index` | `ccindex-search-pointer` |
| `./search_cc_via_meta_indexes.py` | `python -m municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes` | `ccindex-search` |
| `./search_parallel_duckdb_indexes.py` | `python -m municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes` | `ccindex-search-parallel` |
| `./cc_domain_parquet_locator.py` | `python -m municipal_scrape_workspace.ccindex.cc_domain_parquet_locator` | N/A |

### Build Tools

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./build_cc_pointer_duckdb.py` | `python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb` | `ccindex-build-pointer` |
| `./build_index_from_parquet.py` | `python -m municipal_scrape_workspace.ccindex.build_index_from_parquet` | N/A |
| `./build_parallel_duckdb_indexes.py` | `python -m municipal_scrape_workspace.ccindex.build_parallel_duckdb_indexes` | `ccindex-build-parallel` |
| `./build_duckdb_pointer_from_parquet.py` | `python -m municipal_scrape_workspace.ccindex.build_duckdb_pointer_from_parquet` | N/A |
| `./build_cc_parquet_rowgroup_index.py` | `python -m municipal_scrape_workspace.ccindex.build_cc_parquet_rowgroup_index` | N/A |
| `./build_year_meta_indexes.py` | `python -m municipal_scrape_workspace.ccindex.build_year_meta_indexes` | `ccindex-build-meta` |
| `./build_master_index.py` | `python -m municipal_scrape_workspace.ccindex.build_master_index` | N/A |

### Orchestration & Monitoring

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./cc_pipeline_orchestrator.py` | `python -m municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator` | `ccindex-orchestrate` |
| `./cc_pipeline_watch.py` | `python -m municipal_scrape_workspace.ccindex.cc_pipeline_watch` | `ccindex-watch` |
| `./cc_pipeline_hud.py` | `python -m municipal_scrape_workspace.ccindex.cc_pipeline_hud` | `ccindex-hud` |
| `./monitor_progress.py` | `python -m municipal_scrape_workspace.ccindex.monitor_progress` | N/A |
| `./monitor_cc_pointer_build.py` | `python -m municipal_scrape_workspace.ccindex.monitor_cc_pointer_build` | N/A |
| `./cc_pointer_status.py` | `python -m municipal_scrape_workspace.ccindex.cc_pointer_status` | N/A |
| `./queue_cc_pointer_build.py` | `python -m municipal_scrape_workspace.ccindex.queue_cc_pointer_build` | N/A |
| `./launch_cc_pointer_build.py` | `python -m municipal_scrape_workspace.ccindex.launch_cc_pointer_build` | N/A |
| `./watchdog_cc_pointer_build.py` | `python -m municipal_scrape_workspace.ccindex.watchdog_cc_pointer_build` | N/A |
| `./watchdog_monitor.py` | `python -m municipal_scrape_workspace.ccindex.watchdog_monitor` | N/A |

### Validation Tools

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./validate_and_sort_parquet.py` | `python -m municipal_scrape_workspace.ccindex.validate_and_sort_parquet` | `ccindex-validate-parquet` |
| `./validate_collection_completeness.py` | `python -m municipal_scrape_workspace.ccindex.validate_collection_completeness` | `ccindex-validate` |
| `./validate_search_completeness.py` | `python -m municipal_scrape_workspace.ccindex.validate_search_completeness` | N/A |
| `./validate_urlindex_sorted.py` | `python -m municipal_scrape_workspace.ccindex.validate_urlindex_sorted` | N/A |
| `./validate_warc_record_blobs.py` | `python -m municipal_scrape_workspace.ccindex.validate_warc_record_blobs` | N/A |
| `./verify_warc_retrieval.py` | `python -m municipal_scrape_workspace.ccindex.verify_warc_retrieval` | N/A |
| `./parallel_validate_parquet.py` | `python -m municipal_scrape_workspace.ccindex.parallel_validate_parquet` | N/A |

### Conversion Tools

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./bulk_convert_gz_to_parquet.py` | `python -m municipal_scrape_workspace.ccindex.bulk_convert_gz_to_parquet` | N/A |
| `./parallel_convert_missing.py` | `python -m municipal_scrape_workspace.ccindex.parallel_convert_missing` | N/A |
| `./regenerate_parquet_from_gz.py` | `python -m municipal_scrape_workspace.ccindex.regenerate_parquet_from_gz` | N/A |
| `./sample_ccindex_to_parquet.py` | `python -m municipal_scrape_workspace.ccindex.sample_ccindex_to_parquet` | N/A |
| `./extract_cc_index_tarballs.py` | `python -m municipal_scrape_workspace.ccindex.extract_cc_index_tarballs` | N/A |

### Sorting Tools

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./sort_cc_parquet_shards.py` | `python -m municipal_scrape_workspace.ccindex.sort_cc_parquet_shards` | N/A |
| `./sort_unsorted_memory_aware.py` | `python -m municipal_scrape_workspace.ccindex.sort_unsorted_memory_aware` | N/A |

### WARC Tools

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./download_warc_records.py` | `python -m municipal_scrape_workspace.ccindex.download_warc_records` | N/A |
| `./warc_candidates_from_jsonl.py` | `python -m municipal_scrape_workspace.ccindex.warc_candidates_from_jsonl` | N/A |

### Municipal Scraping

| Old Command | New Python Module | Console Script |
|-------------|-------------------|----------------|
| `./orchestrate_municipal_scrape.py` | `python -m municipal_scrape_workspace.orchestrate_municipal_scrape` | `municipal-scrape` |
| `./check_archive_callbacks.py` | `python -m municipal_scrape_workspace.check_archive_callbacks` | N/A |

---

## 🛠️ Setup Instructions

### Step 1: Install Package

```bash
# Navigate to repository
cd municipal_scrape_workspace

# Install in development mode
pip install -e .

# Or with CC index tools
pip install -e '.[ccindex]'

# Or with all extras
pip install -e '.[ccindex,ipfs,dev]'
```

### Step 2: Verify Installation

```bash
# Test console script
ccindex-search-domain --help

# Test Python module
python -m municipal_scrape_workspace.ccindex.search_cc_domain --help

# Both should show help message
```

### Step 3: Update Your Scripts

Use find/replace in your scripts:

```bash
# Example: Update all references in your scripts
find . -name "*.sh" -type f -exec sed -i 's|./search_cc_domain.py|python -m municipal_scrape_workspace.ccindex.search_cc_domain|g' {} +
```

---

## 📚 Common Migration Patterns

### Pattern 1: Batch Script Migration

```bash
#!/bin/bash
# Old script that breaks
./search_cc_domain.py --domain example.com > results1.txt
./search_cc_domain.py --domain test.com > results2.txt
./validate_collection_completeness.py --collection-dir /data

# New script (Option A: Python modules)
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com > results1.txt
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain test.com > results2.txt
python -m municipal_scrape_workspace.ccindex.validate_collection_completeness --collection-dir /data

# New script (Option B: Console scripts - after pip install)
ccindex-search-domain --domain example.com > results1.txt
ccindex-search-domain --domain test.com > results2.txt
ccindex-validate --collection-dir /data
```

### Pattern 2: Cron Job Migration

```bash
# Old crontab entry
0 2 * * * cd /path/to/repo && ./build_cc_pointer_duckdb.py --output-dir /indexes

# New crontab entry (Option A)
0 2 * * * cd /path/to/repo && python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --output-dir /indexes

# New crontab entry (Option B - after pip install)
0 2 * * * ccindex-build-pointer --output-dir /indexes
```

### Pattern 3: Python Script Migration

```python
# Old Python code that breaks
import subprocess
subprocess.run(['./search_cc_domain.py', '--domain', 'example.com'])

# New Python code (Option A: subprocess with module)
import subprocess
subprocess.run([
    'python', '-m', 
    'municipal_scrape_workspace.ccindex.search_cc_domain',
    '--domain', 'example.com'
])

# New Python code (Option B: direct import)
from municipal_scrape_workspace.ccindex.search_cc_domain import main
result = main(['--domain', 'example.com'])

# New Python code (Option C: console script - after pip install)
import subprocess
subprocess.run(['ccindex-search-domain', '--domain', 'example.com'])
```

---

## ❓ FAQ

### Q: Why were the wrappers removed?

**A:** To create a cleaner, more professional repository structure following Python best practices. The package is now used like a standard Python package.

### Q: Can I still use short commands?

**A:** Yes! After `pip install -e .`, you can use console scripts like `ccindex-search-domain` which are even shorter than the old `./search_cc_domain.py`.

### Q: Do I need to reinstall the package?

**A:** Yes, run `pip install -e .` or `pip install -e '.[ccindex]'` to get the console scripts installed in your environment.

### Q: What if I have many scripts to migrate?

**A:** Use find/replace tools:
```bash
# Replace in all shell scripts
find . -name "*.sh" -type f -exec sed -i 's|./\([^.]*\)\.py|python -m municipal_scrape_workspace.ccindex.\1|g' {} +
```

### Q: Can I create my own wrappers?

**A:** Yes! If you prefer the old style, create your own wrapper scripts:
```bash
#!/bin/bash
# my-search-wrapper.sh
python -m municipal_scrape_workspace.ccindex.search_cc_domain "$@"
```

### Q: What if I'm using the package as a library?

**A:** No changes needed! If you were importing from the package, everything still works:
```python
from municipal_scrape_workspace.ccindex import search_cc_domain
# Still works exactly the same
```

---

## ✅ Migration Checklist

Use this checklist to track your migration:

- [ ] Read this migration guide completely
- [ ] Install package: `pip install -e .` or `pip install -e '.[ccindex]'`
- [ ] Test console scripts work: `ccindex-search-domain --help`
- [ ] Test Python modules work: `python -m municipal_scrape_workspace.ccindex.search_cc_domain --help`
- [ ] Identify all scripts/code that call old wrappers
- [ ] Update shell scripts to use Python modules or console scripts
- [ ] Update Python code to use package imports
- [ ] Update cron jobs with new commands
- [ ] Update documentation/README files
- [ ] Test all updated scripts/code
- [ ] Remove any local bookmarks/aliases for old wrappers
- [ ] Update team documentation
- [ ] Notify other users of the changes

---

## 📞 Support

If you encounter issues during migration:

1. **Check Installation**: Ensure package is installed with `pip list | grep municipal`
2. **Check Console Scripts**: Run `ccindex-search-domain --help` to verify
3. **Check Python Modules**: Run `python -m municipal_scrape_workspace.ccindex.search_cc_domain --help`
4. **Check Documentation**: See REFACTORED_STRUCTURE.md for complete structure guide
5. **Create Issue**: If problems persist, open a GitHub issue

---

## 🎉 Benefits of This Change

After migration, you'll enjoy:

1. **Cleaner Repository** - Root directory reduced from 60 to 19 items
2. **Standard Python Package** - Follows Python packaging best practices
3. **Better Discoverability** - Clear package structure under src/
4. **Multiple Access Methods** - Choose module format or console scripts
5. **Proper Installation** - Works like any professional Python package
6. **Improved Maintenance** - Single source of truth, no duplicate wrappers

---

**Migration Support**: See REFACTORED_STRUCTURE.md for complete package structure  
**Last Updated**: 2026-01-20  
**Status**: Active - Wrappers have been removed
