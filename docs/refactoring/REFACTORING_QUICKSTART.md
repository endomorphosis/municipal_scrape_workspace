# Refactoring Quick Reference Guide

**Last Updated**: 2026-01-19  
**Full Details**: See [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md)

---

## 📊 Current Status at a Glance

```
Total Root Python Files: 52
├─ ✅ Migrated (wrappers):    19 files
├─ ⚠️  Needs wrapper fix:      4 files
├─ 📦 Needs migration:        17 files
├─ 🗄️  Should be archived:     7 files
└─ ❓ Needs evaluation:        5 files
```

---

## 🎯 Priority Actions

### 1️⃣ Fix Missing Wrappers (Quick Win - 10 min)

These files are already in `src/` but the root version needs to become a wrapper:

```bash
# Files to fix:
- build_cc_parquet_rowgroup_index.py
- bulk_convert_gz_to_parquet.py
- validate_search_completeness.py
- validate_urlindex_sorted.py
```

**Action**: Replace root file content with wrapper template:

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

### 2️⃣ Migrate Core Orchestration (High Priority)

```bash
# Core pipeline tools:
cc_pipeline_orchestrator.py     → src/.../ccindex/
cc_pipeline_watch.py             → src/.../ccindex/
cc_pipeline_hud.py               → src/.../ccindex/
monitor_progress.py              → src/.../ccindex/
```

### 3️⃣ Archive Superseded Files (Low Risk)

```bash
# Move to archive/ccindex/superseded/:
cc_pipeline_manager.py           # Superseded by orchestrator
search_domain_duckdb_pointer.py  # Duplicate search tool
search_domain_pointer_index.py   # Duplicate search tool
search_duckdb_domain.py          # Duplicate search tool
search_duckdb_pointer_domain.py  # Duplicate search tool
sort_unsorted_files.py           # Superseded by memory-aware version
sort_parquet_external_merge.py   # Superseded
```

---

## 📋 Migration Workflow

### Step-by-Step for Each File:

```bash
# 1. Move file to src/
git mv tool.py src/municipal_scrape_workspace/ccindex/tool.py

# 2. Update imports in the moved file
# Change: import other_tool
# To:     from municipal_scrape_workspace.ccindex.other_tool import ...

# 3. Remove sys.path hacks
# Delete lines like: sys.path.insert(0, ...)

# 4. Ensure main() function exists
# def main(argv=None) -> int:
#     ...
#     return 0

# 5. Create wrapper at root
cat > tool.py << 'EOF'
#!/usr/bin/env python3
"""Backwards-compatible wrapper for Tool.

Moved to:
  municipal_scrape_workspace.ccindex.tool
"""

from municipal_scrape_workspace.ccindex.tool import main

if __name__ == "__main__":
    raise SystemExit(main())
EOF

# 6. Test it works
./tool.py --help
python -m municipal_scrape_workspace.ccindex.tool --help

# 7. Commit
git add .
git commit -m "Migrate tool.py to package structure"
```

---

## 🔍 File Lookup Table

### Already Migrated ✅

| Root File | Location in src/ |
|-----------|------------------|
| search_cc_domain.py | src/.../ccindex/search_cc_domain.py |
| build_cc_pointer_duckdb.py | src/.../ccindex/build_cc_pointer_duckdb.py |
| search_cc_via_meta_indexes.py | src/.../ccindex/search_cc_via_meta_indexes.py |
| validate_collection_completeness.py | src/.../ccindex/validate_collection_completeness.py |
| ... (15 more) | See REFACTORING_ROADMAP.md |

### Need Migration 📦

| Priority | Root File | Destination |
|----------|-----------|-------------|
| P1 | cc_pipeline_orchestrator.py | src/.../ccindex/ |
| P1 | cc_pipeline_watch.py | src/.../ccindex/ |
| P1 | cc_pipeline_hud.py | src/.../ccindex/ |
| P1 | monitor_progress.py | src/.../ccindex/ |
| P2 | queue_cc_pointer_build.py | src/.../ccindex/ |
| P2 | launch_cc_pointer_build.py | src/.../ccindex/ |
| P2 | watchdog_cc_pointer_build.py | src/.../ccindex/ |
| P3 | orchestrate_municipal_scrape.py | src/municipal_scrape_workspace/ |
| P3 | check_archive_callbacks.py | src/municipal_scrape_workspace/ |

### Archive 🗄️

| Root File | Archive Destination | Reason |
|-----------|---------------------|--------|
| cc_pipeline_manager.py | archive/ccindex/superseded/ | Replaced by orchestrator |
| search_domain_duckdb_pointer.py | archive/ccindex/superseded/ | Duplicate |
| search_domain_pointer_index.py | archive/ccindex/superseded/ | Duplicate |
| search_duckdb_domain.py | archive/ccindex/superseded/ | Duplicate |
| search_duckdb_pointer_domain.py | archive/ccindex/superseded/ | Duplicate |

---

## 🔧 Common Import Patterns

### Pattern 1: Simple Tool Import

❌ Before:
```python
import validate_collection_completeness
validator = validate_collection_completeness.CollectionValidator(...)
```

✅ After:
```python
from municipal_scrape_workspace.ccindex.validate_collection_completeness import CollectionValidator
validator = CollectionValidator(...)
```

### Pattern 2: Orchestrator Imports Multiple Tools

❌ Before:
```python
import sys
sys.path.insert(0, str(Path(__file__).parent))

import validate_collection_completeness
import cc_domain_parquet_locator
```

✅ After:
```python
from municipal_scrape_workspace.ccindex.validate_collection_completeness import CollectionValidator
from municipal_scrape_workspace.ccindex.cc_domain_parquet_locator import find_domain_files
```

### Pattern 3: Optional Dependencies

✅ Lazy import for optional deps:
```python
def main(argv=None) -> int:
    # Import heavy dependencies only when needed
    import duckdb
    import pyarrow.parquet as pq
    
    # ... use them
```

---

## 🚀 Running Tools After Migration

### All three methods work:

```bash
# Method 1: Root wrapper (backwards compatible)
./search_cc_domain.py --domain example.com

# Method 2: Python module
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com

# Method 3: After adding console_scripts (future)
ccindex-search-domain --domain example.com
```

---

## 📚 Documentation Structure

```
Repository Documentation:
├── README.md                      # General project overview
├── REFACTORING_ROADMAP.md        # 📘 Comprehensive migration guide
├── REFACTORING_QUICKSTART.md     # 📄 This quick reference
├── REPO_LAYOUT_README.md         # Original layout planning doc
│
├── docs/
│   ├── REPO_LAYOUT_PLAN.md       # Detailed layout plan
│   └── COMMON_CRAWL_USAGE.md     # CC tooling usage
│
└── benchmarks/ccindex/
    └── README.md                  # Benchmark documentation
```

---

## 🐛 Known Issues / Gaps

### Issue 1: Hardcoded ipfs_datasets_py Path

**File**: `pyproject.toml`

```toml
# Current (not portable):
"ipfs_datasets_py @ file:///home/barberb/ipfs_datasets_py"

# Better (use git URL):
"ipfs_datasets_py @ git+https://github.com/endomorphosis/ipfs_datasets_py.git@main"
```

**Action Required**: Update pyproject.toml dependency declaration

### Issue 2: Municipal Scrape Uses Hardcoded Path

**File**: `orchestrate_municipal_scrape.py`

```python
# Current:
sys.path.insert(0, "/home/barberb/ipfs_datasets_py")

# Better:
ipfs_root = os.environ.get("IPFS_DATASETS_PY_ROOT")
if ipfs_root:
    sys.path.insert(0, ipfs_root)
```

**Action Required**: Add environment variable support for dev workflow

---

## ✅ Validation Checklist

After completing migrations:

- [ ] All root `.py` files are wrappers or archived
- [ ] No `sys.path.insert()` in src/ files
- [ ] All imports use package imports
- [ ] `pip install -e .` works
- [ ] `pip install -e '.[ccindex]'` enables CC tools
- [ ] All wrappers execute: `./tool.py --help`
- [ ] Module execution works: `python -m municipal_scrape_workspace.ccindex.tool --help`
- [ ] Tests pass (if any)
- [ ] Documentation updated

---

## 🆘 Need Help?

1. **Full details**: See [REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md)
2. **Layout plan**: See [docs/REPO_LAYOUT_PLAN.md](docs/REPO_LAYOUT_PLAN.md)
3. **Original layout doc**: See [REPO_LAYOUT_README.md](REPO_LAYOUT_README.md)

---

**Status**: Documentation complete. Ready for execution.
