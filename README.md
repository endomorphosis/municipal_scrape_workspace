# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

This repo also contains Common Crawl (CC) index pipeline tooling (Parquet + DuckDB pointer indexes + meta-indexes). The CC tooling is fully organized under the installable package namespace `municipal_scrape_workspace.ccindex`.

## 📚 Repository Structure

**✅ REFACTORING COMPLETE** - The repository has been successfully refactored for improved organization and maintainability.

👉 **Start Here**: [REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md) - **Complete authoritative guide to refactored repository**

**Essential Documentation**:
- 📘 **[REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md)** - **PRIMARY GUIDE** Complete structure, file locations, import patterns, and usage
- 🎯 **[REORGANIZATION_PLAN.md](REORGANIZATION_PLAN.md)** - **ROOT CLEANUP** Details of root directory reorganization
- 📋 [docs/refactoring/FINAL_LAYOUT_README.md](docs/refactoring/FINAL_LAYOUT_README.md) - Detailed post-migration guide
- 📄 [docs/refactoring/FILE_MIGRATION_MAP.md](docs/refactoring/FILE_MIGRATION_MAP.md) - Quick file location lookup table
- 🎯 [docs/refactoring/MIGRATION_COMPLETE.md](docs/refactoring/MIGRATION_COMPLETE.md) - Migration summary & statistics
- 📚 [docs/refactoring/REFACTORING_INDEX.md](docs/refactoring/REFACTORING_INDEX.md) - Complete refactoring documentation index

**Documentation Organization**:
- 📁 [docs/refactoring/](docs/refactoring/) - Refactoring process documentation
- 📁 [docs/ccindex/](docs/ccindex/) - Common Crawl index documentation
- 📁 [docs/pipeline/](docs/pipeline/) - Pipeline orchestration documentation
- 📁 [docs/](docs/) - General project documentation

**Final Status** (2026-01-20):
- ✅ **52 Python files processed** (100% complete)
- ✅ **41 files migrated** to `src/` (canonical implementations)
- ✅ **11 files archived** in `archive/ccindex/superseded/`
- ✅ **Root directory cleaned** - 73 files removed (32 shell wrappers + 41 Python wrappers)
- ✅ **Data organized** - CSV files moved to `data/` directory
- ✅ **Clean package structure** - follows Python best practices
- ✅ **Proper imports** - no sys.path hacks
- ✅ **Installable package** - works with `pip install -e .`
- ✅ **Console script entry points** - 12+ command-line tools available
- ✅ **Comprehensive documentation** - complete structure guide + migration guide

## Quickstart

```bash
# 1. Setup environment
./bootstrap.sh
source .venv/bin/activate

# 2. Install package (basic)
pip install -e .

# 3. (Optional) Install with CC index tooling dependencies
pip install -e '.[ccindex]'

# 4. Run tools - Two methods:

# Method A: Via Python modules
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com
python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --help

# Method B: Via console scripts (shorter, after pip install)
ccindex-search-domain --domain example.com
ccindex-build-pointer --help
municipal-scrape --help

# 5. Run operational scripts:

# All shell scripts are in scripts/ops/ directory
scripts/ops/download_cc_indexes.sh
scripts/ops/overnight_build_duckdb_index.sh
scripts/ops/monitor_progress.sh
```

**📚 For detailed guide, see [REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md)**

## Directory Structure

```
municipal_scrape_workspace/
├── bootstrap.sh                     # Setup script
├── pyproject.toml                   # Package configuration
├── data/                            # Reference data files
│   └── us_towns_and_counties_urls.csv
├── src/municipal_scrape_workspace/  # Canonical Python code
│   ├── ccindex/                     # CC index tools (40 modules)
│   └── ...
├── scripts/ops/                     # All operational shell scripts
│   ├── download_cc_indexes.sh
│   ├── overnight_build_*.sh
│   └── ... (30+ scripts)
├── docs/                            # Documentation
├── tests/                           # Test suite
└── archive/                         # Archived/superseded files
```

**Note**: 
- All Python tools are accessed via Python modules or console scripts (see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md))
- Shell scripts are in `scripts/ops/` directory

## Publishing

- Repository: https://github.com/endomorphosis/municipal_scrape_workspace
- Default branch: `main`
- Push changes:

```bash
git checkout main
git pull --rebase
git add -A
git commit -m "Your change"
git push
```

- If you prefer a PR workflow:

```bash
git checkout -b feature/your-branch
git push -u origin feature/your-branch
# Open a PR against main
```
