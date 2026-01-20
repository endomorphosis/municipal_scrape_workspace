# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

This repo also contains Common Crawl (CC) index pipeline tooling (Parquet + DuckDB pointer indexes + meta-indexes). The CC tooling is fully organized under the installable package namespace `municipal_scrape_workspace.ccindex`.

## 📚 Repository Structure

**✅ REFACTORING COMPLETE** - The repository has been successfully refactored for improved organization and maintainability.

👉 **Start Here**: [REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md) - **Complete authoritative guide to refactored repository**

**Essential Documentation**:
- 📘 **[REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md)** - **PRIMARY GUIDE** Complete structure, file locations, import patterns, and usage
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
- ✅ **52 files processed** (100% complete)
- ✅ **41 files migrated** to `src/` with backwards-compatible wrappers
- ✅ **11 files archived** in `archive/ccindex/superseded/`
- ✅ **Clean package structure** - follows Python best practices
- ✅ **Proper imports** - no sys.path hacks
- ✅ **Installable package** - works with `pip install -e .`
- ✅ **Console script entry points** - 12+ command-line tools available
- ✅ **Comprehensive documentation** - complete structure guide created

## Quickstart

```bash
# 1. Setup environment
./bootstrap.sh
source .venv/bin/activate

# 2. Install package (basic)
pip install -e .

# 3. (Optional) Install with CC index tooling dependencies
pip install -e '.[ccindex]'

# 4. Run tools - Three methods:

# Method A: Via root wrappers (backwards compatible)
./search_cc_domain.py --domain example.com
./build_cc_pointer_duckdb.py --help

# Method B: Via Python modules (recommended)
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com
python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --help

# Method C: Via console script (main CLI)
municipal-scrape --help
```

**📚 For detailed guide, see [REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md)**

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
