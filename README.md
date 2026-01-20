# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

This repo also contains Common Crawl (CC) index pipeline tooling (Parquet + DuckDB pointer indexes + meta-indexes). The CC tooling is being migrated into the installable package namespace under `municipal_scrape_workspace.ccindex`.

## 📚 Repository Structure

**✅ MIGRATION COMPLETE** - The repository has been successfully refactored for improved organization and maintainability.

👉 **Start Here**: [FINAL_LAYOUT_README.md](FINAL_LAYOUT_README.md) - Complete post-migration guide

**Quick Links**:
- 📘 [FINAL_LAYOUT_README.md](FINAL_LAYOUT_README.md) - **NEW!** Complete guide to final structure
- 📋 [MIGRATION_COMPLETE.md](MIGRATION_COMPLETE.md) - **NEW!** Migration summary & next steps
- 📄 [FILE_MIGRATION_MAP.md](FILE_MIGRATION_MAP.md) - File location lookup table
- 📚 [REFACTORING_INDEX.md](REFACTORING_INDEX.md) - All refactoring documentation

**Final Status** (2026-01-19):
- ✅ **52 files processed** (100% complete)
- ✅ **41 files migrated** to `src/` with backwards-compatible wrappers
- ✅ **11 files archived** in `archive/ccindex/superseded/`
- ✅ **Clean package structure** - follows Python best practices
- ✅ **Proper imports** - no sys.path hacks
- ✅ **Installable package** - works with `pip install -e .`
- ⚠️ **1 known issue** - ipfs_datasets_py dependency needs fix (see [MIGRATION_COMPLETE.md](MIGRATION_COMPLETE.md))

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

**📚 For detailed guide, see [FINAL_LAYOUT_README.md](FINAL_LAYOUT_README.md)**

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
