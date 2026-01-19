# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

This repo also contains Common Crawl (CC) index pipeline tooling (Parquet + DuckDB pointer indexes + meta-indexes). The CC tooling is being migrated into the installable package namespace under `municipal_scrape_workspace.ccindex`.

## 📚 Refactoring Documentation

**The repository is undergoing a structural refactoring to improve organization and maintainability.**

- 📘 **[REFACTORING_ROADMAP.md](REFACTORING_ROADMAP.md)** - Comprehensive guide with complete analysis (52 files, detailed plan, import patterns, dependency gaps)
- 📄 **[REFACTORING_QUICKSTART.md](REFACTORING_QUICKSTART.md)** - Quick reference for common tasks and patterns
- 📋 **[FILE_MIGRATION_MAP.md](FILE_MIGRATION_MAP.md)** - Complete file-by-file lookup table with status and actions
- 📖 **[REPO_LAYOUT_README.md](REPO_LAYOUT_README.md)** - Repository layout conventions and rules

**Current Status** (2026-01-19):
- ✅ 19 files migrated with wrappers
- ⚠️ 4 files need wrapper fix
- 📦 17 files awaiting migration
- 🗄️ 7 files identified for archival

## Quickstart

```bash
./bootstrap.sh
source .venv/bin/activate

# Verify ipfs-datasets CLI is installed via ipfs_datasets_py dependency
ipfs-datasets --help

# Run the orchestrator wrapper
municipal-scrape --help

# Optional: install CC index tooling dependencies
pip install -e '.[ccindex]'

# CC tooling entrypoints remain runnable as top-level scripts
python3 search_cc_via_meta_indexes.py --help
python3 validate_collection_completeness.py --help
```

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
