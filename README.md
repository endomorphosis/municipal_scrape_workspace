# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

This repo also contains Common Crawl (CC) index pipeline tooling (Parquet + DuckDB pointer indexes + meta-indexes). The CC tooling now lives under the installable package namespace `common_crawl_search_engine`.

## 📚 Repository Structure

**✅ REFACTORING / REORGANIZATION COMPLETE** - Code is split into two installable packages under `src/`:

- `common_crawl_search_engine` — Common Crawl indexing/search pipeline, unified `ccindex` CLI, dashboard, MCP server
- `municipal_scrape_workspace` — municipal scraping orchestrator + wrapper CLI

👉 **Start Here**:

- [QUICKSTART.md](QUICKSTART.md)
- [docs/README.md](docs/README.md) (component docs index)

**Essential Documentation**:
- 🎯 [docs/municipal_scrape_workspace/reorganization/REORGANIZATION_PLAN.md](docs/municipal_scrape_workspace/reorganization/REORGANIZATION_PLAN.md) — root directory cleanup plan/details
- 📘 [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) — root wrapper removal + command migration map
- 📁 [docs/common_crawl_search_engine/README.md](docs/common_crawl_search_engine/README.md) — CC tooling docs
- 📁 [docs/municipal_scrape_workspace/README.md](docs/municipal_scrape_workspace/README.md) — municipal workflow docs

**Documentation Organization**:
- 📁 [docs/municipal_scrape_workspace/](docs/municipal_scrape_workspace/) - Municipal scrape + refactoring docs
- 📁 [docs/common_crawl_search_engine/](docs/common_crawl_search_engine/) - Common Crawl search engine docs
- 📁 [docs/](docs/) - Top-level docs index

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
python -m common_crawl_search_engine.ccindex.search_cc_domain --domain example.com
python -m common_crawl_search_engine.ccindex.build_cc_pointer_duckdb --help

# Method B: Via console scripts (shorter, after pip install)
ccindex-search-domain --domain example.com
ccindex-build-pointer --help
municipal-scrape --help

# 5. Run operational scripts:

# All shell scripts are in scripts/ops/ directory
scripts/ops/download_cc_indexes.sh
scripts/ops/overnight_build_duckdb_index.sh
scripts/ops/monitor_progress.sh

# Legal daemon operators can use the workspace wrappers and pending-retry watcher
bash scripts/ops/legal_data/run_agentic_legal_daemon.sh
LEGAL_DAEMON_PENDING_RETRY_CORPUS=state_laws bash scripts/ops/legal_data/run_agentic_daemon_pending_retry_watch.sh
```

Agentic legal daemon usage, deferred retry behavior, and the `Legal daemon: pending retry watch` task are documented in [ipfs_datasets_py/docs/AGENTIC_LEGAL_SCRAPER_DAEMON.md](ipfs_datasets_py/docs/AGENTIC_LEGAL_SCRAPER_DAEMON.md).

**📚 For current structure, see [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md)**

## Directory Structure

```
municipal_scrape_workspace/
├── bootstrap.sh                     # Setup script
├── pyproject.toml                   # Package configuration
├── data/                            # Reference data files
│   └── us_towns_and_counties_urls.csv
├── src/                             # Canonical Python packages (src-layout)
│   ├── common_crawl_search_engine/  # Common Crawl tooling (ccindex core + CLI/dashboard/MCP)
│   └── municipal_scrape_workspace/  # Municipal scraping tooling
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
