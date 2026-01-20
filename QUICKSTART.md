# Quick Start Guide - Municipal Scrape Workspace

## Repository Structure

This repository contains:
- **Municipal scraping tools** - Orchestrate scraping of municipal websites
- **Common Crawl (CC) index pipeline** - Tools for building and searching CC indexes
- **Python package** - Installable with `pip install -e .`

## Setup

```bash
# 1. Bootstrap environment
./bootstrap.sh
source .venv/bin/activate

# 2. Install package
pip install -e .

# 3. (Optional) Install with CC index tools
pip install -e '.[ccindex]'
```

## Running Tools

### Three Methods to Run Python Tools

#### Method 1: Root Wrappers (Backwards Compatible)
```bash
./search_cc_domain.py --domain example.com
./build_cc_pointer_duckdb.py --help
```

#### Method 2: Python Modules (Recommended)
```bash
python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com
python -m municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb --help
```

#### Method 3: Console Scripts (Shortest)
```bash
ccindex-search-domain --domain example.com
ccindex-build-pointer --help
```

### Running Shell Scripts

All operational shell scripts are in `scripts/ops/`:

```bash
# Download CC indexes
scripts/ops/download_cc_indexes.sh

# Build pointer index
scripts/ops/overnight_build_duckdb_index.sh

# Monitor progress
scripts/ops/monitor_progress.sh
```

## Common Workflows

### 1. Search Common Crawl for a Domain

```bash
# Using wrapper
./search_cc_domain.py --domain example.com

# Using console script
ccindex-search-domain --domain example.com
```

### 2. Build DuckDB Pointer Index

```bash
# Using wrapper
./build_cc_pointer_duckdb.py --output-dir /path/to/indexes

# Using shell script
scripts/ops/overnight_build_duckdb_index.sh
```

### 3. Orchestrate Full CC Pipeline

```bash
# Using wrapper
./cc_pipeline_orchestrator.py --config pipeline_config.json

# Using console script
ccindex-orchestrate --config pipeline_config.json
```

### 4. Municipal Scraping

```bash
# Scrape municipal websites
python -m municipal_scrape_workspace.orchestrate_municipal_scrape \
  --csv data/us_towns_and_counties_urls.csv \
  --out /path/to/output \
  --limit 10
```

## Data Files

- **CSV Data**: Municipal scraping targets are in `data/us_towns_and_counties_urls.csv`
- **Configs**: `collinfo.json`, `pipeline_config.json` in root directory

## Documentation

- **[README.md](README.md)** - Main entry point
- **[REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md)** - Complete structure guide
- **[REORGANIZATION_PLAN.md](REORGANIZATION_PLAN.md)** - Root directory cleanup details
- **[docs/](docs/)** - Detailed documentation

## Need Help?

1. Check [REFACTORED_STRUCTURE.md](REFACTORED_STRUCTURE.md) for complete structure
2. See [docs/COMMON_CRAWL_USAGE.md](docs/COMMON_CRAWL_USAGE.md) for CC usage
3. Review [REORGANIZATION_PLAN.md](REORGANIZATION_PLAN.md) for recent changes
