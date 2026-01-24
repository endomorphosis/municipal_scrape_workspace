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

# 4. (Optional) Install MCP server for ccindex
pip install -e '.[ccindex-mcp]'

# 5. (Optional) Install ccindex dashboard
pip install -e '.[ccindex-dashboard]'
```

## Running Tools

### Two Methods to Run Python Tools

#### Method 1: Python Modules
```bash
python -m common_crawl_search_engine.ccindex.search_cc_domain --domain example.com
python -m common_crawl_search_engine.ccindex.build_cc_pointer_duckdb --help
```

#### Method 2: Console Scripts (Shorter - After pip install)
```bash
ccindex-search-domain --domain example.com
ccindex-build-pointer --help
```

#### Method 3: Unified ccindex CLI (Recommended)
```bash
# Meta-index search (master -> year -> collection -> parquet)
ccindex search meta --domain example.com --max-matches 50 --stats

# Delegate to the existing scripts when you want their full flag surface
ccindex search domain -- --help
ccindex build pointer -- --help
ccindex orchestrate -- --help
```

**Note**: Root wrapper files have been removed. See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for migration instructions.

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
# Using Python module
python -m common_crawl_search_engine.ccindex.search_cc_domain --domain example.com

# Using console script (after pip install)
ccindex-search-domain --domain example.com

# Using unified ccindex CLI
ccindex search meta --domain example.com --max-matches 50
```

### 1b. Use ccindex as a Library (Imports)

```python
from pathlib import Path
from common_crawl_search_engine.ccindex.api import search_domain_via_meta_indexes

result = search_domain_via_meta_indexes(
  "example.com",
  parquet_root=Path("/storage/ccindex_parquet"),
  max_matches=25,
)

print(result.emitted, result.elapsed_s)
print(result.records[0])
```

### 1c. Run ccindex as an MCP Server

```bash
# after: pip install -e '.[ccindex-mcp]'
ccindex mcp serve

# (legacy entrypoint; still works)
# ccindex-mcp
```

### 1d. Run ccindex Dashboard (Local “Archive-ish” UI)

```bash
# after: pip install -e '.[ccindex-dashboard]'

# starts BOTH the dashboard and the MCP HTTP JSON-RPC endpoint used by the dashboard
ccindex mcp start --host 127.0.0.1 --port 8787

# (legacy entrypoint; still works)
# ccindex-dashboard --host 127.0.0.1 --port 8787

# then open:
# http://127.0.0.1:8787
```

### 2. Build DuckDB Pointer Index

```bash
# Using Python module
python -m common_crawl_search_engine.ccindex.build_cc_pointer_duckdb --output-dir /path/to/indexes

# Using console script
ccindex-build-pointer --output-dir /path/to/indexes

# Using shell script
scripts/ops/overnight_build_duckdb_index.sh
```

### 3. Orchestrate Full CC Pipeline

```bash
# Using Python module
python -m common_crawl_search_engine.ccindex.cc_pipeline_orchestrator --config pipeline_config.json

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
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - **Migration guide for wrapper removal**
- **[docs/README.md](docs/README.md)** - Documentation index (by component)
- **[docs/common_crawl_search_engine/README.md](docs/common_crawl_search_engine/README.md)** - CC search engine docs
- **[docs/municipal_scrape_workspace/README.md](docs/municipal_scrape_workspace/README.md)** - Municipal scrape docs
- **[docs/municipal_scrape_workspace/reorganization/REORGANIZATION_PLAN.md](docs/municipal_scrape_workspace/reorganization/REORGANIZATION_PLAN.md)** - Root directory cleanup details
- **[docs/](docs/)** - Detailed documentation

## Need Help?

1. Start at [docs/README.md](docs/README.md) (component docs index)
2. See [docs/municipal_scrape_workspace/COMMON_CRAWL_USAGE.md](docs/municipal_scrape_workspace/COMMON_CRAWL_USAGE.md) for CC usage
3. Review [docs/municipal_scrape_workspace/reorganization/REORGANIZATION_PLAN.md](docs/municipal_scrape_workspace/reorganization/REORGANIZATION_PLAN.md) for recent changes
