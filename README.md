# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

This repo also contains Common Crawl (CC) index pipeline tooling (Parquet + DuckDB pointer indexes + meta-indexes). The CC tooling is being migrated into the installable package namespace under `municipal_scrape_workspace.ccindex`.

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
