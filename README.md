# municipal-scrape-workspace

This is a standalone Python package + git repo for the municipal scraping workflow.

## Quickstart

```bash
./bootstrap.sh
source .venv/bin/activate

# Verify ipfs-datasets CLI is installed via ipfs_datasets_py dependency
ipfs-datasets --help

# Run the orchestrator wrapper
municipal-scrape --help
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
