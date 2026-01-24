# Repository Structure (Current)

This repo contains two installable Python packages under `src/`:

- **`common_crawl_search_engine`** — Common Crawl indexing/search tooling (DuckDB pointer indexes + Parquet/meta-indexes), unified `ccindex` CLI, dashboard, MCP server
- **`municipal_scrape_workspace`** — municipal scraping workflow + wrapper CLI

## Layout

```
municipal_scrape_workspace/
├── src/
│   ├── common_crawl_search_engine/
│   │   ├── cli.py              # unified `ccindex` CLI
│   │   ├── dashboard.py        # FastAPI dashboard (+ MCP-over-HTTP endpoint)
│   │   ├── mcp_server.py       # MCP stdio server
│   │   └── ccindex/            # core CC index pipeline + search + API
│   └── municipal_scrape_workspace/
│       ├── cli.py              # `municipal-scrape` wrapper CLI
│       ├── orchestrate_municipal_scrape.py
│       └── check_archive_callbacks.py
├── scripts/ops/                # operational shell scripts
├── docs/                       # documentation (split by component)
├── data/                       # reference datasets (e.g., municipal URL CSV)
└── tests/
```

## Start Here

- Repo quickstart: [../QUICKSTART.md](../QUICKSTART.md)
- Docs index: [README.md](README.md)
- Common Crawl docs: [common_crawl_search_engine/README.md](common_crawl_search_engine/README.md)
- Municipal docs: [municipal_scrape_workspace/README.md](municipal_scrape_workspace/README.md)

## Key Commands

After `pip install -e .`:

- CC tools (recommended): `ccindex ...`
  - Example: `ccindex search meta --domain example.com --max-matches 50`
  - Dashboard: `ccindex mcp start --host 127.0.0.1 --port 8787`
- Municipal tools: `municipal-scrape --help`

## Historical Docs

Some refactoring-era documents describe the *previous* layout where Common Crawl tooling lived under `municipal_scrape_workspace.ccindex`.

- Historical structure docs live under: [municipal_scrape_workspace/refactoring/historical/](municipal_scrape_workspace/refactoring/historical/)
