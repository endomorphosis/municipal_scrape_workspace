# Playwright panel E2E report

Date: 2026-01-24

## Scope
These are real end-to-end (E2E) Playwright tests for the dashboard’s three “panel pages”:

- Wayback-like replay page
- Search page (Brave → Common Crawl record)
- Settings page (preferences + cache operations)

The tests start a real dashboard subprocess on a free port, drive the browser UI, and validate that record retrieval/rendering happens via Common Crawl.

## How to run

```bash
cd /home/barberb/municipal_scrape_workspace
RUN_PLAYWRIGHT=1 RUN_DASHBOARD_E2E=1 pytest -q tests/common_crawl_search_engine/dashboard/test_panels_e2e_playwright.py -vv
```

Notes:
- `RUN_DASHBOARD_E2E=1` requires real local CCIndex storage (the test points at the configured `/storage` datasets).
- The Search (Brave) E2E test additionally requires `BRAVE_SEARCH_API_KEY`; otherwise it is skipped.

## Latest results
Command:

```bash
RUN_PLAYWRIGHT=1 RUN_DASHBOARD_E2E=1 pytest -q tests/common_crawl_search_engine/dashboard/test_panels_e2e_playwright.py -vv
```

Outcome:
- `test_wayback_panel_e2e`: PASSED
- `test_search_panel_brave_to_record_e2e`: SKIPPED (no `BRAVE_SEARCH_API_KEY`)
- `test_settings_panel_save_and_cache_clear_affects_record_defaults`: PASSED

Summary: 2 passed, 1 skipped (≈ 6.9s)
