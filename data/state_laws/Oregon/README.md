# Oregon ORS Download Layout

This folder now includes a resilient downloader that stores Oregon Revised Statutes chapter pages locally and falls back to archival sources when direct requests are blocked.

## Run

From workspace root:

```bash
python data/state_laws/Oregon/oregon_ors_downloader.py --max-chapters 50
```

Optional flags:

- `--chapter-start 1 --chapter-end 75` to limit chapter range
- `--force` to redownload existing chapter files
- `--delay-seconds 0.4` to tune request pacing

## Output structure

- `raw_html/` — downloaded chapter HTML files like `ors001.html`
- `manifests/chapter_manifest.json` — per-chapter source and checksum
- `manifests/download_report.json` — run summary
- `manifests/latest.json` — pointers to the latest manifest/report
- `parsed/chapter_summaries.jsonl` — extracted chapter-level summaries

## Fallback behavior

For each chapter URL, the downloader tries:

1. Direct fetch from `oregonlegislature.gov`
2. Common Crawl fallback using `processors.web_archiving.common_crawl_integration`
3. Wayback Machine captures via project web-archiving engine
4. Archive.is retrieval via project web-archiving engine

This allows initial collection to proceed even when direct access is filtered.
