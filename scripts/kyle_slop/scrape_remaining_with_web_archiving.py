#!/usr/bin/env python3

import argparse
import csv
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def _ensure_ipfs_datasets_py_importable() -> None:
    try:
        import ipfs_datasets_py  # noqa: F401
        return
    except Exception:
        pass

    repo_root = Path(__file__).resolve().parents[2]
    submodule_root = repo_root / "ipfs_datasets_py"
    if (submodule_root / "ipfs_datasets_py").exists():
        sys.path.insert(0, str(submodule_root))


def _split_urls(source_url_field: str) -> List[str]:
    if source_url_field is None:
        return []
    urls = [u.strip().strip('"') for u in source_url_field.split(",")]
    return [u for u in urls if u]


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class CrawlPage:
    gnis: str
    place_name: str
    state_code: str
    root_url: str
    url: str
    depth: int
    fetched_at: str
    method_used: str
    title: str
    html: str
    text: str
    links_json: str
    metadata_json: str
    url_sha256: str
    content_sha256: str


def _iter_remaining_targets(csv_path: Path) -> Iterable[dict]:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def _crawl_site(
    scraper: Any,
    gnis: str,
    place_name: str,
    state_code: str,
    root_url: str,
    start_urls: List[str],
    max_pages: int,
    max_depth: int,
    rate_limit_seconds: float,
) -> Tuple[List[CrawlPage], Dict[str, Any]]:
    # Uses UnifiedWebScraper.scrape_sync(), which already includes internal retry logic.
    pages: List[CrawlPage] = []
    visited: Set[str] = set()

    root_domain = _domain(root_url)

    queue: List[Tuple[str, int]] = []
    for u in start_urls:
        if u:
            queue.append((u, 0))

    errors: List[str] = []

    while queue and len(pages) < max_pages:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        if depth > max_depth:
            continue

        if root_domain and _domain(url) and _domain(url) != root_domain:
            visited.add(url)
            continue

        visited.add(url)

        try:
            result = scraper.scrape_sync(url)
        except Exception as e:
            errors.append(f"scrape_sync exception: {url} :: {e}")
            continue

        if not getattr(result, "success", False):
            err_list = getattr(result, "errors", []) or []
            errors.append(f"scrape failed: {url} :: {err_list}")
            continue

        html = getattr(result, "html", "") or ""
        text = getattr(result, "text", "") or ""
        title = getattr(result, "title", "") or ""
        links = getattr(result, "links", []) or []
        metadata = getattr(result, "metadata", {}) or {}
        method_used = getattr(getattr(result, "method_used", None), "value", None) or str(
            getattr(result, "method_used", "")
        )

        fetched_at = datetime.now().isoformat()
        url_hash = _sha256_hex(url.encode("utf-8"))
        content_hash = _sha256_hex((html or text).encode("utf-8", errors="ignore"))

        pages.append(
            CrawlPage(
                gnis=str(gnis),
                place_name=place_name,
                state_code=state_code,
                root_url=root_url,
                url=url,
                depth=int(depth),
                fetched_at=fetched_at,
                method_used=method_used,
                title=title,
                html=html,
                text=text,
                links_json=json.dumps(links, ensure_ascii=False),
                metadata_json=json.dumps(metadata, ensure_ascii=False),
                url_sha256=url_hash,
                content_sha256=content_hash,
            )
        )

        # Enqueue additional pages from links (same-domain only) at next depth.
        if depth < max_depth:
            for link in links:
                href = (link or {}).get("url")
                if not href or not isinstance(href, str):
                    continue
                if href in visited:
                    continue
                if root_domain and _domain(href) and _domain(href) != root_domain:
                    continue
                queue.append((href, depth + 1))

        if rate_limit_seconds > 0:
            try:
                import time

                time.sleep(rate_limit_seconds)
            except Exception:
                pass

    summary = {
        "gnis": str(gnis),
        "root_url": root_url,
        "root_domain": root_domain,
        "attempted": len(visited),
        "saved": len(pages),
        "max_pages": max_pages,
        "max_depth": max_depth,
        "errors": errors,
    }
    return pages, summary


def _append_parquet(out_parquet: Path, rows: List[Dict[str, Any]]) -> None:
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, preserve_index=False)

    if out_parquet.exists():
        existing = pq.read_table(out_parquet)
        combined = pa.concat_tables([existing, table], promote=True)
        pq.write_table(combined, out_parquet)
    else:
        pq.write_table(table, out_parquet)


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape remaining municipal targets using ipfs_datasets_py.web_archiving")
    ap.add_argument(
        "--remaining-csv",
        required=True,
        help="CSV produced by generate_remaining_targets.py",
    )
    ap.add_argument(
        "--out-dir",
        default="datasets/municipal_web_archives_parquet",
        help="Output directory for pages.parquet and runs.parquet",
    )
    ap.add_argument("--max-pages", type=int, default=5, help="Max pages per GNIS (same domain)")
    ap.add_argument("--max-depth", type=int, default=1, help="Max link depth from start URLs")
    ap.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.5,
        help="Sleep between page fetches (politeness)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="If >0, only scrape first N targets (smoke tests)",
    )

    args = ap.parse_args()

    _ensure_ipfs_datasets_py_importable()

    from ipfs_datasets_py.web_archiving.unified_web_scraper import UnifiedWebScraper, ScraperConfig, ScraperMethod

    # Constrain to methods we know exist in this environment.
    config = ScraperConfig(
        preferred_methods=[ScraperMethod.BEAUTIFULSOUP, ScraperMethod.REQUESTS_ONLY],
        fallback_enabled=True,
        extract_links=True,
        extract_text=True,
        rate_limit_delay=0.0,
    )
    scraper = UnifiedWebScraper(config=config)

    out_dir = Path(args.out_dir)
    pages_path = out_dir / "pages.parquet"
    runs_path = out_dir / "crawl_runs.parquet"

    total_targets = 0
    all_page_rows: List[Dict[str, Any]] = []
    all_run_rows: List[Dict[str, Any]] = []

    for row in _iter_remaining_targets(Path(args.remaining_csv)):
        total_targets += 1
        if args.limit and total_targets > args.limit:
            break

        gnis = (row.get("gnis") or "").strip()
        place_name = (row.get("place_name") or "").strip()
        state_code = (row.get("state_code") or "").strip()
        source_url = (row.get("source_url") or "").strip()

        urls = _split_urls(source_url)
        if not urls:
            all_run_rows.append(
                {
                    "gnis": gnis,
                    "root_url": "",
                    "status": "skipped_no_url",
                    "attempted": 0,
                    "saved": 0,
                    "errors_json": json.dumps(["no URLs"], ensure_ascii=False),
                    "run_at": datetime.now().isoformat(),
                }
            )
            continue

        root_url = urls[0]
        pages, summary = _crawl_site(
            scraper=scraper,
            gnis=gnis,
            place_name=place_name,
            state_code=state_code,
            root_url=root_url,
            start_urls=urls,
            max_pages=int(args.max_pages),
            max_depth=int(args.max_depth),
            rate_limit_seconds=float(args.rate_limit_seconds),
        )

        for p in pages:
            all_page_rows.append(p.__dict__)

        all_run_rows.append(
            {
                "gnis": gnis,
                "place_name": place_name,
                "state_code": state_code,
                "root_url": summary.get("root_url", root_url),
                "root_domain": summary.get("root_domain", ""),
                "status": "ok" if summary.get("saved", 0) > 0 else "failed",
                "attempted": summary.get("attempted", 0),
                "saved": summary.get("saved", 0),
                "max_pages": int(args.max_pages),
                "max_depth": int(args.max_depth),
                "errors_json": json.dumps(summary.get("errors", []), ensure_ascii=False),
                "run_at": datetime.now().isoformat(),
            }
        )

        # Periodically flush to disk to avoid holding everything.
        if len(all_page_rows) >= 200:
            _append_parquet(pages_path, all_page_rows)
            all_page_rows = []
        if len(all_run_rows) >= 50:
            _append_parquet(runs_path, all_run_rows)
            all_run_rows = []

    if all_page_rows:
        _append_parquet(pages_path, all_page_rows)
    if all_run_rows:
        _append_parquet(runs_path, all_run_rows)

    print(f"Wrote: {pages_path}")
    print(f"Wrote: {runs_path}")
    print(f"Scrape targets processed: {min(total_targets, args.limit) if args.limit else total_targets}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
