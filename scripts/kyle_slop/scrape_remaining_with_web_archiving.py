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


def _parse_html_for_fields(html: str, *, base_url: str) -> Tuple[str, str, List[Dict[str, str]]]:
    title = ""
    text = ""
    links: List[Dict[str, str]] = []
    if not html:
        return title, text, links

    try:
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin

        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        title = title_tag.get_text() if title_tag else ""

        for script in soup(["script", "style"]):
            script.decompose()
        text = soup.get_text(separator="\n", strip=True)

        for link in soup.find_all("a", href=True):
            href = link.get("href")
            if not href:
                continue
            if href.startswith("/"):
                href = urljoin(base_url, href)
            links.append({"url": href, "text": link.get_text(strip=True)})
    except Exception:
        pass

    return title, text, links


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


def _try_common_crawl_domain(
    *,
    gnis: str,
    place_name: str,
    state_code: str,
    root_url: str,
    max_pages: int,
    timeout_s: float,
) -> Tuple[Optional[List[CrawlPage]], Dict[str, Any]]:
    """Attempt a domain-first scrape using Common Crawl pointers.

    Returns (pages_or_none, summary). When Common Crawl isn't available or fails,
    pages_or_none is None and caller should fall back to origin crawling.
    """

    try:
        from common_crawl_search_engine.ccindex import api as ccapi
    except Exception as e:
        return None, {"status": "cc_unavailable", "error": f"import failed: {type(e).__name__}: {e}"}

    try:
        res = ccapi.search_domain_via_meta_indexes(
            root_url,
            parquet_root=Path("/storage/ccindex_parquet"),
            master_db=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
            max_matches=int(max_pages),
        )
        records = list(getattr(res, "records", []) or [])
        if not records:
            return None, {"status": "cc_no_records", "error": "no records"}

        pages: List[CrawlPage] = []
        errors: List[str] = []

        for r in records[: int(max_pages)]:
            try:
                wf = r.get("warc_filename")
                off = r.get("warc_offset")
                ln = r.get("warc_length")
                page_url = str(r.get("url") or root_url)
                if not wf or off is None or ln is None:
                    errors.append(f"pointer missing fields for url={page_url}")
                    continue

                fetch, source, local_path = ccapi.fetch_warc_record(
                    warc_filename=str(wf),
                    warc_offset=int(off),
                    warc_length=int(ln),
                    timeout_s=float(timeout_s),
                    max_bytes=2_000_000,
                    decode_gzip_text=False,
                    cache_mode="range",
                )
                if not getattr(fetch, "ok", False) or not getattr(fetch, "raw_base64", None):
                    errors.append(f"fetch failed url={page_url} err={getattr(fetch, 'error', None)}")
                    continue

                import base64

                gz_bytes = base64.b64decode(fetch.raw_base64)
                http = ccapi.extract_http_from_warc_gzip_member(
                    gz_bytes,
                    max_body_bytes=2_000_000,
                    max_preview_chars=200_000,
                )

                html = http.body_text_preview or ""
                title, text, links = _parse_html_for_fields(html, base_url=page_url)

                fetched_at = datetime.now().isoformat()
                pages.append(
                    CrawlPage(
                        gnis=str(gnis),
                        place_name=place_name,
                        state_code=state_code,
                        root_url=root_url,
                        url=page_url,
                        depth=0,
                        fetched_at=fetched_at,
                        method_used="common_crawl",
                        title=title,
                        html=html,
                        text=text,
                        links_json=json.dumps(links, ensure_ascii=False),
                        metadata_json=json.dumps(
                            {
                                "cc_record": r,
                                "cc_source": source,
                                "cc_local_warc_path": local_path,
                                "http_status": http.http_status,
                                "http_mime": http.body_mime,
                                "http_charset": http.body_charset,
                                "http_ok": http.ok,
                                "http_error": http.error,
                            },
                            ensure_ascii=False,
                        ),
                        url_sha256=_sha256_hex(page_url.encode("utf-8")),
                        content_sha256=_sha256_hex((html or text).encode("utf-8", errors="ignore")),
                    )
                )
            except Exception as e:
                errors.append(f"record failed: {type(e).__name__}: {e}")
                continue

        if not pages:
            return None, {"status": "cc_failed", "error": "no pages", "errors": errors}

        return pages, {
            "status": "cc_ok",
            "attempted": len(records[: int(max_pages)]),
            "saved": len(pages),
            "errors": errors,
        }
    except Exception as e:
        return None, {"status": "cc_error", "error": f"{type(e).__name__}: {e}"}


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
        preferred_methods=[ScraperMethod.COMMON_CRAWL, ScraperMethod.BEAUTIFULSOUP, ScraperMethod.REQUESTS_ONLY],
        fallback_enabled=True,
        extract_links=True,
        extract_text=True,
        rate_limit_delay=0.0,
        common_crawl_max_matches=int(args.max_pages),
        common_crawl_parquet_root="/storage/ccindex_parquet",
        common_crawl_master_db="/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb",
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

        # 1) Try Common Crawl domain-first (avoids origin fetch / Cloudflare).
        pages_cc, cc_summary = _try_common_crawl_domain(
            gnis=gnis,
            place_name=place_name,
            state_code=state_code,
            root_url=root_url,
            max_pages=int(args.max_pages),
            timeout_s=float(config.timeout),
        )

        if pages_cc is not None:
            pages, summary = pages_cc, {
                "gnis": str(gnis),
                "root_url": root_url,
                "root_domain": _domain(root_url),
                "attempted": cc_summary.get("attempted", 0),
                "saved": cc_summary.get("saved", 0),
                "max_pages": int(args.max_pages),
                "max_depth": 0,
                "errors": cc_summary.get("errors", []),
                "cc_status": cc_summary.get("status"),
                "cc_error": cc_summary.get("error"),
            }
        else:
            # 2) Fall back to origin crawling (UnifiedWebScraper).
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
