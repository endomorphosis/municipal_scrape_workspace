#!/usr/bin/env python3
"""Municipal websites scraping orchestrator (DuckDB + Parquet/HF datasets).

What this does
-------------
1) Ingests `us_towns_and_counties_urls.csv` into DuckDB.
2) Scrapes source URLs in parallel using `ipfs_datasets_py` unified scraper.
3) Computes a deterministic content CID for each fetched page.
4) Writes raw bytes to `content_blobs/<cid>.bin` (dedup by CID).
5) Maintains resumable state in DuckDB so the job can be interrupted/resumed.
6) Exports three Parquet files that can be loaded as HuggingFace datasets:
   - `datasets/towns.parquet` (the input table)
   - `datasets/url_cid_map.parquet` (URL + latest CID + scrape metadata)
   - `datasets/cid_content.parquet` (CID + content metadata + blob path + text)

Multi-server / sharding
----------------------
For now, "multi-server" is supported via deterministic sharding:
`shard = hash(url) % num_workers`. Run this script on multiple machines with the
same CSV, different `--worker-id`, and a shared output root (or later merge).
Each worker writes to its own DuckDB file: `state/worker_<id>.duckdb`.

This keeps writes local (avoids DuckDB write-lock contention across hosts).

Example
-------
# One machine, small smoke run
/home/barberb/ipfs_datasets_py/.venv/bin/python /home/barberb/municipal_scrape_workspace/orchestrate_municipal_scrape.py \
  --csv /home/barberb/municipal_scrape_workspace/us_towns_and_counties_urls.csv \
  --out /home/barberb/municipal_scrape_workspace \
  --limit 5 --max-concurrent 5

# Two machines (or two processes), shard the work
# Machine A
... orchestrate_municipal_scrape.py --num-workers 2 --worker-id 0
# Machine B
... orchestrate_municipal_scrape.py --num-workers 2 --worker-id 1
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb

# Prefer a local development checkout of ipfs_datasets_py (sibling to this repo)
# so subpackages like `ipfs_datasets_py.mcp_server.tools.*` are importable.
_local_ipfs_datasets_repo = Path(__file__).resolve().parent.parent / "ipfs_datasets_py"
if _local_ipfs_datasets_repo.exists():
    sys.path.insert(0, str(_local_ipfs_datasets_repo))

from ipfs_datasets_py.integrations import compute_cid_for_content
from ipfs_datasets_py.unified_web_scraper import ScraperConfig, ScraperMethod, UnifiedWebScraper


def _log(worker_id: int, msg: str, level: str = "INFO") -> None:
    """Verbose logging with timestamp and worker ID."""
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] [W{worker_id}] [{level}] {msg}", flush=True)


def _extract_domain(url: str) -> str:
    """Extract domain from a URL (e.g., 'https://example.com/path' -> 'example.com')."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc or ""
    except Exception:
        return ""


async def _query_cc_cdx_for_domain(domain: str, worker_id: int) -> List[str]:
    """Query Common Crawl CDX API for all archived URLs on a domain.
    
    Returns a list of unique URLs found in Common Crawl's CDX index for the domain.
    """
    try:
        import aiohttp
    except ImportError:
        _log(worker_id, f"aiohttp not available; skipping CDX query for {domain}", "WARN")
        return []

    cdx_api = "https://cdx.commoncrawl.org/search/cdx"
    params = {
        "url": f"{domain}/*",
        "output": "json",
        "collapse": "urlkey",
        "pageSize": 10000,
    }

    _log(worker_id, f"Querying CC CDX for domain: {domain}", "INFO")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(cdx_api, params=params, timeout=30) as resp:
                if resp.status != 200:
                    _log(worker_id, f"CC CDX query failed: HTTP {resp.status} for {domain}", "WARN")
                    return []
                data = await resp.json()
                if not isinstance(data, list) or len(data) < 2:
                    _log(worker_id, f"CC CDX returned no results for {domain}", "DEBUG")
                    return []
                # First row is headers; remaining are [timestamp, status_code, original_url, ...]
                urls: List[str] = []
                seen: set[str] = set()
                for row in data[1:]:
                    if len(row) >= 3:
                        url = row[2]  # original_url column
                        if url not in seen:
                            seen.add(url)
                            urls.append(url)
                _log(worker_id, f"CC CDX found {len(urls)} unique URLs for {domain}", "INFO")
                return urls
    except Exception as e:
        _log(worker_id, f"CC CDX query error: {type(e).__name__}: {e}", "WARN")
        return []


def _load_archived_urls_from_jsonl(path: Path) -> List[str]:
    """Load URLs that appear archived from a JSONL status/callback file.

    Accepts lines containing:
    - `archive_org_present` / `archive_is_present` booleans (from check_archive_callbacks.py)
    - Any event payload that includes those keys.
    """

    urls: List[str] = []
    if not path.exists():
        return urls

    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue

            url = (evt.get("url") or "").strip()
            if not url:
                continue

            ao = bool(evt.get("archive_org_present"))
            ai = bool(evt.get("archive_is_present"))
            if not (ao or ai):
                continue

            if url in seen:
                continue
            seen.add(url)
            urls.append(url)

    return sorted(urls)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_shard(url: str, num_workers: int) -> int:
    """Deterministic shard assignment (stable across runs/machines)."""
    if num_workers <= 1:
        return 0
    digest = hashlib.sha256(url.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big", signed=False)
    return value % num_workers


def _split_urls(source_url_field: str) -> List[str]:
    """CSV field sometimes contains multiple URLs separated by commas."""
    if not source_url_field:
        return []

    # Prefer a regex so we handle commas/whitespace/odd separators robustly.
    # Keep http(s) only.
    field = source_url_field.strip().strip('"').strip("'")
    return re.findall(r"https?://[^\s,;]+", field)


def _extract_domain(url: str) -> str:
    """Extract root domain from URL for crawling scope."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove 'www.' prefix if present
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def _extract_links_from_metadata(metadata_json: str) -> List[str]:
    """Extract discovered links from scraper metadata JSON."""
    try:
        meta = json.loads(metadata_json or "{}")
        links = meta.get("links", [])
        if isinstance(links, list):
            return [link for link in links if isinstance(link, str) and link.startswith(("http://", "https://"))]
    except Exception:
        pass
    return []


def _same_domain(url1: str, url2: str) -> bool:
    """Check if two URLs share the same root domain."""
    d1 = _extract_domain(url1)
    d2 = _extract_domain(url2)
    return bool(d1 and d2 and d1 == d2)


def _ensure_dirs(out_root: Path) -> Dict[str, Path]:
    state_dir = out_root / "state"
    datasets_dir = out_root / "datasets"
    blobs_dir = out_root / "content_blobs"
    logs_dir = out_root / "logs"
    state_dir.mkdir(parents=True, exist_ok=True)
    datasets_dir.mkdir(parents=True, exist_ok=True)
    blobs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return {
        "state": state_dir,
        "datasets": datasets_dir,
        "blobs": blobs_dir,
        "logs": logs_dir,
    }


def _connect_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA enable_object_cache")
    return con


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS towns (
            gnis BIGINT,
            place_name VARCHAR,
            state_code VARCHAR,
            source_url VARCHAR,
            status VARCHAR
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS town_urls (
            gnis BIGINT,
            place_name VARCHAR,
            state_code VARCHAR,
            url VARCHAR,
            source_url_raw VARCHAR,
            shard INTEGER,
            PRIMARY KEY (gnis, url)
        );
        """
    )

    # One row per scrape attempt
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_attempts (
            url VARCHAR,
            attempt INTEGER,
            worker_id INTEGER,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR,
            error VARCHAR,
            method_used VARCHAR,
            status_code INTEGER,
            content_type VARCHAR,
            content_cid VARCHAR,
            content_bytes BIGINT,
            text_bytes BIGINT,
            metadata_json VARCHAR,
            PRIMARY KEY (url, attempt, worker_id)
        );
        """
    )

    # Latest URL->CID mapping + last scrape summary (resumable status)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS url_cid_latest (
            url VARCHAR PRIMARY KEY,
            last_status VARCHAR,
            last_error VARCHAR,
            last_method_used VARCHAR,
            last_status_code INTEGER,
            last_content_type VARCHAR,
            last_content_cid VARCHAR,
            last_ipfs_cid VARCHAR,
            last_content_bytes BIGINT,
            last_text_bytes BIGINT,
            last_finished_at TIMESTAMP,
            attempts INTEGER
        );
        """
    )

    # CID-indexed content records (dedup across URLs)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cid_content (
            content_cid VARCHAR PRIMARY KEY,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            content_type VARCHAR,
            ipfs_cid VARCHAR,
            content_bytes BIGINT,
            text VARCHAR,
            blob_path VARCHAR
        );
        """
    )

    # URL->CID history (many-to-one), useful for provenance
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS url_cid_history (
            url VARCHAR,
            content_cid VARCHAR,
            ipfs_cid VARCHAR,
            seen_at TIMESTAMP,
            worker_id INTEGER,
            PRIMARY KEY (url, content_cid, seen_at)
        );
        """
    )

    # Back-compat for DB files created before new columns existed.
    for stmt in [
        "ALTER TABLE url_cid_latest ADD COLUMN last_ipfs_cid VARCHAR",
        "ALTER TABLE cid_content ADD COLUMN ipfs_cid VARCHAR",
        "ALTER TABLE url_cid_history ADD COLUMN ipfs_cid VARCHAR",
    ]:
        try:
            con.execute(stmt)
        except Exception:
            pass

    # Discovered links table (for full website crawling mode)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS discovered_links (
            source_url VARCHAR,
            discovered_url VARCHAR,
            source_domain VARCHAR,
            discovered_domain VARCHAR,
            discovered_at TIMESTAMP,
            worker_id INTEGER,
            PRIMARY KEY (source_url, discovered_url)
        );
        """
    )


def ingest_csv_to_db(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    num_workers: int,
) -> None:
    """Load CSV into `towns` and normalize urls into `town_urls`."""

    # If towns already ingested, skip re-insert.
    existing_row = con.execute("SELECT COUNT(*) FROM towns").fetchone()
    existing = int(existing_row[0]) if existing_row and existing_row[0] is not None else 0
    if existing == 0:
        print(f"[INGEST] Loading CSV from {csv_path}", flush=True)
        # Robust CSV parse (some fields include commas inside quotes)
        rows: List[Tuple[Any, ...]] = []
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                gnis = int(r.get("gnis") or 0)
                place_name = (r.get("place_name") or "").strip()
                state_code = (r.get("state_code") or "").strip()
                source_url = (r.get("source_url") or "").strip()
                status = (r.get("status") or "").strip()
                rows.append((gnis, place_name, state_code, source_url, status))

        print(f"[INGEST] Inserting {len(rows)} town records", flush=True)
        con.executemany(
            "INSERT INTO towns (gnis, place_name, state_code, source_url, status) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        print(f"[INGEST] Towns ingested successfully", flush=True)

    # Rebuild town_urls idempotently
    print(f"[INGEST] Normalizing URLs and assigning shards for {num_workers} workers", flush=True)
    con.execute("DELETE FROM town_urls")

    normalized_rows: List[Tuple[Any, ...]] = []
    for gnis, place_name, state_code, source_url, _status in con.execute(
        "SELECT gnis, place_name, state_code, source_url, status FROM towns"
    ).fetchall():
        urls = _split_urls(source_url or "")
        for url in urls:
            shard = _stable_shard(url, num_workers)
            normalized_rows.append((gnis, place_name, state_code, url, source_url, shard))
    
        print(f"[INGEST] Inserting {len(normalized_rows)} normalized URLs", flush=True)

    con.executemany(
        "INSERT INTO town_urls (gnis, place_name, state_code, url, source_url_raw, shard) VALUES (?, ?, ?, ?, ?, ?)",
        normalized_rows,
    )


def _blob_path_for_cid(blobs_dir: Path, cid: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", cid)
    return blobs_dir / f"{safe}.bin"


def _upsert_url_latest(
    con: duckdb.DuckDBPyConnection,
    url: str,
    status: str,
    error: Optional[str],
    method_used: Optional[str],
    status_code: Optional[int],
    content_type: Optional[str],
    content_cid: Optional[str],
    ipfs_cid: Optional[str],
    content_bytes: Optional[int],
    text_bytes: Optional[int],
    finished_at: Optional[str],
) -> None:
    con.execute(
        """
        INSERT INTO url_cid_latest AS t (
            url, last_status, last_error, last_method_used, last_status_code,
            last_content_type, last_content_cid, last_ipfs_cid, last_content_bytes, last_text_bytes,
            last_finished_at, attempts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(url) DO UPDATE SET
            last_status=excluded.last_status,
            last_error=excluded.last_error,
            last_method_used=excluded.last_method_used,
            last_status_code=excluded.last_status_code,
            last_content_type=excluded.last_content_type,
            last_content_cid=excluded.last_content_cid,
            last_ipfs_cid=COALESCE(excluded.last_ipfs_cid, t.last_ipfs_cid),
            last_content_bytes=excluded.last_content_bytes,
            last_text_bytes=excluded.last_text_bytes,
            last_finished_at=excluded.last_finished_at,
            attempts=t.attempts + 1;
        """,
        [
            url,
            status,
            error,
            method_used,
            status_code,
            content_type,
            content_cid,
            ipfs_cid,
            content_bytes,
            text_bytes,
            finished_at,
        ],
    )


def _upsert_cid_content(
    con: duckdb.DuckDBPyConnection,
    cid: str,
    content_type: Optional[str],
    ipfs_cid: Optional[str],
    content_bytes: int,
    text: str,
    blob_path: str,
    seen_at: str,
) -> None:
    con.execute(
        """
        INSERT INTO cid_content AS c (
            content_cid, first_seen_at, last_seen_at, content_type, ipfs_cid, content_bytes, text, blob_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_cid) DO UPDATE SET
            last_seen_at=excluded.last_seen_at,
            content_type=COALESCE(excluded.content_type, c.content_type),
            ipfs_cid=COALESCE(c.ipfs_cid, excluded.ipfs_cid),
            content_bytes=GREATEST(c.content_bytes, excluded.content_bytes),
            text=CASE
                WHEN c.text IS NULL OR length(c.text) < length(excluded.text) THEN excluded.text
                ELSE c.text
            END,
            blob_path=c.blob_path;
        """,
        [cid, seen_at, seen_at, content_type, ipfs_cid, content_bytes, text, blob_path],
    )


def _ipfs_add_file(ipfs_bin: str, file_path: Path, *, pin: bool) -> str:
    args = [
        ipfs_bin,
        "add",
        "--cid-version=1",
        "--hash=sha2-256",
        f"--pin={'true' if pin else 'false'}",
        "-Q",
        str(file_path),
    ]
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "ipfs add failed").strip())
    return (proc.stdout or "").strip()


async def _scrape_one(
    scraper: UnifiedWebScraper,
    url: str,
    timeout: int,
    worker_id: int,
    attempt: int,
) -> Dict[str, Any]:
    started_at = _utc_now_iso()
    try:
        # UnifiedWebScraper config already controls timeouts, etc.
        result = await scraper.scrape(url)
        finished_at = _utc_now_iso()

        if not result.success:
            status_code = None
            content_type = None
            try:
                status_code = (result.metadata or {}).get("status_code")
                content_type = (result.metadata or {}).get("content_type")
            except Exception:
                status_code = None
                content_type = None

            errors: List[str] = []
            try:
                errors = list(getattr(result, "errors", []) or [])
            except Exception:
                errors = []

            return {
                "url": url,
                "attempt": attempt,
                "worker_id": worker_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": "error",
                "error": "; ".join(errors) if errors else "unknown_error",
                "method_used": getattr(result.method_used, "value", None) if result.method_used else None,
                "status_code": status_code,
                "content_type": content_type,
                "content_bytes": None,
                "text": "",
                "content_cid": None,
                "metadata_json": json.dumps(result.metadata or {}, ensure_ascii=False),
            }

        status_code = None
        content_type = None
        try:
            status_code = (result.metadata or {}).get("status_code")
            content_type = (result.metadata or {}).get("content_type")
        except Exception:
            status_code = None
            content_type = None

        # Prefer HTML bytes (more canonical) then content/text.
        payload = (result.html or "") or (result.content or "") or (result.text or "")
        if isinstance(payload, bytes):
            content_bytes = payload
        else:
            content_bytes = str(payload).encode("utf-8", errors="replace")

        text = result.text or (result.content if isinstance(result.content, str) else "") or ""
        if not text and content_bytes:
            try:
                text = content_bytes.decode("utf-8", errors="replace")
            except Exception:
                text = ""

        cid = compute_cid_for_content(content_bytes) or f"sha256-{hashlib.sha256(content_bytes).hexdigest()}"

        return {
            "url": url,
            "attempt": attempt,
            "worker_id": worker_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": "success",
            "error": None,
            "method_used": getattr(result.method_used, "value", None) if result.method_used else None,
            "status_code": status_code,
            "content_type": content_type,
            "content_bytes": len(content_bytes),
            "text": text,
            "text_bytes": len(text.encode("utf-8", errors="replace")),
            "content_cid": cid,
            "content_raw": content_bytes,
            "metadata_json": json.dumps(result.metadata or {}, ensure_ascii=False),
        }

    except Exception as e:
        finished_at = _utc_now_iso()
        return {
            "url": url,
            "attempt": attempt,
            "worker_id": worker_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": "error",
            "error": f"exception:{type(e).__name__}:{e}",
            "method_used": None,
            "status_code": None,
            "content_type": None,
            "content_bytes": None,
            "text": "",
            "text_bytes": None,
            "content_cid": None,
            "metadata_json": "{}",
        }


async def run_scrape(
    con: duckdb.DuckDBPyConnection,
    blobs_dir: Path,
    worker_id: int,
    num_workers: int,
    max_concurrent: int,
    timeout: int,
    limit: Optional[int],
    resume: bool,
    rescrape_archive_status_jsonl: Optional[str],
    rescrape_include_success: bool,
    common_crawl_only: bool,
    ipfs: bool,
    ipfs_bin: str,
    ipfs_pin: bool,
    crawl_discovered_links: bool = False,
    crawl_depth: int = 1,
    full_domain_crawl: bool = False,
) -> None:
    # Build list of URLs assigned to this worker.
    # If rescrape mode is enabled, derive URLs from the archive status JSONL.
    if rescrape_archive_status_jsonl:
        archived_urls = _load_archived_urls_from_jsonl(Path(rescrape_archive_status_jsonl))
        urls = [u for u in archived_urls if _stable_shard(u, num_workers) == (worker_id % max(num_workers, 1))]
    else:
        urls_query = "SELECT url FROM town_urls WHERE shard = ? ORDER BY url"
        urls = [r[0] for r in con.execute(urls_query, [worker_id % max(num_workers, 1)]).fetchall()]

    if limit is not None:
        urls = urls[: int(limit)]

        _log(worker_id, f"Assigned {len(urls)} URLs from CSV", "INFO")
    
        # If full domain crawl is enabled, expand URLs to all URLs on their domains from Common Crawl CDX
        if crawl_discovered_links:
            _log(worker_id, f"Full domain crawl enabled; querying CC CDX for each domain", "INFO")
            all_urls: set[str] = set(urls)
            for url in urls:
                domain = _extract_domain(url)
                if domain:
                    cc_urls = await _query_cc_cdx_for_domain(domain, worker_id)
                    all_urls.update(cc_urls)
            urls = sorted(list(all_urls))
            _log(worker_id, f"After domain expansion from CDX: {len(urls)} total URLs", "INFO")
    
    # Skip successes if requested.
    if resume or (rescrape_archive_status_jsonl and not rescrape_include_success):
        done = set(r[0] for r in con.execute("SELECT url FROM url_cid_latest WHERE last_status = 'success'").fetchall())
        urls = [u for u in urls if u not in done]
        _log(worker_id, f"After filtering successes: {len(urls)} URLs remaining", "INFO")

    out_root = blobs_dir.parent
    archive_callback_file = str((out_root / "state" / "archive_jobs.jsonl").resolve())

    # Chrome-like UA string (kept static for reproducibility).
    chrome_ua = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # In rescrape-from-archive mode, focus on archive sources only.
    preferred_methods: List[ScraperMethod]
    fallback_enabled: bool
    archive_async_submit_on_failure: bool
    archive_async_submit_on_challenge: bool

    if rescrape_archive_status_jsonl:
        preferred_methods = [ScraperMethod.WAYBACK_MACHINE, ScraperMethod.ARCHIVE_IS]
        fallback_enabled = False
        archive_async_submit_on_failure = False
        archive_async_submit_on_challenge = False
    else:
        if common_crawl_only:
            _log(worker_id, "Common Crawl only mode enabled", "INFO")
            preferred_methods = [ScraperMethod.COMMON_CRAWL]
            fallback_enabled = False
            archive_async_submit_on_failure = False
            archive_async_submit_on_challenge = False
        else:
            _log(worker_id, "Full fallback chain enabled (CC -> Wayback -> Archive.is -> Playwright -> BeautifulSoup -> Requests)", "INFO")
            preferred_methods = [
                ScraperMethod.COMMON_CRAWL,
                ScraperMethod.WAYBACK_MACHINE,
                ScraperMethod.ARCHIVE_IS,
                ScraperMethod.PLAYWRIGHT,
                ScraperMethod.BEAUTIFULSOUP,
                ScraperMethod.REQUESTS_ONLY,
            ]
            fallback_enabled = True
            archive_async_submit_on_failure = True
            archive_async_submit_on_challenge = True

    config = ScraperConfig(
        timeout=timeout,
        user_agent=chrome_ua,
        extract_links=True,
        extract_text=True,
        fallback_enabled=fallback_enabled,
        preferred_methods=preferred_methods,

        # Common Crawl: CDX host is often blocked; enable direct-index fallback via data.commoncrawl.org.
        common_crawl_direct_index_enabled=True,
        common_crawl_direct_index_prefix_fallback=True,

        # Playwright: deterministic "1050p-ish" viewport + chromium.
        playwright_browser="chromium",
        playwright_viewport_width=1920,
        playwright_viewport_height=1050,

        # When everything fails (or bot challenge detected), enqueue async archive submissions.
        archive_async_submit_on_failure=archive_async_submit_on_failure,
        archive_async_submit_on_challenge=archive_async_submit_on_challenge,
        archive_async_submit_if_missing=True,
        archive_async_callback_file=archive_callback_file,
    )
    _log(worker_id, f"Scraper config: timeout={timeout}s, concurrent={max_concurrent}", "INFO")
    scraper = UnifiedWebScraper(config)

    sem = asyncio.Semaphore(max_concurrent)

    async def _task(url: str) -> None:
        async with sem:
            attempts = con.execute(
                "SELECT attempts FROM url_cid_latest WHERE url = ?",
                [url],
            ).fetchone()
            attempt = int(attempts[0]) + 1 if attempts else 1

            row = await _scrape_one(scraper, url, timeout=timeout, worker_id=worker_id, attempt=attempt)

            status_str = str(row.get("status") or "error")

            # Persist to DB
            con.execute(
                """
                INSERT INTO scrape_attempts (
                    url, attempt, worker_id, started_at, finished_at, status, error,
                    method_used, status_code, content_type, content_cid, content_bytes, text_bytes, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row.get("url"),
                    row.get("attempt"),
                    row.get("worker_id"),
                    row.get("started_at"),
                    row.get("finished_at"),
                    row.get("status"),
                    row.get("error"),
                    row.get("method_used"),
                    row.get("status_code"),
                    row.get("content_type"),
                    row.get("content_cid"),
                    row.get("content_bytes"),
                    row.get("text_bytes"),
                    row.get("metadata_json"),
                ],
            )
            
            if status_str == "success":
                _log(worker_id, f"✓ SUCCESS {url} (method={row.get('method_used')}, bytes={row.get('content_bytes')}, cid={row.get('content_cid')[:16]}...)", "INFO")
            else:
                _log(worker_id, f"✗ ERROR {url} (error={row.get('error')[:80] if row.get('error') else 'unknown'})", "WARN")

            _upsert_url_latest(
                con,
                url=url,
                status=status_str,
                error=row.get("error"),
                method_used=row.get("method_used"),
                status_code=row.get("status_code"),
                content_type=row.get("content_type"),
                content_cid=row.get("content_cid"),
                ipfs_cid=None,
                content_bytes=row.get("content_bytes"),
                text_bytes=row.get("text_bytes"),
                finished_at=row.get("finished_at"),
            )

            cid = row.get("content_cid")
            if row.get("status") == "success" and cid and row.get("content_raw") is not None:
                blob_path = _blob_path_for_cid(blobs_dir, cid)
                if not blob_path.exists():
                    blob_path.write_bytes(row["content_raw"])
                    _log(worker_id, f"  → Blob written: {blob_path.name} ({len(row['content_raw'])} bytes)", "DEBUG")
                else:
                    _log(worker_id, f"  → Blob already exists: {blob_path.name}", "DEBUG")

                ipfs_cid: Optional[str] = None
                if ipfs:
                    try:
                        ipfs_cid = _ipfs_add_file(ipfs_bin, blob_path, pin=ipfs_pin)
                        _log(worker_id, f"  → IPFS CID: {ipfs_cid}", "DEBUG")
                    except Exception as e:
                        # Best-effort: record the error but continue.
                        con.execute(
                            "UPDATE url_cid_latest SET last_error = ? WHERE url = ?",
                            [f"ipfs_add_error:{type(e).__name__}:{e}", url],
                        )
                        _log(worker_id, f"  → IPFS add failed: {type(e).__name__}: {e}", "WARN")

                if ipfs_cid:
                    con.execute(
                        "UPDATE url_cid_latest SET last_ipfs_cid = ? WHERE url = ?",
                        [ipfs_cid, url],
                    )

                seen_at = row.get("finished_at") or _utc_now_iso()

                con.execute(
                    "INSERT INTO url_cid_history (url, content_cid, ipfs_cid, seen_at, worker_id) VALUES (?, ?, ?, ?, ?)",
                    [url, cid, ipfs_cid, seen_at, worker_id],
                )

                _upsert_cid_content(
                    con,
                    cid=cid,
                    content_type=row.get("content_type"),
                    ipfs_cid=ipfs_cid,
                    content_bytes=int(row.get("content_bytes") or 0),
                    text=row.get("text") or "",
                    blob_path=str(blob_path),
                    seen_at=seen_at,
                )

                # Extract and store discovered links if crawling enabled
                if crawl_discovered_links:
                    discovered = _extract_links_from_metadata(row.get("metadata_json") or "{}")
                    source_domain = _extract_domain(url)
                    for link in discovered:
                        link_domain = _extract_domain(link)
                        # Only follow same-domain links
                        if source_domain and link_domain and source_domain == link_domain:
                            try:
                                con.execute(
                                    """
                                    INSERT INTO discovered_links
                                    (source_url, discovered_url, source_domain, discovered_domain, discovered_at, worker_id)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    [url, link, source_domain, link_domain, _utc_now_iso(), worker_id],
                                )
                            except Exception:
                                pass  # Ignore constraint violations

    # Run in bounded-parallel batches (DuckDB connection is shared; keep batches modest).
    batch_size = max(1, max_concurrent * 5)
    for start in range(0, len(urls), batch_size):
        batch = urls[start : start + batch_size]
        await asyncio.gather(*[_task(u) for u in batch])
        con.execute("CHECKPOINT")

    # If crawl mode is enabled, queue discovered links and scrape them
    if crawl_discovered_links:
        discovered_urls = [
            r[0]
            for r in con.execute(
                """
                SELECT DISTINCT discovered_url
                FROM discovered_links
                WHERE discovered_url NOT IN (SELECT url FROM url_cid_latest)
                ORDER BY discovered_url
                """
            ).fetchall()
        ]
        if discovered_urls:
            print(f"[Worker {worker_id}] Found {len(discovered_urls)} undiscovered links to crawl")
            for start in range(0, len(discovered_urls), batch_size):
                batch = discovered_urls[start : start + batch_size]
                await asyncio.gather(*[_task(u) for u in batch])
                con.execute("CHECKPOINT")
    
    # Final progress summary
    successes = con.execute("SELECT COUNT(*) FROM url_cid_latest WHERE last_status = 'success'").fetchone()[0]
    failures = con.execute("SELECT COUNT(*) FROM url_cid_latest WHERE last_status = 'error'").fetchone()[0]
    total_blobs = len(list((blobs_dir).glob("*.bin")))
    _log(worker_id, f"FINAL: {successes} successes, {failures} failures, {total_blobs} unique blobs", "INFO")


def export_parquets(con: duckdb.DuckDBPyConnection, datasets_dir: Path) -> Dict[str, Path]:
    out = {
        "towns": datasets_dir / "towns.parquet",
        "url_cid_map": datasets_dir / "url_cid_map.parquet",
        "cid_content": datasets_dir / "cid_content.parquet",
    }

    con.execute(
        "COPY (SELECT * FROM towns) TO ? (FORMAT PARQUET)",
        [str(out["towns"])],
    )

    con.execute(
        """
        COPY (
            SELECT
                u.gnis,
                u.place_name,
                u.state_code,
                u.url,
                l.last_status,
                l.last_error,
                l.last_method_used,
                l.last_status_code,
                l.last_content_type,
                l.last_content_cid,
                l.last_ipfs_cid,
                l.last_content_bytes,
                l.last_text_bytes,
                l.last_finished_at,
                l.attempts
            FROM town_urls u
            LEFT JOIN url_cid_latest l
            ON u.url = l.url
        ) TO ? (FORMAT PARQUET)
        """,
        [str(out["url_cid_map"])],
    )

    con.execute(
        """
        COPY (
            SELECT
                content_cid,
                first_seen_at,
                last_seen_at,
                content_type,
                ipfs_cid,
                content_bytes,
                blob_path,
                text
            FROM cid_content
        ) TO ? (FORMAT PARQUET)
        """,
        [str(out["cid_content"])],
    )

    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=str, required=True)
    p.add_argument("--out", type=str, required=True)

    p.add_argument("--worker-id", type=int, default=0)
    p.add_argument("--num-workers", type=int, default=1)

    p.add_argument("--max-concurrent", type=int, default=5)
    p.add_argument("--timeout", type=int, default=45)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")

    p.add_argument(
        "--rescrape-archive-status-jsonl",
        type=str,
        default=None,
        help=(
            "If set, rescrape only URLs that are marked archived in this JSONL file "
            "(lines with archive_org_present/archive_is_present). Rescrape uses Wayback/Archive.is only."
        ),
    )
    p.add_argument(
        "--rescrape-include-success",
        action="store_true",
        default=False,
        help="Include URLs already marked success in url_cid_latest when rescraping archived URLs.",
    )
    p.add_argument(
        "--common-crawl-only",
        action="store_true",
        default=False,
        help="Restrict scraping to Common Crawl only (no fallbacks)",
    )

    # Do NOT call resolve() here: venv python is often a symlink to /usr/bin/python.
    default_ipfs_bin = str(Path(sys.executable).parent / "ipfs")
    p.add_argument(
        "--ipfs",
        action="store_true",
        default=False,
        help="Run 'ipfs add' for each stored blob to compute a UnixFS CID",
    )
    p.add_argument(
        "--ipfs-bin",
        type=str,
        default=default_ipfs_bin,
        help="Path to ipfs binary (default: alongside the current Python interpreter)",
    )
    p.add_argument(
        "--ipfs-pin",
        action="store_true",
        default=False,
        help="Pin added content in the local IPFS repo",
    )
    p.add_argument(
        "--crawl-discovered-links",
        action="store_true",
        default=False,
        help="Extract and scrape links discovered within successful pages (same domain only)",
    )
    p.add_argument(
        "--crawl-depth",
        type=int,
        default=1,
        help="Maximum depth for link crawling (future: currently only stores discovered links at depth 1)",
    )
    p.add_argument(
        "--full-domain-crawl",
        action="store_true",
        default=False,
        help="Query Common Crawl CDX for all URLs on each domain and scrape them all (not just the input URLs)",
    )

    args = p.parse_args()

    out_root = Path(args.out).expanduser().resolve()
    csv_path = Path(args.csv).expanduser().resolve()
    paths = _ensure_dirs(out_root)

    db_path = paths["state"] / f"worker_{args.worker_id}.duckdb"
    con = _connect_db(db_path)
    try:
        _init_schema(con)
        ingest_csv_to_db(con, csv_path, num_workers=int(args.num_workers))

        asyncio.run(
            run_scrape(
                con,
                blobs_dir=paths["blobs"],
                worker_id=int(args.worker_id),
                num_workers=int(args.num_workers),
                max_concurrent=int(args.max_concurrent),
                timeout=int(args.timeout),
                limit=args.limit,
                resume=bool(args.resume),
                rescrape_archive_status_jsonl=args.rescrape_archive_status_jsonl,
                rescrape_include_success=bool(args.rescrape_include_success),
                common_crawl_only=bool(args.common_crawl_only),
                ipfs=bool(args.ipfs),
                ipfs_bin=str(args.ipfs_bin),
                ipfs_pin=bool(args.ipfs_pin),
                crawl_discovered_links=bool(args.crawl_discovered_links),
                crawl_depth=int(args.crawl_depth),
                full_domain_crawl=bool(args.full_domain_crawl),
            )
        )

        con.execute("CHECKPOINT")
        exported = export_parquets(con, paths["datasets"])
        print("Exported:")
        for k, v in exported.items():
            print(f"  - {k}: {v}")

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
