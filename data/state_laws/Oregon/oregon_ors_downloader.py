"""Download and organize Oregon Revised Statutes with archival fallbacks.

Primary source:
    https://www.oregonlegislature.gov/bills_laws/ors/ors001.html

When direct access is blocked or filtered, this downloader falls back to
Wayback Machine and Archive.is via the project's web archiving engines.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import importlib
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

ORS_LINK_PATTERN = re.compile(r"ors(\d{3})\.html$", re.IGNORECASE)


@dataclass
class FetchResult:
    url: str
    content: bytes
    source: str
    fetched_at: str
    status_code: Optional[int] = None
    archive_url: Optional[str] = None
    archive_timestamp: Optional[str] = None


class OregonORSArchivalDownloader:
    """Resilient Oregon ORS downloader with archive-based fallback support."""

    def __init__(
        self,
        output_dir: Path,
        *,
        request_timeout_seconds: int = 30,
        delay_seconds: float = 0.4,
        user_agent: str = (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    ):
        self.output_dir = output_dir
        self.raw_dir = output_dir / "raw_html"
        self.manifests_dir = output_dir / "manifests"
        self.parsed_dir = output_dir / "parsed"

        self.request_timeout_seconds = request_timeout_seconds
        self.delay_seconds = delay_seconds
        self.user_agent = user_agent

        self.seed_url = "https://www.oregonlegislature.gov/bills_laws/ors/ors001.html"

        for directory in (self.output_dir, self.raw_dir, self.manifests_dir, self.parsed_dir):
            directory.mkdir(parents=True, exist_ok=True)

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.user_agent})

    async def run(
        self,
        *,
        max_chapters: int = 75,
        chapter_start: int = 1,
        chapter_end: Optional[int] = None,
        force: bool = False,
        workers: int = 1,
    ) -> Dict[str, Any]:
        """Download and organize Oregon ORS chapters into local folders."""

        started_at = datetime.now(timezone.utc).isoformat()

        seed_fetch = await self.fetch_with_fallback(self.seed_url)
        candidate_urls = self.discover_chapter_urls(seed_fetch.content, self.seed_url)

        if len(candidate_urls) <= 1:
            logger.warning(
                "Insufficient chapter links discovered from seed page (%s found); generating fallback chapter range",
                len(candidate_urls),
            )
            fallback_end = chapter_end or max(chapter_start + max_chapters - 1, chapter_start)
            candidate_urls = [
                f"https://www.oregonlegislature.gov/bills_laws/ors/ors{chapter:03d}.html"
                for chapter in range(chapter_start, fallback_end + 1)
            ]

        filtered_urls = self.filter_chapter_urls(
            candidate_urls,
            chapter_start=chapter_start,
            chapter_end=chapter_end,
        )

        if max_chapters > 0:
            filtered_urls = filtered_urls[:max_chapters]

        logger.info("Preparing to download %s Oregon chapter pages", len(filtered_urls))

        worker_count = max(1, int(workers))
        logger.info("Using %s parallel worker(s)", worker_count)
        semaphore = asyncio.Semaphore(worker_count)

        async def _process_chapter(index: int, chapter_url: str) -> Dict[str, Any]:
            chapter_num = self.chapter_number_from_url(chapter_url)
            if chapter_num is None:
                return {"manifest": None, "summary": None}

            file_path = self.raw_dir / f"ors{chapter_num:03d}.html"
            async with semaphore:
                if file_path.exists() and not force:
                    logger.info("[%s/%s] Skipping existing %s", index, len(filtered_urls), file_path.name)
                    existing_bytes = file_path.read_bytes()
                    summary = self.extract_chapter_summary(existing_bytes, chapter_url, chapter_num, "cached", str(file_path))
                    manifest = {
                        "chapter_number": chapter_num,
                        "url": chapter_url,
                        "file": str(file_path),
                        "source": "cached",
                        "fetched_at": None,
                        "status": "skipped_existing",
                        "sha256": hashlib.sha256(existing_bytes).hexdigest(),
                    }
                    return {"manifest": manifest, "summary": summary}

                try:
                    fetch = await self.fetch_with_fallback(chapter_url)
                    file_path.write_bytes(fetch.content)
                    file_sha = hashlib.sha256(fetch.content).hexdigest()

                    summary = self.extract_chapter_summary(fetch.content, chapter_url, chapter_num, fetch.source, str(file_path))
                    manifest = {
                        "chapter_number": chapter_num,
                        "url": chapter_url,
                        "file": str(file_path),
                        "source": fetch.source,
                        "fetched_at": fetch.fetched_at,
                        "status_code": fetch.status_code,
                        "archive_url": fetch.archive_url,
                        "archive_timestamp": fetch.archive_timestamp,
                        "status": "downloaded",
                        "sha256": file_sha,
                    }
                    logger.info("[%s/%s] Saved %s via %s", index, len(filtered_urls), file_path.name, fetch.source)
                    result = {"manifest": manifest, "summary": summary}
                except Exception as exc:
                    logger.warning("[%s/%s] Failed %s: %s", index, len(filtered_urls), chapter_url, exc)
                    result = {
                        "manifest": {
                            "chapter_number": chapter_num,
                            "url": chapter_url,
                            "status": "error",
                            "error": str(exc),
                        },
                        "summary": None,
                    }

                if self.delay_seconds > 0:
                    await asyncio.sleep(self.delay_seconds)
                return result

        chapter_jobs = [
            _process_chapter(index=index, chapter_url=chapter_url)
            for index, chapter_url in enumerate(filtered_urls, start=1)
        ]
        chapter_results = await asyncio.gather(*chapter_jobs)

        chapter_manifest: List[Dict[str, Any]] = []
        parsed_summary_rows: List[Dict[str, Any]] = []
        for item in chapter_results:
            manifest = item.get("manifest")
            summary = item.get("summary")
            if isinstance(manifest, dict):
                chapter_manifest.append(manifest)
            if isinstance(summary, dict):
                parsed_summary_rows.append(summary)

        chapter_manifest.sort(key=lambda row: int(row.get("chapter_number") or 0))
        parsed_summary_rows.sort(key=lambda row: int(row.get("chapter_number") or 0))

        parsed_jsonl = self.parsed_dir / "chapter_summaries.jsonl"
        with parsed_jsonl.open("w", encoding="utf-8") as handle:
            for row in parsed_summary_rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        source_counts = Counter(item.get("source", "unknown") for item in chapter_manifest)
        success_count = sum(1 for item in chapter_manifest if item.get("status") in {"downloaded", "skipped_existing"})

        report = {
            "status": "success" if success_count else "error",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "seed_url": self.seed_url,
            "output_dir": str(self.output_dir),
            "requested_count": len(filtered_urls),
            "successful_count": success_count,
            "source_counts": dict(source_counts),
            "manifest_file": str(self.manifests_dir / "chapter_manifest.json"),
            "parsed_summary_file": str(parsed_jsonl),
        }

        manifest_file = self.manifests_dir / "chapter_manifest.json"
        manifest_file.write_text(json.dumps(chapter_manifest, indent=2), encoding="utf-8")

        report_file = self.manifests_dir / "download_report.json"
        report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

        latest_file = self.manifests_dir / "latest.json"
        latest_file.write_text(
            json.dumps(
                {
                    "report_file": str(report_file),
                    "manifest_file": str(manifest_file),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        return report

    async def fetch_with_fallback(self, url: str) -> FetchResult:
        """Fetch URL directly; fallback to web-archiving sources when blocked."""

        direct = await asyncio.to_thread(self._fetch_direct, url)
        if direct is not None:
            return direct

        common_crawl = await asyncio.to_thread(self._fetch_from_common_crawl, url)
        if common_crawl is not None:
            return common_crawl

        wayback = await self._fetch_from_wayback(url)
        if wayback is not None:
            return wayback

        archive_is = await self._fetch_from_archive_is(url)
        if archive_is is not None:
            return archive_is

        raise RuntimeError(f"Unable to fetch URL via direct or archival fallback: {url}")

    def _fetch_direct(self, url: str) -> Optional[FetchResult]:
        try:
            response = self._session.get(url, timeout=self.request_timeout_seconds)
            if response.status_code == 200 and self._looks_like_ors_html(response.content):
                return FetchResult(
                    url=url,
                    content=response.content,
                    source="direct",
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    status_code=response.status_code,
                )

            logger.info("Direct fetch rejected for %s (status=%s)", url, response.status_code)
            return None
        except Exception as exc:
            logger.info("Direct fetch failed for %s: %s", url, exc)
            return None

    def _fetch_from_common_crawl(self, url: str) -> Optional[FetchResult]:
        """Attempt to recover page content from Common Crawl search/index metadata."""
        parsed = urlparse(url)
        if not parsed.netloc:
            return None

        try:
            cc_module = importlib.import_module(
                "ipfs_datasets_py.processors.web_archiving.common_crawl_integration"
            )
            engine_cls = getattr(cc_module, "CommonCrawlSearchEngine")
        except Exception as exc:
            logger.warning("Common Crawl integration import failed: %s", exc)
            return None

        modes_to_try: List[tuple[str, Dict[str, Any]]] = [("local", {}), ("cli", {})]
        remote_endpoint = os.environ.get("CCINDEX_MCP_ENDPOINT")
        if remote_endpoint:
            modes_to_try.append(("remote", {"mcp_endpoint": remote_endpoint}))

        records: List[Dict[str, Any]] = []
        for mode, mode_kwargs in modes_to_try:
            try:
                engine = engine_cls(mode=mode, **mode_kwargs)
                if not getattr(engine, "is_available", lambda: False)():
                    continue
                mode_records = engine.search_domain(parsed.netloc, max_matches=300)
                if mode_records:
                    records = mode_records
                    logger.info("Common Crawl returned %s records in %s mode", len(records), mode)
                    break
            except Exception as exc:
                logger.debug("Common Crawl %s mode failed for %s: %s", mode, url, exc)

        if not records:
            return None

        normalized_target = url.rstrip("/")

        preferred: List[Dict[str, Any]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            candidate_url = str(record.get("url", "")).rstrip("/")
            if candidate_url == normalized_target:
                preferred.append(record)

        candidates = preferred if preferred else [r for r in records if isinstance(r, dict)]

        for record in candidates:
            archive_url = record.get("archive_url") or record.get("wayback_url")
            if archive_url:
                fetched = self._fetch_candidate_archive_url(url, str(archive_url), record)
                if fetched is not None:
                    return fetched

            warc_fetch = self._fetch_from_common_crawl_warc_record(url, record)
            if warc_fetch is not None:
                return warc_fetch

            candidate_url = str(record.get("url", "")).strip()
            if candidate_url and candidate_url != url:
                fetched = self._fetch_candidate_archive_url(url, candidate_url, record)
                if fetched is not None:
                    return fetched

        return None

    def _fetch_candidate_archive_url(
        self,
        original_url: str,
        candidate_url: str,
        record: Dict[str, Any],
    ) -> Optional[FetchResult]:
        try:
            response = self._session.get(candidate_url, timeout=self.request_timeout_seconds)
            if response.status_code != 200:
                return None
            if not self._looks_like_ors_html(response.content):
                return None
            return FetchResult(
                url=original_url,
                content=response.content,
                source="common_crawl",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                status_code=response.status_code,
                archive_url=candidate_url,
                archive_timestamp=str(record.get("timestamp") or "") or None,
            )
        except Exception:
            return None

    def _fetch_from_common_crawl_warc_record(
        self,
        original_url: str,
        record: Dict[str, Any],
    ) -> Optional[FetchResult]:
        warc_filename = record.get("warc_filename") or record.get("filename")
        warc_offset = record.get("warc_offset") or record.get("offset")
        warc_length = record.get("warc_length") or record.get("length")

        if not warc_filename or warc_offset is None or warc_length is None:
            return None

        try:
            offset = int(warc_offset)
            length = int(warc_length)
            if length <= 0:
                return None
        except Exception:
            return None

        range_end = offset + length - 1
        warc_url = f"https://data.commoncrawl.org/{warc_filename}"

        try:
            response = self._session.get(
                warc_url,
                headers={"Range": f"bytes={offset}-{range_end}"},
                timeout=max(self.request_timeout_seconds, 45),
            )
            if response.status_code not in (200, 206):
                return None

            html_payload = self._extract_html_from_warc_bytes(response.content)
            if not html_payload or not self._looks_like_ors_html(html_payload):
                return None

            return FetchResult(
                url=original_url,
                content=html_payload,
                source="common_crawl",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                status_code=response.status_code,
                archive_url=warc_url,
                archive_timestamp=str(record.get("timestamp") or "") or None,
            )
        except Exception as exc:
            logger.debug("Failed to fetch Common Crawl WARC segment for %s: %s", original_url, exc)
            return None

    @staticmethod
    def _extract_html_from_warc_bytes(raw_bytes: bytes) -> bytes:
        """Extract HTTP payload bytes from a ranged WARC response."""
        if not raw_bytes:
            return b""

        blob = raw_bytes
        try:
            blob = gzip.decompress(raw_bytes)
        except Exception:
            blob = raw_bytes

        http_start = blob.find(b"HTTP/")
        if http_start != -1:
            http_blob = blob[http_start:]
            header_end = http_blob.find(b"\r\n\r\n")
            if header_end != -1:
                return http_blob[header_end + 4 :]

        return blob

    async def _fetch_from_wayback(self, url: str) -> Optional[FetchResult]:
        try:
            wayback_module = importlib.import_module(
                "ipfs_datasets_py.processors.web_archiving.wayback_machine_engine"
            )
            archive_to_wayback = getattr(wayback_module, "archive_to_wayback")
            get_wayback_content = getattr(wayback_module, "get_wayback_content")
            search_wayback_machine = getattr(wayback_module, "search_wayback_machine")
        except Exception as exc:
            logger.warning("Wayback engine import failed: %s", exc)
            return None

        try:
            search_result = await search_wayback_machine(
                url=url,
                limit=6,
                collapse="timestamp:8",
                output_format="json",
            )
        except Exception as exc:
            logger.warning("Wayback search exception for %s: %s", url, exc)
            search_result = {"status": "error", "results": []}

        captures = search_result.get("results", []) if isinstance(search_result, dict) else []

        for capture in captures:
            timestamp = capture.get("timestamp")
            try:
                content_result = await get_wayback_content(url=url, timestamp=timestamp, closest=True)
                if content_result.get("status") != "success":
                    continue

                content = content_result.get("content", b"")
                if isinstance(content, str):
                    content = content.encode("utf-8", errors="replace")

                if not content or not self._looks_like_ors_html(content):
                    continue

                return FetchResult(
                    url=url,
                    content=content,
                    source="wayback",
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    archive_url=content_result.get("wayback_url") or capture.get("wayback_url"),
                    archive_timestamp=content_result.get("capture_timestamp") or timestamp,
                )
            except Exception as exc:
                logger.debug("Wayback capture fetch failed for %s (%s): %s", url, timestamp, exc)

        try:
            latest_result = await get_wayback_content(url=url, timestamp=None, closest=True)
            if latest_result.get("status") == "success":
                latest_content = latest_result.get("content", b"")
                if isinstance(latest_content, str):
                    latest_content = latest_content.encode("utf-8", errors="replace")
                if latest_content and self._looks_like_ors_html(latest_content):
                    return FetchResult(
                        url=url,
                        content=latest_content,
                        source="wayback",
                        fetched_at=datetime.now(timezone.utc).isoformat(),
                        archive_url=latest_result.get("wayback_url"),
                        archive_timestamp=latest_result.get("capture_timestamp"),
                    )
        except Exception as exc:
            logger.debug("Wayback latest capture lookup failed for %s: %s", url, exc)

        cdx_fallback = await asyncio.to_thread(self._fetch_from_wayback_cdx_direct, url)
        if cdx_fallback is not None:
            return cdx_fallback

        try:
            await archive_to_wayback(url)
        except Exception:
            pass

        return None

    async def _fetch_from_archive_is(self, url: str) -> Optional[FetchResult]:
        try:
            archive_module = importlib.import_module(
                "ipfs_datasets_py.processors.web_archiving.archive_is_engine"
            )
            archive_to_archive_is = getattr(archive_module, "archive_to_archive_is")
            get_archive_is_content = getattr(archive_module, "get_archive_is_content")
        except Exception as exc:
            logger.warning("Archive.is engine import failed: %s", exc)
            return None

        try:
            submit_result = await archive_to_archive_is(url, wait_for_completion=False)
            archive_url = submit_result.get("archive_url") if isinstance(submit_result, dict) else None
            if not archive_url:
                return None

            content_result = await get_archive_is_content(archive_url)
            if content_result.get("status") != "success":
                return None

            content = content_result.get("content", b"")
            if isinstance(content, str):
                content = content.encode("utf-8", errors="replace")

            if not content or not self._looks_like_ors_html(content):
                return None

            return FetchResult(
                url=url,
                content=content,
                source="archive_is",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                archive_url=archive_url,
            )
        except Exception as exc:
            logger.warning("Archive.is fallback failed for %s: %s", url, exc)
            return None

    def _fetch_from_wayback_cdx_direct(self, url: str) -> Optional[FetchResult]:
        """Fallback using Wayback CDX API directly when engine-level search fails."""
        cdx_url = "https://web.archive.org/cdx/search/cdx"
        params = {
            "url": url,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype",
            "filter": "statuscode:200",
            "limit": 8,
        }

        try:
            response = self._session.get(cdx_url, params=params, timeout=self.request_timeout_seconds)
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list) or len(rows) <= 1:
                return None

            records = rows[1:]
            for record in reversed(records):
                if not isinstance(record, list) or not record:
                    continue
                timestamp = record[0]
                archive_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
                archive_response = self._session.get(archive_url, timeout=self.request_timeout_seconds)
                if archive_response.status_code != 200:
                    continue
                if not self._looks_like_ors_html(archive_response.content):
                    continue
                return FetchResult(
                    url=url,
                    content=archive_response.content,
                    source="wayback",
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    archive_url=archive_url,
                    archive_timestamp=timestamp,
                    status_code=archive_response.status_code,
                )

            return None
        except Exception as exc:
            logger.debug("Direct CDX fallback failed for %s: %s", url, exc)
            return None

    @staticmethod
    def chapter_number_from_url(url: str) -> Optional[int]:
        match = ORS_LINK_PATTERN.search(urlparse(url).path)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    def discover_chapter_urls(self, html_bytes: bytes, base_url: str) -> List[str]:
        soup = BeautifulSoup(html_bytes, "html.parser")
        found: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            raw_href = anchor.get("href")
            if not raw_href:
                continue
            abs_url = urljoin(base_url, raw_href)
            if ORS_LINK_PATTERN.search(urlparse(abs_url).path):
                found.add(abs_url)

        found.add(self.seed_url)

        return sorted(found, key=lambda item: self.chapter_number_from_url(item) or 0)

    def filter_chapter_urls(
        self,
        urls: Sequence[str],
        *,
        chapter_start: int,
        chapter_end: Optional[int],
    ) -> List[str]:
        filtered: List[str] = []
        end = chapter_end if chapter_end is not None else 999

        for url in urls:
            chapter_num = self.chapter_number_from_url(url)
            if chapter_num is None:
                continue
            if chapter_num < chapter_start or chapter_num > end:
                continue
            filtered.append(url)

        return sorted(dict.fromkeys(filtered), key=lambda item: self.chapter_number_from_url(item) or 0)

    def extract_chapter_summary(
        self,
        html_bytes: bytes,
        source_url: str,
        chapter_num: int,
        source: str,
        file_path: str,
    ) -> Dict[str, Any]:
        soup = BeautifulSoup(html_bytes, "html.parser")
        title = self._extract_title(soup)

        text = soup.get_text("\n", strip=True)
        section_re = re.compile(rf"\b{chapter_num}\.(\d{{3}})\b")
        sections = sorted({f"{chapter_num}.{m.group(1)}" for m in section_re.finditer(text)})

        return {
            "chapter_number": chapter_num,
            "chapter_title": title,
            "source_url": source_url,
            "download_source": source,
            "raw_file": file_path,
            "section_count_detected": len(sections),
            "sections_sample": sections[:15],
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _extract_title(soup: BeautifulSoup) -> str:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(" ", strip=True)

        for tag_name in ("h2", "title", "strong"):
            tag = soup.find(tag_name)
            if tag and tag.get_text(strip=True):
                text = tag.get_text(" ", strip=True)
                if "Chapter" in text or "Oregon Revised Statute" in text:
                    return text

        return "Oregon Revised Statutes"

    @staticmethod
    def _looks_like_ors_html(content: bytes) -> bool:
        sample = content.decode("utf-8", errors="ignore")
        lowered = sample.lower()
        return (
            "oregon revised statute" in lowered
            or "chapter" in lowered and "ors" in lowered
            or "oregonlegislature" in lowered
        )


def _default_output_dir() -> Path:
    return Path(__file__).resolve().parent


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download and organize Oregon ORS chapters with archival fallback support.",
    )
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument("--max-chapters", type=int, default=75)
    parser.add_argument("--chapter-start", type=int, default=1)
    parser.add_argument("--chapter-end", type=int, default=None)
    parser.add_argument("--delay-seconds", type=float, default=0.4)
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel chapter workers")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--force", action="store_true", help="Redownload even if local HTML exists")
    parser.add_argument("--log-level", default="INFO")
    return parser


async def _run_async(args: argparse.Namespace) -> Dict[str, Any]:
    downloader = OregonORSArchivalDownloader(
        output_dir=args.output_dir,
        request_timeout_seconds=args.timeout_seconds,
        delay_seconds=args.delay_seconds,
    )
    return await downloader.run(
        max_chapters=args.max_chapters,
        chapter_start=args.chapter_start,
        chapter_end=args.chapter_end,
        force=args.force,
        workers=args.workers,
    )


def run(argv: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    repo_root = Path(__file__).resolve().parents[3]
    package_root = repo_root / "ipfs_datasets_py"
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

    result = asyncio.run(_run_async(args))
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    run()
