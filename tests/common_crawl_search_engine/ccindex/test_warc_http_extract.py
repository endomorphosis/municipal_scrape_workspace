from __future__ import annotations

import gzip

from common_crawl_search_engine.ccindex.api import extract_http_from_warc_gzip_member


def _make_warc_gz_member(http_bytes: bytes) -> bytes:
    # Minimal WARC record payload: WARC headers + blank line + HTTP response bytes.
    warc_headers = (
        "WARC/1.0\r\n"
        "WARC-Type: response\r\n"
        "WARC-Target-URI: https://example.test/\r\n"
        f"Content-Length: {len(http_bytes)}\r\n"
        "\r\n"
    ).encode("utf-8")
    return gzip.compress(warc_headers + http_bytes)


def test_extract_http_simple_html() -> None:
    http = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body>Hello</body></html>"
    )
    gz = _make_warc_gz_member(http)

    res = extract_http_from_warc_gzip_member(gz, max_body_bytes=1024, max_preview_chars=1024)
    assert res.ok
    assert res.http_status == 200
    assert res.body_is_html is True
    assert (res.body_text_preview or "").startswith("<html")
    assert res.body_mime == "text/html"


def test_extract_http_chunked_decodes() -> None:
    body_chunked = b"5\r\nhello\r\n0\r\n\r\n"
    http = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        + body_chunked
    )
    gz = _make_warc_gz_member(http)

    res = extract_http_from_warc_gzip_member(gz, max_body_bytes=1024, max_preview_chars=1024)
    assert res.ok
    assert res.http_status == 200
    assert (res.body_text_preview or "") == "hello"
