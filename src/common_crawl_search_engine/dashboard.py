"""Web dashboard for ccindex (application layer).

Run (dev):
  python -m common_crawl_search_engine.ccsearch.dashboard --host 127.0.0.1 --port 8787
"""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from common_crawl_search_engine.ccindex import api


_CSS = """
:root {
  --bg: #0b0f1a;
  --panel: #111827;
  --panel2: #0f172a;
  --text: #e5e7eb;
  --muted: rgba(229, 231, 235, 0.72);
  --accent: #60a5fa;
  --ok: #34d399;
  --err: #fb7185;
  --border: rgba(148, 163, 184, 0.22);
}

* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
}
header {
  padding: 16px 18px;
  border-bottom: 1px solid var(--border);
  background: linear-gradient(180deg, rgba(17, 24, 39, 0.92), rgba(17, 24, 39, 0.72));
}
.brand { display: flex; gap: 12px; align-items: baseline; flex-wrap: wrap; }
.brand h1 { margin: 0; font-size: 18px; letter-spacing: 0.2px; }
.brand span { color: var(--muted); font-size: 12px; }

main { padding: 18px; max-width: 1200px; margin: 0 auto; }

a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }

.card {
  background: rgba(17, 24, 39, 0.8);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px;
}

.row { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }
.field { display: flex; flex-direction: column; gap: 6px; }
label { color: var(--muted); font-size: 12px; }
input {
  width: 100%;
  padding: 10px 12px;
  border: 1px solid var(--border);
  background: rgba(15, 23, 42, 0.65);
  border-radius: 8px;
  color: var(--text);
}
button {
  padding: 10px 14px;
  border: 1px solid var(--border);
  background: rgba(96, 165, 250, 0.18);
  color: var(--text);
  border-radius: 8px;
  cursor: pointer;
}
button:hover { background: rgba(96, 165, 250, 0.28); }

.small { color: var(--muted); font-size: 12px; }
.code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

.badge {
  display: inline-block;
  padding: 3px 8px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: rgba(15, 23, 42, 0.5);
  font-size: 12px;
}
.badge.ok { border-color: rgba(52, 211, 153, 0.35); background: rgba(52, 211, 153, 0.14); }
.badge.err { border-color: rgba(251, 113, 133, 0.35); background: rgba(251, 113, 133, 0.14); }

.table { width: 100%; border-collapse: collapse; }
.table th, .table td { padding: 10px; border-top: 1px solid rgba(34, 48, 74, 0.7); vertical-align: top; }
.table th { text-align: left; color: var(--muted); font-size: 12px; }

pre { white-space: pre-wrap; word-break: break-word; }
hr { border: none; border-top: 1px solid rgba(34, 48, 74, 0.7); margin: 12px 0; }
"""


def _q(s: Optional[str]) -> str:
    return "" if s is None else str(s)


def _layout(title: str, body_html: str) -> str:
    nav = """
  <div style='margin-top: 10px; display:flex; gap: 12px;'>
    <a class='badge' href='/'>Wayback</a>
    <a class='badge' href='/discover'>Discover</a>
  </div>
"""
    return f"""<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{html.escape(title)}</title>
  <style>{_CSS}</style>
</head>
<body>
<header>
  <div class='brand'>
    <h1>ccindex</h1>
    <span>local dashboard • Common Crawl pointers → WARC</span>
  </div>
{nav}
</header>
<main>
{body_html}
</main>
</body>
</html>"""


def _jsonrpc_error(req_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": int(code), "message": str(message)}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def create_app(master_db: Path) -> Any:
    try:
        from fastapi import FastAPI, Query, Request
        from fastapi.responses import HTMLResponse, JSONResponse, Response
        from fastapi.staticfiles import StaticFiles
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "Missing dashboard dependencies. Install with: pip install -e '.[ccindex-dashboard]'\n"
            f"Import error: {e}"
        )

    # NOTE: This module uses `from __future__ import annotations`, which stores
    # type annotations as strings. FastAPI resolves those strings using the
    # function's global namespace (the module globals), not the create_app() local
    # scope. Ensure Request is present globally so `request: Request` is treated
    # as the Starlette request object (not a required query param).
    globals()["Request"] = Request

    app = FastAPI(title="ccindex dashboard", version="0.1")

    # Support multiple layouts during re-org. We serve the first static dir we find.
    static_candidates = [
        Path(__file__).parent / "static",
        Path(__file__).parent / "ccsearch" / "static",
        Path(__file__).parent / "ccindex" / "static",
    ]
    for static_dir in static_candidates:
        if static_dir.exists():
            app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
            break

    @app.post("/mcp")
    async def mcp(request: Request) -> JSONResponse:
        payload = await request.json()
        req_id = payload.get("id")
        method = payload.get("method")
        params = payload.get("params")

        tools = [
            {
                "name": "search_domain_meta",
                "description": "Search CCIndex via meta-indexes for a domain",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "domain": {"type": "string"},
                        "year": {"type": ["string", "null"]},
                        "parquet_root": {"type": "string"},
                        "master_db": {"type": "string"},
                        "max_matches": {"type": "integer"},
                    },
                    "required": ["domain"],
                },
            },
            {
                "name": "fetch_warc_record",
                "description": "Fetch a WARC record via HTTP range and optionally decode a text preview",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "warc_filename": {"type": "string"},
                        "warc_offset": {"type": "integer"},
                        "warc_length": {"type": "integer"},
                        "prefix": {"type": "string"},
                      "max_bytes": {"type": "integer"},
                      "max_preview_chars": {"type": "integer"},
                    },
                    "required": ["warc_filename", "warc_offset", "warc_length"],
                },
            },
            {
                "name": "list_collections",
                "description": "List registered collections from master meta-index",
                "inputSchema": {"type": "object", "properties": {"year": {"type": ["string", "null"]}}},
            },
            {
                "name": "brave_search_ccindex",
                "description": "Brave web search + resolve result URLs to CCIndex pointers (no live-site visits)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "count": {"type": "integer"},
                        "year": {"type": ["string", "null"]},
                        "parquet_root": {"type": "string"},
                    },
                    "required": ["query"],
                },
            },
        ]

        if method == "tools/list":
            return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}})

        if method != "tools/call":
            return JSONResponse(_jsonrpc_error(req_id, -32601, f"Method not found: {method}"))

        if not isinstance(params, dict):
            return JSONResponse(_jsonrpc_error(req_id, -32602, "Invalid params"))

        tool_name = params.get("name")
        tool_args = params.get("arguments") or {}
        if not isinstance(tool_name, str) or not tool_name:
            return JSONResponse(_jsonrpc_error(req_id, -32602, "Missing tool name"))
        if not isinstance(tool_args, dict):
            return JSONResponse(_jsonrpc_error(req_id, -32602, "Tool arguments must be an object"))

        try:
            if tool_name == "search_domain_meta":
                q = str(tool_args.get("domain") or "")
                year = tool_args.get("year")
                parquet_root = Path(str(tool_args.get("parquet_root") or "/storage/ccindex_parquet"))
                master_db_arg = Path(str(tool_args.get("master_db") or str(master_db)))
                max_matches = int(tool_args.get("max_matches") or 200)

                res = api.search_domain_via_meta_indexes(
                    q,
                    parquet_root=parquet_root,
                    master_db=master_db_arg,
                    year=str(year) if year else None,
                    max_matches=max_matches,
                )
                out: Any = {
                    "meta_source": res.meta_source,
                    "collections_considered": res.collections_considered,
                    "emitted": res.emitted,
                    "elapsed_s": res.elapsed_s,
                    "records": res.records,
                }
            elif tool_name == "fetch_warc_record":
                fetch = api.fetch_warc_record_range(
                    warc_filename=str(tool_args.get("warc_filename") or ""),
                    warc_offset=int(tool_args.get("warc_offset") or 0),
                    warc_length=int(tool_args.get("warc_length") or 0),
                    prefix=str(tool_args.get("prefix") or "https://data.commoncrawl.org/"),
                    max_bytes=int(tool_args.get("max_bytes") or 2_000_000),
                    decode_gzip_text=True,
                    max_preview_chars=int(tool_args.get("max_preview_chars") or 80_000),
                )
                out = {
                    "ok": fetch.ok,
                    "status": fetch.status,
                    "url": fetch.url,
                    "bytes_requested": fetch.bytes_requested,
                    "bytes_returned": fetch.bytes_returned,
                    "sha256": fetch.sha256,
                    "decoded_text_preview": fetch.decoded_text_preview,
                    "error": fetch.error,
                }

                # Prefer a structured extraction of the HTTP payload.
                if fetch.ok and fetch.raw_base64:
                    try:
                        import base64 as _b64

                        raw = _b64.b64decode(fetch.raw_base64)
                        parsed = api.extract_http_from_warc_gzip_member(
                            raw,
                            max_body_bytes=int(tool_args.get("max_bytes") or 2_000_000),
                            max_preview_chars=int(tool_args.get("max_preview_chars") or 80_000),
                            include_body_base64=False,
                        )
                        out["http"] = {
                            "ok": parsed.ok,
                            "warc_headers": parsed.warc_headers,
                            "status": parsed.http_status,
                            "status_line": parsed.http_status_line,
                            "headers": parsed.http_headers,
                            "body_text_preview": parsed.body_text_preview,
                            "body_is_html": parsed.body_is_html,
                            "body_mime": parsed.body_mime,
                            "body_charset": parsed.body_charset,
                            "error": parsed.error,
                        }
                    except Exception as e:
                        out["http"] = {
                            "ok": False,
                            "error": f"parse_failed: {type(e).__name__}: {e}",
                        }
            elif tool_name == "list_collections":
                year = tool_args.get("year")
                cols = api.list_collections(master_db=Path(master_db), year=str(year) if year else None)
                out = [
                    {"year": c.year, "collection": c.collection, "collection_db_path": str(c.collection_db_path)}
                    for c in cols
                ]
            elif tool_name == "brave_search_ccindex":
                q = str(tool_args.get("query") or "")
                year = tool_args.get("year")
                parquet_root = Path(str(tool_args.get("parquet_root") or "/storage/ccindex_parquet"))
                count = int(tool_args.get("count") or 8)

                res = api.brave_search_ccindex(
                    q,
                    count=count,
                    parquet_root=parquet_root,
                    master_db=Path(master_db),
                    year=str(year) if year else None,
                )
                out = {"query": res.query, "elapsed_s": res.elapsed_s, "results": res.results}
            else:
                return JSONResponse(_jsonrpc_error(req_id, -32601, f"Unknown tool: {tool_name}"))

            return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": out})
        except Exception as e:
            return JSONResponse(_jsonrpc_error(req_id, -32000, f"Tool error: {type(e).__name__}: {e}"))

    @app.get("/", response_class=HTMLResponse)
    def home(
        q: str = Query(default="", description="domain or url"),
        year: str = Query(default="", description="optional year"),
        max_matches: int = Query(default=200, ge=1, le=5000),
        parquet_root: str = Query(default="/storage/ccindex_parquet"),
    ) -> str:
        form = f"""
<div class='card'>
  <div class='row'>
    <div class='field' style='min-width: 360px; flex: 1;'>
      <label>Domain / URL</label>
      <input id='q' name='q' value='{html.escape(_q(q))}' placeholder='18f.gov or https://18f.gov'>
    </div>
    <div class='field'>
      <label>Year (optional)</label>
      <input id='year' name='year' value='{html.escape(_q(year))}' placeholder='2024'>
    </div>
    <div class='field'>
      <label>Max matches</label>
      <input id='max_matches' name='max_matches' value='{int(max_matches)}' type='number' min='1' max='5000'>
    </div>
    <div class='field' style='min-width: 320px; flex: 1;'>
      <label>Parquet root</label>
      <input id='parquet_root' name='parquet_root' value='{html.escape(_q(parquet_root))}'>
    </div>
    <div class='field'>
      <button type='submit'>Search</button>
    </div>
  </div>
  <div class='small' style='margin-top: 10px;'>
    Uses master meta-index: <span class='code'>{html.escape(str(master_db))}</span>
    • MCP JSON-RPC: <span class='code'>POST /mcp</span>
    • SDK: <a class='code' href='/static/ccindex-mcp-sdk.js'>ccindex-mcp-sdk.js</a>
  </div>
</div>
"""

        initial = {"q": q, "year": year, "max_matches": int(max_matches), "parquet_root": parquet_root}
        body = "\n".join(
            [
                "<form method='get' id='searchForm'>",
                form,
                "</form>",
                "<div id='status' class='card' style='margin-top: 14px;'><div class='small'>Enter a domain and search.</div></div>",
                "<div id='results' class='card' style='margin-top: 14px; padding: 0; display:none;'></div>",
                f"""
<script type='module'>
  import {{ ccindexMcp }} from '/static/ccindex-mcp-sdk.js';

  const initial = {json.dumps(initial)};
  const form = document.getElementById('searchForm');
  const statusEl = document.getElementById('status');
  const resultsEl = document.getElementById('results');

  function esc(s) {{
    return String(s ?? '')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#39;');
  }}

  function renderTable(records) {{
    const rows = (records || []).map((r, idx) => {{
      const url = esc(r.url || '');
      const ts = esc(r.timestamp || '');
      const status = esc(r.status ?? '');
      const mime = esc(r.mime || '');
      const coll = esc(r.collection || '');
      const warc = esc(r.warc_filename || '');
      const off = esc(r.warc_offset ?? '');
      const len = esc(r.warc_length ?? '');
      const recHref = `/record?warc_filename=${{encodeURIComponent(r.warc_filename||'')}}&warc_offset=${{encodeURIComponent(r.warc_offset||'')}}&warc_length=${{encodeURIComponent(r.warc_length||'')}}&parquet_root=${{encodeURIComponent(document.getElementById('parquet_root').value || '')}}`;
      return `
        <tr>
          <td class='small'>${{idx+1}}</td>
          <td><div class='code'>${{url}}</div><div class='small'>${{ts}}</div></td>
          <td><span class='badge'>${{status}}</span><div class='small'>${{mime}}</div></td>
          <td><div class='code'>${{coll}}</div><div class='small code'>${{warc}}</div></td>
          <td class='code'>${{off}}<div class='small'>len ${{len}}</div></td>
          <td><a class='code' href='${{recHref}}'>view record</a></td>
        </tr>
      `;
    }}).join("\\n");

    return `
      <table class='table'>
        <thead><tr>
          <th>#</th><th>URL</th><th>Status/MIME</th><th>Collection / WARC</th><th>Offset</th><th>Actions</th>
        </tr></thead>
        <tbody>${{rows || "<tr><td colspan='6' class='small'>No results.</td></tr>"}}</tbody>
      </table>
    `;
  }}

  async function runSearch() {{
    const q = document.getElementById('q').value;
    const year = document.getElementById('year').value;
    const maxMatches = parseInt(document.getElementById('max_matches').value || '200', 10);
    const parquetRoot = document.getElementById('parquet_root').value;

    if (!q.trim()) {{
      statusEl.innerHTML = "<div class='small'>Enter a domain and search.</div>";
      resultsEl.style.display = 'none';
      return;
    }}

    statusEl.innerHTML = "<div class='small'>Searching via MCP…</div>";
    resultsEl.style.display = 'none';

    try {{
      const res = await ccindexMcp.callTool('search_domain_meta', {{
        domain: q,
        year: year.trim() || null,
        max_matches: maxMatches,
        parquet_root: parquetRoot,
      }});

      const elapsed = (typeof res.elapsed_s === 'number') ? res.elapsed_s.toFixed(2) : String(res.elapsed_s ?? '');
      statusEl.innerHTML = `
        <span class='badge ok'>ok</span>
        meta_source=<span class='code'>${{esc(res.meta_source)}}</span>
        collections=<span class='code'>${{esc(res.collections_considered)}}</span>
        emitted=<span class='code'>${{esc(res.emitted)}}</span>
        elapsed_s=<span class='code'>${{esc(elapsed)}}</span>
      `;

      resultsEl.innerHTML = renderTable(res.records || []);
      resultsEl.style.display = 'block';
    }} catch (e) {{
      statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(e.message || e)}}</span>`;
      resultsEl.style.display = 'none';
    }}
  }}

  form.addEventListener('submit', (ev) => {{
    ev.preventDefault();
    runSearch();
  }});

  if ((initial.q || '').trim()) {{
    runSearch();
  }}
</script>
""",
            ]
        )
        return _layout("ccindex", body)

    @app.get("/download_record")
    def download_record(
        warc_filename: str,
        warc_offset: int,
        warc_length: int,
        prefix: str = "https://data.commoncrawl.org/",
        max_bytes: int = 20_000_000,
    ) -> Response:
        """Download the exact record byte-range as a file.

        This is more practical than downloading the full multi-GB WARC.
        """

        fetch = api.fetch_warc_record_range(
            warc_filename=str(warc_filename),
            warc_offset=int(warc_offset),
            warc_length=int(warc_length),
            prefix=str(prefix),
            max_bytes=int(max_bytes),
            decode_gzip_text=False,
            max_preview_chars=0,
        )
        if not fetch.ok or not fetch.raw_base64:
            msg = fetch.error or "failed to fetch record"
            return Response(content=msg, status_code=400, media_type="text/plain")

        import base64

        data = base64.b64decode(fetch.raw_base64)
        safe_warc = Path(str(warc_filename)).name
        fn = f"record_{safe_warc}_off{int(warc_offset)}_len{int(warc_length)}.warc.gz"
        headers = {"Content-Disposition": f"attachment; filename={fn}"}
        return Response(content=data, media_type="application/gzip", headers=headers)

    @app.get("/record", response_class=HTMLResponse)
    def record(
        warc_filename: str,
        warc_offset: int,
        warc_length: int,
        prefix: str = "https://data.commoncrawl.org/",
      parquet_root: str = "/storage/ccindex_parquet",
    ) -> str:
        pointer = {
            "warc_filename": warc_filename,
            "warc_offset": int(warc_offset),
            "warc_length": int(warc_length),
            "prefix": prefix,
        "max_bytes": 2_000_000,
        "max_preview_chars": 80_000,
        }

        head = f"""
<div class='card'>
  <div><a href='/'>← back</a></div>
  <hr>
  <div class='small'>WARC pointer</div>
  <div class='code'>{html.escape(str(warc_filename))}</div>
  <div class='row' style='margin-top: 10px;'>
    <div class='field'><label>Offset</label><div class='code'>{html.escape(str(warc_offset))}</div></div>
    <div class='field'><label>Length</label><div class='code'>{html.escape(str(warc_length))}</div></div>
    <div class='field'><label>Prefix</label><div class='code'>{html.escape(str(prefix))}</div></div>
  </div>
</div>
"""

        body = head + (
            "<div id='recStatus' class='card' style='margin-top: 14px;'><div class='small'>Fetching record via MCP…</div></div>"
            "<div id='recBody' class='card' style='margin-top: 14px; display:none;'>"
            "<div class='row' style='align-items:center; justify-content: space-between;'>"
            "  <div class='small'>Best-effort render (scripts disabled)</div>"
            "  <div style='display:flex; gap:12px; align-items:center;'>"
            "    <a class='code' id='dlRangeLink' href='#' target='_blank' rel='noreferrer'>download record range</a>"
            "    <a class='code' id='dlWarcLink' href='#' target='_blank' rel='noreferrer'>open full WARC</a>"
            "  </div>"
            "</div>"
            "<div class='row' style='margin-top: 10px;'>"
            "  <div class='field'><label>max_bytes</label><input id='max_bytes' type='number' min='1' step='1' value='2000000'></div>"
            "  <div class='field'><label>max_preview_chars</label><input id='max_preview_chars' type='number' min='0' step='1' value='80000'></div>"
            "  <div class='field'><button id='refetchBtn' type='button'>Re-fetch</button></div>"
            "</div>"
            "<div style='margin-top: 10px; border: 1px solid rgba(34, 48, 74, 0.7); border-radius: 10px; overflow:hidden;'>"
            "  <iframe id='recFrame' sandbox='' style='width: 100%; height: 70vh; border: 0; background: white;'></iframe>"
            "</div>"
            "<hr>"
            "<div class='small'>Decoded gzip preview (utf-8, errors=replace)</div>"
            "<pre id='recPreview' class='code'></pre>"
            "</div>"
            f"""
<script type='module'>
  import {{ ccindexMcp }} from '/static/ccindex-mcp-sdk.js';

  const pointer = {json.dumps(pointer)};
  const parquetRoot = {json.dumps(str(parquet_root))};
  const statusEl = document.getElementById('recStatus');
  const bodyEl = document.getElementById('recBody');
  const previewEl = document.getElementById('recPreview');
  const frameEl = document.getElementById('recFrame');
  const dlRangeLinkEl = document.getElementById('dlRangeLink');
  const dlWarcLinkEl = document.getElementById('dlWarcLink');
  const maxBytesEl = document.getElementById('max_bytes');
  const maxPreviewEl = document.getElementById('max_preview_chars');
  const refetchBtn = document.getElementById('refetchBtn');

  function esc(s) {{
    return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
  }}

  async function run() {{
    try {{
      pointer.max_bytes = parseInt(maxBytesEl.value || '2000000', 10);
      pointer.max_preview_chars = parseInt(maxPreviewEl.value || '80000', 10);
      const res = await ccindexMcp.callTool('fetch_warc_record', pointer);
      if (!res.ok) {{
        statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(res.error || 'unknown')}}</span>`;
        bodyEl.style.display = 'none';
        return;
      }}

      statusEl.innerHTML = `
        <span class='badge ok'>ok</span>
        status=<span class='code'>${{esc(res.status)}}</span>
        bytes=<span class='code'>${{esc(res.bytes_returned)}}/${{esc(res.bytes_requested)}}</span>
        sha256=<span class='code'>${{esc(res.sha256 || '')}}</span>
        <div class='small'>download_url: <span class='code'>${{esc(res.url)}}</span></div>
      `;

      const preview = res.decoded_text_preview || '';
      previewEl.textContent = preview;

      // Best-effort HTML extraction:
      // Prefer server-parsed HTTP payload, fall back to a string-slice heuristic.
      let htmlText = '';
      let redirectLoc = '';

      if (res.http && res.http.ok) {{
        const bodyText = res.http.body_text_preview || '';
        if (res.http.body_is_html) {{
          htmlText = bodyText;
        }}
        const hdrs = res.http.headers || {{}};
        redirectLoc = String(hdrs.location || hdrs.Location || '').trim();
      }} else {{
        // WARC headers + HTTP response headers + body. We try to locate the HTTP
        // response, then split headers/body on the first blank line.
        const httpIdx = preview.indexOf('HTTP/');
        if (httpIdx >= 0) {{
          const httpPart = preview.slice(httpIdx);
          const sep = httpPart.indexOf("\\r\\n\\r\\n");
          if (sep >= 0) {{
            htmlText = httpPart.slice(sep + 4);
          }} else {{
            const sep2 = httpPart.indexOf("\\n\\n");
            if (sep2 >= 0) htmlText = httpPart.slice(sep2 + 2);
          }}

          const m = httpPart.match(/^Location:\\s*(.+)$/im);
          if (m && m[1]) redirectLoc = String(m[1] || '').trim();
        }}
      }}

      // If this was a redirect, offer a helper to follow it.
      if (redirectLoc) {{
        const followHref = `/?q=${{encodeURIComponent(redirectLoc)}}&parquet_root=${{encodeURIComponent(parquetRoot)}}&max_matches=25`;
        statusEl.innerHTML += `<div class='small' style='margin-top:8px;'>redirect location: <a class='code' href='${{followHref}}'>${{esc(redirectLoc)}}</a> <button id='followBtn' type='button' style='margin-left:10px;'>follow in CCIndex</button></div>`;
        const btn = document.getElementById('followBtn');
        if (btn) {{
          btn.addEventListener('click', async () => {{
            try {{
              statusEl.innerHTML = "<div class='small'>Following redirect via CCIndex…</div>";
              const sres = await ccindexMcp.callTool('search_domain_meta', {{
                domain: redirectLoc,
                year: null,
                max_matches: 25,
                parquet_root: parquetRoot,
              }});
              const rec = (sres.records || [])[0];
              if (!rec) throw new Error('No CCIndex records for redirect target');
              const href = `/record?warc_filename=${{encodeURIComponent(rec.warc_filename||'')}}&warc_offset=${{encodeURIComponent(rec.warc_offset||'')}}&warc_length=${{encodeURIComponent(rec.warc_length||'')}}&parquet_root=${{encodeURIComponent(parquetRoot)}}`;
              window.location.href = href;
            }} catch (e) {{
              statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(e.message || e)}}</span>`;
            }}
          }});
        }}
      }}

      // Render into a sandboxed iframe. If we didn't detect HTML, show a simple
      // placeholder so the pane isn't blank.
      if ((htmlText || '').trim()) {{
        frameEl.srcdoc = htmlText;
      }} else {{
        frameEl.srcdoc = "<pre style='white-space:pre-wrap;word-break:break-word;padding:12px;'>No HTML detected in decoded preview.\\n\\nThis record may be non-HTML or the preview may be truncated.</pre>";
      }}

      // Download helpers.
      dlWarcLinkEl.href = (res.url || '#');
      const rangeHref = `/download_record?warc_filename=${{encodeURIComponent(pointer.warc_filename)}}&warc_offset=${{encodeURIComponent(pointer.warc_offset)}}&warc_length=${{encodeURIComponent(pointer.warc_length)}}&prefix=${{encodeURIComponent(pointer.prefix)}}&max_bytes=${{encodeURIComponent(pointer.max_bytes)}}`;
      dlRangeLinkEl.href = rangeHref;

      bodyEl.style.display = 'block';
    }} catch (e) {{
      statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(e.message || e)}}</span>`;
      bodyEl.style.display = 'none';
    }}
  }}

  refetchBtn.addEventListener('click', () => run());
  run();
</script>
"""
        )

        return _layout("ccindex record", body)

    @app.get("/discover", response_class=HTMLResponse)
    def discover(
        q: str = Query(default="", description="brave query"),
        year: str = Query(default="", description="optional year"),
        count: int = Query(default=8, ge=1, le=20),
        parquet_root: str = Query(default="/storage/ccindex_parquet"),
    ) -> str:
        initial = {"q": q, "year": year, "count": int(count), "parquet_root": parquet_root}

        body = f"""
<div class='card'>
  <div class='small'>Brave Search -> Common Crawl pointers (no live-site visits)</div>
  <hr>
  <form id='discoverForm'>
    <div class='row'>
      <div class='field' style='min-width: 520px; flex: 1;'>
        <label>Query</label>
        <input id='dq' name='q' value='{html.escape(_q(q))}' placeholder='site:.gov climate resilience'>
      </div>
      <div class='field'>
        <label>Year (optional)</label>
        <input id='dyear' name='year' value='{html.escape(_q(year))}' placeholder='2024'>
      </div>
      <div class='field'>
        <label>Count</label>
        <input id='dcount' name='count' value='{int(count)}' type='number' min='1' max='20'>
      </div>
      <div class='field' style='min-width: 320px; flex: 1;'>
        <label>Parquet root</label>
        <input id='dparquet_root' name='parquet_root' value='{html.escape(_q(parquet_root))}'>
      </div>
      <div class='field'>
        <button type='submit'>Search</button>
      </div>
    </div>
  </form>
  <div class='small' style='margin-top: 10px;'>
    Server-side Brave API key required: <span class='code'>BRAVE_SEARCH_API_KEY</span>
    • MCP JSON-RPC: <span class='code'>POST /mcp</span>
  </div>
</div>

<div id='dstatus' class='card' style='margin-top: 14px;'><div class='small'>Enter a query and search.</div></div>
<div id='dresults' class='card' style='margin-top: 14px; display:none; padding: 0;'></div>

<script type='module'>
  import {{ ccindexMcp }} from '/static/ccindex-mcp-sdk.js';

  const initial = {json.dumps(initial)};
  const form = document.getElementById('discoverForm');
  const statusEl = document.getElementById('dstatus');
  const resultsEl = document.getElementById('dresults');

  function esc(s) {{
    return String(s ?? '')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#39;');
  }}

  function firstRecordLink(ccMatches) {{
    const r = (ccMatches || [])[0];
    if (!r) return null;
    return `/record?warc_filename=${{encodeURIComponent(r.warc_filename||'')}}&warc_offset=${{encodeURIComponent(r.warc_offset||'')}}&warc_length=${{encodeURIComponent(r.warc_length||'')}}`;
  }}

  function render(res) {{
    const items = (res.results || []).map((it) => {{
      const title = esc(it.title || '');
      const url = esc(it.url || '');
      const desc = esc(it.description || '');
      const matches = it.cc_matches || [];
      const view = firstRecordLink(matches);
      const badge = matches.length ? `<span class='badge ok'>${{matches.length}} captures</span>` : `<span class='badge'>no capture</span>`;
      const actions = view
        ? `<a class='code' href='${{view}}'>view record</a>`
        : `<span class='small'>no record found</span>`;

      return `
        <div style='padding: 12px; border-bottom: 1px solid rgba(34,48,74,.7);'>
          <div style='display:flex; justify-content: space-between; gap: 10px; align-items: baseline;'>
            <div>
              <div>${{title || url}}</div>
              <div class='small code'>${{url}}</div>
            </div>
            <div style='display:flex; gap: 10px; align-items:center;'>
              ${{badge}}
              ${{actions}}
            </div>
          </div>
          <div class='small' style='margin-top: 8px;'>${{desc}}</div>
        </div>
      `;
    }}).join("\\n");

    return `<div>${{items || "<div class='small' style='padding: 12px;'>No results.</div>"}}</div>`;
  }}

  async function runDiscover() {{
    const q = document.getElementById('dq').value;
    const year = document.getElementById('dyear').value;
    const count = parseInt(document.getElementById('dcount').value || '8', 10);
    const parquetRoot = document.getElementById('dparquet_root').value;

    if (!q.trim()) {{
      statusEl.innerHTML = "<div class='small'>Enter a query and search.</div>";
      resultsEl.style.display = 'none';
      return;
    }}

    statusEl.innerHTML = "<div class='small'>Searching Brave + resolving via CCIndex…</div>";
    resultsEl.style.display = 'none';

    try {{
      const res = await ccindexMcp.callTool('brave_search_ccindex', {{
        query: q,
        count,
        year: year.trim() || null,
        parquet_root: parquetRoot,
      }});

      const elapsed = (typeof res.elapsed_s === 'number') ? res.elapsed_s.toFixed(2) : String(res.elapsed_s ?? '');
      statusEl.innerHTML = `<span class='badge ok'>ok</span> elapsed_s=<span class='code'>${{esc(elapsed)}}</span> results=<span class='code'>${{esc((res.results||[]).length)}}</span>`;
      resultsEl.innerHTML = render(res);
      resultsEl.style.display = 'block';
    }} catch (e) {{
      statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(e.message || e)}}</span>`;
      resultsEl.style.display = 'none';
    }}
  }}

  form.addEventListener('submit', (ev) => {{
    ev.preventDefault();
    runDiscover();
  }});

  if ((initial.q || '').trim()) {{
    runDiscover();
  }}
</script>
"""

        return _layout("ccindex discover", body)

    return app


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Run the ccindex web dashboard")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument(
        "--master-db",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
        help="Master meta-index DuckDB",
    )
    ap.add_argument("--reload", action="store_true", default=False, help="Enable uvicorn reload")

    args = ap.parse_args(argv)

    try:
        import uvicorn  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "Missing dashboard dependencies. Install with: pip install -e '.[ccindex-dashboard]'\n"
            f"Import error: {e}"
        )

    app = create_app(master_db=Path(args.master_db).expanduser().resolve())
    uvicorn.run(app, host=str(args.host), port=int(args.port), reload=bool(args.reload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
