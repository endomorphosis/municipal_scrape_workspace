"""Web dashboard for ccindex (application layer).

Run (dev):
  python -m common_crawl_search_engine.ccsearch.dashboard --host 127.0.0.1 --port 8787
"""

from __future__ import annotations

import argparse
import html
import json
import os
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
select {
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

/* Prevent long URLs/WARC filenames from blowing out tables */
.table td { overflow-wrap: anywhere; word-break: break-word; }
.table td .code { overflow-wrap: anywhere; word-break: break-word; }

.spinner {
  display: inline-block;
  width: 14px;
  height: 14px;
  border-radius: 999px;
  border: 2px solid rgba(148, 163, 184, 0.35);
  border-top-color: rgba(148, 163, 184, 0.95);
  animation: spin 0.85s linear infinite;
  vertical-align: -2px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

pre { white-space: pre-wrap; word-break: break-word; }
hr { border: none; border-top: 1px solid rgba(34, 48, 74, 0.7); margin: 12px 0; }
"""


def _q(s: Optional[str]) -> str:
    return "" if s is None else str(s)


def _layout(title: str, body_html: str, *, embed: bool = False, base_path: str = "") -> str:
    base_path = (base_path or "").strip()
    if base_path != "/" and base_path.endswith("/"):
        base_path = base_path[:-1]
    if base_path and not base_path.startswith("/"):
        base_path = "/" + base_path

    def _p(path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{base_path}{path}" if base_path else path

    nav = f"""
  <div style='margin-top: 10px; display:flex; gap: 12px; flex-wrap: wrap;'>
    <a class='badge' href='{html.escape(_p("/"))}'>Wayback</a>
    <a class='badge' href='{html.escape(_p("/discover"))}'>Search</a>
    <a class='badge' href='{html.escape(_p("/index"))}'>Index</a>
    <a class='badge' href='{html.escape(_p("/settings"))}'>Settings</a>
  </div>
"""

    header_html = ""
    if not embed:
        header_html = f"""
<header>
  <div class='brand'>
    <h1>Common Crawl Search Engine</h1>
    <span>MCP server • search + WARC replay</span>
  </div>
{nav}
</header>
"""

    main_style = "" if not embed else "max-width:none; padding: 12px;"

    return f"""<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <meta name='ccindex-base-path' content='{html.escape(base_path)}'>
  <title>{html.escape(title)}</title>
  <style>{_CSS}</style>
</head>
<body>
{header_html}
<main style='{main_style}'>
{body_html}
</main>
</body>
</html>"""


def _jsonrpc_error(req_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": int(code), "message": str(message)}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


_DEFAULT_MASTER_DB = Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb")


def _env_master_db() -> Path:
    raw = (
        os.environ.get("CCINDEX_MASTER_DB")
        or os.environ.get("COMMON_CRAWL_MASTER_DB")
        or os.environ.get("CCSEARCH_MASTER_DB")
        or ""
    ).strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _DEFAULT_MASTER_DB


def create_app(master_db: Path) -> Any:
    try:
        from fastapi import FastAPI, Query, Request
        from fastapi.responses import HTMLResponse, JSONResponse, Response
        from fastapi.staticfiles import StaticFiles
        from starlette.concurrency import run_in_threadpool
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "Missing dashboard dependencies. Install with: pip install -e '.[ccindex-dashboard]'\n"
            f"Import error: {e}"
        )

    # Optional CORS for remote JS SDK usage.
    cors_origins_raw = (os.environ.get("CCINDEX_CORS_ORIGINS") or os.environ.get("CCSEARCH_CORS_ORIGINS") or "").strip()
    cors_allow_origins: list[str] = []
    cors_allow_credentials = True
    if cors_origins_raw:
        cors_allow_origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]
        if cors_allow_origins == ["*"]:
            # With wildcard origins, credentials must be disabled.
            cors_allow_credentials = False

    # NOTE: This module uses `from __future__ import annotations`, which stores
    # type annotations as strings. FastAPI resolves those strings using the
    # function's global namespace (the module globals), not the create_app() local
    # scope. Ensure Request is present globally so `request: Request` is treated
    # as the Starlette request object (not a required query param).
    globals()["Request"] = Request
    globals()["Response"] = Response
    globals()["HTMLResponse"] = HTMLResponse
    globals()["JSONResponse"] = JSONResponse

    app = FastAPI(title="Common Crawl Search Engine Dashboard", version="0.1")

    class ForwardedPrefixMiddleware:
      """Honor reverse-proxy prefix headers (X-Forwarded-Prefix / X-Script-Name).

      If the proxy forwards the prefix *and* also leaves it in the URL path,
      we strip it so routes like `/mcp` still match.
      """

      def __init__(self, inner_app: Any) -> None:
        self.app = inner_app

      async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") not in ("http", "websocket"):
          await self.app(scope, receive, send)
          return

        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in (scope.get("headers") or [])}
        prefix = (headers.get("x-forwarded-prefix") or headers.get("x-script-name") or "").strip()
        if not prefix:
          await self.app(scope, receive, send)
          return

        if not prefix.startswith("/"):
          prefix = "/" + prefix
        if prefix != "/" and prefix.endswith("/"):
          prefix = prefix[:-1]

        new_scope = dict(scope)
        new_scope["root_path"] = prefix

        path = str(new_scope.get("path") or "")
        if path.startswith(prefix):
          stripped = path[len(prefix) :]
          new_scope["path"] = stripped if stripped else "/"

        await self.app(new_scope, receive, send)

    app.add_middleware(ForwardedPrefixMiddleware)

    # Brave web search enforces a per-request max `count` (commonly 20). Keep
    # UI/tool defaults in-bounds to avoid Brave HTTP 422 validation errors.
    try:
      from common_crawl_search_engine.ccsearch.brave_search import brave_web_search_max_count

      brave_max_count = int(brave_web_search_max_count())
    except Exception:
      brave_max_count = 20
    brave_max_count = max(1, int(brave_max_count))

    def _base_path(request: Request) -> str:
      root = str(getattr(request, "scope", {}).get("root_path") or "").strip()
      if root != "/" and root.endswith("/"):
        root = root[:-1]
      return root

    if cors_allow_origins:
        try:
            from fastapi.middleware.cors import CORSMiddleware

            app.add_middleware(
                CORSMiddleware,
                allow_origins=cors_allow_origins,
                allow_credentials=cors_allow_credentials,
                allow_methods=["*"],
                allow_headers=["*"],
            )
        except Exception:
            # CORS is best-effort; if middleware isn't available, proceed.
            pass

    def _settings_path() -> Path:
      state_dir = Path("state")
      state_dir.mkdir(parents=True, exist_ok=True)
      return state_dir / "dashboard_settings.json"

    def _default_settings() -> Dict[str, Any]:
      return {
        "default_cache_mode": "range",  # range | auto | full
        "default_max_bytes": 2_000_000,
        "default_max_preview_chars": 80_000,
        "range_cache_max_bytes": 2_000_000_000,
        "range_cache_max_item_bytes": 25_000_000,
        "full_warc_cache_dir": None,
        "full_warc_max_bytes": 5_000_000_000,
        "full_warc_cache_max_total_bytes": 0,
        "brave_search_api_key": None,
      }

    def _load_settings() -> Dict[str, Any]:
      p = _settings_path()
      if not p.exists():
        return _default_settings()
      try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
          return _default_settings()
        out = _default_settings()
        out.update({k: data.get(k) for k in out.keys()})
        return out
      except Exception:
        return _default_settings()

    def _save_settings(settings: Dict[str, Any]) -> None:
      p = _settings_path()
      p.write_text(json.dumps(settings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

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
    async def mcp(request: Request) -> Response:
      payload = await request.json()

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
          "description": "Fetch a WARC record (range or cached full WARC) and optionally decode a text preview",
          "inputSchema": {
            "type": "object",
            "properties": {
              "warc_filename": {"type": "string"},
              "warc_offset": {"type": "integer"},
              "warc_length": {"type": "integer"},
              "prefix": {"type": "string"},
              "max_bytes": {"type": "integer"},
              "max_preview_chars": {"type": "integer"},
              "cache_mode": {"type": "string", "enum": ["range", "auto", "full"]},
              "full_warc_cache_dir": {"type": ["string", "null"]},
              "full_warc_max_bytes": {"type": "integer"},
              "full_warc_cache_max_total_bytes": {"type": "integer"},
              "range_cache_max_bytes": {"type": "integer"},
              "range_cache_max_item_bytes": {"type": "integer"},
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
              "count": {"type": "integer", "minimum": 1, "maximum": int(brave_max_count)},
              "offset": {"type": "integer", "minimum": 0},
              "year": {"type": ["string", "null"]},
              "parquet_root": {"type": "string"},
            },
            "required": ["query"],
          },
        },
        {
          "name": "brave_cache_stats",
          "description": "Return stats for the on-disk Brave Search cache",
          "inputSchema": {"type": "object", "properties": {}},
        },
        {
          "name": "brave_cache_clear",
          "description": "Clear the on-disk Brave Search cache",
          "inputSchema": {"type": "object", "properties": {}},
        },
        {
          "name": "brave_resolve_cache_stats",
          "description": "Return stats for the on-disk Brave->CCIndex resolve cache",
          "inputSchema": {"type": "object", "properties": {}},
        },
        {
          "name": "brave_resolve_cache_clear",
          "description": "Clear the on-disk Brave->CCIndex resolve cache",
          "inputSchema": {"type": "object", "properties": {}},
        },
        {
          "name": "orchestrator_settings_get",
          "description": "Get persisted ccindex orchestrator settings",
          "inputSchema": {"type": "object", "properties": {}},
        },
        {
          "name": "orchestrator_settings_set",
          "description": "Update persisted ccindex orchestrator settings (partial update)",
          "inputSchema": {
            "type": "object",
            "properties": {
              "settings": {"type": "object"}
            },
            "required": ["settings"],
          },
        },
        {
          "name": "orchestrator_collection_status",
          "description": "Return validator status for a collection (download/convert/sort/index completeness)",
          "inputSchema": {
            "type": "object",
            "properties": {"collection": {"type": "string"}},
            "required": ["collection"],
          },
        },
        {
          "name": "orchestrator_delete_collection_index",
          "description": "Delete per-collection DuckDB index artifacts so the next run rebuilds",
          "inputSchema": {
            "type": "object",
            "properties": {"collection": {"type": "string"}},
            "required": ["collection"],
          },
        },
        {
          "name": "orchestrator_job_plan",
          "description": "Plan the orchestrator subprocess command for a long-running job",
          "inputSchema": {
            "type": "object",
            "properties": {
              "mode": {"type": "string", "enum": ["pipeline", "download_only", "cleanup_only", "build_meta_indexes"]},
              "filter": {"type": ["string", "null"]},
              "workers": {"type": ["integer", "null"]},
              "force_reindex": {"type": ["boolean", "null"]},
              "cleanup_dry_run": {"type": ["boolean", "null"]},
              "yes": {"type": ["boolean", "null"]},
              "heartbeat_seconds": {"type": ["integer", "null"]},
              "sort_workers": {"type": ["integer", "null"]},
              "sort_memory_per_worker_gb": {"type": ["number", "null"]},
              "sort_temp_dir": {"type": ["string", "null"]}
            },
            "required": ["mode"],
          },
        },
        {
          "name": "orchestrator_job_start",
          "description": "Start a long-running orchestrator job in a background subprocess",
          "inputSchema": {
            "type": "object",
            "properties": {
              "planned": {"type": "object"},
              "label": {"type": "string"}
            },
            "required": ["planned"],
          },
        },
        {
          "name": "orchestrator_job_stop",
          "description": "Stop a running orchestrator job by PID",
          "inputSchema": {
            "type": "object",
            "properties": {"pid": {"type": "integer"}, "sig": {"type": "string"}},
            "required": ["pid"],
          },
        },
        {
          "name": "orchestrator_job_tail",
          "description": "Tail the orchestrator job log",
          "inputSchema": {
            "type": "object",
            "properties": {"log_path": {"type": "string"}, "lines": {"type": "integer"}},
            "required": ["log_path"],
          },
        },
        {
          "name": "cc_collinfo_list",
          "description": "List known Common Crawl collections from cached collinfo.json (or repo fallback)",
          "inputSchema": {
            "type": "object",
            "properties": {
              "prefer_cache": {"type": ["boolean", "null"]},
            },
          },
        },
        {
          "name": "cc_collinfo_update",
          "description": "Refresh cached collinfo.json from the Common Crawl website",
          "inputSchema": {
            "type": "object",
            "properties": {
              "url": {"type": ["string", "null"]},
              "timeout_s": {"type": ["number", "null"]},
            },
          },
        },
        {
          "name": "orchestrator_collections_status",
          "description": "Return validator status for many collections",
          "inputSchema": {
            "type": "object",
            "properties": {
              "collections": {"type": "array", "items": {"type": "string"}},
              "parallelism": {"type": ["integer", "null"]},
            },
            "required": ["collections"],
          },
        },
        {
          "name": "orchestrator_delete_collection_indexes",
          "description": "Delete DuckDB index artifacts for multiple collections",
          "inputSchema": {
            "type": "object",
            "properties": {
              "collections": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["collections"],
          },
        },
        {
          "name": "orchestrator_jobs_list",
          "description": "List recent orchestrator jobs started from the dashboard/CLI",
          "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": ["integer", "null"]}},
          },
        },
        {
          "name": "orchestrator_job_status",
          "description": "Get running/dead status and heuristic progress for a job",
          "inputSchema": {
            "type": "object",
            "properties": {
              "pid": {"type": ["integer", "null"]},
              "log_path": {"type": ["string", "null"]},
              "lines": {"type": ["integer", "null"]},
            },
          },
        },
      ]

      def _call_tool_sync(*, tool_name: str, tool_args: Dict[str, Any]) -> Any:
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
          return {
            "meta_source": res.meta_source,
            "collections_considered": res.collections_considered,
            "emitted": res.emitted,
            "elapsed_s": res.elapsed_s,
            "records": res.records,
          }

        if tool_name == "fetch_warc_record":
          s = _load_settings()
          max_bytes = int(tool_args.get("max_bytes") or int(s.get("default_max_bytes") or 2_000_000))
          max_preview_chars = int(
            tool_args.get("max_preview_chars") or int(s.get("default_max_preview_chars") or 80_000)
          )

          range_cache_max_bytes = int(
            tool_args.get("range_cache_max_bytes") or int(s.get("range_cache_max_bytes") or 2_000_000_000)
          )
          range_cache_max_item_bytes = int(
            tool_args.get("range_cache_max_item_bytes")
            or int(s.get("range_cache_max_item_bytes") or 25_000_000)
          )
          full_warc_cache_max_total_bytes = int(
            tool_args.get("full_warc_cache_max_total_bytes")
            or int(s.get("full_warc_cache_max_total_bytes") or 0)
          )

          fetch, source, local_path = api.fetch_warc_record(
            warc_filename=str(tool_args.get("warc_filename") or ""),
            warc_offset=int(tool_args.get("warc_offset") or 0),
            warc_length=int(tool_args.get("warc_length") or 0),
            prefix=str(tool_args.get("prefix") or "https://data.commoncrawl.org/"),
            max_bytes=max_bytes,
            decode_gzip_text=True,
            max_preview_chars=max_preview_chars,
            cache_mode=str(tool_args.get("cache_mode") or str(s.get("default_cache_mode") or "range")),
            range_cache_max_bytes=range_cache_max_bytes,
            range_cache_max_item_bytes=range_cache_max_item_bytes,
            full_warc_cache_dir=(
              Path(str(tool_args.get("full_warc_cache_dir")))
              if tool_args.get("full_warc_cache_dir")
              else (Path(str(s.get("full_warc_cache_dir"))) if s.get("full_warc_cache_dir") else None)
            ),
            full_warc_max_bytes=int(
              tool_args.get("full_warc_max_bytes") or int(s.get("full_warc_max_bytes") or 5_000_000_000)
            ),
            full_warc_cache_max_total_bytes=full_warc_cache_max_total_bytes,
          )

          out: Dict[str, Any] = {
            "ok": fetch.ok,
            "status": fetch.status,
            "url": fetch.url,
            "source": source,
            "local_warc_path": local_path,
            "bytes_requested": fetch.bytes_requested,
            "bytes_returned": fetch.bytes_returned,
            "sha256": fetch.sha256,
            "decoded_text_preview": fetch.decoded_text_preview,
            "error": fetch.error,
          }

          if fetch.ok and fetch.raw_base64:
            try:
              import base64 as _b64

              raw = _b64.b64decode(fetch.raw_base64)
              parsed = api.extract_http_from_warc_gzip_member(
                raw,
                max_body_bytes=max_bytes,
                max_preview_chars=max_preview_chars,
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
              out["http"] = {"ok": False, "error": f"parse_failed: {type(e).__name__}: {e}"}

          return out

        if tool_name == "list_collections":
          year = tool_args.get("year")
          cols = api.list_collections(master_db=Path(master_db), year=str(year) if year else None)
          return [
            {"year": c.year, "collection": c.collection, "collection_db_path": str(c.collection_db_path)}
            for c in cols
          ]

        if tool_name == "brave_search_ccindex":
          s = _load_settings()
          q = str(tool_args.get("query") or "")
          year = tool_args.get("year")
          parquet_root = Path(str(tool_args.get("parquet_root") or "/storage/ccindex_parquet"))
          count = int(tool_args.get("count") or int(brave_max_count))
          offset = int(tool_args.get("offset") or 0)

          api_key = None
          if not (os.environ.get("BRAVE_SEARCH_API_KEY") or "").strip():
            api_key = (str(s.get("brave_search_api_key") or "").strip() or None)

          res = api.brave_search_ccindex(
            q,
            count=count,
            offset=offset,
            parquet_root=parquet_root,
            master_db=Path(master_db),
            year=str(year) if year else None,
            api_key=api_key,
          )
          return {
            "query": res.query,
            "count": res.count,
            "offset": res.offset,
            "total_results": res.total_results,
            "brave_cached": res.brave_cached,
            "resolved_cached": res.resolved_cached,
            "elapsed_s": res.elapsed_s,
            "brave_elapsed_s": res.brave_elapsed_s,
            "resolve_elapsed_s": res.resolve_elapsed_s,
            "resolve_mode": res.resolve_mode,
            "resolve_domains": res.resolve_domains,
            "resolve_parquet_files": res.resolve_parquet_files,
            "results": res.results,
          }

        if tool_name == "brave_cache_stats":
          from common_crawl_search_engine.ccsearch.brave_search import brave_search_cache_stats

          return brave_search_cache_stats()

        if tool_name == "brave_cache_clear":
          from common_crawl_search_engine.ccsearch.brave_search import clear_brave_search_cache

          return clear_brave_search_cache()

        if tool_name == "brave_resolve_cache_stats":
          from common_crawl_search_engine.ccindex.api import brave_resolve_cache_stats

          return brave_resolve_cache_stats()

        if tool_name == "brave_resolve_cache_clear":
          from common_crawl_search_engine.ccindex.api import clear_brave_resolve_cache

          return clear_brave_resolve_cache()

        if tool_name == "orchestrator_settings_get":
          from common_crawl_search_engine.ccindex.orchestrator_manager import load_orchestrator_settings

          return load_orchestrator_settings()

        if tool_name == "orchestrator_settings_set":
          from common_crawl_search_engine.ccindex.orchestrator_manager import save_orchestrator_settings

          upd = tool_args.get("settings")
          if not isinstance(upd, dict):
            raise ValueError("settings must be an object")
          return save_orchestrator_settings(upd)

        if tool_name == "orchestrator_collection_status":
          from common_crawl_search_engine.ccindex.orchestrator_manager import validate_collection_status

          collection = str(tool_args.get("collection") or "").strip()
          if not collection:
            raise ValueError("collection is required")
          return validate_collection_status(collection)

        if tool_name == "orchestrator_delete_collection_index":
          from common_crawl_search_engine.ccindex.orchestrator_manager import delete_collection_index

          collection = str(tool_args.get("collection") or "").strip()
          if not collection:
            raise ValueError("collection is required")
          return delete_collection_index(collection)

        if tool_name == "orchestrator_job_plan":
          from common_crawl_search_engine.ccindex.orchestrator_manager import plan_orchestrator_command

          mode = str(tool_args.get("mode") or "").strip()
          return plan_orchestrator_command(
            mode=mode,  # type: ignore[arg-type]
            filter=(tool_args.get("filter") if tool_args.get("filter") is not None else None),
            workers=(int(tool_args.get("workers")) if tool_args.get("workers") is not None else None),
            force_reindex=(bool(tool_args.get("force_reindex")) if tool_args.get("force_reindex") is not None else None),
            cleanup_dry_run=(bool(tool_args.get("cleanup_dry_run")) if tool_args.get("cleanup_dry_run") is not None else None),
            yes=(bool(tool_args.get("yes")) if tool_args.get("yes") is not None else None),
            heartbeat_seconds=(int(tool_args.get("heartbeat_seconds")) if tool_args.get("heartbeat_seconds") is not None else None),
            sort_workers=(int(tool_args.get("sort_workers")) if tool_args.get("sort_workers") is not None else None),
            sort_memory_per_worker_gb=(float(tool_args.get("sort_memory_per_worker_gb")) if tool_args.get("sort_memory_per_worker_gb") is not None else None),
            sort_temp_dir=(str(tool_args.get("sort_temp_dir")) if tool_args.get("sort_temp_dir") is not None else None),
          )

        if tool_name == "orchestrator_job_start":
          from common_crawl_search_engine.ccindex.orchestrator_manager import start_orchestrator_job

          planned = tool_args.get("planned")
          if not isinstance(planned, dict):
            raise ValueError("planned must be an object")
          label = str(tool_args.get("label") or "orchestrator")
          job = start_orchestrator_job(planned=planned, label=label)
          return {"pid": job.pid, "log_path": job.log_path, "cmd": job.cmd}

        if tool_name == "orchestrator_job_stop":
          from common_crawl_search_engine.ccindex.orchestrator_manager import stop_job

          pid = int(tool_args.get("pid") or 0)
          if pid <= 0:
            raise ValueError("pid is required")
          sig = str(tool_args.get("sig") or "TERM")
          return stop_job(pid, sig=sig)

        if tool_name == "orchestrator_job_tail":
          from common_crawl_search_engine.ccindex.orchestrator_manager import tail_file

          log_path = str(tool_args.get("log_path") or "")
          if not log_path:
            raise ValueError("log_path is required")
          lines = int(tool_args.get("lines") or 200)
          return {"log_path": log_path, "tail": tail_file(log_path, lines=lines)}

        if tool_name == "cc_collinfo_list":
          from common_crawl_search_engine.ccindex.orchestrator_manager import load_collinfo

          prefer_cache = tool_args.get("prefer_cache")
          return load_collinfo(prefer_cache=(bool(prefer_cache) if prefer_cache is not None else True))

        if tool_name == "cc_collinfo_update":
          from common_crawl_search_engine.ccindex.orchestrator_manager import update_collinfo

          url = tool_args.get("url")
          timeout_s = tool_args.get("timeout_s")
          return update_collinfo(
            url=(str(url) if url is not None else "https://index.commoncrawl.org/collinfo.json"),
            timeout_s=(float(timeout_s) if timeout_s is not None else 15.0),
          )

        if tool_name == "orchestrator_collections_status":
          from common_crawl_search_engine.ccindex.orchestrator_manager import validate_collections_status

          cols = tool_args.get("collections")
          if not isinstance(cols, list):
            raise ValueError("collections must be an array")
          parallelism = tool_args.get("parallelism")
          return validate_collections_status(
            [str(c) for c in cols],
            parallelism=(int(parallelism) if parallelism is not None else 8),
          )

        if tool_name == "orchestrator_delete_collection_indexes":
          from common_crawl_search_engine.ccindex.orchestrator_manager import delete_collection_indexes

          cols = tool_args.get("collections")
          if not isinstance(cols, list):
            raise ValueError("collections must be an array")
          return delete_collection_indexes([str(c) for c in cols])

        if tool_name == "orchestrator_jobs_list":
          from common_crawl_search_engine.ccindex.orchestrator_manager import list_jobs

          limit = tool_args.get("limit")
          return {"ok": True, "jobs": list_jobs(limit=(int(limit) if limit is not None else 50))}

        if tool_name == "orchestrator_job_status":
          from common_crawl_search_engine.ccindex.orchestrator_manager import job_status

          pid = tool_args.get("pid")
          log_path = tool_args.get("log_path")
          lines = tool_args.get("lines")
          return job_status(
            pid=(int(pid) if pid is not None else None),
            log_path=(str(log_path) if log_path is not None else None),
            lines=(int(lines) if lines is not None else 200),
          )

        raise KeyError(tool_name)

      async def _handle_one(req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params")

        # Notification: no id means no response.
        if req_id is None:
          return None

        if method == "tools/list":
          return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}}

        if method != "tools/call":
          return _jsonrpc_error(req_id, -32601, f"Method not found: {method}")

        if not isinstance(params, dict):
          return _jsonrpc_error(req_id, -32602, "Invalid params")

        tool_name = params.get("name")
        tool_args = params.get("arguments") or {}
        if not isinstance(tool_name, str) or not tool_name:
          return _jsonrpc_error(req_id, -32602, "Missing tool name")
        if not isinstance(tool_args, dict):
          return _jsonrpc_error(req_id, -32602, "Tool arguments must be an object")

        try:
          out = await run_in_threadpool(_call_tool_sync, tool_name=tool_name, tool_args=tool_args)
          return {"jsonrpc": "2.0", "id": req_id, "result": out}
        except KeyError:
          return _jsonrpc_error(req_id, -32601, f"Unknown tool: {tool_name}")
        except Exception as e:
          return _jsonrpc_error(req_id, -32000, f"Tool error: {type(e).__name__}: {e}")

      if isinstance(payload, list):
        responses: list[Dict[str, Any]] = []
        for item in payload:
          if not isinstance(item, dict):
            responses.append(_jsonrpc_error(None, -32600, "Invalid Request"))
            continue
          r = await _handle_one(item)
          if r is not None:
            responses.append(r)

        if not responses:
          return Response(status_code=204)
        return JSONResponse(responses)

      if not isinstance(payload, dict):
        return JSONResponse(_jsonrpc_error(None, -32600, "Invalid Request"))

      resp = await _handle_one(payload)
      if resp is None:
        return Response(status_code=204)
      return JSONResponse(resp)

    @app.get("/healthz")
    def healthz() -> Dict[str, Any]:
      return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    def home(
        request: Request,
        q: str = Query(default="", description="domain or url"),
        year: str = Query(default="", description="optional year"),
        max_matches: int = Query(default=500, ge=1, le=5000),
        parquet_root: str = Query(default="/storage/ccindex_parquet"),
        embed: int = Query(default=0, ge=0, le=1),
    ) -> str:
        base_path = _base_path(request)
        form = f"""
<div class='card'>
  <div class='row'>
    <div class='field' style='min-width: 360px; flex: 1;'>
      <label>Domain / URL</label>
      <input id='q' name='q' value='{html.escape(_q(q))}' placeholder='18f.gov or https://18f.gov'>
    </div>
    <div class='field'>
      <label>Year (optional)</label>
      <select id='year' name='year'></select>
    </div>
    <div class='field'>
      <label>Status (optional)</label>
      <select id='status_filter'>
        <option value=''>any</option>
        <option value='200'>200</option>
        <option value='301'>301</option>
        <option value='302'>302</option>
        <option value='404'>404</option>
        <option value='500'>500</option>
      </select>
    </div>
    <div class='field' style='min-width: 220px;'>
      <label>MIME (optional)</label>
      <select id='mime_filter'>
        <option value=''>any</option>
        <option value='text/html'>text/html</option>
        <option value='application/pdf'>application/pdf</option>
        <option value='text/plain'>text/plain</option>
        <option value='application/json'>application/json</option>
        <option value='application/xml'>application/xml</option>
      </select>
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
    • MCP JSON-RPC: <span class='code'>POST {html.escape(base_path + "/mcp")}</span>
    • SDK: <a class='code' href='{html.escape(base_path + "/static/ccindex-mcp-sdk.js")}'>ccindex-mcp-sdk.js</a>
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
  const basePath = document.querySelector("meta[name='ccindex-base-path']")?.content || '';
  const {{ ccindexMcp }} = await import(`${{basePath}}/static/ccindex-mcp-sdk.js`);

  const braveMaxCount = {int(brave_max_count)};

  const initial = {json.dumps(initial)};
  const form = document.getElementById('searchForm');
  const statusEl = document.getElementById('status');
  const resultsEl = document.getElementById('results');
  const yearEl = document.getElementById('year');
  const statusFilterEl = document.getElementById('status_filter');
  const mimeFilterEl = document.getElementById('mime_filter');

  let lastRecords = [];
  let baseStatusHtml = "";
  let pageIndex = 0;
  let pageSize = 50;

  function esc(s) {{
    return String(s ?? '')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#39;');
  }}

  function renderTable(records, pageIndex, pageSize) {{
    const start = Math.max(0, (Number(pageIndex) || 0) * (Number(pageSize) || 50));
    const page = (records || []).slice(start, start + (Number(pageSize) || 50));
    const rows = page.map((r, idx) => {{
      const url = esc(r.url || '');
      const ts = esc(r.timestamp || '');
      const status = esc(r.status ?? '');
      const mime = esc(r.mime || '');
      const coll = esc(r.collection || '');
      const warc = esc(r.warc_filename || '');
      const off = esc(r.warc_offset ?? '');
      const len = esc(r.warc_length ?? '');
      const recHref = `${{basePath}}/record?warc_filename=${{encodeURIComponent(r.warc_filename||'')}}&warc_offset=${{encodeURIComponent(r.warc_offset||'')}}&warc_length=${{encodeURIComponent(r.warc_length||'')}}&parquet_root=${{encodeURIComponent(document.getElementById('parquet_root').value || '')}}`;
      return `
        <tr>
          <td class='small'>${{start + idx + 1}}</td>
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

  function renderPager(total, pageIndex, pageSize) {{
    const totalN = Number(total) || 0;
    const sizeN = Math.max(1, Number(pageSize) || 50);
    const pages = Math.max(1, Math.ceil(totalN / sizeN));
    const idx = Math.min(Math.max(0, Number(pageIndex) || 0), pages - 1);
    const start = totalN ? (idx * sizeN + 1) : 0;
    const end = Math.min(totalN, (idx + 1) * sizeN);
    const prevDisabled = idx <= 0 ? 'disabled' : '';
    const nextDisabled = idx >= pages - 1 ? 'disabled' : '';

    function pageButtonsHtml(pages, idx) {{
      const btns = [];
      const maxButtons = 9;
      const window = 2;

      function addBtn(p, label, active) {{
        const dis = active ? 'disabled' : '';
        const style = active ? " style='opacity:0.9; border-color: rgba(96,165,250,0.55); background: rgba(96,165,250,0.28);'" : '';
        btns.push("<button type='button' data-page='" + String(p) + "' " + dis + style + ">" + String(label) + "</button>");
      }}

      function addEllipsis() {{
        btns.push("<span class='small' style='padding:0 4px;'>…</span>");
      }}

      if (pages <= maxButtons) {{
        for (let p = 0; p < pages; p++) addBtn(p, String(p + 1), p === idx);
        return btns.join('');
      }}

      addBtn(0, '1', idx === 0);
      let startP = Math.max(1, idx - window);
      let endP = Math.min(pages - 2, idx + window);
      if (startP > 1) addEllipsis();
      for (let p = startP; p <= endP; p++) addBtn(p, String(p + 1), p === idx);
      if (endP < pages - 2) addEllipsis();
      addBtn(pages - 1, String(pages), idx === pages - 1);
      return btns.join('');
    }}

    return "<div style='padding: 12px; display:flex; gap: 10px; align-items:center; flex-wrap: wrap; border-bottom: 1px solid rgba(34, 48, 74, 0.7);'>"
      + "<span class='small'>" + (totalN ? ("Rows " + String(start) + "–" + String(end) + " of " + String(totalN)) : "No results") + "</span>"
      + "<span class='small'>•</span>"
      + "<button type='button' id='pagerPrev' " + prevDisabled + ">Prev</button>"
      + "<button type='button' id='pagerNext' " + nextDisabled + ">Next</button>"
      + "<div id='pagerNumWrap' style='display:flex; gap: 6px; align-items:center;'>" + pageButtonsHtml(pages, idx) + "</div>"
      + "<span class='small'>•</span>"
      + "<span class='small'>Per page</span>"
      + "<select id='pagerSize' style='width:auto; min-width: 90px;'>"
      + "<option value='25'>25</option><option value='50'>50</option><option value='100'>100</option><option value='200'>200</option>"
      + "</select>"
      + "</div>";
  }}

  function setSelectOptions(selectEl, options, selectedValue) {{
    const want = String(selectedValue ?? '');
    const existing = new Set(Array.from(selectEl.options).map(o => o.value));
    const toAdd = options.filter(o => !existing.has(String(o.value)));
    if (toAdd.length) {{
      for (const opt of toAdd) {{
        const o = document.createElement('option');
        o.value = String(opt.value);
        o.textContent = String(opt.label ?? opt.value);
        selectEl.appendChild(o);
      }}
    }}
    if (want) selectEl.value = want;
  }}

  async function populateYears() {{
    const initialYear = String(initial.year || '').trim();
    const current = (new Date()).getFullYear();
    const fallbackYears = [];
    for (let y = current; y >= 2010; y--) fallbackYears.push(String(y));

    yearEl.innerHTML = "<option value=''>any</option>";
    try {{
      const info = await ccindexMcp.callTool('cc_collinfo_list', {{}});
      const years = new Set();
      for (const it of (info.collections || [])) {{
        const id = String(it.id || '');
        const m = id.match(/CC-MAIN-([0-9]{{4}})-/);
        if (m && m[1]) years.add(m[1]);
      }}
      const sorted = Array.from(years).sort().reverse();
      const use = sorted.length ? sorted : fallbackYears;
      for (const y of use) {{
        const o = document.createElement('option');
        o.value = String(y);
        o.textContent = String(y);
        yearEl.appendChild(o);
      }}
    }} catch (e) {{
      for (const y of fallbackYears) {{
        const o = document.createElement('option');
        o.value = String(y);
        o.textContent = String(y);
        yearEl.appendChild(o);
      }}
    }}
    if (initialYear) yearEl.value = initialYear;
  }}

  function applyFilters(records) {{
    const statusWanted = String(statusFilterEl.value || '').trim();
    const mimeWanted = String(mimeFilterEl.value || '').trim();
    return (records || []).filter((r) => {{
      const s = String(r.status ?? '').trim();
      const m = String(r.mime ?? '').trim();
      if (statusWanted && s !== statusWanted) return false;
      if (mimeWanted && m !== mimeWanted) return false;
      return true;
    }});
  }}

  function updateFilterOptionsFromRecords(records) {{
    const statuses = new Set();
    const mimes = new Set();
    for (const r of (records || [])) {{
      const s = String(r.status ?? '').trim();
      const m = String(r.mime ?? '').trim();
      if (s) statuses.add(s);
      if (m) mimes.add(m);
    }}

    const statusOpts = Array.from(statuses)
      .sort((a, b) => Number(a) - Number(b))
      .map((v) => ({{ value: v, label: v }}));
    const mimeOpts = Array.from(mimes)
      .sort()
      .map((v) => ({{ value: v, label: v }}));

    const prevStatus = statusFilterEl.value;
    const prevMime = mimeFilterEl.value;
    setSelectOptions(statusFilterEl, statusOpts, prevStatus);
    setSelectOptions(mimeFilterEl, mimeOpts, prevMime);
  }}

  function renderFiltered() {{
    const filtered = applyFilters(lastRecords);

    const total = (filtered || []).length;
    const sizeN = Math.max(1, Number(pageSize) || 50);
    const pages = Math.max(1, Math.ceil(total / sizeN));
    pageIndex = Math.min(Math.max(0, Number(pageIndex) || 0), pages - 1);

    const pagerHtml = renderPager(total, pageIndex, pageSize);
    const tableHtml = renderTable(filtered, pageIndex, pageSize);
    resultsEl.innerHTML = pagerHtml + tableHtml;
    resultsEl.style.display = 'block';

    const showing = filtered.length;
    const totalAll = (lastRecords || []).length;
    const extra = (showing === totalAll)
      ? ''
      : (" <span class='small'>(showing " + String(showing) + "/" + String(totalAll) + " after filters)</span>");
    statusEl.innerHTML = baseStatusHtml + extra;

    const prevBtn = document.getElementById('pagerPrev');
    const nextBtn = document.getElementById('pagerNext');
    const sizeSel = document.getElementById('pagerSize');
    if (sizeSel) sizeSel.value = String(pageSize);

    for (const b of Array.from(resultsEl.querySelectorAll("button[data-page]"))) {{
      b.addEventListener('click', () => {{
        const p = parseInt(b.getAttribute('data-page') || '0', 10);
        pageIndex = isNaN(p) ? 0 : Math.max(0, p);
        renderFiltered();
      }});
    }}

    if (prevBtn) prevBtn.addEventListener('click', () => {{
      pageIndex = Math.max(0, Number(pageIndex) - 1);
      renderFiltered();
    }});
    if (nextBtn) nextBtn.addEventListener('click', () => {{
      pageIndex = Number(pageIndex) + 1;
      renderFiltered();
    }});
    if (sizeSel) sizeSel.addEventListener('change', () => {{
      pageSize = Math.max(1, parseInt(sizeSel.value || '50', 10));
      pageIndex = 0;
      renderFiltered();
    }});
  }}

  async function runSearch() {{
    const q = document.getElementById('q').value;
    const year = yearEl.value;
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
      const returned = (res.records || []).length;
      const cappedNote = (returned >= maxMatches)
        ? " <span class='small'>(hit limit; increase Max matches for more)</span>"
        : "";
      statusEl.innerHTML = `
        <span class='badge ok'>ok</span>
        meta_source=<span class='code'>${{esc(res.meta_source)}}</span>
        collections=<span class='code'>${{esc(res.collections_considered)}}</span>
        returned=<span class='code'>${{esc(returned)}}</span>
        limit=<span class='code'>${{esc(maxMatches)}}</span>
        elapsed_s=<span class='code'>${{esc(elapsed)}}</span>
        ${{cappedNote}}
      `;
      baseStatusHtml = statusEl.innerHTML;

      lastRecords = res.records || [];
      pageIndex = 0;
      updateFilterOptionsFromRecords(lastRecords);
      renderFiltered();
    }} catch (e) {{
      statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(e.message || e)}}</span>`;
      resultsEl.style.display = 'none';
    }}
  }}

  statusFilterEl.addEventListener('change', () => {{
    if ((lastRecords || []).length) {{ pageIndex = 0; renderFiltered(); }}
  }});
  mimeFilterEl.addEventListener('change', () => {{
    if ((lastRecords || []).length) {{ pageIndex = 0; renderFiltered(); }}
  }});

  form.addEventListener('submit', (ev) => {{
    ev.preventDefault();
    runSearch();
  }});

  await populateYears();

  // Preserve any previously-typed year by setting the select value.
  if (String(initial.year || '').trim()) yearEl.value = String(initial.year).trim();

  if ((initial.q || '').trim()) {{
    runSearch();
  }}
</script>
""",
            ]
        )
        return _layout("Common Crawl Search Engine", body, embed=bool(embed), base_path=base_path)

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

    @app.get("/settings", response_class=HTMLResponse)
    def settings_page(request: Request, embed: int = Query(default=0, ge=0, le=1)) -> str:
        base_path = _base_path(request)
        s = _load_settings()

        # Surface server-side cache defaults (these are env-controlled).
        range_cache_env = os.environ.get("CCINDEX_WARC_CACHE_DIR")
        full_cache_env = os.environ.get("CCINDEX_FULL_WARC_CACHE_DIR")
        range_cache_hint = "state/warc_cache" if (range_cache_env is None or range_cache_env.strip()) else "disabled"
        full_cache_hint = "state/warc_files" if (full_cache_env is None or full_cache_env.strip()) else "disabled"

        brave_env_set = bool((os.environ.get("BRAVE_SEARCH_API_KEY") or "").strip())
        brave_saved_set = bool((str(s.get("brave_search_api_key") or "").strip()))

        body = f"""
<div class='card'>
  <div class='small'>Dashboard Settings (persisted to <span class='code'>state/dashboard_settings.json</span>)</div>
  <hr>
  <div class='row'>
    <div class='field'>
      <label>default_cache_mode</label>
      <select id='default_cache_mode' style='padding: 10px 12px; border: 1px solid var(--border); background: rgba(15, 23, 42, 0.65); border-radius: 8px; color: var(--text);'>
        <option value='range'>range</option>
        <option value='auto'>auto</option>
        <option value='full'>full (download WARC)</option>
      </select>
    </div>
    <div class='field'>
      <label>default_max_bytes</label>
      <input id='default_max_bytes' type='number' min='1' step='1' value='{html.escape(str(s.get("default_max_bytes") or 2000000))}'>
    </div>
    <div class='field'>
      <label>default_max_preview_chars</label>
      <input id='default_max_preview_chars' type='number' min='0' step='1' value='{html.escape(str(s.get("default_max_preview_chars") or 80000))}'>
    </div>
  </div>

  <hr>
  <div class='small'>Brave Search API</div>
  <div class='row' style='margin-top:10px;'>
    <div class='field' style='min-width:520px; flex: 1;'>
      <label>brave_search_api_key</label>
      <input id='brave_search_api_key' type='password' value='' placeholder='(leave blank to keep current)'>
      <div class='small'>env set: <span class='code'>{str(brave_env_set).lower()}</span> • saved key present: <span class='code'>{str(brave_saved_set).lower()}</span></div>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='clearBraveKeyBtn' type='button'>Clear saved key</button>
    </div>
  </div>

  <div class='row' style='margin-top:8px;'>
    <div class='field' style='min-width:520px; flex: 1;'>
      <label>Brave search cache (on-disk)</label>
      <div class='small'>Caches Brave search results to avoid repeated API calls.</div>
      <div id='braveCacheStats' class='small' style='margin-top:6px;'></div>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='refreshBraveCacheStatsBtn' type='button'>Refresh Brave cache</button>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='clearBraveCacheBtn' type='button'>Clear Brave cache</button>
    </div>
  </div>

  <div class='row' style='margin-top:8px;'>
    <div class='field' style='min-width:520px; flex: 1;'>
      <label>Brave resolve cache (on-disk)</label>
      <div class='small'>Caches resolved CCIndex pointers for Brave result URLs (avoids re-resolving on repeat queries).</div>
      <div id='braveResolveCacheStats' class='small' style='margin-top:6px;'></div>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='refreshBraveResolveCacheStatsBtn' type='button'>Refresh resolve cache</button>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='clearBraveResolveCacheBtn' type='button'>Clear resolve cache</button>
    </div>
  </div>

  <hr>
  <div class='small'>Full-WARC cache (opt-in fallback)</div>
  <div class='row' style='margin-top:10px;'>
    <div class='field' style='min-width:520px; flex: 1;'>
      <label>full_warc_cache_dir (optional override)</label>
      <input id='full_warc_cache_dir' value='{html.escape(str(s.get("full_warc_cache_dir") or ""))}' placeholder='state/warc_files'>
      <div class='small'>Leave empty to use the default (<span class='code'>{html.escape(full_cache_hint)}</span>)</div>
    </div>
    <div class='field'>
      <label>full_warc_max_bytes</label>
      <input id='full_warc_max_bytes' type='number' min='0' step='1' value='{html.escape(str(s.get("full_warc_max_bytes") or 5000000000))}'>
      <div class='small'>0 disables the size guard</div>
    </div>
    <div class='field'>
      <label>full_warc_cache_max_total_bytes</label>
      <input id='full_warc_cache_max_total_bytes' type='number' min='0' step='1' value='{html.escape(str(s.get("full_warc_cache_max_total_bytes") or 0))}'>
      <div class='small'>0 disables pruning (delete oldest first)</div>
    </div>
  </div>

  <hr>
  <div class='small'>Range cache (always used for Range mode)</div>
  <div class='small'>Server-side default: <span class='code'>{html.escape(range_cache_hint)}</span> (env: <span class='code'>CCINDEX_WARC_CACHE_DIR</span>)</div>

  <div class='row' style='margin-top:10px;'>
    <div class='field'>
      <label>range_cache_max_bytes</label>
      <input id='range_cache_max_bytes' type='number' min='0' step='1' value='{html.escape(str(s.get("range_cache_max_bytes") or 2000000000))}'>
    </div>
    <div class='field'>
      <label>range_cache_max_item_bytes</label>
      <input id='range_cache_max_item_bytes' type='number' min='0' step='1' value='{html.escape(str(s.get("range_cache_max_item_bytes") or 25000000))}'>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='refreshCacheStatsBtn' type='button'>Refresh cache stats</button>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='clearRangeCacheBtn' type='button'>Clear range cache</button>
    </div>
    <div class='field'>
      <label>&nbsp;</label>
      <button id='clearFullCacheBtn' type='button'>Clear full-WARC cache</button>
    </div>
  </div>
  <div id='cacheStats' class='small' style='margin-top:8px;'></div>

  <div style='margin-top:14px; display:flex; gap:10px; align-items:center;'>
    <button id='saveBtn' type='button'>Save</button>
    <span id='status' class='small'></span>
  </div>
</div>

<div class='card' style='margin-top: 14px;'>
  <div class='small'>Orchestrator Settings (persisted to <span class='code'>state/orchestrator_settings.json</span>)</div>
  <hr>
  <div class='row'>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>ccindex_root</label>
      <input id='orch_ccindex_root' placeholder='/storage/ccindex'>
    </div>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>parquet_root</label>
      <input id='orch_parquet_root' placeholder='/storage/ccindex_parquet'>
    </div>
  </div>
  <div class='row' style='margin-top: 8px;'>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>duckdb_collection_root</label>
      <input id='orch_duckdb_collection_root' placeholder='/storage/ccindex_duckdb/cc_pointers_by_collection'>
    </div>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>duckdb_year_root</label>
      <input id='orch_duckdb_year_root' placeholder='/storage/ccindex_duckdb/cc_pointers_by_year'>
    </div>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>duckdb_master_root</label>
      <input id='orch_duckdb_master_root' placeholder='/storage/ccindex_duckdb/cc_pointers_master'>
    </div>
  </div>

  <div class='row' style='margin-top: 8px;'>
    <div class='field'>
      <label>max_workers</label>
      <input id='orch_max_workers' type='number' min='1' step='1' value='8'>
    </div>
    <div class='field'>
      <label>heartbeat_seconds</label>
      <input id='orch_heartbeat_seconds' type='number' min='1' step='1' value='30'>
    </div>
    <div class='field' style='min-width: 320px; flex: 1;'>
      <label>collections_filter</label>
      <input id='orch_collections_filter' placeholder='2024 or CC-MAIN-2024-10 or all'>
    </div>
  </div>

  <div class='row' style='margin-top: 8px;'>
    <div class='field'>
      <label>cleanup_extraneous</label>
      <select id='orch_cleanup_extraneous' style='padding: 10px 12px; border: 1px solid var(--border); background: rgba(15, 23, 42, 0.65); border-radius: 8px; color: var(--text);'>
        <option value='1'>enabled</option>
        <option value='0'>disabled</option>
      </select>
    </div>
    <div class='field'>
      <label>cleanup_source_archives</label>
      <select id='orch_cleanup_source_archives' style='padding: 10px 12px; border: 1px solid var(--border); background: rgba(15, 23, 42, 0.65); border-radius: 8px; color: var(--text);'>
        <option value='1'>enabled</option>
        <option value='0'>disabled</option>
      </select>
    </div>
    <div class='field'>
      <label>cleanup_dry_run</label>
      <select id='orch_cleanup_dry_run' style='padding: 10px 12px; border: 1px solid var(--border); background: rgba(15, 23, 42, 0.65); border-radius: 8px; color: var(--text);'>
        <option value='0'>false</option>
        <option value='1'>true</option>
      </select>
    </div>
    <div class='field'>
      <label>force_reindex</label>
      <select id='orch_force_reindex' style='padding: 10px 12px; border: 1px solid var(--border); background: rgba(15, 23, 42, 0.65); border-radius: 8px; color: var(--text);'>
        <option value='0'>false</option>
        <option value='1'>true</option>
      </select>
    </div>
  </div>

  <div class='row' style='margin-top: 8px;'>
    <div class='field'>
      <label>sort_workers</label>
      <input id='orch_sort_workers' type='number' min='1' step='1' placeholder='(blank = max_workers)'>
    </div>
    <div class='field'>
      <label>sort_memory_per_worker_gb</label>
      <input id='orch_sort_mem' type='number' min='0.5' step='0.5' value='4.0'>
    </div>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>sort_temp_dir (optional)</label>
      <input id='orch_sort_temp_dir' placeholder='/mnt/ssd/tmp'>
    </div>
  </div>

  <div style='margin-top:14px; display:flex; gap:10px; align-items:center;'>
    <button id='saveOrchBtn' type='button'>Save orchestrator settings</button>
    <span id='orchStatus' class='small'></span>
  </div>
</div>

<script type='module'>
  const basePath = document.querySelector("meta[name='ccindex-base-path']")?.content || '';
  const {{ ccindexMcp }} = await import(`${{basePath}}/static/ccindex-mcp-sdk.js`);
  const esc = (s) => String(s ?? '');
  const modeEl = document.getElementById('default_cache_mode');
  const maxBytesEl = document.getElementById('default_max_bytes');
  const maxPrevEl = document.getElementById('default_max_preview_chars');
  const fullDirEl = document.getElementById('full_warc_cache_dir');
  const fullMaxEl = document.getElementById('full_warc_max_bytes');
  const fullTotalEl = document.getElementById('full_warc_cache_max_total_bytes');
  const rangeMaxEl = document.getElementById('range_cache_max_bytes');
  const rangeItemMaxEl = document.getElementById('range_cache_max_item_bytes');
  const braveKeyEl = document.getElementById('brave_search_api_key');
  const statusEl = document.getElementById('status');
  const cacheStatsEl = document.getElementById('cacheStats');
  const braveCacheStatsEl = document.getElementById('braveCacheStats');
  const braveResolveCacheStatsEl = document.getElementById('braveResolveCacheStats');

  const orchCcindexRootEl = document.getElementById('orch_ccindex_root');
  const orchParquetRootEl = document.getElementById('orch_parquet_root');
  const orchDuckdbCollectionEl = document.getElementById('orch_duckdb_collection_root');
  const orchDuckdbYearEl = document.getElementById('orch_duckdb_year_root');
  const orchDuckdbMasterEl = document.getElementById('orch_duckdb_master_root');
  const orchMaxWorkersEl = document.getElementById('orch_max_workers');
  const orchHeartbeatEl = document.getElementById('orch_heartbeat_seconds');
  const orchFilterEl = document.getElementById('orch_collections_filter');
  const orchCleanupExtraneousEl = document.getElementById('orch_cleanup_extraneous');
  const orchCleanupSourceEl = document.getElementById('orch_cleanup_source_archives');
  const orchCleanupDryEl = document.getElementById('orch_cleanup_dry_run');
  const orchForceReindexEl = document.getElementById('orch_force_reindex');
  const orchSortWorkersEl = document.getElementById('orch_sort_workers');
  const orchSortMemEl = document.getElementById('orch_sort_mem');
  const orchSortTempEl = document.getElementById('orch_sort_temp_dir');
  const orchStatusEl = document.getElementById('orchStatus');

  modeEl.value = {json.dumps(str(s.get("default_cache_mode") or "range"))};

  async function loadOrchestratorSettings() {{
    try {{
      const s = await ccindexMcp.callTool('orchestrator_settings_get', {{}});
      orchCcindexRootEl.value = esc(s.ccindex_root || '');
      orchParquetRootEl.value = esc(s.parquet_root || '');
      orchDuckdbCollectionEl.value = esc(s.duckdb_collection_root || '');
      orchDuckdbYearEl.value = esc(s.duckdb_year_root || '');
      orchDuckdbMasterEl.value = esc(s.duckdb_master_root || '');
      orchMaxWorkersEl.value = esc(s.max_workers || '8');
      orchHeartbeatEl.value = esc(s.heartbeat_seconds || '30');
      orchFilterEl.value = esc(s.collections_filter || '');
      orchCleanupExtraneousEl.value = s.cleanup_extraneous ? '1' : '0';
      orchCleanupSourceEl.value = s.cleanup_source_archives ? '1' : '0';
      orchCleanupDryEl.value = s.cleanup_dry_run ? '1' : '0';
      orchForceReindexEl.value = s.force_reindex ? '1' : '0';
      orchSortWorkersEl.value = s.sort_workers == null ? '' : esc(s.sort_workers);
      orchSortMemEl.value = esc(s.sort_memory_per_worker_gb || '4.0');
      orchSortTempEl.value = esc(s.sort_temp_dir || '');
      orchStatusEl.textContent = '';
    }} catch (e) {{
      orchStatusEl.textContent = 'error loading: ' + esc(e.message || e);
    }}
  }}

  document.getElementById('saveOrchBtn').addEventListener('click', async () => {{
    orchStatusEl.textContent = 'saving…';
    try {{
      const payload = {{
        ccindex_root: (String(orchCcindexRootEl.value || '').trim() || null),
        parquet_root: (String(orchParquetRootEl.value || '').trim() || null),
        duckdb_collection_root: (String(orchDuckdbCollectionEl.value || '').trim() || null),
        duckdb_year_root: (String(orchDuckdbYearEl.value || '').trim() || null),
        duckdb_master_root: (String(orchDuckdbMasterEl.value || '').trim() || null),
        max_workers: parseInt(String(orchMaxWorkersEl.value || '8'), 10),
        heartbeat_seconds: parseInt(String(orchHeartbeatEl.value || '30'), 10),
        collections_filter: (String(orchFilterEl.value || '').trim() || null),
        cleanup_extraneous: String(orchCleanupExtraneousEl.value || '1') === '1',
        cleanup_source_archives: String(orchCleanupSourceEl.value || '1') === '1',
        cleanup_dry_run: String(orchCleanupDryEl.value || '0') === '1',
        force_reindex: String(orchForceReindexEl.value || '0') === '1',
        sort_workers: (String(orchSortWorkersEl.value || '').trim() ? parseInt(String(orchSortWorkersEl.value), 10) : null),
        sort_memory_per_worker_gb: parseFloat(String(orchSortMemEl.value || '4.0')),
        sort_temp_dir: (String(orchSortTempEl.value || '').trim() || null),
      }};

      const res = await ccindexMcp.callTool('orchestrator_settings_set', {{ settings: payload }});
      orchStatusEl.textContent = 'saved';
      // Round-trip updated settings.
      orchCcindexRootEl.value = esc(res.ccindex_root || '');
    }} catch (e) {{
      orchStatusEl.textContent = 'error: ' + esc(e.message || e);
    }}
  }});

  document.getElementById('saveBtn').addEventListener('click', async () => {{
    statusEl.textContent = 'saving…';
    try {{
      const payload = {{
        default_cache_mode: String(modeEl.value || 'range'),
        default_max_bytes: parseInt(maxBytesEl.value || '2000000', 10),
        default_max_preview_chars: parseInt(maxPrevEl.value || '80000', 10),
        range_cache_max_bytes: parseInt(rangeMaxEl.value || '2000000000', 10),
        range_cache_max_item_bytes: parseInt(rangeItemMaxEl.value || '25000000', 10),
        full_warc_cache_dir: (String(fullDirEl.value || '').trim() || null),
        full_warc_max_bytes: parseInt(fullMaxEl.value || '5000000000', 10),
        full_warc_cache_max_total_bytes: parseInt(fullTotalEl.value || '0', 10),
        // If blank, server keeps current saved key.
        brave_search_api_key: (String(braveKeyEl.value || '').trim() || null),
      }};

      const resp = await fetch(`${{basePath}}/settings`, {{
        method: 'POST',
        headers: {{ 'content-type': 'application/json' }},
        body: JSON.stringify(payload),
      }});
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'save failed');
      statusEl.textContent = 'saved';
    }} catch (e) {{
      statusEl.textContent = 'error: ' + esc(e.message || e);
    }}
  }});

  async function refreshCacheStats() {{
    cacheStatsEl.textContent = 'loading cache stats…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/cache_stats`);
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'stats failed');
      cacheStatsEl.textContent = `range_cache: ${{res.range.items}} items, ${{res.range.bytes}} bytes • full_warc_cache: ${{res.full.items}} items, ${{res.full.bytes}} bytes`;
    }} catch (e) {{
      cacheStatsEl.textContent = 'cache stats error: ' + esc(e.message || e);
    }}
  }}

  async function refreshBraveCacheStats() {{
    braveCacheStatsEl.textContent = 'loading Brave cache…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/brave_cache_stats`);
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'stats failed');
      const path = esc(res.path || '');
      const disabled = !!res.disabled;
      const ttl = parseInt(res.ttl_s || '0', 10);
      const ttlText = (ttl > 0) ? `${{ttl}}s` : 'disabled';
      braveCacheStatsEl.textContent = `path: ${{path}} • ${{res.entries}} entries, ${{res.bytes}} bytes • ttl: ${{ttlText}} • disabled: ${{disabled}}`;
    }} catch (e) {{
      braveCacheStatsEl.textContent = 'Brave cache stats error: ' + esc(e.message || e);
    }}
  }}

  async function refreshBraveResolveCacheStats() {{
    braveResolveCacheStatsEl.textContent = 'loading resolve cache…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/brave_resolve_cache_stats`);
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'stats failed');
      const path = esc(res.path || '');
      const disabled = !!res.disabled;
      const ttl = parseInt(res.ttl_s || '0', 10);
      const ttlText = (ttl > 0) ? `${{ttl}}s` : 'disabled';
      braveResolveCacheStatsEl.textContent = `path: ${{path}} • ${{res.entries}} entries, ${{res.bytes}} bytes • ttl: ${{ttlText}} • disabled: ${{disabled}}`;
    }} catch (e) {{
      braveResolveCacheStatsEl.textContent = 'Resolve cache stats error: ' + esc(e.message || e);
    }}
  }}

  async function clearBraveCache() {{
    braveCacheStatsEl.textContent = 'clearing Brave cache…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/clear_brave_cache`, {{ method: 'POST' }});
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'clear failed');
      braveCacheStatsEl.textContent = `cleared Brave cache: deleted=${{res.deleted}} freed_bytes=${{res.freed_bytes}}`;
      await refreshBraveCacheStats();
    }} catch (e) {{
      braveCacheStatsEl.textContent = 'Brave cache clear error: ' + esc(e.message || e);
    }}
  }}

  async function clearBraveResolveCache() {{
    braveResolveCacheStatsEl.textContent = 'clearing resolve cache…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/clear_brave_resolve_cache`, {{ method: 'POST' }});
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'clear failed');
      braveResolveCacheStatsEl.textContent = `cleared resolve cache: deleted=${{res.deleted}} freed_bytes=${{res.freed_bytes}}`;
      await refreshBraveResolveCacheStats();
    }} catch (e) {{
      braveResolveCacheStatsEl.textContent = 'Resolve cache clear error: ' + esc(e.message || e);
    }}
  }}

  async function clearCache(which) {{
    cacheStatsEl.textContent = 'clearing…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/clear_cache`, {{
        method: 'POST',
        headers: {{ 'content-type': 'application/json' }},
        body: JSON.stringify({{ which }}),
      }});
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'clear failed');
      cacheStatsEl.textContent = `cleared ${{which}}: deleted=${{res.deleted_items}} freed_bytes=${{res.freed_bytes}}`;
      await refreshCacheStats();
    }} catch (e) {{
      cacheStatsEl.textContent = 'clear error: ' + esc(e.message || e);
    }}
  }}

  document.getElementById('refreshCacheStatsBtn').addEventListener('click', () => refreshCacheStats());
  document.getElementById('clearRangeCacheBtn').addEventListener('click', () => clearCache('range'));
  document.getElementById('clearFullCacheBtn').addEventListener('click', () => clearCache('full'));
  document.getElementById('refreshBraveCacheStatsBtn').addEventListener('click', () => refreshBraveCacheStats());
  document.getElementById('clearBraveCacheBtn').addEventListener('click', () => clearBraveCache());
  document.getElementById('refreshBraveResolveCacheStatsBtn').addEventListener('click', () => refreshBraveResolveCacheStats());
  document.getElementById('clearBraveResolveCacheBtn').addEventListener('click', () => clearBraveResolveCache());
  document.getElementById('clearBraveKeyBtn').addEventListener('click', async () => {{
    statusEl.textContent = 'clearing brave key…';
    try {{
      const resp = await fetch(`${{basePath}}/settings/clear_brave_key`, {{ method: 'POST' }});
      const res = await resp.json();
      if (!res.ok) throw new Error(res.error || 'clear failed');
      braveKeyEl.value = '';
      statusEl.textContent = 'cleared';
    }} catch (e) {{
      statusEl.textContent = 'error: ' + esc(e.message || e);
    }}
  }});

  refreshCacheStats();
  refreshBraveCacheStats();
  refreshBraveResolveCacheStats();
  loadOrchestratorSettings();
</script>
"""
        return _layout("Common Crawl Search Engine • Settings", body, embed=bool(embed), base_path=base_path)

    @app.get("/index", response_class=HTMLResponse)
    def index_page(request: Request, embed: int = Query(default=0, ge=0, le=1)) -> str:
        base_path = _base_path(request)

        body = """
<div class='card'>
  <div class='small'>CCIndex Orchestrator Console (all collections)</div>
  <div class='small' style='margin-top: 6px;'>
    Collections are sourced from cached <span class='code'>collinfo.json</span>. Refresh pulls from
    <span class='code'>https://index.commoncrawl.org/collinfo.json</span>.
  </div>
</div>

<div class='card' style='margin-top: 14px;'>
  <div class='row'>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>Filter</label>
      <input id='filterText' placeholder='e.g. 2024 or CC-MAIN-2024-10'>
      <div class='small'>Client-side filter (does not change orchestrator settings).</div>
    </div>
    <div class='field'>
      <label>Workers</label>
      <input id='workers' type='number' min='1' step='1' value='8'>
      <div class='small'>Overrides saved setting for jobs started here.</div>
    </div>
  </div>

  <div class='row' style='margin-top: 8px;'>
    <button id='btnCollinfoList' type='button'>load collections</button>
    <button id='btnCollinfoUpdate' type='button'>refresh collinfo</button>
    <button id='btnBulkStatus' type='button'>bulk status</button>
    <button id='btnBulkDelete' type='button'>bulk delete indexes</button>
    <select id='jobMode'>
      <option value='pipeline'>update (pipeline)</option>
      <option value='pipeline_force'>rebuild (force reindex)</option>
      <option value='download_only'>download-only</option>
      <option value='cleanup_only'>cleanup-only</option>
      <option value='build_meta_indexes'>build meta-indexes</option>
    </select>
    <button id='btnStartJobs' type='button'>start jobs for selected</button>
  </div>

  <div class='small' style='margin-top: 10px;'>
    Tip: select many collections, then start jobs. Each selection starts its own background subprocess.
  </div>

  <div id='collectionsInfo' class='small' style='margin-top: 10px;'></div>
  <div style='margin-top: 10px; overflow: auto; max-height: 360px; border: 1px solid #eee; border-radius: 8px;'>
    <table class='table' style='width: 100%; border-collapse: collapse;'>
      <thead>
        <tr>
          <th style='text-align:left; padding: 10px; width: 36px;'><input id='selAll' type='checkbox'></th>
          <th style='text-align:left; padding: 10px;'>Collection</th>
          <th style='text-align:left; padding: 10px; width: 110px;'>Status</th>
          <th style='text-align:left; padding: 10px; width: 140px;'>Size on disk</th>
          <th style='text-align:left; padding: 10px;'>Name</th>
          <th style='text-align:left; padding: 10px;'>Time Range</th>
        </tr>
      </thead>
      <tbody id='collectionsTbody'></tbody>
    </table>
  </div>
</div>

<div class='card' style='margin-top: 14px;'>
  <div class='row'>
    <div class='field' style='min-width: 420px; flex: 1;'>
      <label>Job (pid / log)</label>
      <input id='jobPid' placeholder='pid'>
      <input id='jobLog' placeholder='log_path' style='margin-top: 6px;'>
    </div>
    <div class='field'>
      <label>Log lines</label>
      <input id='jobLines' type='number' min='10' step='10' value='200'>
      <div class='row' style='margin-top: 8px;'>
        <button id='btnJobStatus' type='button'>status</button>
        <button id='btnTail' type='button'>tail</button>
        <button id='btnStop' type='button'>stop</button>
      </div>
    </div>
  </div>

  <div class='row' style='margin-top: 10px;'>
    <button id='btnJobsList' type='button'>load recent jobs</button>
    <div class='small' style='margin-left: 10px;'>Stored in <span class='code'>state/orchestrator_jobs.jsonl</span></div>
  </div>

  <div id='jobsTbodyWrap' style='margin-top: 10px; overflow:auto; max-height: 220px; border: 1px solid #eee; border-radius: 8px;'>
    <table class='table' style='width: 100%; border-collapse: collapse;'>
      <thead>
        <tr>
          <th style='text-align:left; padding: 10px;'>When</th>
          <th style='text-align:left; padding: 10px;'>Label</th>
          <th style='text-align:left; padding: 10px;'>PID</th>
          <th style='text-align:left; padding: 10px;'>Log</th>
          <th style='text-align:left; padding: 10px; width: 90px;'>Open</th>
        </tr>
      </thead>
      <tbody id='jobsTbody'></tbody>
    </table>
  </div>

  <div id='status' class='code' style='margin-top: 12px; white-space: pre-wrap; max-height: 320px; overflow: auto;'></div>

  <div class='small' style='margin-top: 12px;'>Persisted orchestrator settings live in <span class='code'>state/orchestrator_settings.json</span> (editable in Settings tab).</div>
</div>

<script type='module'>
  const basePath = document.querySelector("meta[name='ccindex-base-path']")?.content || '';
  const { ccindexMcp } = await import(`${basePath}/static/ccindex-mcp-sdk.js`);

  const $ = (id) => document.getElementById(id);
  const statusEl = $('status');
  const collectionsInfoEl = $('collectionsInfo');
  const collectionsTbody = $('collectionsTbody');
  const jobsTbody = $('jobsTbody');

  let allCollections = [];
  let selectedIds = new Set();
  let lastBulkStatus = null;
  let statusRefreshSeq = 0;
  let statusLoading = false;

  function setStatus(obj) {
    if (typeof obj === 'string') { statusEl.textContent = obj; return; }
    statusEl.textContent = JSON.stringify(obj, null, 2);
  }

  function esc(s) {
    return String(s ?? '')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;');
  }

  async function loadDefaults() {
    try {
      const s = await ccindexMcp.callTool('orchestrator_settings_get', {});
      if (s && s.max_workers) $('workers').value = String(s.max_workers);
    } catch (e) {
      // ignore
    }
  }

  function selectedCollections() {
    return Array.from(selectedIds);
  }

  function _classifyCollectionStatus(st) {
    if (!st || typeof st !== 'object') return 'unknown';
    if (st.ok === false || st.error) return 'error';
    if (st.fully_complete === true || st.complete === true) return 'complete';
    return 'partial';
  }

  function _statusLabel(cls) {
    if (cls === 'complete') return 'complete';
    if (cls === 'partial') return 'partial';
    if (cls === 'error') return 'error';
    return '—';
  }

  function fmtBytes(n) {
    const v = Number(n || 0);
    if (!Number.isFinite(v) || v <= 0) return '—';
    const units = ['B','KB','MB','GB','TB','PB'];
    let x = v;
    let i = 0;
    while (x >= 1024 && i < units.length - 1) { x /= 1024; i += 1; }
    const digits = (i <= 1) ? 0 : (x < 10 ? 2 : (x < 100 ? 1 : 0));
    return `${x.toFixed(digits)} ${units[i]}`;
  }

  function updateCollectionsInfo({ shownCount } = {}) {
    const shown = Number.isFinite(shownCount) ? shownCount : filteredCollections().length;
    const total = allCollections.length;
    const selected = selectedCollections();

    let statusSummary = '';
    if (lastBulkStatus && lastBulkStatus.collections && typeof lastBulkStatus.collections === 'object') {
      let complete = 0;
      let partial = 0;
      let error = 0;
      for (const id of selected) {
        const st = lastBulkStatus.collections[id];
        const cls = _classifyCollectionStatus(st);
        if (cls === 'complete') complete += 1;
        else if (cls === 'partial') partial += 1;
        else if (cls === 'error') error += 1;
      }
      statusSummary = ` Status: complete=${complete} partial=${partial} error=${error}.`;
    }

    collectionsInfoEl.textContent = `Showing ${shown} / ${total} collections. Selected: ${selected.length}.${statusSummary}`;
  }

  function filteredCollections() {
    const ft = String($('filterText').value || '').trim().toLowerCase();
    if (!ft) return allCollections;
    return allCollections.filter((c) => {
      const id = String(c.id || c.collection || '').toLowerCase();
      const name = String(c.name || '').toLowerCase();
      return id.includes(ft) || name.includes(ft);
    });
  }

  function renderCollections() {
    const cols = filteredCollections();
    collectionsTbody.innerHTML = cols.map((c) => {
      const id = String(c.id || c.collection || '');
      const name = String(c.name || '');
      const tr = String(c['time_range'] || c['timeRange'] || '');
      const from = c['from'] || c['crawl-start'] || c['crawlStart'] || '';
      const to = c['to'] || c['crawl-end'] || c['crawlEnd'] || '';
      const timeRange = tr || ((from || to) ? `${from || ''} ${to ? '→ ' + to : ''}`.trim() : '');
      const checked = selectedIds.has(id) ? "checked" : "";

      const st = (lastBulkStatus && lastBulkStatus.collections && typeof lastBulkStatus.collections === 'object')
        ? lastBulkStatus.collections[id]
        : null;
      const isLoading = !!statusLoading && (!st || typeof st !== 'object');
      const cls = _classifyCollectionStatus(st);
      const label = _statusLabel(cls);
      const color = (cls === 'complete') ? '#067d68' : ((cls === 'error') ? '#b42318' : ((cls === 'partial') ? '#b54708' : '#667085'));
      const sizeBytes = (st && typeof st === 'object') ? (st.size_on_disk_bytes ?? st.size_bytes ?? null) : null;

      const statusCellHtml = isLoading
        ? `<span class='spinner' title='loading'></span>`
        : esc(label);
      const sizeCellHtml = isLoading
        ? `<span class='spinner' title='loading'></span>`
        : esc(fmtBytes(sizeBytes));
      return `
        <tr>
          <td style='padding: 10px;'><input type='checkbox' data-coll='${esc(id)}' ${checked}></td>
          <td style='padding: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;'>${esc(id)}</td>
          <td style='padding: 10px; color: ${color}; font-weight: 600;'>${statusCellHtml}</td>
          <td style='padding: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;'>${sizeCellHtml}</td>
          <td style='padding: 10px;'>${esc(name)}</td>
          <td style='padding: 10px;'>${esc(timeRange)}</td>
        </tr>`;
    }).join('');

    for (const el of document.querySelectorAll('input[data-coll][type=checkbox]')) {
      el.addEventListener('change', () => {
        const id = String(el.getAttribute('data-coll') || '');
        if (!id) return;
        if (el.checked) selectedIds.add(id); else selectedIds.delete(id);
        updateCollectionsInfo({ shownCount: cols.length });
      });
    }

    updateCollectionsInfo({ shownCount: cols.length });
  }

  async function loadCollections({ refresh } = {}) {
    try {
      if (refresh) {
        setStatus('Refreshing collinfo…');
        await ccindexMcp.callTool('cc_collinfo_update', {});
      }
      setStatus('Loading collection catalog…');
      const res = await ccindexMcp.callTool('cc_collinfo_list', { prefer_cache: true });
      allCollections = Array.isArray(res.collections) ? res.collections : [];
      renderCollections();
      setStatus({ ok: true, loaded: allCollections.length, source_path: res.source_path });

      refreshAllStatusesInBackground().catch((e) => {
        setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
      });
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  }

  async function doBulkStatus() {
    const cols = selectedCollections();
    if (!cols.length) return setStatus('select at least one collection');
    setStatus('Validating collections…');
    try {
      const res = await ccindexMcp.callTool('orchestrator_collections_status', { collections: cols, parallelism: 8 });
      const existing = (lastBulkStatus && lastBulkStatus.collections && typeof lastBulkStatus.collections === 'object')
        ? lastBulkStatus.collections
        : {};
      const merged = { ...existing };
      if (res && res.collections && typeof res.collections === 'object') {
        for (const [k, v] of Object.entries(res.collections)) merged[String(k)] = v;
      }
      lastBulkStatus = { ok: true, collections: merged, summary: res.summary || null };
      renderCollections();
      setStatus(res);
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  }

  async function refreshAllStatusesInBackground() {
    const seq = ++statusRefreshSeq;
    statusLoading = true;
    const ids = allCollections
      .map((c) => String(c.id || c.collection || ''))
      .filter((s) => s);
    if (!ids.length) return;

    const batchSize = 25;
    const parallelism = 8;
    const existing = (lastBulkStatus && lastBulkStatus.collections && typeof lastBulkStatus.collections === 'object')
      ? lastBulkStatus.collections
      : {};
    const merged = { ...existing };

    try {
      renderCollections();

      for (let i = 0; i < ids.length; i += batchSize) {
        if (seq !== statusRefreshSeq) return; // superseded
        const batch = ids.slice(i, i + batchSize);
        setStatus(`Loading status ${i + 1}-${Math.min(i + batch.length, ids.length)} / ${ids.length}…`);
        const res = await ccindexMcp.callTool('orchestrator_collections_status', { collections: batch, parallelism });
        if (res && res.collections && typeof res.collections === 'object') {
          for (const [k, v] of Object.entries(res.collections)) merged[String(k)] = v;
          lastBulkStatus = { ok: true, collections: merged, summary: res.summary || null };
          renderCollections();
        }
      }

      setStatus({ ok: true, status_loaded: ids.length, summary: lastBulkStatus ? lastBulkStatus.summary : null });
    } finally {
      if (seq === statusRefreshSeq) {
        statusLoading = false;
        renderCollections();
      }
    }
  }

  async function doBulkDelete() {
    const cols = selectedCollections();
    if (!cols.length) return setStatus('select at least one collection');
    if (!confirm(`Delete DuckDB index artifacts for ${cols.length} collections?`)) return;
    setStatus('Deleting index artifacts…');
    try {
      const res = await ccindexMcp.callTool('orchestrator_delete_collection_indexes', { collections: cols });
      setStatus(res);
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  }

  async function startJobsForSelected() {
    const cols = selectedCollections();
    if (!cols.length) return setStatus('select at least one collection');

    const modeSel = String($('jobMode').value || 'pipeline');
    let mode = modeSel;
    let extra = {};
    if (modeSel === 'pipeline_force') {
      mode = 'pipeline';
      extra = { force_reindex: true };
    } else if (modeSel === 'pipeline') {
      extra = { force_reindex: false };
    }

    const workers = parseInt(String($('workers').value || '8'), 10);
    if (!confirm(`Start ${modeSel} jobs for ${cols.length} collections?`)) return;

    const started = [];
    for (const c of cols) {
      setStatus(`Planning ${modeSel} for ${c}…`);
      const planned = await ccindexMcp.callTool('orchestrator_job_plan', {
        mode,
        filter: c,
        workers: Number.isFinite(workers) ? workers : null,
        ...extra,
      });
      const job = await ccindexMcp.callTool('orchestrator_job_start', { planned, label: `cc_pipeline_${modeSel}_${c}` });
      started.push({ collection: c, job });
      $('jobPid').value = String(job.pid || '');
      $('jobLog').value = String(job.log_path || '');
    }
    setStatus({ ok: true, started_count: started.length, started });
  }

  async function loadJobs() {
    try {
      const res = await ccindexMcp.callTool('orchestrator_jobs_list', { limit: 50 });
      const jobs = Array.isArray(res.jobs) ? res.jobs : [];
      jobsTbody.innerHTML = jobs.map((j) => {
        const when = String(j.started_at || '');
        const label = String(j.label || '');
        const pid = String(j.pid || '');
        const lp = String(j.log_path || '');
        return `
          <tr>
            <td style='padding: 10px;'>${esc(when)}</td>
            <td style='padding: 10px;'>${esc(label)}</td>
            <td style='padding: 10px;'>${esc(pid)}</td>
            <td style='padding: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;'>${esc(lp)}</td>
            <td style='padding: 10px;'><button type='button' data-open-job='1' data-pid='${esc(pid)}' data-log='${esc(lp)}'>open</button></td>
          </tr>`;
      }).join('');

      for (const btn of document.querySelectorAll('button[data-open-job]')) {
        btn.addEventListener('click', () => {
          $('jobPid').value = String(btn.getAttribute('data-pid') || '');
          $('jobLog').value = String(btn.getAttribute('data-log') || '');
        });
      }
      setStatus({ ok: true, jobs_count: jobs.length });
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  }

  $('btnCollinfoList').addEventListener('click', () => loadCollections({ refresh: false }));
  $('btnCollinfoUpdate').addEventListener('click', () => loadCollections({ refresh: true }));
  $('btnBulkStatus').addEventListener('click', () => doBulkStatus());
  $('btnBulkDelete').addEventListener('click', () => doBulkDelete());
  $('btnStartJobs').addEventListener('click', () => startJobsForSelected());
  $('btnJobsList').addEventListener('click', () => loadJobs());

  $('filterText').addEventListener('input', () => renderCollections());
  $('selAll').addEventListener('change', (e) => {
    const checked = !!(e && e.target && e.target.checked);
    for (const c of filteredCollections()) {
      const id = String(c.id || c.collection || '');
      if (!id) continue;
      if (checked) selectedIds.add(id); else selectedIds.delete(id);
    }
    renderCollections();
  });

  $('btnTail').addEventListener('click', async () => {
    const log_path = String($('jobLog').value || '').trim();
    const lines = parseInt(String($('jobLines').value || '200'), 10);
    if (!log_path) return setStatus('log_path is required');
    setStatus('Fetching log tail…');
    try {
      const res = await ccindexMcp.callTool('orchestrator_job_tail', { log_path, lines: Number.isFinite(lines) ? lines : 200 });
      setStatus(res.tail || '');
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  });

  $('btnJobStatus').addEventListener('click', async () => {
    const pidRaw = String($('jobPid').value || '').trim();
    const pid = pidRaw ? parseInt(pidRaw, 10) : null;
    const log_path = String($('jobLog').value || '').trim() || null;
    const lines = parseInt(String($('jobLines').value || '200'), 10);
    if (!pid && !log_path) return setStatus('pid or log_path is required');
    setStatus('Fetching job status…');
    try {
      const res = await ccindexMcp.callTool('orchestrator_job_status', { pid, log_path, lines: Number.isFinite(lines) ? lines : 200 });
      setStatus(res);
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  });

  $('btnStop').addEventListener('click', async () => {
    const pid = parseInt(String($('jobPid').value || '0'), 10);
    if (!pid) return setStatus('pid is required');
    setStatus('Stopping job…');
    try {
      const res = await ccindexMcp.callTool('orchestrator_job_stop', { pid, sig: 'TERM' });
      setStatus(res);
    } catch (e) {
      setStatus({ ok: false, error: String(e && e.message ? e.message : e) });
    }
  });

  await loadDefaults();
  await loadCollections({ refresh: false });
</script>
"""

        return _layout("Common Crawl Search Engine • Index", body, embed=bool(embed), base_path=base_path)

    @app.post("/settings")
    async def settings_save(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                return JSONResponse({"ok": False, "error": "invalid payload"}, status_code=400)

            prev = _load_settings()
            out = _default_settings()

            # Preserve sensitive values by default.
            out["brave_search_api_key"] = prev.get("brave_search_api_key")

            mode = str(payload.get("default_cache_mode") or out["default_cache_mode"]).strip().lower()
            if mode not in ("range", "auto", "full"):
                return JSONResponse(
                    {"ok": False, "error": "default_cache_mode must be range|auto|full"}, status_code=400
                )
            out["default_cache_mode"] = mode

            out["default_max_bytes"] = int(payload.get("default_max_bytes") or out["default_max_bytes"])
            out["default_max_preview_chars"] = int(
                payload.get("default_max_preview_chars") or out["default_max_preview_chars"]
            )

            out["range_cache_max_bytes"] = int(payload.get("range_cache_max_bytes") or out["range_cache_max_bytes"])
            out["range_cache_max_item_bytes"] = int(
                payload.get("range_cache_max_item_bytes") or out["range_cache_max_item_bytes"]
            )

            full_dir = payload.get("full_warc_cache_dir")
            if full_dir is None or str(full_dir).strip() == "":
                out["full_warc_cache_dir"] = None
            else:
                out["full_warc_cache_dir"] = str(full_dir)

            out["full_warc_max_bytes"] = int(payload.get("full_warc_max_bytes") or out["full_warc_max_bytes"])
            out["full_warc_cache_max_total_bytes"] = int(
                payload.get("full_warc_cache_max_total_bytes") or out["full_warc_cache_max_total_bytes"]
            )

            # Brave key: if provided and non-empty, replace.
            if "brave_search_api_key" in payload:
                v = payload.get("brave_search_api_key")
                if v is not None and str(v).strip() != "":
                    out["brave_search_api_key"] = str(v).strip()

            _save_settings(out)
            return JSONResponse({"ok": True, "settings": out})
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    @app.get("/settings/cache_stats")
    def settings_cache_stats() -> JSONResponse:
        try:
            s = _load_settings()

            def _dir_stats(p: Optional[Path], *, glob: str) -> Dict[str, int]:
                if p is None:
                    return {"items": 0, "bytes": 0}
                try:
                    items = 0
                    total = 0
                    for fp in p.glob(glob):
                        if not fp.is_file():
                            continue
                        items += 1
                        try:
                            total += int(fp.stat().st_size)
                        except Exception:
                            pass
                    return {"items": items, "bytes": total}
                except Exception:
                    return {"items": 0, "bytes": 0}

            # Range cache dir (env + default behavior mirrors api._default_warc_cache_dir).
            range_env = os.environ.get("CCINDEX_WARC_CACHE_DIR")
            range_dir: Optional[Path]
            if range_env is not None and str(range_env).strip() == "":
                range_dir = None
            elif range_env:
                range_dir = Path(range_env)
            else:
                range_dir = Path("state") / "warc_cache"

            full_dir: Optional[Path]
            if s.get("full_warc_cache_dir"):
                full_dir = Path(str(s.get("full_warc_cache_dir")))
            else:
                full_env = os.environ.get("CCINDEX_FULL_WARC_CACHE_DIR")
                if full_env is not None and str(full_env).strip() == "":
                    full_dir = None
                elif full_env:
                    full_dir = Path(full_env)
                else:
                    full_dir = Path("state") / "warc_files"

            return JSONResponse(
                {
                    "ok": True,
                    "range": _dir_stats(range_dir, glob="*.bin"),
                    "full": _dir_stats(full_dir, glob="*"),
                }
            )
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    @app.post("/settings/clear_cache")
    async def settings_clear_cache(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
            which = str((payload or {}).get("which") or "").strip().lower()
            if which not in {"range", "full", "all"}:
                return JSONResponse({"ok": False, "error": "which must be range|full|all"}, status_code=400)

            s = _load_settings()

            def _clear_dir(p: Optional[Path], *, glob: str) -> tuple[int, int]:
                if p is None:
                    return 0, 0
                deleted = 0
                freed = 0
                try:
                    for fp in list(p.glob(glob)):
                        if not fp.is_file():
                            continue
                        try:
                            freed += int(fp.stat().st_size)
                        except Exception:
                            pass
                        try:
                            fp.unlink()
                            deleted += 1
                        except Exception:
                            pass
                except Exception:
                    pass
                return deleted, freed

            # Resolve dirs (same as stats).
            range_env = os.environ.get("CCINDEX_WARC_CACHE_DIR")
            range_dir: Optional[Path]
            if range_env is not None and str(range_env).strip() == "":
                range_dir = None
            elif range_env:
                range_dir = Path(range_env)
            else:
                range_dir = Path("state") / "warc_cache"

            full_dir: Optional[Path]
            if s.get("full_warc_cache_dir"):
                full_dir = Path(str(s.get("full_warc_cache_dir")))
            else:
                full_env = os.environ.get("CCINDEX_FULL_WARC_CACHE_DIR")
                if full_env is not None and str(full_env).strip() == "":
                    full_dir = None
                elif full_env:
                    full_dir = Path(full_env)
                else:
                    full_dir = Path("state") / "warc_files"

            deleted_total = 0
            freed_total = 0
            if which in {"range", "all"}:
                d, b = _clear_dir(range_dir, glob="*.bin")
                deleted_total += d
                freed_total += b
            if which in {"full", "all"}:
                d, b = _clear_dir(full_dir, glob="*")
                deleted_total += d
                freed_total += b

            return JSONResponse({"ok": True, "deleted_items": deleted_total, "freed_bytes": freed_total})
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    @app.post("/settings/clear_brave_key")
    def settings_clear_brave_key() -> JSONResponse:
        try:
            s = _load_settings()
            s["brave_search_api_key"] = None
            _save_settings(s)
            return JSONResponse({"ok": True})
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    @app.get("/settings/brave_cache_stats")
    def settings_brave_cache_stats() -> JSONResponse:
        try:
            from common_crawl_search_engine.ccsearch.brave_search import brave_search_cache_stats

            stats = brave_search_cache_stats()
            return JSONResponse({"ok": True, **stats})
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    @app.post("/settings/clear_brave_cache")
    def settings_clear_brave_cache() -> JSONResponse:
        try:
            from common_crawl_search_engine.ccsearch.brave_search import clear_brave_search_cache

            res = clear_brave_search_cache()
            return JSONResponse({"ok": True, **res})
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

        @app.get("/settings/brave_resolve_cache_stats")
        def settings_brave_resolve_cache_stats() -> JSONResponse:
          try:
            from common_crawl_search_engine.ccindex.api import brave_resolve_cache_stats

            stats = brave_resolve_cache_stats()
            return JSONResponse({"ok": True, **stats})
          except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

        @app.post("/settings/clear_brave_resolve_cache")
        def settings_clear_brave_resolve_cache() -> JSONResponse:
          try:
            from common_crawl_search_engine.ccindex.api import clear_brave_resolve_cache

            res = clear_brave_resolve_cache()
            return JSONResponse({"ok": True, **res})
          except Exception as e:
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    @app.get("/record", response_class=HTMLResponse)
    def record(
        request: Request,
        warc_filename: str,
        warc_offset: int,
        warc_length: int,
        prefix: str = "https://data.commoncrawl.org/",
        parquet_root: str = "/storage/ccindex_parquet",
    ) -> str:
        base_path = _base_path(request)
        s = _load_settings()
        pointer = {
            "warc_filename": warc_filename,
            "warc_offset": int(warc_offset),
            "warc_length": int(warc_length),
            "prefix": prefix,
            "max_bytes": int(s.get("default_max_bytes") or 2_000_000),
            "max_preview_chars": int(s.get("default_max_preview_chars") or 80_000),
            "cache_mode": str(s.get("default_cache_mode") or "range"),
            "full_warc_cache_dir": s.get("full_warc_cache_dir"),
            "full_warc_max_bytes": int(s.get("full_warc_max_bytes") or 5_000_000_000),
        }

        head = f"""
<div class='card'>
  <div><a href='{html.escape(base_path + "/")}'>← back</a></div>
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
            f"  <div class='field'><label>max_bytes</label><input id='max_bytes' type='number' min='1' step='1' value='{html.escape(str(pointer.get('max_bytes') or 2000000))}'></div>"
            f"  <div class='field'><label>max_preview_chars</label><input id='max_preview_chars' type='number' min='0' step='1' value='{html.escape(str(pointer.get('max_preview_chars') or 80000))}'></div>"
            "  <div class='field'><label>cache_mode</label>"
            "    <select id='cache_mode' style='padding: 10px 12px; border: 1px solid var(--border); background: rgba(15, 23, 42, 0.65); border-radius: 8px; color: var(--text);'>"
            "      <option value='range' selected>range</option>"
            "      <option value='auto'>auto</option>"
            "      <option value='full'>full (download WARC)</option>"
            "    </select>"
            "  </div>"
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
  const basePath = document.querySelector("meta[name='ccindex-base-path']")?.content || '';
  const {{ ccindexMcp }} = await import(`${{basePath}}/static/ccindex-mcp-sdk.js`);

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
  const cacheModeEl = document.getElementById('cache_mode');
  const refetchBtn = document.getElementById('refetchBtn');

  cacheModeEl.value = String(pointer.cache_mode || 'range');

  function esc(s) {{
    return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
  }}

  async function run() {{
    try {{
      pointer.max_bytes = parseInt(maxBytesEl.value || '2000000', 10);
      pointer.max_preview_chars = parseInt(maxPreviewEl.value || '80000', 10);
      pointer.cache_mode = String(cacheModeEl.value || 'range');

      // If settings provided full-warc cache options, keep passing them.
      if (pointer.full_warc_cache_dir) pointer.full_warc_cache_dir = String(pointer.full_warc_cache_dir);
      if (pointer.full_warc_max_bytes) pointer.full_warc_max_bytes = parseInt(String(pointer.full_warc_max_bytes), 10);

      const res = await ccindexMcp.callTool('fetch_warc_record', pointer);
      if (!res.ok) {{
        statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(res.error || 'unknown')}}</span>`;
        bodyEl.style.display = 'none';
        return;
      }}

      statusEl.innerHTML = `
        <span class='badge ok'>ok</span>
        status=<span class='code'>${{esc(res.status)}}</span>
        source=<span class='code'>${{esc(res.source || '')}}</span>
        bytes=<span class='code'>${{esc(res.bytes_returned)}}/${{esc(res.bytes_requested)}}</span>
        sha256=<span class='code'>${{esc(res.sha256 || '')}}</span>
        <div class='small'>download_url: <span class='code'>${{esc(res.url)}}</span></div>
      `;

      if (res.local_warc_path) {{
        statusEl.innerHTML += `<div class='small'>local_warc_path: <span class='code'>${{esc(res.local_warc_path)}}</span></div>`;
      }}

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
        const followHref = `${{basePath}}/?q=${{encodeURIComponent(redirectLoc)}}&parquet_root=${{encodeURIComponent(parquetRoot)}}&max_matches=25`;
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
              const href = `${{basePath}}/record?warc_filename=${{encodeURIComponent(rec.warc_filename||'')}}&warc_offset=${{encodeURIComponent(rec.warc_offset||'')}}&warc_length=${{encodeURIComponent(rec.warc_length||'')}}&parquet_root=${{encodeURIComponent(parquetRoot)}}`;
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
      const rangeHref = `${{basePath}}/download_record?warc_filename=${{encodeURIComponent(pointer.warc_filename)}}&warc_offset=${{encodeURIComponent(pointer.warc_offset)}}&warc_length=${{encodeURIComponent(pointer.warc_length)}}&prefix=${{encodeURIComponent(pointer.prefix)}}&max_bytes=${{encodeURIComponent(pointer.max_bytes)}}`;
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

        return _layout("Common Crawl Search Engine • Record", body, base_path=base_path)

    @app.get("/discover", response_class=HTMLResponse)
    def discover(
        request: Request,
        q: str = Query(default="", description="brave query"),
        year: str = Query(default="", description="optional year"),
      count: int = Query(default=int(brave_max_count), ge=1, le=1000),
        parquet_root: str = Query(default="/storage/ccindex_parquet"),
        embed: int = Query(default=0, ge=0, le=1),
    ) -> str:
        base_path = _base_path(request)
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
        <select id='dyear' name='year'></select>
      </div>
      <div class='field'>
        <label>Status (optional)</label>
        <select id='dstatus_filter'>
          <option value=''>any</option>
          <option value='200'>200</option>
          <option value='301'>301</option>
          <option value='302'>302</option>
          <option value='404'>404</option>
          <option value='500'>500</option>
        </select>
      </div>
      <div class='field' style='min-width: 220px;'>
        <label>MIME (optional)</label>
        <select id='dmime_filter'>
          <option value=''>any</option>
          <option value='text/html'>text/html</option>
          <option value='application/pdf'>application/pdf</option>
          <option value='text/plain'>text/plain</option>
          <option value='application/json'>application/json</option>
          <option value='application/xml'>application/xml</option>
        </select>
      </div>
      <div class='field'>
        <label>Count</label>
        <input id='dcount' name='count' value='{int(count)}' type='number' min='1' max='{int(brave_max_count)}'>
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
    • MCP JSON-RPC: <span class='code'>POST {html.escape(base_path + "/mcp")}</span>
  </div>
</div>

<div id='dstatus' class='card' style='margin-top: 14px;'><div class='small'>Enter a query and search.</div></div>
<div id='dresults' class='card' style='margin-top: 14px; display:none; padding: 0;'></div>

<script type='module'>
  const basePath = document.querySelector("meta[name='ccindex-base-path']")?.content || '';
  const {{ ccindexMcp }} = await import(`${{basePath}}/static/ccindex-mcp-sdk.js`);

  const braveMaxCount = {int(brave_max_count)};

  const initial = {json.dumps(initial)};
  const form = document.getElementById('discoverForm');
  const statusEl = document.getElementById('dstatus');
  const resultsEl = document.getElementById('dresults');
  const yearEl = document.getElementById('dyear');
  const statusFilterEl = document.getElementById('dstatus_filter');
  const mimeFilterEl = document.getElementById('dmime_filter');

  let lastResponse = null;

  function esc(s) {{
    return String(s ?? '')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#39;');
  }}

  function applyFiltersToMatches(ccMatches) {{
    const statusWanted = String(statusFilterEl.value || '').trim();
    const mimeWanted = String(mimeFilterEl.value || '').trim();
    return (ccMatches || []).filter((m) => {{
      const s = String(m.status ?? '').trim();
      const mt = String(m.mime ?? '').trim();
      if (statusWanted && s !== statusWanted) return false;
      if (mimeWanted && mt !== mimeWanted) return false;
      return true;
    }});
  }}

  function firstRecordLink(ccMatches) {{
    const r = (ccMatches || [])[0];
    if (!r) return null;
    return `${{basePath}}/record?warc_filename=${{encodeURIComponent(r.warc_filename||'')}}&warc_offset=${{encodeURIComponent(r.warc_offset||'')}}&warc_length=${{encodeURIComponent(r.warc_length||'')}}`;
  }}

  function render(res) {{
    const items = (res.results || []).map((it) => {{
      const title = esc(it.title || '');
      const url = esc(it.url || '');
      const desc = esc(it.description || '');
      const matches = it.cc_matches || [];
      const filteredMatches = applyFiltersToMatches(matches);
      const view = firstRecordLink(filteredMatches);
      const filtersApplied = Boolean((statusFilterEl.value || '').trim() || (mimeFilterEl.value || '').trim());
      const baseCount = matches.length;
      const shownCount = filteredMatches.length;
      const plural = (shownCount === 1) ? '' : 's';
      const badgeText = shownCount
        ? (String(shownCount) + ' capture' + plural)
        : (filtersApplied && baseCount ? ('0 (filtered from ' + String(baseCount) + ')') : 'no capture');
      const badge = shownCount
        ? `<span class='badge ok'>${{badgeText}}</span>`
        : `<span class='badge'>${{badgeText}}</span>`;
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

  let pageIndex = 0;

  function clampCount(n) {{
    const x = parseInt(String(n || ''), 10);
    const v = isNaN(x) ? braveMaxCount : x;
    return Math.max(1, Math.min(braveMaxCount, v));
  }}

  function renderPager(meta) {{
    const totalN = (meta && typeof meta.total_results === 'number') ? meta.total_results : 0;
    const pagesKnown = totalN > 0;
    const pageSize = clampCount(meta?.page_size ?? braveMaxCount);
    const idx = Math.max(0, Number(meta?.page_index ?? 0) || 0);
    const pages = pagesKnown ? Math.max(1, Math.ceil(totalN / pageSize)) : 0;
    const idxC = pagesKnown ? Math.min(idx, pages - 1) : idx;
    const start = (meta && typeof meta.offset === 'number') ? (meta.offset + 1) : (idxC * pageSize + 1);
    const shown = Math.max(0, Number(meta?.shown ?? 0) || 0);
    const end = shown ? (start + shown - 1) : start;
    const prevDisabled = idxC <= 0 ? 'disabled' : '';
    const nextDisabled = (meta && meta.has_next === false) ? 'disabled' : '';

    function pageButtonsHtml(pages, idx) {{
      const btns = [];
      const maxButtons = 9;
      const window = 2;

      function addBtn(p, label, active) {{
        const dis = active ? 'disabled' : '';
        const style = active ? " style='opacity:0.9; border-color: rgba(96,165,250,0.55); background: rgba(96,165,250,0.28);'" : '';
        btns.push("<button type='button' data-page='" + String(p) + "' " + dis + style + ">" + String(label) + "</button>");
      }}

      function addEllipsis() {{
        btns.push("<span class='small' style='padding:0 4px;'>…</span>");
      }}

      if (pages <= maxButtons) {{
        for (let p = 0; p < pages; p++) addBtn(p, String(p + 1), p === idx);
        return btns.join('');
      }}

      addBtn(0, '1', idx === 0);
      let startP = Math.max(1, idx - window);
      let endP = Math.min(pages - 2, idx + window);
      if (startP > 1) addEllipsis();
      for (let p = startP; p <= endP; p++) addBtn(p, String(p + 1), p === idx);
      if (endP < pages - 2) addEllipsis();
      addBtn(pages - 1, String(pages), idx === pages - 1);
      return btns.join('');
    }}

    const left = pagesKnown
      ? ("Results " + String(start) + "–" + String(end) + " of " + String(totalN))
      : (shown ? ("Results " + String(start) + "–" + String(end)) : "No results");
    const pageInfo = pagesKnown
      ? ("Page " + String(idxC + 1) + " of " + String(pages))
      : ("Page " + String(idxC + 1));
    const nums = pagesKnown
      ? ("<div id='dpagerNumWrap' style='display:flex; gap: 6px; align-items:center;'>" + pageButtonsHtml(pages, idxC) + "</div>")
      : "";

    return "<div style='padding: 12px; display:flex; gap: 10px; align-items:center; flex-wrap: wrap; border-bottom: 1px solid rgba(34,48,74,.7);'>"
      + "<span class='small'>" + left + "</span>"
      + "<span class='small'>•</span>"
      + "<span class='small'>" + pageInfo + "</span>"
      + "<span class='small'>•</span>"
      + "<button type='button' id='dpagerPrev' " + prevDisabled + ">Prev</button>"
      + "<button type='button' id='dpagerNext' " + nextDisabled + ">Next</button>"
      + nums
      + "</div>";
  }}

  function renderCurrent(res) {{
    const shown = (res && res.results) ? res.results.length : 0;
    const inputCount = clampCount(document.getElementById('dcount').value);
    const effCount = clampCount(res?.count ?? inputCount);
    const effOffset = Math.max(0, Number(res?.offset ?? (pageIndex * effCount)) || 0);
    pageIndex = Math.max(0, Math.floor(effOffset / effCount));

    const total = (typeof res?.total_results === 'number') ? res.total_results : null;
    const hasNext = (total !== null) ? (effOffset + shown < total) : (shown >= effCount);

    const pager = renderPager({{
      total_results: (total !== null) ? total : 0,
      page_index: pageIndex,
      page_size: effCount,
      offset: effOffset,
      shown: shown,
      has_next: hasNext,
    }});
    resultsEl.innerHTML = pager + render(res || {{results: []}});
    resultsEl.style.display = 'block';
  }}

  async function populateYears() {{
    const initialYear = String(initial.year || '').trim();
    const current = (new Date()).getFullYear();
    const fallbackYears = [];
    for (let y = current; y >= 2010; y--) fallbackYears.push(String(y));

    yearEl.innerHTML = "<option value=''>any</option>";
    try {{
      const info = await ccindexMcp.callTool('cc_collinfo_list', {{}});
      const years = new Set();
      for (const it of (info.collections || [])) {{
        const id = String(it.id || '');
        const m = id.match(/CC-MAIN-([0-9]{{4}})-/);
        if (m && m[1]) years.add(m[1]);
      }}
      const sorted = Array.from(years).sort().reverse();
      const use = sorted.length ? sorted : fallbackYears;
      for (const y of use) {{
        const o = document.createElement('option');
        o.value = String(y);
        o.textContent = String(y);
        yearEl.appendChild(o);
      }}
    }} catch (e) {{
      for (const y of fallbackYears) {{
        const o = document.createElement('option');
        o.value = String(y);
        o.textContent = String(y);
        yearEl.appendChild(o);
      }}
    }}
    if (initialYear) yearEl.value = initialYear;
  }}

  function rerenderWithFilters() {{
    if (!lastResponse) return;
    renderCurrent(lastResponse);
  }}

  async function runDiscover() {{
    const q = document.getElementById('dq').value;
    const year = yearEl.value;
    const count = clampCount(document.getElementById('dcount').value);
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
        offset: Math.max(0, Number(pageIndex) || 0) * count,
        year: year.trim() || null,
        parquet_root: parquetRoot,
      }});

      const elapsed = (typeof res.elapsed_s === 'number') ? res.elapsed_s.toFixed(2) : String(res.elapsed_s ?? '');
      const braveElapsed = (typeof res.brave_elapsed_s === 'number') ? res.brave_elapsed_s.toFixed(2) : '';
      const resolveElapsed = (typeof res.resolve_elapsed_s === 'number') ? res.resolve_elapsed_s.toFixed(2) : '';
      const totalTxt = (typeof res.total_results === 'number') ? String(res.total_results) : 'unknown';
      const offTxt = (typeof res.offset === 'number') ? String(res.offset) : String(pageIndex * count);
      const braveCachedTxt = (res && (res.brave_cached === true)) ? 'yes' : 'no';
      const resolvedCachedTxt = (res && (res.resolved_cached === true)) ? 'yes' : 'no';
      statusEl.innerHTML = `<span class='badge ok'>ok</span> elapsed_s=<span class='code'>${{esc(elapsed)}}</span> brave_s=<span class='code'>${{esc(braveElapsed)}}</span> resolve_s=<span class='code'>${{esc(resolveElapsed)}}</span> brave_cached=<span class='code'>${{esc(braveCachedTxt)}}</span> resolved_cached=<span class='code'>${{esc(resolvedCachedTxt)}}</span> total=<span class='code'>${{esc(totalTxt)}}</span> offset=<span class='code'>${{esc(offTxt)}}</span> returned=<span class='code'>${{esc((res.results||[]).length)}}</span>`;
      lastResponse = res;
      // Sync page index to server-effective offset/count.
      const effCount = clampCount(res.count ?? count);
      const effOffset = Math.max(0, Number(res.offset || 0) || 0);
      pageIndex = Math.max(0, Math.floor(effOffset / effCount));
      renderCurrent(res);
    }} catch (e) {{
      statusEl.innerHTML = `<span class='badge err'>error</span> <span class='code'>${{esc(e.message || e)}}</span>`;
      resultsEl.style.display = 'none';
    }}
  }}

  resultsEl.addEventListener('click', (ev) => {{
    const t = ev.target;
    if (!(t instanceof HTMLElement)) return;
    const pageAttr = t.getAttribute('data-page');
    if (pageAttr !== null) {{
      const p = parseInt(pageAttr || '0', 10);
      pageIndex = isNaN(p) ? 0 : Math.max(0, p);
      runDiscover();
      return;
    }}
    if (t.id === 'dpagerPrev') {{
      pageIndex = Math.max(0, Number(pageIndex) - 1);
      runDiscover();
      return;
    }}
    if (t.id === 'dpagerNext') {{
      pageIndex = Math.max(0, Number(pageIndex) + 1);
      runDiscover();
      return;
    }}
  }});

  statusFilterEl.addEventListener('change', () => rerenderWithFilters());
  mimeFilterEl.addEventListener('change', () => rerenderWithFilters());

  form.addEventListener('submit', (ev) => {{
    ev.preventDefault();
    pageIndex = 0;
    runDiscover();
  }});

  await populateYears();

  if ((initial.q || '').trim()) {{
    runDiscover();
  }}
</script>
"""

        return _layout("Common Crawl Search Engine • Search", body, embed=bool(embed), base_path=base_path)

    return app


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Run the Common Crawl Search Engine dashboard + MCP JSON-RPC")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument(
        "--master-db",
        type=Path,
        default=_DEFAULT_MASTER_DB,
        help="Master meta-index DuckDB",
    )
    ap.add_argument("--reload", action="store_true", default=False, help="Enable uvicorn reload")
    ap.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("CCINDEX_DASHBOARD_WORKERS") or 1),
        help="Uvicorn worker processes (recommended behind reverse proxies)",
    )

    args = ap.parse_args(argv)

    try:
        import uvicorn  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "Missing dashboard dependencies. Install with: pip install -e '.[ccindex-dashboard]'\n"
            f"Import error: {e}"
        )

    resolved_master_db = Path(args.master_db).expanduser().resolve()

    # When using multiple workers or reload, uvicorn needs an import string.
    # We pass configuration through env vars so workers can construct the app.
    os.environ["CCINDEX_MASTER_DB"] = str(resolved_master_db)

    workers = max(1, int(args.workers or 1))
    if workers > 1 or bool(args.reload):
        uvicorn.run(
            "common_crawl_search_engine.dashboard:app",
            host=str(args.host),
            port=int(args.port),
            reload=bool(args.reload),
            workers=workers,
            proxy_headers=True,
            forwarded_allow_ips="*",
        )
    else:
        app_obj = create_app(master_db=resolved_master_db)
        uvicorn.run(
            app_obj,
            host=str(args.host),
            port=int(args.port),
            reload=False,
            proxy_headers=True,
            forwarded_allow_ips="*",
        )

    return 0


def create_app_from_env() -> Any:
    return create_app(master_db=_env_master_db())


# Importable ASGI app for `uvicorn common_crawl_search_engine.dashboard:app --workers N`.
app = create_app_from_env()


if __name__ == "__main__":
    raise SystemExit(main())
