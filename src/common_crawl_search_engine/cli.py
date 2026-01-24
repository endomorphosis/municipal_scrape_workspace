"""Unified ccindex CLI (application layer).

This module is separated from the core library code in
`common_crawl_search_engine.ccindex` so the extraction boundary is clean.

Examples:
    python -m common_crawl_search_engine.cli --help
    ccindex search meta --domain 18f.gov --max-matches 50
    ccindex mcp start
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import time
from pathlib import Path
from datetime import datetime
import subprocess

from common_crawl_search_engine.ccindex import api


def _delegate(module_path: str, argv: list[str]) -> int:
    """Delegate to an existing module's main() using the provided argv."""

    mod = importlib.import_module(module_path)
    if not hasattr(mod, "main"):
        raise RuntimeError(f"Module {module_path} has no main()")

    old_argv = sys.argv
    sys.argv = [module_path] + list(argv)
    try:
        # Some legacy modules accept main(argv), others define main() and read sys.argv.
        try:
            return int(mod.main(argv))
        except TypeError:
            return int(mod.main())
    finally:
        sys.argv = old_argv


def _cmd_search_meta(args: argparse.Namespace) -> int:
    res = api.search_domain_via_meta_indexes(
        args.domain,
        parquet_root=args.parquet_root,
        master_db=args.master_db,
        year_db=args.year_db,
        collection_db=args.collection_db,
        year=args.year,
        max_parquet_files=args.max_parquet_files,
        max_matches=args.max_matches,
        per_parquet_limit=args.per_parquet_limit,
    )

    if args.stats:
        sys.stderr.write(
            f"meta_source={res.meta_source} collections={res.collections_considered} "
            f"emitted={res.emitted} elapsed_s={res.elapsed_s:.2f}\n"
        )

    sys.stdout.write(api.to_jsonl(res.records))
    return 0


def _cmd_warc_cache(args: argparse.Namespace) -> int:
    p = api.ensure_full_warc_cached(
        warc_filename=str(args.warc_filename),
        prefix=str(args.prefix),
        cache_dir=Path(args.full_warc_cache_dir).expanduser().resolve() if args.full_warc_cache_dir else None,
        timeout_s=float(args.timeout_s),
        max_full_bytes=int(args.full_warc_max_bytes),
        overwrite=bool(args.overwrite),
    )
    sys.stdout.write(str(p) + "\n")
    return 0


def _cmd_warc_fetch_record(args: argparse.Namespace) -> int:
    fetch, source, local_path = api.fetch_warc_record(
        warc_filename=str(args.warc_filename),
        warc_offset=int(args.warc_offset),
        warc_length=int(args.warc_length),
        prefix=str(args.prefix),
        timeout_s=float(args.timeout_s),
        max_bytes=int(args.max_bytes),
        decode_gzip_text=bool(args.decode_gzip_text),
        max_preview_chars=int(args.max_preview_chars),
        cache_mode=str(args.cache_mode),
        full_warc_cache_dir=Path(args.full_warc_cache_dir).expanduser().resolve() if args.full_warc_cache_dir else None,
        full_warc_max_bytes=int(args.full_warc_max_bytes),
    )

    out: dict[str, object] = {
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

    if args.include_raw_base64:
        out["raw_base64"] = fetch.raw_base64

    if args.http and fetch.ok and fetch.raw_base64:
        try:
            import base64 as _b64

            raw = _b64.b64decode(fetch.raw_base64)
            parsed = api.extract_http_from_warc_gzip_member(
                raw,
                max_body_bytes=int(args.max_bytes),
                max_preview_chars=int(args.max_preview_chars),
                include_body_base64=bool(args.include_http_body_base64),
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
                "body_base64": parsed.body_base64,
                "error": parsed.error,
            }
        except Exception as e:
            out["http"] = {"ok": False, "error": f"parse_failed: {type(e).__name__}: {e}"}

    sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ccindex", description="Common Crawl index CLI (unified entrypoint)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # ---- search ----
    ap_search = sub.add_parser("search", help="Search indexes")
    sub_search = ap_search.add_subparsers(dest="search_cmd", required=True)

    ap_meta = sub_search.add_parser("meta", help="Search via master/year meta-indexes")
    ap_meta.add_argument("--domain", required=True, help="Domain or URL to search")

    src = ap_meta.add_mutually_exclusive_group()
    src.add_argument(
        "--master-db",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
        help="Master meta-index DuckDB",
    )
    src.add_argument("--year-db", type=Path, help="Year meta-index DuckDB")
    src.add_argument("--collection-db", type=Path, help="Single collection DuckDB")

    ap_meta.add_argument("--year", type=str, default=None, help="Restrict to a year (only used with --master-db)")
    ap_meta.add_argument(
        "--parquet-root",
        type=Path,
        default=Path("/storage/ccindex_parquet"),
        help="Parquet root",
    )
    ap_meta.add_argument("--max-parquet-files", type=int, default=200)
    ap_meta.add_argument("--max-matches", type=int, default=200)
    ap_meta.add_argument("--per-parquet-limit", type=int, default=2000)
    ap_meta.add_argument("--stats", action="store_true", help="Emit stats to stderr")
    ap_meta.set_defaults(func=_cmd_search_meta)

    ap_domain = sub_search.add_parser("domain", help="Delegate to search_cc_domain (legacy behavior)")
    ap_domain.add_argument("argv", nargs=argparse.REMAINDER, help="Arguments for search_cc_domain")
    ap_domain.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.ccindex.search_cc_domain", a.argv))

    ap_parallel = sub_search.add_parser("parallel", help="Delegate to search_parallel_duckdb_indexes")
    ap_parallel.add_argument("argv", nargs=argparse.REMAINDER)
    ap_parallel.set_defaults(
        func=lambda a: _delegate("common_crawl_search_engine.ccindex.search_parallel_duckdb_indexes", a.argv)
    )

    # ---- build ----
    ap_build = sub.add_parser("build", help="Build indexes")
    sub_build = ap_build.add_subparsers(dest="build_cmd", required=True)

    ap_build_pointer = sub_build.add_parser("pointer", help="Build pointer DuckDB index")
    ap_build_pointer.add_argument("argv", nargs=argparse.REMAINDER)
    ap_build_pointer.set_defaults(
        func=lambda a: _delegate("common_crawl_search_engine.ccindex.build_cc_pointer_duckdb", a.argv)
    )

    ap_build_parallel = sub_build.add_parser("parallel", help="Build parallel DuckDB indexes")
    ap_build_parallel.add_argument("argv", nargs=argparse.REMAINDER)
    ap_build_parallel.set_defaults(
        func=lambda a: _delegate("common_crawl_search_engine.ccindex.build_parallel_duckdb_indexes", a.argv)
    )

    ap_build_meta = sub_build.add_parser("meta", help="Build year meta-indexes")
    ap_build_meta.add_argument("argv", nargs=argparse.REMAINDER)
    ap_build_meta.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.ccindex.build_year_meta_indexes", a.argv))

    # ---- orchestration ----
    ap_orch = sub.add_parser("orchestrate", help="Delegate to pipeline orchestrator")
    ap_orch.add_argument("argv", nargs=argparse.REMAINDER)
    ap_orch.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.ccindex.cc_pipeline_orchestrator", a.argv))

    ap_watch = sub.add_parser("watch", help="Delegate to pipeline watch")
    ap_watch.add_argument("argv", nargs=argparse.REMAINDER)
    ap_watch.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.ccindex.cc_pipeline_watch", a.argv))

    ap_hud = sub.add_parser("hud", help="Delegate to pipeline HUD")
    ap_hud.add_argument("argv", nargs=argparse.REMAINDER)
    ap_hud.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.ccindex.cc_pipeline_hud", a.argv))

    # ---- validate ----
    ap_validate = sub.add_parser("validate", help="Validation tools")
    sub_val = ap_validate.add_subparsers(dest="validate_cmd", required=True)

    ap_val_coll = sub_val.add_parser("collection", help="Validate collection completeness")
    ap_val_coll.add_argument("argv", nargs=argparse.REMAINDER)
    ap_val_coll.set_defaults(
        func=lambda a: _delegate("common_crawl_search_engine.ccindex.validate_collection_completeness", a.argv)
    )

    ap_val_pq = sub_val.add_parser("parquet", help="Validate and sort Parquet")
    ap_val_pq.add_argument("argv", nargs=argparse.REMAINDER)
    ap_val_pq.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.ccindex.validate_and_sort_parquet", a.argv))

    # ---- mcp ----
    ap_mcp = sub.add_parser("mcp", help="MCP server + dashboard")
    sub_mcp = ap_mcp.add_subparsers(dest="mcp_cmd", required=True)

    ap_mcp_start = sub_mcp.add_parser(
        "start",
        help="Start the MCP HTTP JSON-RPC endpoint and dashboard (single process)",
    )
    ap_mcp_start.add_argument("--host", default="127.0.0.1")
    ap_mcp_start.add_argument("--port", type=int, default=8787)
    ap_mcp_start.add_argument(
        "--master-db",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
        help="Master meta-index DuckDB",
    )
    ap_mcp_start.add_argument("--reload", action="store_true", default=False)
    ap_mcp_start.add_argument(
        "--detach",
        action="store_true",
        default=False,
        help="Start the dashboard in the background and return immediately",
    )

    def _spawn_dashboard(*, host: str, port: int, master_db: Path, reload: bool) -> int:
        logs_dir = Path("logs")
        state_dir = Path("state")
        logs_dir.mkdir(parents=True, exist_ok=True)
        state_dir.mkdir(parents=True, exist_ok=True)

        log_path = logs_dir / f"ccindex_dashboard_{int(port)}.log"
        pid_path = state_dir / f"ccindex_dashboard_{int(port)}.pid"

        args = [
            sys.executable,
            "-m",
            "common_crawl_search_engine.dashboard",
            "--host",
            str(host),
            "--port",
            str(int(port)),
            "--master-db",
            str(master_db),
        ]
        if reload:
            args.append("--reload")

        with open(log_path, "a", encoding="utf-8") as logf:
            proc = subprocess.Popen(
                args,
                stdout=logf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        pid_path.write_text(str(proc.pid) + "\n", encoding="utf-8")
        sys.stdout.write(f"Started dashboard pid={proc.pid} on http://{host}:{int(port)}\n")
        sys.stdout.write(f"Log: {log_path}\n")
        sys.stdout.write(f"PID file: {pid_path}\n")
        return 0

    def _mcp_start(ns: argparse.Namespace) -> int:
        if ns.detach:
            return _spawn_dashboard(
                host=str(ns.host),
                port=int(ns.port),
                master_db=Path(ns.master_db),
                reload=bool(ns.reload),
            )

        from common_crawl_search_engine.dashboard import main as dashboard_main

        args2: list[str] = [
            "--host",
            str(ns.host),
            "--port",
            str(int(ns.port)),
            "--master-db",
            str(ns.master_db),
        ]
        if ns.reload:
            args2.append("--reload")
        return int(dashboard_main(args2))

    ap_mcp_start.set_defaults(func=_mcp_start)

    ap_mcp_restart = sub_mcp.add_parser(
        "restart",
        help="Stop any running dashboard on the port, then start it again",
    )
    ap_mcp_restart.add_argument("--host", default="127.0.0.1")
    ap_mcp_restart.add_argument("--port", type=int, default=8787)
    ap_mcp_restart.add_argument(
        "--master-db",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
        help="Master meta-index DuckDB",
    )
    ap_mcp_restart.add_argument("--reload", action="store_true", default=False)
    ap_mcp_restart.add_argument(
        "--detach",
        action="store_true",
        default=False,
        help="Start the dashboard in the background and return immediately",
    )
    ap_mcp_restart.add_argument(
        "--grace-seconds",
        type=float,
        default=3.0,
        help="How long to wait after terminating the old server before starting",
    )

    def _kill_listeners(host: str, port: int) -> int:
        """Best-effort kill of processes listening on host:port.

        Uses psutil if available.
        Returns number of processes signaled.
        """

        try:
            import psutil  # type: ignore
        except Exception:
            sys.stderr.write(
                "psutil not installed; cannot auto-kill existing server. "
                "Install with: pip install -e '.[ccindex]'\n"
            )
            return 0

        target_port = int(port)
        target_host = str(host)
        signaled: set[int] = set()

        try:
            for conn in psutil.net_connections(kind="inet"):
                laddr = getattr(conn, "laddr", None)
                if not laddr:
                    continue
                lhost = getattr(laddr, "ip", None) or (laddr[0] if isinstance(laddr, tuple) else None)
                lport = getattr(laddr, "port", None) or (laddr[1] if isinstance(laddr, tuple) else None)
                if int(lport or -1) != target_port:
                    continue

                # If host is 0.0.0.0, we accept any listener on that port.
                if target_host not in ("0.0.0.0", "::") and lhost not in (target_host, "0.0.0.0", "::"):
                    continue

                pid = getattr(conn, "pid", None)
                if not pid:
                    continue
                signaled.add(int(pid))
        except Exception as e:
            sys.stderr.write(f"Failed to enumerate listening processes: {e}\n")
            return 0

        killed = 0
        for pid in sorted(signaled):
            try:
                p = psutil.Process(pid)
                sys.stderr.write(f"Terminating pid={pid} ({' '.join(p.cmdline()[:3])})\n")
                p.terminate()
                killed += 1
            except Exception as e:
                sys.stderr.write(f"Failed to terminate pid={pid}: {e}\n")

        # Give processes a moment to exit.
        if killed:
            time.sleep(0.5)
            for pid in sorted(signaled):
                try:
                    p = psutil.Process(pid)
                    if p.is_running():
                        p.kill()
                except Exception:
                    pass

        return killed

    def _mcp_restart(ns: argparse.Namespace) -> int:
        _kill_listeners(str(ns.host), int(ns.port))
        time.sleep(max(0.0, float(ns.grace_seconds)))

        if ns.detach:
            return _spawn_dashboard(
                host=str(ns.host),
                port=int(ns.port),
                master_db=Path(ns.master_db),
                reload=bool(ns.reload),
            )

        from common_crawl_search_engine.dashboard import main as dashboard_main

        args2: list[str] = [
            "--host",
            str(ns.host),
            "--port",
            str(int(ns.port)),
            "--master-db",
            str(ns.master_db),
        ]
        if ns.reload:
            args2.append("--reload")
        return int(dashboard_main(args2))

    ap_mcp_restart.set_defaults(func=_mcp_restart)

    ap_mcp_analyze = sub_mcp.add_parser(
        "analyze",
        help="Run automated browser analysis of the dashboard (Playwright) and write screenshots/logs to artifacts/",
    )
    ap_mcp_analyze.add_argument("--domain", default="iana.org")
    ap_mcp_analyze.add_argument("--parquet-root", type=Path, default=Path("/storage/ccindex_parquet"))
    ap_mcp_analyze.add_argument(
        "--master-db",
        type=Path,
        default=Path("/storage/ccindex_duckdb/cc_pointers_master/cc_master_index.duckdb"),
    )
    ap_mcp_analyze.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("artifacts/dashboard_analyze"),
        help="Where to write screenshots and logs (a timestamped subfolder is created)",
    )
    ap_mcp_analyze.add_argument("--headed", action="store_true", default=False)
    ap_mcp_analyze.add_argument("--timeout-s", type=float, default=60.0)

    def _mcp_analyze(ns: argparse.Namespace) -> int:
        try:
            from common_crawl_search_engine.dashboard_e2e import run_dashboard_analysis
        except Exception as e:
            sys.stderr.write(f"Failed to import dashboard analyzer: {e}\n")
            return 2

        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(ns.artifacts_dir) / run_id
        out_dir.mkdir(parents=True, exist_ok=True)

        ok = run_dashboard_analysis(
            output_dir=out_dir,
            domain=str(ns.domain),
            parquet_root=Path(ns.parquet_root),
            master_db=Path(ns.master_db),
            headless=not bool(ns.headed),
            timeout_s=float(ns.timeout_s),
        )

        sys.stdout.write(f"Artifacts: {out_dir}\n")
        return 0 if ok else 1

    ap_mcp_analyze.set_defaults(func=_mcp_analyze)

    ap_mcp_serve = sub_mcp.add_parser("serve", help="Start stdio MCP server (for MCP clients)")
    ap_mcp_serve.set_defaults(func=lambda a: _delegate("common_crawl_search_engine.mcp_server", []))

    # ---- brave cache ----
    ap_bcache = sub.add_parser("brave-cache", help="Brave Search cache utilities")
    sub_bcache = ap_bcache.add_subparsers(dest="brave_cache_cmd", required=True)

    ap_bcache_stats = sub_bcache.add_parser("stats", help="Show Brave Search cache stats")

    def _bcache_stats(_ns: argparse.Namespace) -> int:
        from common_crawl_search_engine.ccsearch.brave_search import brave_search_cache_stats

        sys.stdout.write(json.dumps(brave_search_cache_stats(), ensure_ascii=False) + "\n")
        return 0

    ap_bcache_stats.set_defaults(func=_bcache_stats)

    ap_bcache_clear = sub_bcache.add_parser("clear", help="Clear Brave Search cache")

    def _bcache_clear(_ns: argparse.Namespace) -> int:
        from common_crawl_search_engine.ccsearch.brave_search import clear_brave_search_cache

        sys.stdout.write(json.dumps(clear_brave_search_cache(), ensure_ascii=False) + "\n")
        return 0

    ap_bcache_clear.set_defaults(func=_bcache_clear)

    # ---- warc ----
    ap_warc = sub.add_parser("warc", help="WARC utilities (fetch records, cache full WARCs)")
    sub_warc = ap_warc.add_subparsers(dest="warc_cmd", required=True)

    ap_warc_cache = sub_warc.add_parser("cache", help="Download/cache a full *.warc.gz locally")
    ap_warc_cache.add_argument("--warc-filename", required=True, help="Common Crawl WARC filename (path within CC)")
    ap_warc_cache.add_argument("--prefix", default="https://data.commoncrawl.org/", help="WARC base URL prefix")
    ap_warc_cache.add_argument(
        "--full-warc-cache-dir",
        default=None,
        help="Where to store full WARCs (default: state/warc_files; disable via env CCINDEX_FULL_WARC_CACHE_DIR='')",
    )
    ap_warc_cache.add_argument("--full-warc-max-bytes", type=int, default=5_000_000_000)
    ap_warc_cache.add_argument("--timeout-s", type=float, default=60.0)
    ap_warc_cache.add_argument("--overwrite", action="store_true", default=False)
    ap_warc_cache.set_defaults(func=_cmd_warc_cache)

    ap_warc_fetch = sub_warc.add_parser("fetch-record", help="Fetch a WARC record by offset/length")
    ap_warc_fetch.add_argument("--warc-filename", required=True)
    ap_warc_fetch.add_argument("--warc-offset", type=int, required=True)
    ap_warc_fetch.add_argument("--warc-length", type=int, required=True)
    ap_warc_fetch.add_argument("--prefix", default="https://data.commoncrawl.org/")
    ap_warc_fetch.add_argument("--timeout-s", type=float, default=30.0)
    ap_warc_fetch.add_argument("--max-bytes", type=int, default=2_000_000)
    ap_warc_fetch.add_argument("--decode-gzip-text", dest="decode_gzip_text", action="store_true", default=True)
    ap_warc_fetch.add_argument("--no-decode-gzip-text", dest="decode_gzip_text", action="store_false")
    ap_warc_fetch.add_argument("--max-preview-chars", type=int, default=80_000)
    ap_warc_fetch.add_argument("--cache-mode", choices=["range", "auto", "full"], default="range")
    ap_warc_fetch.add_argument("--full-warc-cache-dir", default=None)
    ap_warc_fetch.add_argument("--full-warc-max-bytes", type=int, default=5_000_000_000)
    ap_warc_fetch.add_argument("--include-raw-base64", action="store_true", default=False)
    ap_warc_fetch.add_argument("--http", dest="http", action="store_true", default=True, help="Parse HTTP payload from WARC")
    ap_warc_fetch.add_argument("--no-http", dest="http", action="store_false")
    ap_warc_fetch.add_argument("--include-http-body-base64", action="store_true", default=False)
    ap_warc_fetch.set_defaults(func=_cmd_warc_fetch_record)

    ns = ap.parse_args(argv)
    return int(ns.func(ns))


if __name__ == "__main__":
    raise SystemExit(main())
