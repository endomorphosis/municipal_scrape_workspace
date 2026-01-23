"""Unified ccindex CLI.

This complements the existing `ccindex-*` console scripts by providing a single
entrypoint with subcommands.

Examples:
  ccindex search meta --domain 18f.gov --max-matches 50
  ccindex search domain example.com --db /storage/ccindex_duckdb/cc_pointers.duckdb
  ccindex orchestrate --config pipeline_config.json
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

from . import api


def _delegate(module_path: str, argv: list[str]) -> int:
    """Delegate to an existing module's main() using the provided argv."""

    mod = importlib.import_module(module_path)
    if not hasattr(mod, "main"):
        raise RuntimeError(f"Module {module_path} has no main()")

    old_argv = sys.argv
    sys.argv = [module_path] + list(argv)
    try:
        rc = mod.main()
        return int(rc) if rc is not None else 0
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
    ap_domain.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.search_cc_domain", a.argv))

    ap_parallel = sub_search.add_parser("parallel", help="Delegate to search_parallel_duckdb_indexes")
    ap_parallel.add_argument("argv", nargs=argparse.REMAINDER)
    ap_parallel.set_defaults(
        func=lambda a: _delegate("municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes", a.argv)
    )

    # ---- build ----
    ap_build = sub.add_parser("build", help="Build indexes")
    sub_build = ap_build.add_subparsers(dest="build_cmd", required=True)

    ap_build_pointer = sub_build.add_parser("pointer", help="Build pointer DuckDB index")
    ap_build_pointer.add_argument("argv", nargs=argparse.REMAINDER)
    ap_build_pointer.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb", a.argv))

    ap_build_parallel = sub_build.add_parser("parallel", help="Build parallel DuckDB indexes")
    ap_build_parallel.add_argument("argv", nargs=argparse.REMAINDER)
    ap_build_parallel.set_defaults(
        func=lambda a: _delegate("municipal_scrape_workspace.ccindex.build_parallel_duckdb_indexes", a.argv)
    )

    ap_build_meta = sub_build.add_parser("meta", help="Build year meta-indexes")
    ap_build_meta.add_argument("argv", nargs=argparse.REMAINDER)
    ap_build_meta.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.build_year_meta_indexes", a.argv))

    # ---- orchestration ----
    ap_orch = sub.add_parser("orchestrate", help="Delegate to pipeline orchestrator")
    ap_orch.add_argument("argv", nargs=argparse.REMAINDER)
    ap_orch.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator", a.argv))

    ap_watch = sub.add_parser("watch", help="Delegate to pipeline watch")
    ap_watch.add_argument("argv", nargs=argparse.REMAINDER)
    ap_watch.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.cc_pipeline_watch", a.argv))

    ap_hud = sub.add_parser("hud", help="Delegate to pipeline HUD")
    ap_hud.add_argument("argv", nargs=argparse.REMAINDER)
    ap_hud.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.cc_pipeline_hud", a.argv))

    # ---- validate ----
    ap_validate = sub.add_parser("validate", help="Validation tools")
    sub_val = ap_validate.add_subparsers(dest="validate_cmd", required=True)

    ap_val_coll = sub_val.add_parser("collection", help="Validate collection completeness")
    ap_val_coll.add_argument("argv", nargs=argparse.REMAINDER)
    ap_val_coll.set_defaults(
        func=lambda a: _delegate("municipal_scrape_workspace.ccindex.validate_collection_completeness", a.argv)
    )

    ap_val_pq = sub_val.add_parser("parquet", help="Validate and sort Parquet")
    ap_val_pq.add_argument("argv", nargs=argparse.REMAINDER)
    ap_val_pq.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.validate_and_sort_parquet", a.argv))

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

    def _mcp_start(ns: argparse.Namespace) -> int:
        from municipal_scrape_workspace.ccindex import dashboard

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
        return int(dashboard.main(args2))

    ap_mcp_start.set_defaults(func=_mcp_start)

    ap_mcp_serve = sub_mcp.add_parser("serve", help="Start stdio MCP server (for MCP clients)")
    ap_mcp_serve.set_defaults(func=lambda a: _delegate("municipal_scrape_workspace.ccindex.mcp_server", []))

    ns = ap.parse_args(argv)
    return int(ns.func(ns))


if __name__ == "__main__":
    raise SystemExit(main())
