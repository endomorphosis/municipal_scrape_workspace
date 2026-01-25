"""Import-friendly management layer for the CCIndex pipeline orchestrator.

The pipeline orchestrator (`cc_pipeline_orchestrator.py`) is primarily a CLI.
This module provides a stable surface for:
- persisted orchestrator settings,
- lightweight status checks, and
- starting long-running orchestrator jobs (pipeline/download/cleanup/meta-index)
  in a background subprocess.

This is used by the dashboard, MCP tools, and the unified `ccindex` CLI.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional


JobMode = Literal["pipeline", "download_only", "cleanup_only", "build_meta_indexes"]

DEFAULT_COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"


def _state_dir() -> Path:
    d = Path(os.environ.get("CCINDEX_STATE_DIR") or "state")
    d.mkdir(parents=True, exist_ok=True)
    return d


def orchestrator_settings_path() -> Path:
    p = os.environ.get("CCINDEX_ORCHESTRATOR_SETTINGS_PATH")
    if p:
        return Path(p).expanduser().resolve()
    return _state_dir() / "orchestrator_settings.json"


def collinfo_cache_path() -> Path:
    p = os.environ.get("CCINDEX_COLLINFO_CACHE_PATH")
    if p:
        return Path(p).expanduser().resolve()
    return _state_dir() / "collinfo.json"


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _resolve_repo_collinfo_fallback() -> Path | None:
    """Best-effort resolve of a repo-shipped collinfo.json."""

    try:
        here = Path(__file__).resolve()
        for parent in [here.parent, *here.parents]:
            candidate = parent / "collinfo.json"
            if candidate.exists() and candidate.is_file():
                return candidate
    except Exception:
        return None
    return None


def load_collinfo(*, prefer_cache: bool = True) -> dict[str, Any]:
    """Load Common Crawl collinfo JSON.

    Returns a dict with keys:
      - ok: bool
      - source_path: str | None
      - fetched_at: str | None
      - collections: list[dict]
    """

    cache = collinfo_cache_path()
    if prefer_cache and cache.exists():
        try:
            data = json.loads(cache.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {"ok": True, "source_path": str(cache), "fetched_at": None, "collections": data}
        except Exception:
            pass

    repo = _resolve_repo_collinfo_fallback()
    if repo and repo.exists():
        try:
            data = json.loads(repo.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {"ok": True, "source_path": str(repo), "fetched_at": None, "collections": data}
        except Exception:
            pass

    return {"ok": False, "source_path": None, "fetched_at": None, "collections": []}


def update_collinfo(*, url: str = DEFAULT_COLLINFO_URL, timeout_s: float = 15.0) -> dict[str, Any]:
    """Fetch collinfo.json from Common Crawl and persist to state."""

    req = urllib.request.Request(str(url), headers={"user-agent": "ccindex-dashboard/1.0"})
    with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
        raw = resp.read()

    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, list):
        raise ValueError("collinfo payload is not a list")

    out_path = collinfo_cache_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "url": str(url),
        "path": str(out_path),
        "fetched_at": _iso_now(),
        "count": len(data),
    }


@contextmanager
def _collinfo_env_if_present() -> Iterable[None]:
    """Temporarily set $CC_COLLINFO_PATH if we have a cached collinfo.json."""

    old = os.environ.get("CC_COLLINFO_PATH")
    p = collinfo_cache_path()
    try:
        if p.exists():
            os.environ["CC_COLLINFO_PATH"] = str(p)
        yield
    finally:
        if old is None:
            os.environ.pop("CC_COLLINFO_PATH", None)
        else:
            os.environ["CC_COLLINFO_PATH"] = old


def _pipeline_config_path_default() -> Path:
    return Path(os.environ.get("CCINDEX_PIPELINE_CONFIG") or "pipeline_config.json")


def _load_pipeline_config_defaults() -> Dict[str, Any]:
    p = _pipeline_config_path_default()
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def default_orchestrator_settings() -> Dict[str, Any]:
    cfg = _load_pipeline_config_defaults()

    # Settings here are JSON-serializable primitives only.
    # (Paths are stored as strings.)
    return {
        "config_path": str(cfg.get("config_path") or _pipeline_config_path_default()),
        "ccindex_root": str(cfg.get("ccindex_root") or "/storage/ccindex"),
        "parquet_root": str(cfg.get("parquet_root") or "/storage/ccindex_parquet"),
        "duckdb_collection_root": str(cfg.get("duckdb_collection_root") or "/storage/ccindex_duckdb/cc_pointers_by_collection"),
        "duckdb_year_root": str(cfg.get("duckdb_year_root") or "/storage/ccindex_duckdb/cc_pointers_by_year"),
        "duckdb_master_root": str(cfg.get("duckdb_master_root") or "/storage/ccindex_duckdb/cc_pointers_master"),
        "build_domain_rowgroup_index": bool(cfg.get("build_domain_rowgroup_index", True)),
        "domain_rowgroup_index_root": str(cfg.get("domain_rowgroup_index_root") or "/storage/ccindex_duckdb/cc_domain_rowgroups_by_collection"),
        "domain_rowgroup_index_batch_size": int(cfg.get("domain_rowgroup_index_batch_size") or 1),
        "max_workers": int(cfg.get("max_workers") or 8),
        "collections_filter": cfg.get("collections_filter"),
        "heartbeat_seconds": int(cfg.get("heartbeat_seconds") or 30),
        "cleanup_extraneous": bool(cfg.get("cleanup_extraneous", True)),
        "cleanup_dry_run": bool(cfg.get("cleanup_dry_run", False)),
        "cleanup_source_archives": bool(cfg.get("cleanup_source_archives", True)),
        "sort_workers": cfg.get("sort_workers"),
        "sort_memory_per_worker_gb": float(cfg.get("sort_memory_per_worker_gb") or 4.0),
        "sort_temp_dir": str(cfg.get("sort_temp_dir") or "") or None,
        "force_reindex": bool(cfg.get("force_reindex", False)),
    }


def load_orchestrator_settings() -> Dict[str, Any]:
    p = orchestrator_settings_path()
    defaults = default_orchestrator_settings()
    if not p.exists():
        return defaults

    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return defaults

        out = dict(defaults)
        # Only accept known keys.
        for k in defaults.keys():
            if k in raw:
                out[k] = raw.get(k)

        # Normalize
        out["max_workers"] = int(out.get("max_workers") or defaults["max_workers"])
        out["heartbeat_seconds"] = int(out.get("heartbeat_seconds") or defaults["heartbeat_seconds"])
        out["cleanup_extraneous"] = bool(out.get("cleanup_extraneous"))
        out["cleanup_dry_run"] = bool(out.get("cleanup_dry_run"))
        out["cleanup_source_archives"] = bool(out.get("cleanup_source_archives"))
        out["sort_memory_per_worker_gb"] = float(out.get("sort_memory_per_worker_gb") or defaults["sort_memory_per_worker_gb"])
        out["force_reindex"] = bool(out.get("force_reindex"))

        out["build_domain_rowgroup_index"] = bool(out.get("build_domain_rowgroup_index"))
        out["domain_rowgroup_index_batch_size"] = int(out.get("domain_rowgroup_index_batch_size") or defaults["domain_rowgroup_index_batch_size"])

        if out.get("domain_rowgroup_index_root") is not None:
            s = str(out.get("domain_rowgroup_index_root") or "").strip()
            out["domain_rowgroup_index_root"] = s or None

        if out.get("collections_filter") is not None:
            s = str(out.get("collections_filter") or "").strip()
            out["collections_filter"] = s if s else None

        if out.get("sort_temp_dir") is not None:
            s = str(out.get("sort_temp_dir") or "").strip()
            out["sort_temp_dir"] = s or None

        sw = out.get("sort_workers")
        if sw is None or sw == "":
            out["sort_workers"] = None
        else:
            out["sort_workers"] = int(sw)

        return out
    except Exception:
        return defaults


def save_orchestrator_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    defaults = default_orchestrator_settings()
    # Merge updates into existing persisted settings (partial update semantics).
    existing = load_orchestrator_settings()
    out = dict(existing)
    for k in defaults.keys():
        if k in settings:
            out[k] = settings.get(k)

    # Validate + normalize.
    out["max_workers"] = int(out.get("max_workers") or defaults["max_workers"])
    out["heartbeat_seconds"] = int(out.get("heartbeat_seconds") or defaults["heartbeat_seconds"])
    out["sort_memory_per_worker_gb"] = float(out.get("sort_memory_per_worker_gb") or defaults["sort_memory_per_worker_gb"])

    out["build_domain_rowgroup_index"] = bool(out.get("build_domain_rowgroup_index"))
    out["domain_rowgroup_index_batch_size"] = int(out.get("domain_rowgroup_index_batch_size") or defaults["domain_rowgroup_index_batch_size"])
    if out.get("domain_rowgroup_index_root") is not None:
        s = str(out.get("domain_rowgroup_index_root") or "").strip()
        out["domain_rowgroup_index_root"] = s or None

    p = orchestrator_settings_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def build_pipeline_config(settings: Optional[Dict[str, Any]] = None) -> "object":
    """Build a `PipelineConfig` instance from persisted settings."""

    s = load_orchestrator_settings() if settings is None else dict(settings)

    from common_crawl_search_engine.ccindex.cc_pipeline_orchestrator import PipelineConfig

    return PipelineConfig(
        ccindex_root=Path(str(s.get("ccindex_root") or "/storage/ccindex")),
        parquet_root=Path(str(s.get("parquet_root") or "/storage/ccindex_parquet")),
        duckdb_collection_root=Path(str(s.get("duckdb_collection_root") or "/storage/ccindex_duckdb/cc_pointers_by_collection")),
        duckdb_year_root=Path(str(s.get("duckdb_year_root") or "/storage/ccindex_duckdb/cc_pointers_by_year")),
        duckdb_master_root=Path(str(s.get("duckdb_master_root") or "/storage/ccindex_duckdb/cc_pointers_master")),
        build_domain_rowgroup_index=bool(s.get("build_domain_rowgroup_index", True)),
        domain_rowgroup_index_root=(Path(str(s["domain_rowgroup_index_root"])) if s.get("domain_rowgroup_index_root") else None),
        domain_rowgroup_index_batch_size=int(s.get("domain_rowgroup_index_batch_size") or 1),
        max_workers=int(s.get("max_workers") or 8),
        memory_limit_gb=float(_load_pipeline_config_defaults().get("memory_limit_gb") or 10.0),
        min_free_space_gb=float(_load_pipeline_config_defaults().get("min_free_space_gb") or 50.0),
        collections_filter=s.get("collections_filter"),
        heartbeat_seconds=int(s.get("heartbeat_seconds") or 30),
        cleanup_extraneous=bool(s.get("cleanup_extraneous")),
        cleanup_dry_run=bool(s.get("cleanup_dry_run")),
        cleanup_source_archives=bool(s.get("cleanup_source_archives")),
        sort_workers=(int(s["sort_workers"]) if s.get("sort_workers") is not None else None),
        sort_memory_per_worker_gb=float(s.get("sort_memory_per_worker_gb") or 4.0),
        sort_temp_dir=(Path(str(s["sort_temp_dir"])) if s.get("sort_temp_dir") else None),
        force_reindex=bool(s.get("force_reindex")),
    )


def validate_collection_status(collection: str, *, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return orchestrator validator status for a single collection.

    This wraps the underlying validator output with a few normalized fields that
    are useful for the dashboard/UI:
    - ok: bool
    - fully_complete: bool (normalized from legacy `complete`)
    - size_on_disk_bytes: int (best-effort)
    - size_breakdown_bytes: dict[str,int]
    """

    cfg = build_pipeline_config(settings)
    from common_crawl_search_engine.ccindex.cc_pipeline_orchestrator import PipelineOrchestrator

    with _collinfo_env_if_present():
        orch = PipelineOrchestrator(cfg)
        status = orch.validator.validate_collection(str(collection))

    if not isinstance(status, dict):
        return {"ok": False, "error": "validator returned non-object", "collection": str(collection)}

    coll = str(status.get("collection") or collection)

    # Normalize completeness key naming.
    fully_complete = bool(status.get("fully_complete")) or bool(status.get("complete"))

    # Best-effort disk usage: duckdb + parquet + source gz (if present).
    sizes = _collection_disk_usage_bytes(coll, settings=settings)
    total_bytes = int(sum(int(v) for v in sizes.values() if isinstance(v, (int, float))))

    out = dict(status)
    out["ok"] = True
    out["fully_complete"] = fully_complete
    out["size_on_disk_bytes"] = int(total_bytes)
    out["size_breakdown_bytes"] = sizes
    return out


def _safe_sum_file_sizes(paths: Iterable[Path]) -> int:
    total = 0
    for p in paths:
        try:
            if p.exists() and p.is_file():
                total += int(p.stat().st_size)
        except Exception:
            continue
    return int(total)


def _collection_parquet_files(collection: str, *, parquet_root: Path) -> list[Path]:
    """Return best-effort list of parquet artifacts for a collection."""

    coll = str(collection)
    parts = coll.split("-")
    year = parts[2] if len(parts) > 2 else None

    candidates: list[Path] = []
    if year:
        candidates.append(parquet_root / "cc_pointers_by_collection" / year / coll)
        candidates.append(parquet_root / year / coll)
    candidates.append(parquet_root)

    out: list[Path] = []
    seen: set[str] = set()
    for d in candidates:
        if not d.exists() or not d.is_dir():
            continue

        globs = [
            "cdx-*.gz.parquet",
            "cdx-*.gz.sorted.parquet",
            f"{coll}-cdx-*.gz.parquet",
            f"{coll}-cdx-*.gz.sorted.parquet",
        ]
        for pat in globs:
            for fp in d.glob(pat):
                key = str(fp.resolve())
                if key in seen:
                    continue
                seen.add(key)
                out.append(fp)
    return out


def _collection_gz_files(collection: str, *, ccindex_root: Path) -> list[Path]:
    coll_dir = ccindex_root / str(collection)
    if not coll_dir.exists() or not coll_dir.is_dir():
        return []
    return [p for p in coll_dir.glob("cdx-*.gz") if p.is_file()]


def _collection_duckdb_files(collection: str, *, duckdb_collection_root: Path) -> list[Path]:
    """Return duckdb + sidecar files (.wal/.shm) for a collection."""

    base = duckdb_collection_root / f"{collection}.duckdb"
    files = [base, base.with_suffix(base.suffix + ".wal"), base.with_suffix(base.suffix + ".shm")]
    # Some runs may have variant filenames; include any matching duckdb in the dir.
    try:
        if duckdb_collection_root.exists() and duckdb_collection_root.is_dir():
            for fp in duckdb_collection_root.glob(f"{collection}*.duckdb*"):
                if fp.is_file():
                    files.append(fp)
    except Exception:
        pass
    # De-dupe
    seen: set[str] = set()
    out: list[Path] = []
    for p in files:
        try:
            key = str(p.resolve())
        except Exception:
            key = str(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def _collection_disk_usage_bytes(collection: str, *, settings: Optional[Dict[str, Any]] = None) -> dict[str, int]:
    """Best-effort disk usage for a collection across pipeline artifacts."""

    cfg = build_pipeline_config(settings)
    # PipelineConfig uses these attribute names.
    ccindex_root = Path(getattr(cfg, "ccindex_root"))
    parquet_root = Path(getattr(cfg, "parquet_root"))
    duckdb_collection_root = Path(getattr(cfg, "duckdb_collection_root"))

    gz_bytes = _safe_sum_file_sizes(_collection_gz_files(collection, ccindex_root=ccindex_root))
    parquet_bytes = _safe_sum_file_sizes(_collection_parquet_files(collection, parquet_root=parquet_root))
    duckdb_bytes = _safe_sum_file_sizes(_collection_duckdb_files(collection, duckdb_collection_root=duckdb_collection_root))

    return {
        "tar_gz_bytes": int(gz_bytes),
        "parquet_bytes": int(parquet_bytes),
        "duckdb_bytes": int(duckdb_bytes),
    }


def validate_collections_status(
    collections: list[str],
    *,
    settings: Optional[Dict[str, Any]] = None,
    parallelism: int = 8,
) -> Dict[str, Any]:
    """Validate many collections, returning a mapping and a small summary."""

    from concurrent.futures import ThreadPoolExecutor, as_completed

    cols = [str(c).strip() for c in collections if str(c).strip()]
    cols = sorted(set(cols))
    if not cols:
        return {"ok": True, "collections": {}, "summary": {"total": 0}}

    results: dict[str, Any] = {}
    par = max(1, int(parallelism or 1))

    def _one(c: str) -> Any:
        try:
            return validate_collection_status(c, settings=settings)
        except Exception as e:
            return {"ok": False, "error": str(e), "collection": c}

    with ThreadPoolExecutor(max_workers=par) as ex:
        futs = {ex.submit(_one, c): c for c in cols}
        for fut in as_completed(futs):
            c = futs[fut]
            results[c] = fut.result()

    # Basic summary: count normalized fully_complete.
    complete = 0
    total_bytes = 0
    for _c, st in results.items():
        if isinstance(st, dict):
            if st.get("fully_complete") is True:
                complete += 1
            try:
                total_bytes += int(st.get("size_on_disk_bytes") or 0)
            except Exception:
                pass

    return {
        "ok": True,
        "collections": results,
        "summary": {"total": len(cols), "fully_complete": complete, "size_on_disk_bytes": int(total_bytes)},
    }


def delete_collection_index(collection: str, *, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Delete per-collection DuckDB index artifacts so the next run rebuilds."""

    cfg = build_pipeline_config(settings)
    from common_crawl_search_engine.ccindex.cc_pipeline_orchestrator import PipelineOrchestrator

    with _collinfo_env_if_present():
        orch = PipelineOrchestrator(cfg)
        before = orch.validator.validate_collection(str(collection))

        # This method deletes the index + marker/wal/shm files.
        orch._invalidate_duckdb_index(str(collection))  # type: ignore[attr-defined]

        after = orch.validator.validate_collection(str(collection))
    return {"collection": str(collection), "before": before, "after": after}


def delete_collection_indexes(
    collections: list[str],
    *,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cols = [str(c).strip() for c in collections if str(c).strip()]
    cols = sorted(set(cols))
    out: dict[str, Any] = {}
    for c in cols:
        try:
            out[c] = delete_collection_index(c, settings=settings)
        except Exception as e:
            out[c] = {"ok": False, "error": str(e)}
    return {"ok": True, "collections": out, "summary": {"total": len(cols)}}


def _logs_dir() -> Path:
    d = Path(os.environ.get("CCINDEX_LOG_DIR") or "logs")
    d.mkdir(parents=True, exist_ok=True)
    return d


@dataclass(frozen=True)
class OrchestratorJob:
    pid: int
    log_path: str
    cmd: list[str]


def _jobs_registry_path() -> Path:
    p = os.environ.get("CCINDEX_JOBS_REGISTRY_PATH")
    if p:
        return Path(p).expanduser().resolve()
    return _state_dir() / "orchestrator_jobs.jsonl"


def _append_job_record(rec: dict[str, Any]) -> None:
    p = _jobs_registry_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def list_jobs(*, limit: int = 50) -> list[dict[str, Any]]:
    p = _jobs_registry_path()
    if not p.exists():
        return []
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for line in lines[-max(1, int(limit)) :]:
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
        except Exception:
            continue
    # newest first
    return list(reversed(out))


def _parse_progress_from_tail(tail: str) -> dict[str, Any]:
    """Heuristic log parsing: infer current stage/collection from recent lines."""

    stage = None
    collection = None
    last_line = None
    for line in tail.splitlines()[::-1]:
        s = line.strip()
        if not s:
            continue
        if last_line is None:
            last_line = s
        # orchestrator uses log prefixes like "[cleanup]", "[download]", etc.
        if s.startswith("[") and "]" in s:
            stage = s[1 : s.index("]")]
        # common patterns: "Sweeping CC-MAIN-...." or "Downloading CC-MAIN-...."
        if "CC-MAIN-" in s:
            idx = s.find("CC-MAIN-")
            tok = s[idx:].split()[0].rstrip(".:,)")
            if tok.startswith("CC-MAIN-"):
                collection = tok
        if stage and collection:
            break
    return {"stage": stage, "collection": collection, "last_line": last_line}


def plan_orchestrator_command(
    *,
    mode: JobMode,
    filter: Optional[str] = None,
    workers: Optional[int] = None,
    force_reindex: Optional[bool] = None,
    resume: Optional[bool] = None,
    cleanup_dry_run: Optional[bool] = None,
    yes: Optional[bool] = None,
    heartbeat_seconds: Optional[int] = None,
    sort_workers: Optional[int] = None,
    sort_memory_per_worker_gb: Optional[float] = None,
    sort_temp_dir: Optional[str] = None,
    build_domain_rowgroup_index: Optional[bool] = None,
    domain_rowgroup_index_root: Optional[str] = None,
    domain_rowgroup_index_batch_size: Optional[int] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    s = load_orchestrator_settings() if settings is None else dict(settings)

    cmd = [sys.executable, "-m", "common_crawl_search_engine.ccindex.cc_pipeline_orchestrator"]

    config_path = str(s.get("config_path") or _pipeline_config_path_default())
    if config_path:
        cmd += ["--config", config_path]

    cmd += ["--ccindex-root", str(s.get("ccindex_root"))]
    cmd += ["--parquet-root", str(s.get("parquet_root"))]

    eff_workers = int(workers) if workers is not None else int(s.get("max_workers") or 8)
    cmd += ["--workers", str(eff_workers)]

    eff_filter = filter if filter is not None else s.get("collections_filter")
    if eff_filter:
        cmd += ["--filter", str(eff_filter)]

    eff_hb = int(heartbeat_seconds) if heartbeat_seconds is not None else int(s.get("heartbeat_seconds") or 30)
    cmd += ["--heartbeat-seconds", str(eff_hb)]

    # Rowgroup-slice index build knobs (can be overridden per run)
    eff_build_rg = bool(build_domain_rowgroup_index) if build_domain_rowgroup_index is not None else bool(s.get("build_domain_rowgroup_index", True))
    if eff_build_rg:
        cmd += ["--build-domain-rowgroup-index"]
    else:
        cmd += ["--no-build-domain-rowgroup-index"]

    if domain_rowgroup_index_root is not None:
        eff_rg_root = str(domain_rowgroup_index_root or "").strip() or None
    else:
        eff_rg_root = str(s.get("domain_rowgroup_index_root") or "").strip() or None
    if eff_rg_root:
        cmd += ["--domain-rowgroup-index-root", str(eff_rg_root)]

    try:
        if domain_rowgroup_index_batch_size is not None:
            batch_sz = int(domain_rowgroup_index_batch_size)
        else:
            batch_sz = int(s.get("domain_rowgroup_index_batch_size") or 1)
    except Exception:
        batch_sz = 1
    cmd += ["--domain-rowgroup-index-batch-size", str(max(1, batch_sz))]

    if bool(cleanup_dry_run) if cleanup_dry_run is not None else bool(s.get("cleanup_dry_run")):
        cmd += ["--cleanup-dry-run"]

    eff_cleanup_extraneous = bool(s.get("cleanup_extraneous"))
    if not eff_cleanup_extraneous:
        cmd += ["--no-cleanup-extraneous"]

    eff_cleanup_source = bool(s.get("cleanup_source_archives"))
    if not eff_cleanup_source:
        cmd += ["--no-cleanup-source-archives"]

    if yes:
        cmd += ["--yes"]

    eff_force = bool(force_reindex) if force_reindex is not None else bool(s.get("force_reindex"))
    if eff_force:
        cmd += ["--force-reindex"]

    if resume is False:
        # CLI default is resume=True; there is no --no-resume, so we omit/keep default.
        pass

    eff_sort_workers = sort_workers if sort_workers is not None else s.get("sort_workers")
    if eff_sort_workers is not None:
        cmd += ["--sort-workers", str(int(eff_sort_workers))]

    eff_sort_mem = float(sort_memory_per_worker_gb) if sort_memory_per_worker_gb is not None else float(s.get("sort_memory_per_worker_gb") or 4.0)
    cmd += ["--sort-memory-per-worker-gb", str(eff_sort_mem)]

    eff_sort_tmp = sort_temp_dir if sort_temp_dir is not None else s.get("sort_temp_dir")
    if eff_sort_tmp:
        cmd += ["--sort-temp-dir", str(eff_sort_tmp)]

    if mode == "download_only":
        cmd += ["--download-only"]
    elif mode == "cleanup_only":
        cmd += ["--cleanup-only"]
    elif mode == "build_meta_indexes":
        # Meta-index builds happen automatically after full-year runs.
        # Here we force a "pipeline" run for the year filter; callers should set filter='2024'.
        # (A dedicated CLI flag doesn't exist today.)
        pass
    elif mode == "pipeline":
        pass
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return {"cmd": cmd}


def start_orchestrator_job(*, planned: Dict[str, Any], label: str = "orchestrator") -> OrchestratorJob:
    cmd = planned.get("cmd")
    if not isinstance(cmd, list) or not all(isinstance(x, str) for x in cmd):
        raise ValueError("planned.cmd must be a list[str]")

    ts = time.strftime("%Y%m%d_%H%M%S")
    log_path = _logs_dir() / f"{label}_{ts}.log"

    # Detach and stream stdout/stderr into a file.
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"# started {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# cmd: {' '.join(cmd)}\n")
        f.flush()

        env = dict(os.environ)
        # Ensure validator/orchestrator sees our freshest collinfo, if present.
        cp = collinfo_cache_path()
        if cp.exists():
            env["CC_COLLINFO_PATH"] = str(cp)

        proc = subprocess.Popen(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(Path.cwd()),
            env=env,
        )

    job = OrchestratorJob(pid=int(proc.pid), log_path=str(log_path), cmd=list(cmd))
    _append_job_record({
        "pid": job.pid,
        "log_path": job.log_path,
        "cmd": job.cmd,
        "label": str(label),
        "started_at": _iso_now(),
    })
    return job


def job_is_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def stop_job(pid: int, *, sig: str = "TERM") -> Dict[str, Any]:
    signame = str(sig or "TERM").upper()
    signum = signal.SIGTERM
    if signame in {"KILL", "SIGKILL"}:
        signum = signal.SIGKILL
    elif signame in {"INT", "SIGINT"}:
        signum = signal.SIGINT

    alive_before = job_is_alive(int(pid))
    if alive_before:
        os.kill(int(pid), int(signum))

    return {"pid": int(pid), "signal": signame, "alive_before": alive_before, "alive_after": job_is_alive(int(pid))}


def tail_file(path: str, *, lines: int = 200) -> str:
    p = Path(str(path))
    if not p.exists():
        return ""

    n = max(1, int(lines))
    try:
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            data = f.read().splitlines()
        return "\n".join(data[-n:])
    except Exception:
        return ""


def job_status(*, pid: int | None = None, log_path: str | None = None, lines: int = 200) -> Dict[str, Any]:
    lp = str(log_path or "").strip() or None
    p = int(pid) if pid is not None else None
    tail = tail_file(lp, lines=int(lines)) if lp else ""
    return {
        "ok": True,
        "pid": p,
        "alive": (job_is_alive(p) if p else None),
        "log_path": lp,
        "tail": tail,
        "progress": _parse_progress_from_tail(tail) if tail else {"stage": None, "collection": None, "last_line": None},
    }
