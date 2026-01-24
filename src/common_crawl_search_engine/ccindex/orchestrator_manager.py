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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional


JobMode = Literal["pipeline", "download_only", "cleanup_only", "build_meta_indexes"]


def _state_dir() -> Path:
    d = Path(os.environ.get("CCINDEX_STATE_DIR") or "state")
    d.mkdir(parents=True, exist_ok=True)
    return d


def orchestrator_settings_path() -> Path:
    p = os.environ.get("CCINDEX_ORCHESTRATOR_SETTINGS_PATH")
    if p:
        return Path(p).expanduser().resolve()
    return _state_dir() / "orchestrator_settings.json"


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
    out = dict(defaults)
    for k in defaults.keys():
        if k in settings:
            out[k] = settings.get(k)

    # Validate + normalize.
    out["max_workers"] = int(out.get("max_workers") or defaults["max_workers"])
    out["heartbeat_seconds"] = int(out.get("heartbeat_seconds") or defaults["heartbeat_seconds"])
    out["sort_memory_per_worker_gb"] = float(out.get("sort_memory_per_worker_gb") or defaults["sort_memory_per_worker_gb"])

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
    """Return orchestrator validator status for a single collection."""

    cfg = build_pipeline_config(settings)
    from common_crawl_search_engine.ccindex.cc_pipeline_orchestrator import PipelineOrchestrator

    orch = PipelineOrchestrator(cfg)
    status = orch.validator.validate_collection(str(collection))
    return status


def delete_collection_index(collection: str, *, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Delete per-collection DuckDB index artifacts so the next run rebuilds."""

    cfg = build_pipeline_config(settings)
    from common_crawl_search_engine.ccindex.cc_pipeline_orchestrator import PipelineOrchestrator

    orch = PipelineOrchestrator(cfg)
    before = orch.validator.validate_collection(str(collection))

    # This method deletes the index + marker/wal/shm files.
    orch._invalidate_duckdb_index(str(collection))  # type: ignore[attr-defined]

    after = orch.validator.validate_collection(str(collection))
    return {"collection": str(collection), "before": before, "after": after}


def _logs_dir() -> Path:
    d = Path(os.environ.get("CCINDEX_LOG_DIR") or "logs")
    d.mkdir(parents=True, exist_ok=True)
    return d


@dataclass(frozen=True)
class OrchestratorJob:
    pid: int
    log_path: str
    cmd: list[str]


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

        proc = subprocess.Popen(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(Path.cwd()),
        )

    return OrchestratorJob(pid=int(proc.pid), log_path=str(log_path), cmd=list(cmd))


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
