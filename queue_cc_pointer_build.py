#!/usr/bin/env python3
"""Queue-based launcher for Common Crawl pointer-index builds.

What this is
- A supervisor that runs *one build process per collection* (CC-MAIN-YYYY-WW).
- Maintains a queue and runs up to --max-parallel collections at once.
- Each worker process runs build_cc_pointer_duckdb.py with:
    --shard-by-collection --collections <single-collection>
  so each collection produces exactly:
    - one DuckDB pointer DB: cc_pointers_<collection>.duckdb
    - ~300 Parquet files (one per cdx-*.gz shard), ZSTD-compressed by default

Why this exists
- The old round-robin launcher could bundle multiple collections into a single
  process, which increases per-process memory pressure (multiple open DuckDB
  connections + multiple Parquet writers over time).
- This queue launcher keeps the unit of work = one collection, which makes it
  easier to scale parallelism safely and recover cleanly.

Resumability
- build_cc_pointer_duckdb.py is resumable via cc_ingested_files, so restarting a
  collection simply skips already-ingested shard files.

This script is stdlib-only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen
from typing import Any, Dict, List, Optional, Tuple

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None


def _now() -> float:
    return time.time()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _parse_meminfo_kb() -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split(":", 1)
                if len(parts) != 2:
                    continue
                key = parts[0].strip()
                rest = parts[1].strip().split()
                if not rest:
                    continue
                try:
                    out[key] = int(rest[0])
                except Exception:
                    continue
    except Exception:
        pass
    return out


def _mem_available_gib() -> float:
    mi = _parse_meminfo_kb()
    kb = float(mi.get("MemAvailable", 0))
    return kb / 1024.0 / 1024.0


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def _abspath_preserve_symlinks(path: Path) -> Path:
    return Path(os.path.abspath(str(path.expanduser())))


def _list_collections(input_root: Path, collections_regex: Optional[str]) -> List[str]:
    rx = re.compile(collections_regex) if collections_regex else None
    cols: List[str] = []
    for entry in sorted(input_root.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if rx and not rx.search(name):
            continue
        cols.append(name)
    return cols


def _collection_has_any_cdx_shard(input_root: Path, collection: str) -> bool:
    """Fast existence check for at least one cdx-*.gz shard in a collection dir."""
    try:
        col_dir = input_root / collection
        with os.scandir(col_dir) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                name = entry.name
                if name.startswith("cdx-") and name.endswith(".gz"):
                    return True
    except Exception:
        return False
    return False


@dataclass
class Running:
    collection: str
    proc: Popen
    log_file: Path
    started_at: float
    attempts: int


def _build_cmd(
    *,
    python_exe: Path,
    build_script: Path,
    input_root: Path,
    db_dir: Path,
    parquet_out: Optional[Path],
    progress_dir: Optional[Path],
    progress_interval_seconds: int,
    threads: int,
    batch_rows: Optional[int],
    memory_limit_gib: Optional[float],
    parquet_compression: str,
    parquet_compression_level: Optional[int],
    collection: str,
) -> List[str]:
    cmd: List[str] = [
        str(python_exe),
        str(build_script),
        "--input-root",
        str(input_root),
        "--db",
        str(db_dir),
        "--shard-by-collection",
        "--threads",
        str(int(threads)),
        "--progress-interval-seconds",
        str(int(progress_interval_seconds)),
        "--parquet-compression",
        str(parquet_compression),
    ]

    if parquet_out is not None:
        cmd += ["--parquet-out", str(parquet_out)]

    if progress_dir is not None:
        cmd += ["--progress-dir", str(progress_dir)]

    if batch_rows is not None:
        cmd += ["--batch-rows", str(int(batch_rows))]

    if memory_limit_gib is not None:
        cmd += ["--memory-limit-gib", str(float(memory_limit_gib))]

    if parquet_compression_level is not None:
        cmd += ["--parquet-compression-level", str(int(parquet_compression_level))]

    cmd += ["--collections", str(collection)]
    return cmd


def _stop_proc(proc: Popen, grace_seconds: int) -> None:
    try:
        if proc.poll() is not None:
            return
        proc.send_signal(signal.SIGINT)
    except Exception:
        return

    deadline = _now() + float(max(0, grace_seconds))
    while _now() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.5)

    try:
        if proc.poll() is None:
            proc.kill()
    except Exception:
        return


def main() -> int:
    # When stdout is redirected (e.g. "> queue_launcher.log"), Python will fully-buffer
    # writes by default. Enable line buffering so the log stays live.
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="Queue-based per-collection pointer-index builder")
    ap.add_argument("--input-root", required=True, type=str, help="Root folder containing CC shards (e.g. /storage/ccindex)")
    ap.add_argument("--db-dir", required=True, type=str, help="Directory for cc_pointers_<collection>.duckdb outputs")
    ap.add_argument("--parquet-out", type=str, default=None, help="Parquet output root (one Parquet per shard file)")
    ap.add_argument("--collections-regex", type=str, default=None, help="Regex to select collections under --input-root")
    ap.add_argument(
        "--collections",
        action="append",
        default=None,
        help="Explicit collections list (repeatable). If provided, skips filesystem enumeration.",
    )

    ap.add_argument(
        "--skip-empty-collections",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip collections with no cdx-*.gz shards present under --input-root (default: true)",
    )

    ap.add_argument("--max-parallel", type=int, default=8, help="Max concurrent collections")
    ap.add_argument("--threads-per-worker", type=int, default=2, help="DuckDB threads per build process")
    ap.add_argument("--batch-rows", type=int, default=None, help="Rows per insert batch (passed through)")
    ap.add_argument("--memory-limit-gib", type=float, default=None, help="DuckDB memory_limit per process (GiB)")

    ap.add_argument("--progress-dir", type=str, default=None, help="Progress snapshot dir (passed through)")
    ap.add_argument("--progress-interval-seconds", type=int, default=30, help="Snapshot interval (passed through)")

    ap.add_argument("--parquet-compression", type=str, default="zstd", choices=["zstd", "snappy", "gzip"], help="Parquet compression")
    ap.add_argument("--parquet-compression-level", type=int, default=None, help="Parquet compression level")

    ap.add_argument("--python", type=str, default=None, help="Python executable to run builder with (default: current interpreter)")
    ap.add_argument("--log-dir", type=str, default=None, help="Where to write per-collection logs (default: --db-dir)")
    ap.add_argument("--state-file", type=str, default=None, help="Path to JSON state (default: <db-dir>/queue_state.json)")

    ap.add_argument("--min-mem-to-start-gib", type=float, default=0.0, help="Only start new workers if MemAvailable >= this")
    ap.add_argument("--poll-interval", type=int, default=5, help="Seconds between scheduler ticks")

    ap.add_argument("--max-attempts", type=int, default=5, help="Max attempts per collection before giving up")
    ap.add_argument("--retry-backoff-seconds", type=int, default=60, help="Base backoff for retrying failed collections")

    ap.add_argument("--stop-grace-seconds", type=int, default=30, help="Grace period when stopping on SIGINT")
    ap.add_argument("--dry-run", action="store_true", default=False, help="Print plan and exit")

    args = ap.parse_args()

    input_root = Path(args.input_root).expanduser().resolve()
    db_dir = Path(args.db_dir).expanduser().resolve()
    parquet_out = Path(args.parquet_out).expanduser().resolve() if args.parquet_out else None
    log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else db_dir
    progress_dir = Path(args.progress_dir).expanduser().resolve() if args.progress_dir else db_dir

    db_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    progress_dir.mkdir(parents=True, exist_ok=True)

    python_exe = _abspath_preserve_symlinks(Path(args.python)) if args.python else _abspath_preserve_symlinks(Path(sys.executable))
    build_script = (Path(__file__).parent / "build_cc_pointer_duckdb.py").resolve()
    if not build_script.exists():
        raise SystemExit(f"Missing build script: {build_script}")

    if args.collections:
        collections = [c for c in args.collections if c]
    else:
        collections = _list_collections(input_root, args.collections_regex)

    if args.skip_empty_collections:
        before = len(collections)
        collections = [c for c in collections if _collection_has_any_cdx_shard(input_root, c)]
        skipped = before - len(collections)
        if skipped:
            print(f"info\tskipped_empty\tcount={skipped}", flush=True)

    if not collections:
        raise SystemExit("No collections selected")

    max_parallel = max(1, int(args.max_parallel))

    state_path = Path(args.state_file).expanduser().resolve() if args.state_file else (db_dir / "queue_state.json")

    # State: per collection attempts + next eligible time.
    attempts: Dict[str, int] = {}
    next_ok: Dict[str, float] = {}
    completed: Dict[str, bool] = {}

    if state_path.exists():
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                attempts = {str(k): int(v) for k, v in (data.get("attempts") or {}).items()} if isinstance(data.get("attempts"), dict) else {}
                next_ok = {str(k): float(v) for k, v in (data.get("next_ok") or {}).items()} if isinstance(data.get("next_ok"), dict) else {}
                completed = {str(k): bool(v) for k, v in (data.get("completed") or {}).items()} if isinstance(data.get("completed"), dict) else {}
        except Exception:
            pass

    # If the selected collection set changed (e.g. skipping empties), prune state.
    sel = set(collections)
    attempts = {k: v for k, v in attempts.items() if k in sel}
    next_ok = {k: v for k, v in next_ok.items() if k in sel}
    completed = {k: v for k, v in completed.items() if k in sel}

    def persist_state(running: List[Running]) -> None:
        payload: Dict[str, Any] = {
            "updated_at_epoch": _now(),
            "mem_avail_gib": _mem_available_gib(),
            "selected_collections": list(collections),
            "attempts": attempts,
            "next_ok": next_ok,
            "completed": completed,
            "running": [
                {
                    "collection": r.collection,
                    "pid": int(r.proc.pid),
                    "started_at_epoch": r.started_at,
                    "log_file": str(r.log_file),
                    "attempts": int(r.attempts),
                }
                for r in running
                if r.proc.pid is not None
            ],
        }
        _atomic_write_json(state_path, payload)

    # Determine whether a collection is fully done by comparing progress snapshot files count to expected.
    # Prefer querying the collection DB (cc_ingested_files) if duckdb is available; fall back to snapshots.
    def _expected_shards_for_collection(col: str) -> int:
        n = 0
        try:
            for f in (input_root / col).iterdir():
                if f.is_file() and f.name.startswith("cdx-") and f.name.endswith(".gz"):
                    n += 1
        except Exception:
            return 0
        return n

    def _db_path_for_collection(col: str) -> Path:
        return db_dir / f"cc_pointers_{col}.duckdb"

    def _ingested_files_from_db(col: str) -> Optional[int]:
        if duckdb is None:
            return None
        db_path = _db_path_for_collection(col)
        if not db_path.exists():
            return None
        try:
            con = duckdb.connect(str(db_path), read_only=True, config={"lock_configuration": "none"})
            try:
                row = con.execute("SELECT count(*) FROM cc_ingested_files").fetchone()
                n = int(row[0]) if row and row[0] is not None else 0
                return n
            finally:
                con.close()
        except Exception:
            return None

    def _snapshot_path_for_collection(col: str) -> Path:
        # builder uses shard_key == collection in per-collection mode
        safe = re.sub(r"[^a-zA-Z0-9._-]", "_", col or "unknown")
        return progress_dir / f"progress_{safe}.json"

    def _is_completed(col: str) -> bool:
        if completed.get(col):
            return True

        exp = _expected_shards_for_collection(col)
        if exp > 0:
            db_n = _ingested_files_from_db(col)
            if db_n is not None and int(db_n) >= int(exp):
                completed[col] = True
                return True

        snap_path = _snapshot_path_for_collection(col)
        if not snap_path.exists():
            return False
        try:
            snap = json.loads(snap_path.read_text(encoding="utf-8"))
            if not isinstance(snap, dict):
                return False
            ing_files = int(snap.get("ingested_files", 0) or 0)
            last_event = str(snap.get("last_event") or "")
            if exp > 0 and ing_files >= exp and last_event == "ingested":
                completed[col] = True
                return True
        except Exception:
            return False
        return False

    # Build plan and optionally dry-run.
    print(f"Selected collections: {len(collections)}", flush=True)
    print(
        f"max_parallel={max_parallel}\tthreads_per_worker={int(args.threads_per_worker)}\tmemory_limit_gib={args.memory_limit_gib}",
        flush=True,
    )
    print(f"db_dir={db_dir}", flush=True)
    if parquet_out is not None:
        print(f"parquet_out={parquet_out}\tcompression={args.parquet_compression}", flush=True)
    print(f"progress_dir={progress_dir}", flush=True)
    print(f"state_file={state_path}", flush=True)

    if args.dry_run:
        for c in collections:
            cmd = _build_cmd(
                python_exe=python_exe,
                build_script=build_script,
                input_root=input_root,
                db_dir=db_dir,
                parquet_out=parquet_out,
                progress_dir=progress_dir,
                progress_interval_seconds=int(args.progress_interval_seconds),
                threads=int(args.threads_per_worker),
                batch_rows=(int(args.batch_rows) if args.batch_rows is not None else None),
                memory_limit_gib=(float(args.memory_limit_gib) if args.memory_limit_gib is not None else None),
                parquet_compression=str(args.parquet_compression),
                parquet_compression_level=(int(args.parquet_compression_level) if args.parquet_compression_level is not None else None),
                collection=c,
            )
            print(" ".join(shlex.quote(x) for x in cmd))
        return 0

    running: List[Running] = []

    def _start_collection(col: str) -> None:
        attempts[col] = int(attempts.get(col, 0)) + 1
        log_file = log_dir / f"build_{col}.log"
        cmd = _build_cmd(
            python_exe=python_exe,
            build_script=build_script,
            input_root=input_root,
            db_dir=db_dir,
            parquet_out=parquet_out,
            progress_dir=progress_dir,
            progress_interval_seconds=int(args.progress_interval_seconds),
            threads=int(args.threads_per_worker),
            batch_rows=(int(args.batch_rows) if args.batch_rows is not None else None),
            memory_limit_gib=(float(args.memory_limit_gib) if args.memory_limit_gib is not None else None),
            parquet_compression=str(args.parquet_compression),
            parquet_compression_level=(int(args.parquet_compression_level) if args.parquet_compression_level is not None else None),
            collection=col,
        )

        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"

        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "ab", buffering=0) as log_f:
            log_f.write(f"\n# queue_start_at={_now()}\n".encode("utf-8"))
            log_f.write((" ".join(cmd) + "\n").encode("utf-8"))
            log_f.flush()
            proc = Popen(cmd, stdout=log_f, stderr=log_f, close_fds=True, env=env)

        running.append(Running(collection=col, proc=proc, log_file=log_file, started_at=_now(), attempts=int(attempts[col])))
        print(
            f"action\tstart\tcollection={col}\tpid={proc.pid}\tattempt={attempts[col]}\tmem_avail_gib={_mem_available_gib():.1f}",
            flush=True,
        )

    def _stop_all(reason: str) -> None:
        print(f"signal\t{reason}\tstopping", flush=True)
        for r in list(running):
            _stop_proc(r.proc, grace_seconds=int(args.stop_grace_seconds))
        persist_state(running=[])
        raise SystemExit(130)

    def _on_sigint(_signum: int, _frame: object) -> None:
        _stop_all("SIGINT")

    def _on_sigterm(_signum: int, _frame: object) -> None:
        _stop_all("SIGTERM")

    def _on_sighup(_signum: int, _frame: object) -> None:
        _stop_all("SIGHUP")

    signal.signal(signal.SIGINT, _on_sigint)
    signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        signal.signal(signal.SIGHUP, _on_sighup)
    except Exception:
        pass

    while True:
        # Reap finished
        still: List[Running] = []
        for r in running:
            rc = r.proc.poll()
            if rc is None:
                still.append(r)
                continue

            if _is_completed(r.collection):
                completed[r.collection] = True

            status = "ok" if rc == 0 else "fail"
            print(f"event\texit\tcollection={r.collection}\tpid={r.proc.pid}\trc={rc}\tstatus={status}", flush=True)

            if rc == 0 and not completed.get(r.collection, False):
                # Avoid tight respawn loops if the process exits quickly but completion criteria
                # hasn't been observed yet (e.g., stale/missing snapshots). Apply a small backoff.
                next_ok[r.collection] = _now() + float(max(5, int(args.retry_backoff_seconds)))
            elif rc != 0 and not completed.get(r.collection, False):
                # backoff + retry
                if int(attempts.get(r.collection, 0)) < int(args.max_attempts):
                    backoff = int(args.retry_backoff_seconds) * (2 ** max(0, int(attempts.get(r.collection, 1)) - 1))
                    next_ok[r.collection] = _now() + float(backoff)
                    print(f"event\tbackoff\tcollection={r.collection}\tseconds={backoff}", flush=True)
                else:
                    print(f"event\tgive_up\tcollection={r.collection}\tattempts={attempts.get(r.collection)}", flush=True)

        running = still

        # Mark completed based on snapshots even if never started (e.g. resumed run)
        for col in collections:
            if not completed.get(col) and _is_completed(col):
                completed[col] = True

        persist_state(running)

        # Done?
        done_count = sum(1 for c in collections if completed.get(c))
        if done_count >= len(collections):
            print(f"done\tcollections={done_count}/{len(collections)}", flush=True)
            return 0

        # Start new work if slots free and memory OK
        mem_av = _mem_available_gib()
        if mem_av < float(args.min_mem_to_start_gib):
            time.sleep(max(1, int(args.poll_interval)))
            continue

        slots = max_parallel - len(running)
        if slots <= 0:
            time.sleep(max(1, int(args.poll_interval)))
            continue

        # Find startable collections
        startable: List[str] = []
        running_cols = {r.collection for r in running}
        now = _now()
        for col in collections:
            if col in running_cols:
                continue
            if completed.get(col):
                continue
            if now < float(next_ok.get(col, 0.0) or 0.0):
                continue
            exp = _expected_shards_for_collection(col)
            if exp <= 0:
                # No input shards present yet (or the collection dir is empty).
                # Do not consume attempts or permanently block completion; just wait and recheck.
                next_ok[col] = now + float(max(5, int(args.retry_backoff_seconds)))
                # If an earlier buggy run exhausted attempts, clear it so this collection can
                # run later once shards appear.
                if int(attempts.get(col, 0)) >= int(args.max_attempts):
                    attempts[col] = 0
                print(f"event\twait_no_input\tcollection={col}\tseconds={int(args.retry_backoff_seconds)}", flush=True)
                continue

            if int(attempts.get(col, 0)) >= int(args.max_attempts):
                continue
            startable.append(col)

        # Start up to slots
        for col in startable[: max(0, slots)]:
            _start_collection(col)

        time.sleep(max(1, int(args.poll_interval)))


if __name__ == "__main__":
    raise SystemExit(main())
