#!/usr/bin/env python3
"""Build CID-keyed embeddings parquet files for state admin rules.

Embeds only semantic text columns from the CID-indexed source parquet:
- name
- text
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from ipfs_datasets_py import embeddings_router


def _iter_input_parquets(input_dir: Path, recursive: bool, include_combined: bool) -> Iterable[Path]:
    globber = input_dir.rglob if recursive else input_dir.glob
    for path in sorted(globber("*.parquet")):
        if path.name.endswith("_embeddings.parquet"):
            continue
        if not include_combined and path.name == "state_admin_rules_all_states.parquet":
            continue
        if path.is_file():
            yield path


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _semantic_text(name: Any, text: Any, min_chars: int) -> str:
    parts: List[str] = []
    n = _norm(name)
    t = _norm(text)
    if len(n) >= min_chars:
        parts.append(n)
    if len(t) >= min_chars:
        parts.append(t)
    return "\n\n".join(parts).strip()


def _embed_rows(
    frame: pd.DataFrame,
    *,
    model: str,
    provider: str,
    device: str,
    router_batch_size: int,
    flush_size: int,
    min_chars: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    required_cols = {"ipfs_cid", "state_code", "name", "text"}
    missing = [c for c in required_cols if c not in frame.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")

    total_rows = len(frame)
    seen_cids: set[str] = set()
    pending_cids: List[str] = []
    pending_states: List[str] = []
    pending_texts: List[str] = []

    out_cids: List[str] = []
    out_states: List[str] = []
    out_semantic_texts: List[str] = []
    out_embeddings: List[List[float]] = []

    def flush() -> None:
        if not pending_texts:
            return
        vectors = embeddings_router.embed_texts_batched(
            pending_texts,
            batch_size=router_batch_size,
            model_name=model,
            provider=provider,
            device=device,
        )
        out_cids.extend(pending_cids)
        out_states.extend(pending_states)
        out_semantic_texts.extend(pending_texts)
        out_embeddings.extend(vectors)
        pending_cids.clear()
        pending_states.clear()
        pending_texts.clear()

    skipped_empty = 0
    skipped_duplicate = 0

    for _, row in frame.iterrows():
        cid = _norm(row.get("ipfs_cid"))
        if not cid:
            skipped_empty += 1
            continue
        if cid in seen_cids:
            skipped_duplicate += 1
            continue

        semantic_text = _semantic_text(row.get("name"), row.get("text"), min_chars=min_chars)
        if not semantic_text:
            skipped_empty += 1
            continue

        seen_cids.add(cid)
        pending_cids.append(cid)
        pending_states.append(_norm(row.get("state_code")))
        pending_texts.append(semantic_text)

        if len(pending_texts) >= flush_size:
            flush()

    flush()

    out = pd.DataFrame(
        {
            "ipfs_cid": out_cids,
            "state_code": out_states,
            "semantic_text": out_semantic_texts,
            "embedding_model": [model] * len(out_cids),
            "embedding": out_embeddings,
        }
    )
    out = out.sort_values("ipfs_cid").reset_index(drop=True)

    stats = {
        "source_rows": int(total_rows),
        "embedded_rows": int(len(out)),
        "unique_cids": int(out["ipfs_cid"].nunique() if not out.empty else 0),
        "skipped_empty_or_short": int(skipped_empty),
        "skipped_duplicate_cid": int(skipped_duplicate),
    }
    return out, stats


def run(
    *,
    input_dir: Path,
    recursive: bool,
    model: str,
    provider: str,
    device: str,
    router_batch_size: int,
    flush_size: int,
    min_chars: int,
    overwrite: bool,
    include_combined: bool,
) -> Dict[str, Any]:
    files = list(_iter_input_parquets(input_dir, recursive=recursive, include_combined=include_combined))
    if not files:
        raise FileNotFoundError(f"no parquet files found in {input_dir}")

    manifest: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_dir": str(input_dir),
        "model": model,
        "provider": provider,
        "device": device,
        "files": {},
        "totals": {"source_rows": 0, "embedded_rows": 0, "unique_cids": 0},
    }

    global_cids: set[str] = set()
    for source_path in files:
        output_path = source_path.with_name(f"{source_path.stem}_embeddings.parquet")
        if output_path.exists() and not overwrite:
            manifest["files"][source_path.name] = {
                "source_file": str(source_path),
                "output_file": str(output_path),
                "skipped": "exists",
            }
            continue

        frame = pd.read_parquet(source_path)
        embedded, stats = _embed_rows(
            frame,
            model=model,
            provider=provider,
            device=device,
            router_batch_size=router_batch_size,
            flush_size=flush_size,
            min_chars=min_chars,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        embedded.to_parquet(output_path, compression="zstd", index=False)

        for cid in embedded.get("ipfs_cid", pd.Series(dtype=str)).tolist():
            global_cids.add(str(cid))

        manifest["files"][source_path.name] = {
            "source_file": str(source_path),
            "output_file": str(output_path),
            **stats,
        }
        manifest["totals"]["source_rows"] += stats["source_rows"]
        manifest["totals"]["embedded_rows"] += stats["embedded_rows"]

        print(
            f"[done] {source_path.name}: source_rows={stats['source_rows']} embedded_rows={stats['embedded_rows']}"
        )

    manifest["totals"]["unique_cids"] = len(global_cids)
    manifest_path = input_dir / "embeddings_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build CID-keyed semantic embeddings parquet files")
    parser.add_argument("--input-dir", required=True, help="Directory containing CID-keyed source parquet files")
    parser.add_argument("--recursive", action="store_true", help="Scan input dir recursively")
    parser.add_argument("--model", default="thenlper/gte-small", help="Embeddings model")
    parser.add_argument("--provider", default="local_adapter", help="embeddings_router provider")
    parser.add_argument("--device", default="cuda", help="Device for embeddings provider")
    parser.add_argument("--router-batch-size", type=int, default=64, help="Batch size passed to embeddings_router")
    parser.add_argument("--flush-size", type=int, default=512, help="Rows accumulated before each embed flush")
    parser.add_argument("--min-chars", type=int, default=16, help="Minimum chars for name/text snippet inclusion")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing *_embeddings.parquet")
    parser.add_argument(
        "--include-combined",
        action="store_true",
        help="Also process state_admin_rules_all_states.parquet (disabled by default)",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input dir not found: {input_dir}")

    manifest = run(
        input_dir=input_dir,
        recursive=bool(args.recursive),
        model=str(args.model),
        provider=str(args.provider),
        device=str(args.device),
        router_batch_size=int(args.router_batch_size),
        flush_size=int(args.flush_size),
        min_chars=int(args.min_chars),
        overwrite=bool(args.overwrite),
        include_combined=bool(args.include_combined),
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
