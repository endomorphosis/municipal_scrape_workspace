#!/usr/bin/env python3
"""Extract Common Crawl index tarballs into the cdx-*.gz layout expected by the pipeline.

Why
- The pointer build pipeline expects shards on disk as:
    <out-root>/<CC-MAIN-YYYY-WW>/cdx-00000.gz
- Some download workflows store these shards inside *.tar.gz bundles.

This script extracts those bundles *atomically* (tmp -> rename) and is resumable.

It tries to determine the collection (CC-MAIN-YYYY-WW) from:
- the member path inside the tarball, or
- the tarball filename.

It only extracts files that look like shards (default: names matching cdx-\d{5}.gz).
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional


_COLLECTION_RX = re.compile(r"CC-MAIN-\d{4}-\d{2}")
_CDX_RX = re.compile(r"cdx-(\d{5})\.gz$")


def _now() -> float:
    return time.time()


def _atomic_write_jsonl_line(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _guess_collection_from_text(text: str) -> Optional[str]:
    m = _COLLECTION_RX.search(text or "")
    return m.group(0) if m else None


def _is_cdx_member(name: str) -> bool:
    base = os.path.basename(name or "")
    return bool(_CDX_RX.match(base))


def _gzip_is_readable(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
            # Read a small amount to ensure gzip stream is intact.
            _ = f.readline()
        return True
    except Exception:
        return False


@dataclass(frozen=True)
class ExtractPlanItem:
    tar_path: Path
    member_name: str
    collection: str
    out_path: Path


def _iter_tarballs(tar_root: Path) -> Iterator[Path]:
    if tar_root.is_file():
        yield tar_root
        return
    if not tar_root.is_dir():
        return
    for p in sorted(tar_root.glob("*.tar.gz")):
        if p.is_file():
            yield p
    for p in sorted(tar_root.glob("*.tgz")):
        if p.is_file():
            yield p


def _build_plan(tarballs: Iterable[Path], *, out_root: Path) -> Iterator[ExtractPlanItem]:
    for tar_path in tarballs:
        guessed_from_tar = _guess_collection_from_text(tar_path.name)
        try:
            with tarfile.open(tar_path, mode="r:gz") as tf:
                for m in tf.getmembers():
                    if not m.isfile():
                        continue
                    if not _is_cdx_member(m.name):
                        continue

                    col = _guess_collection_from_text(m.name) or guessed_from_tar
                    if not col:
                        # Cannot place it deterministically.
                        continue

                    base = os.path.basename(m.name)
                    out_path = out_root / col / base
                    yield ExtractPlanItem(tar_path=tar_path, member_name=m.name, collection=col, out_path=out_path)
        except tarfile.TarError:
            continue


def extract_one(item: ExtractPlanItem, *, overwrite: bool, verify_gzip: bool) -> str:
    dest = item.out_path
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0 and not overwrite:
        if not verify_gzip or _gzip_is_readable(dest):
            return "skipped_exists"

    tmp = dest.with_suffix(dest.suffix + ".tmp")
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass

    try:
        with tarfile.open(item.tar_path, mode="r:gz") as tf:
            fobj = tf.extractfile(item.member_name)
            if fobj is None:
                return "missing_member"
            with open(tmp, "wb") as out_f:
                while True:
                    chunk = fobj.read(1024 * 1024)
                    if not chunk:
                        break
                    out_f.write(chunk)

        if tmp.exists() and tmp.stat().st_size > 0:
            if verify_gzip and not _gzip_is_readable(tmp):
                try:
                    tmp.unlink()
                except Exception:
                    pass
                return "bad_gzip"

            tmp.replace(dest)
            return "extracted"

        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return "empty_output"
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return "error"


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract CC index tarballs into cdx-*.gz layout")
    ap.add_argument("--tar-root", required=True, type=str, help="Directory containing *.tar.gz/*.tgz, or a single tarball path")
    ap.add_argument("--out-root", required=True, type=str, help="Output root where <collection>/cdx-*.gz will be created")
    ap.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing extracted shard files")
    ap.add_argument(
        "--verify-gzip",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verify extracted .gz is readable by reading a line (default: true)",
    )
    ap.add_argument(
        "--log-jsonl",
        type=str,
        default=None,
        help="Optional JSONL log path (default: <out-root>/extract_cc_index_tarballs.jsonl)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Only print how many shards would be extracted; do not write files",
    )

    args = ap.parse_args()

    tar_root = Path(args.tar_root).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    log_path = Path(args.log_jsonl).expanduser().resolve() if args.log_jsonl else (out_root / "extract_cc_index_tarballs.jsonl")

    tarballs = list(_iter_tarballs(tar_root))
    if not tarballs:
        raise SystemExit(f"No tarballs found under: {tar_root}")

    plan = list(_build_plan(tarballs, out_root=out_root))
    if args.dry_run:
        print(f"tarballs={len(tarballs)}")
        print(f"planned_shards={len(plan)}")
        return 0

    extracted = 0
    skipped = 0
    errors = 0
    for item in plan:
        status = extract_one(item, overwrite=bool(args.overwrite), verify_gzip=bool(args.verify_gzip))
        if status == "extracted":
            extracted += 1
        elif status.startswith("skipped"):
            skipped += 1
        else:
            errors += 1

        _atomic_write_jsonl_line(
            log_path,
            {
                "ts": _now(),
                "tar": str(item.tar_path),
                "member": item.member_name,
                "collection": item.collection,
                "out": str(item.out_path),
                "status": status,
            },
        )

    print(f"done extracted={extracted} skipped={skipped} errors={errors}")
    print(f"log={log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
