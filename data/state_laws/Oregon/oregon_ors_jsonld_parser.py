"""Parse Oregon ORS chapter HTML files into structured JSON-LD.

This parser reads chapter pages under raw_html (e.g. ors001.html) and emits
one JSON-LD document per chapter under parsed/jsonld.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup

try:
    from ipfs_datasets_py.processors.legal_scrapers.state_scrapers.citation_history import (
        extract_trailing_history_citations,
    )
except Exception:
    workspace_root = Path(__file__).resolve().parents[3]
    utility_path = (
        workspace_root
        / "ipfs_datasets_py"
        / "ipfs_datasets_py"
        / "processors"
        / "legal_scrapers"
        / "state_scrapers"
        / "citation_history.py"
    )
    spec = importlib.util.spec_from_file_location("local_citation_history", utility_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load citation utility from {utility_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    extract_trailing_history_citations = module.extract_trailing_history_citations

FILENAME_CHAPTER_RE = re.compile(r"^ors(\d{3}[a-z]?)\.html$", re.IGNORECASE)
SUBSEC_TOKEN_RE = re.compile(r"\(([0-9]+|[A-Za-z]{1,6})\)")
ROMAN_LOWER_RE = re.compile(r"^[ivxlcdm]+$")
ROMAN_UPPER_RE = re.compile(r"^[IVXLCDM]+$")
COMMON_ROMAN_LOWER = {
    "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x", "xi", "xii", "xiii", "xiv", "xv"
}
COMMON_ROMAN_UPPER = {token.upper() for token in COMMON_ROMAN_LOWER}


def _norm_space(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = text.replace("\ufeff", "")
    text = text.replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _lineify(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = []
    for raw_line in text.splitlines():
        line = _norm_space(raw_line)
        if line:
            lines.append(line)
    return lines


def _chapter_slug_from_filename(path: Path) -> Optional[str]:
    match = FILENAME_CHAPTER_RE.match(path.name)
    if not match:
        return None
    return match.group(1).lower()


def _chapter_number_display(chapter_slug: str) -> str:
    digits = "".join(ch for ch in chapter_slug if ch.isdigit())
    suffix = "".join(ch for ch in chapter_slug if ch.isalpha())
    if not digits:
        return chapter_slug
    return f"{int(digits)}{suffix}"


def _extract_chapter_title(lines: Sequence[str], chapter_display: str) -> Optional[str]:
    pattern = re.compile(rf"^chapter\s+{re.escape(chapter_display)}\b\s*[\-\u2013\u2014\u00ad\u00a0\s:]*\s*(.*)$", re.IGNORECASE)
    for line in lines[:200]:
        match = pattern.match(line)
        if match:
            title = _norm_space(match.group(1))
            if title:
                return title
    return None


def _extract_edition(lines: Sequence[str]) -> Optional[str]:
    for line in lines[:300]:
        match = re.search(r"\b(20\d{2})\s+edition\b", line, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _section_start_regex(chapter_display: str) -> re.Pattern[str]:
    return re.compile(
        rf"^\s*({re.escape(chapter_display)}\.\d{{3}}[a-z]?)\b\s*(.*)$",
        re.IGNORECASE,
    )


def _classify_subsec_kind(token: str, prev_kind: Optional[str]) -> str:
    if token.isdigit():
        return "numeric"

    if token.islower():
        if token in COMMON_ROMAN_LOWER and prev_kind in {"alpha_upper", "roman_lower", "roman_upper"}:
            return "roman_lower"
        if len(token) > 1 and ROMAN_LOWER_RE.match(token):
            return "roman_lower"
        return "alpha_lower"

    if token.isupper():
        if token in COMMON_ROMAN_UPPER and prev_kind in {"roman_lower", "roman_upper"}:
            return "roman_upper"
        if len(token) > 1 and ROMAN_UPPER_RE.match(token):
            return "roman_upper"
        return "alpha_upper"

    return "other"


def _subsec_level(kind: str) -> int:
    order = {
        "numeric": 1,
        "alpha_lower": 2,
        "alpha_upper": 3,
        "roman_lower": 4,
        "roman_upper": 5,
        "other": 6,
    }
    return order.get(kind, 6)


def _find_subsec_markers(text: str) -> List[Tuple[int, int, str]]:
    markers: List[Tuple[int, int, str]] = []
    for match in SUBSEC_TOKEN_RE.finditer(text):
        start = match.start()
        end = match.end()
        token = match.group(1)

        if len(token) > 6:
            continue
        if token.isdigit() and len(token) > 3:
            continue
        if token.isalpha():
            if not (token.islower() or token.isupper()):
                continue
            if len(token) > 1:
                if token.islower() and not ROMAN_LOWER_RE.match(token):
                    continue
                if token.isupper() and not ROMAN_UPPER_RE.match(token):
                    continue

        prev_ch = text[start - 1] if start > 0 else ""
        next_ch = text[end] if end < len(text) else ""

        valid_left = (start == 0) or prev_ch.isspace() or prev_ch in ";:.(["
        valid_right = (end == len(text)) or next_ch.isspace() or next_ch in "(),;:.]"
        if not (valid_left and valid_right):
            continue

        markers.append((start, end, token))
    return markers


def _parse_subsections(text: str) -> List[Dict[str, Any]]:
    text = _norm_space(text)
    markers = _find_subsec_markers(text)
    if not markers:
        return []

    items: List[Dict[str, Any]] = []
    prev_kind: Optional[str] = None
    for idx, (start, end, token) in enumerate(markers):
        next_start = markers[idx + 1][0] if idx + 1 < len(markers) else len(text)
        body = _norm_space(text[end:next_start])
        kind = _classify_subsec_kind(token, prev_kind)
        prev_kind = kind
        items.append(
            {
                "label": f"({token})",
                "token": token,
                "kind": kind,
                "level": _subsec_level(kind),
                "text": body,
                "subsections": [],
            }
        )

    roots: List[Dict[str, Any]] = []
    stack: List[Dict[str, Any]] = []

    for item in items:
        level = int(item["level"])
        while stack and int(stack[-1]["level"]) >= level:
            stack.pop()

        parent_subsections = roots if not stack else stack[-1]["subsections"]

        existing_node: Optional[Dict[str, Any]] = None
        for sibling in reversed(parent_subsections):
            if sibling.get("label") == item["label"]:
                existing_node = sibling
                break

        if existing_node is None:
            node = {
                "label": item["label"],
                "token": item["token"],
                "kind": item["kind"],
                "text": item["text"],
                "subsections": [],
            }
            parent_subsections.append(node)
        else:
            node = existing_node
            new_text = item["text"]
            old_text = _norm_space(str(node.get("text", "")))
            if new_text:
                if not old_text:
                    node["text"] = new_text
                elif new_text not in old_text:
                    node["text"] = f"{old_text} {new_text}".strip()

        stack.append({"level": level, "subsections": node["subsections"]})

    return roots


def _validate_subsection_tree(nodes: Sequence[Dict[str, Any]], *, max_depth: int = 6) -> List[str]:
    issues: List[str] = []

    def walk(siblings: Sequence[Dict[str, Any]], depth: int, path: str) -> None:
        if depth > max_depth:
            issues.append(f"depth>{max_depth} at {path or 'root'}")

        seen_labels: Dict[str, int] = {}
        for index, node in enumerate(siblings, start=1):
            label = str(node.get("label", ""))
            kind = str(node.get("kind", ""))
            text = _norm_space(str(node.get("text", "")))
            children = node.get("subsections", [])

            if label:
                seen_labels[label] = seen_labels.get(label, 0) + 1
                if seen_labels[label] > 1:
                    issues.append(f"duplicate sibling label {label} at {path or 'root'}")

            if not text and not children:
                issues.append(f"empty leaf node {label or '#'+str(index)} at {path or 'root'}")

            if kind not in {"numeric", "alpha_lower", "alpha_upper", "roman_lower", "roman_upper", "other"}:
                issues.append(f"unknown kind {kind} for {label or '#'+str(index)}")

            child_path = f"{path}/{label}" if path else label
            if isinstance(children, list) and children:
                walk(children, depth + 1, child_path)

    walk(nodes, depth=1, path="")
    return sorted(set(issues))


def _collect_sections(lines: Sequence[str], chapter_display: str) -> List[Dict[str, Any]]:
    start_re = _section_start_regex(chapter_display)

    collected: List[Dict[str, Any]] = []
    current_id: Optional[str] = None
    current_title: str = ""
    buffer: List[str] = []

    def flush() -> None:
        nonlocal current_id, current_title, buffer
        if not current_id:
            return
        text = _norm_space("\n".join(buffer))
        text, history_raw_blocks, history_citations = extract_trailing_history_citations(text)
        collected.append(
            {
                "section_id": current_id,
                "heading": _norm_space(current_title),
                "text": text,
                "history_citation_blocks": history_raw_blocks,
                "history_citations": history_citations,
                "subsections": _parse_subsections(text),
            }
        )
        current_id = None
        current_title = ""
        buffer = []

    for line in lines:
        match = start_re.match(line)
        if match:
            flush()
            current_id = match.group(1).lower()
            current_title = match.group(2) or ""
            buffer = []
            continue
        if current_id:
            buffer.append(line)

    flush()

    # Keep the richest occurrence if duplicated in a chapter page
    by_id: Dict[str, Dict[str, Any]] = {}
    for row in collected:
        sid = row["section_id"]
        prev = by_id.get(sid)
        if prev is None:
            by_id[sid] = row
            continue
        prev_len = len(prev.get("text", ""))
        cur_len = len(row.get("text", ""))
        if cur_len > prev_len:
            by_id[sid] = row

    def section_sort_key(section_id: str) -> Tuple[int, str]:
        # e.g., 1.001a
        m = re.match(r"^([0-9]+)\.([0-9]+)([a-z]?)$", section_id)
        if not m:
            return (10**9, section_id)
        return (int(m.group(2)), m.group(3) or "")

    ordered = [by_id[sid] for sid in sorted(by_id.keys(), key=section_sort_key)]
    for idx, row in enumerate(ordered, start=1):
        row["position"] = idx
        row["parser_warnings"] = _validate_subsection_tree(row.get("subsections", []))
    return ordered


def _chapter_jsonld(
    *,
    file_path: Path,
    chapter_slug: str,
    chapter_title: Optional[str],
    edition_year: Optional[str],
    sections: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    chapter_display = _chapter_number_display(chapter_slug)
    chapter_name = f"ORS Chapter {chapter_display}"
    if chapter_title:
        chapter_name = f"{chapter_name} — {chapter_title}"

    chapter_url = f"https://www.oregonlegislature.gov/bills_laws/ors/ors{chapter_slug}.html"

    has_part = []
    for section in sections:
        sec_id = section["section_id"]
        sec_name = section["heading"] or f"Section {sec_id}"
        has_part.append(
            {
                "@type": "Legislation",
                "@id": f"{chapter_url}#section-{sec_id}",
                "identifier": f"ORS {sec_id}",
                "name": sec_name,
                "text": section.get("text", ""),
                "history_citation_blocks": section.get("history_citation_blocks", []),
                "history_citations": section.get("history_citations", []),
                "subsections": section.get("subsections", []),
                "parser_warnings": section.get("parser_warnings", []),
                "position": section.get("position"),
                "isPartOf": chapter_url,
            }
        )

    jsonld: Dict[str, Any] = {
        "@context": {
            "@vocab": "https://schema.org/",
            "orlaw": "https://www.oregonlegislature.gov/ors#",
        },
        "@type": "Legislation",
        "@id": chapter_url,
        "identifier": chapter_name.split(" — ")[0],
        "name": chapter_name,
        "legislationType": "Statute",
        "jurisdiction": "US-OR",
        "isPartOf": {
            "@type": "CreativeWork",
            "identifier": "ORS",
            "name": "Oregon Revised Statutes",
            "url": "https://www.oregonlegislature.gov/bills_laws/ors",
        },
        "dateModified": datetime.now(timezone.utc).isoformat(),
        "source_file": str(file_path),
        "hasPart": has_part,
    }

    if chapter_title:
        jsonld["alternateName"] = chapter_title
    if edition_year:
        jsonld["legislationDate"] = edition_year

    return jsonld


def parse_chapter_file(file_path: Path) -> Optional[Dict[str, Any]]:
    chapter_slug = _chapter_slug_from_filename(file_path)
    if not chapter_slug:
        return None

    html = file_path.read_text(encoding="utf-8", errors="ignore")
    lines = _lineify(html)
    if not lines:
        return None

    chapter_display = _chapter_number_display(chapter_slug)
    chapter_title = _extract_chapter_title(lines, chapter_display)
    edition_year = _extract_edition(lines)
    sections = _collect_sections(lines, chapter_display)

    return _chapter_jsonld(
        file_path=file_path,
        chapter_slug=chapter_slug,
        chapter_title=chapter_title,
        edition_year=edition_year,
        sections=sections,
    )


def _default_root() -> Path:
    return Path(__file__).resolve().parent


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse ORS chapter HTML files into JSON-LD")
    parser.add_argument("--root", type=Path, default=_default_root(), help="Oregon state_laws root directory")
    parser.add_argument("--raw-dir", type=Path, default=None, help="Raw HTML directory (default: <root>/raw_html)")
    parser.add_argument("--out-dir", type=Path, default=None, help="JSON-LD output directory (default: <root>/parsed/jsonld)")
    parser.add_argument("--manifests-dir", type=Path, default=None, help="Manifest directory (default: <root>/manifests)")
    return parser


def run(argv: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    parser = _build_parser()
    args = parser.parse_args(argv)

    root = args.root.resolve()
    raw_dir = (args.raw_dir or (root / "raw_html")).resolve()
    out_dir = (args.out_dir or (root / "parsed" / "jsonld")).resolve()
    manifests_dir = (args.manifests_dir or (root / "manifests")).resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in raw_dir.glob("ors*.html") if p.is_file()])

    parsed = 0
    skipped = 0
    errors: List[Dict[str, str]] = []
    outputs: List[Dict[str, Any]] = []

    for file_path in files:
        try:
            jsonld = parse_chapter_file(file_path)
            if jsonld is None:
                skipped += 1
                continue
            chapter_slug = _chapter_slug_from_filename(file_path)
            if not chapter_slug:
                skipped += 1
                continue
            out_file = out_dir / f"ors{chapter_slug}.jsonld"
            out_file.write_text(json.dumps(jsonld, ensure_ascii=False, indent=2), encoding="utf-8")
            parsed += 1
            outputs.append(
                {
                    "source": str(file_path),
                    "jsonld": str(out_file),
                    "section_count": len(jsonld.get("hasPart", [])),
                    "chapter": jsonld.get("identifier"),
                }
            )
        except Exception as exc:
            errors.append({"file": str(file_path), "error": str(exc)})

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    manifest_path = manifests_dir / f"ors_jsonld_parse_{run_id}.json"
    report = {
        "status": "success" if parsed > 0 else "error",
        "run_id": run_id,
        "root": str(root),
        "raw_dir": str(raw_dir),
        "out_dir": str(out_dir),
        "processed_files": len(files),
        "parsed_files": parsed,
        "skipped_files": skipped,
        "error_count": len(errors),
        "errors": errors,
        "outputs": outputs,
    }
    manifest_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "manifest": str(manifest_path),
        "processed_files": len(files),
        "parsed_files": parsed,
        "error_count": len(errors),
        "out_dir": str(out_dir),
    }, indent=2))
    return report


if __name__ == "__main__":
    run()
