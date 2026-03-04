#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _norm(value: Any) -> str:
    text = str(value or "").strip()
    return " ".join(text.split())


def _iter_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_norm(v) for v in value if _norm(v)]
    if isinstance(value, str):
        value = _norm(value)
        return [value] if value else []
    return []


def _add_node(nodes: dict[str, dict[str, Any]], node_id: str, node_type: str, label: str, **props: Any) -> None:
    if node_id not in nodes:
        nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "label": label,
            "properties": props,
        }


def build_graph(
    input_jsonl: Path,
    output_json: Path,
    *,
    text_mode: str = "none",
    text_chars: int = 4000,
) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: set[tuple[str, str, str]] = set()

    records = 0
    rules = 0

    with input_jsonl.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue

            records += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            structured = row.get("structured_data") or {}
            citations = structured.get("citations") or {}

            official_cite = _norm(row.get("official_cite") or row.get("section_number") or row.get("statute_id"))
            if not official_cite:
                continue

            rule_id = f"oar:{official_cite}"
            rules += 1

            chapter_number = _norm(row.get("chapter_number") or structured.get("chapter"))
            section_number = _norm(row.get("section_number"))
            source_url = _norm(row.get("source_url") or structured.get("rule_url"))

            title = _norm(row.get("section_name") or row.get("short_title"))
            summary = _norm(row.get("summary"))

            rule_props: dict[str, Any] = {
                "official_cite": official_cite,
                "section_number": section_number,
                "chapter_number": chapter_number,
                "source_url": source_url,
                "title": title,
            }

            if summary:
                rule_props["summary"] = summary

            if text_mode in {"snippet", "full"}:
                full_text = str(row.get("full_text") or "")
                if text_mode == "snippet":
                    rule_props["full_text_snippet"] = full_text[: max(0, int(text_chars))]
                else:
                    rule_props["full_text"] = full_text

            _add_node(
                nodes,
                rule_id,
                "oar_rule",
                official_cite,
                **rule_props,
            )

            if chapter_number:
                chapter_id = f"oar_chapter:{chapter_number}"
                _add_node(
                    nodes,
                    chapter_id,
                    "oar_chapter",
                    f"OAR Chapter {chapter_number}",
                    chapter_number=chapter_number,
                )
                edges.add((rule_id, chapter_id, "in_chapter"))

            for ors in _iter_list(citations.get("ors_citations")):
                ors_id = f"ors:{ors}"
                _add_node(nodes, ors_id, "ors_statute", f"ORS {ors}", citation=ors)
                edges.add((rule_id, ors_id, "cites_ors"))

            for oar in _iter_list(citations.get("oar_citations")):
                target_rule_id = f"oar:{oar}"
                _add_node(nodes, target_rule_id, "oar_rule", oar, official_cite=oar)
                if target_rule_id != rule_id:
                    edges.add((rule_id, target_rule_id, "cites_oar"))

            for usc in _iter_list(citations.get("usc_citations")):
                usc_id = f"usc:{usc}"
                _add_node(nodes, usc_id, "usc_statute", f"USC {usc}", citation=usc)
                edges.add((rule_id, usc_id, "cites_usc"))

            for public_law in _iter_list(citations.get("public_laws")):
                law_id = f"public_law:{public_law}"
                _add_node(nodes, law_id, "public_law", public_law, citation=public_law)
                edges.add((rule_id, law_id, "cites_public_law"))

            for section_ref in _iter_list(citations.get("section_references")):
                ref_id = f"section_ref:{section_ref}"
                _add_node(nodes, ref_id, "section_reference", section_ref, citation=section_ref)
                edges.add((rule_id, ref_id, "references_section"))

    graph = {
        "graph_name": "oregon_administrative_rules_knowledge_graph",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(input_jsonl),
        "stats": {
            "records_read": records,
            "rules_indexed": rules,
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
        "nodes": list(nodes.values()),
        "edges": [
            {"source": s, "target": t, "type": r}
            for (s, t, r) in sorted(edges)
        ],
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(graph, ensure_ascii=False), encoding="utf-8")

    return graph["stats"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Oregon Administrative Rules knowledge graph from JSONL")
    parser.add_argument("--input", required=True, help="Path to oregon_administrative_rules.jsonl")
    parser.add_argument("--output", required=True, help="Output path for knowledge graph JSON")
    parser.add_argument(
        "--text-mode",
        choices=["none", "snippet", "full"],
        default="none",
        help="Retain no full text (none), truncated full text (snippet), or complete full text (full)",
    )
    parser.add_argument(
        "--text-chars",
        type=int,
        default=4000,
        help="Snippet length when --text-mode=snippet",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")

    stats = build_graph(
        input_jsonl=input_path,
        output_json=output_path,
        text_mode=args.text_mode,
        text_chars=args.text_chars,
    )
    print("done", json.dumps(stats, ensure_ascii=False))
    print(f"output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
