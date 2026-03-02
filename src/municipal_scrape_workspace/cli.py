from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ipfs_datasets_py.processors.legal_data.reasoner import HybridLawReasoner
from ipfs_datasets_py.processors.legal_data.reasoner.models import SourceProvenance
from ipfs_datasets_py.processors.legal_data.reasoner.serialization import (
    append_proof_to_store,
    load_legal_ir_from_json,
    load_proof_store,
    proof_from_dict,
    proof_to_dict,
    write_json,
)


def _legacy_passthrough(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="municipal-scrape",
        description="Wrapper entrypoint for orchestrate_municipal_scrape.py",
    )
    parser.add_argument(
        "--script",
        default=str(Path.cwd() / "orchestrate_municipal_scrape.py"),
        help="Path to orchestrate_municipal_scrape.py (default: ./orchestrate_municipal_scrape.py)",
    )
    parser.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to orchestrate_municipal_scrape.py (use `--` before them)",
    )

    ns = parser.parse_args(argv)
    script = Path(ns.script)
    if not script.exists():
        raise SystemExit(f"Script not found: {script}")

    passthrough_args = list(ns.args or [])
    if passthrough_args[:1] == ["--"]:
        passthrough_args = passthrough_args[1:]

    cmd = [sys.executable, str(script)]
    if passthrough_args:
        cmd.extend(passthrough_args)

    return subprocess.call(cmd)


def _read_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _default_provenance_for_kb(kb) -> dict[str, SourceProvenance]:
    provenance: dict[str, SourceProvenance] = {}
    for nid, norm in kb.norms.items():
        source_id = str(getattr(norm, "attrs", {}).get("source_id") or nid)
        source_path = str(getattr(norm, "attrs", {}).get("source_path") or "unknown")
        source_span = getattr(norm, "attrs", {}).get("source_span")
        provenance[nid] = SourceProvenance(
            source_path=source_path,
            source_id=source_id,
            source_span=(str(source_span) if source_span is not None else None),
        )
    return provenance


def _run_reasoner_check_compliance(args: argparse.Namespace) -> int:
    kb = load_legal_ir_from_json(args.kb)
    reasoner = HybridLawReasoner(kb, provenance_by_norm=_default_provenance_for_kb(kb))
    query = _read_json(args.query)
    time_context = _read_json(args.time_context)
    result = reasoner.check_compliance(query, time_context)

    out = {"result": result}
    if args.proof_out:
        proof = reasoner.get_proof(result["proof_id"])
        write_json(args.proof_out, proof_to_dict(proof))
        out["proof_out"] = str(args.proof_out)
    if args.proof_store:
        proof = reasoner.get_proof(result["proof_id"])
        append_proof_to_store(args.proof_store, proof)
        out["proof_store"] = str(args.proof_store)

    if args.output:
        write_json(args.output, out)
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


def _run_reasoner_find_violations(args: argparse.Namespace) -> int:
    kb = load_legal_ir_from_json(args.kb)
    reasoner = HybridLawReasoner(kb, provenance_by_norm=_default_provenance_for_kb(kb))
    state = _read_json(args.state)
    result = reasoner.find_violations(state, (args.time_start, args.time_end))

    out = {"result": result}
    if args.proof_out:
        proof = reasoner.get_proof(result["proof_id"])
        write_json(args.proof_out, proof_to_dict(proof))
        out["proof_out"] = str(args.proof_out)
    if args.proof_store:
        proof = reasoner.get_proof(result["proof_id"])
        append_proof_to_store(args.proof_store, proof)
        out["proof_store"] = str(args.proof_store)

    if args.output:
        write_json(args.output, out)
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


def _run_reasoner_explain_proof(args: argparse.Namespace) -> int:
    kb = load_legal_ir_from_json(args.kb)
    reasoner = HybridLawReasoner(kb, provenance_by_norm=_default_provenance_for_kb(kb))
    selected_proof_id = ""

    if args.proof_store:
        store = load_proof_store(args.proof_store)
        for po in store.values():
            reasoner.register_proof(po)

    if args.proof_json:
        proof_payload = _read_json(args.proof_json)
        proof = proof_from_dict(proof_payload)
        reasoner.register_proof(proof)
        if not args.proof_id:
            selected_proof_id = proof.proof_id

    if args.proof_id:
        selected_proof_id = str(args.proof_id)

    if not selected_proof_id:
        raise SystemExit("explain-proof requires --proof-id or --proof-json")

    if selected_proof_id not in reasoner.list_proof_ids():
        raise SystemExit(f"proof_id not found in loaded proof sources: {selected_proof_id}")

    result = reasoner.explain_proof(selected_proof_id, format=args.format)
    if args.output:
        write_json(args.output, result)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def _reasoner_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="municipal-scrape",
        description="Municipal scrape CLI with reasoner tools and legacy passthrough mode.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run-script", help="Run orchestrate_municipal_scrape.py (legacy wrapper behavior).")
    p_run.add_argument(
        "--script",
        default=str(Path.cwd() / "orchestrate_municipal_scrape.py"),
        help="Path to orchestrate_municipal_scrape.py (default: ./orchestrate_municipal_scrape.py)",
    )
    p_run.add_argument("args", nargs=argparse.REMAINDER, help="Arguments passed through (use -- before them)")

    p_reasoner = sub.add_parser("reasoner", help="Reasoner operations for hybrid legal IR.")
    rs = p_reasoner.add_subparsers(dest="reasoner_cmd", required=True)

    p_cc = rs.add_parser("check-compliance", help="Run check_compliance(query, time_context).")
    p_cc.add_argument("--kb", required=True, help="Path to LegalIR JSON snapshot.")
    p_cc.add_argument("--query", required=True, help="Path to JSON query payload.")
    p_cc.add_argument("--time-context", required=True, help="Path to JSON time context payload.")
    p_cc.add_argument("--proof-out", default="", help="Optional output path for serialized proof object.")
    p_cc.add_argument("--proof-store", default="", help="Optional proof store JSON path to append this proof.")
    p_cc.add_argument("--output", default="", help="Optional output path for command result JSON.")

    p_fv = rs.add_parser("find-violations", help="Run find_violations(state, time_range).")
    p_fv.add_argument("--kb", required=True, help="Path to LegalIR JSON snapshot.")
    p_fv.add_argument("--state", required=True, help="Path to JSON state payload (events/facts).")
    p_fv.add_argument("--time-start", required=True, help="ISO-8601 start timestamp.")
    p_fv.add_argument("--time-end", required=True, help="ISO-8601 end timestamp.")
    p_fv.add_argument("--proof-out", default="", help="Optional output path for serialized proof object.")
    p_fv.add_argument("--proof-store", default="", help="Optional proof store JSON path to append this proof.")
    p_fv.add_argument("--output", default="", help="Optional output path for command result JSON.")

    p_ep = rs.add_parser("explain-proof", help="Run explain_proof(proof_id, format).")
    p_ep.add_argument("--kb", required=True, help="Path to LegalIR JSON snapshot.")
    p_ep.add_argument("--proof-json", default="", help="Path to a proof JSON created by reasoner commands.")
    p_ep.add_argument("--proof-store", default="", help="Path to proof store JSON containing saved proofs.")
    p_ep.add_argument("--proof-id", default="", help="Proof ID to explain (required when using --proof-store only).")
    p_ep.add_argument("--format", choices=["nl", "json", "graph"], default="nl", help="Explanation format.")
    p_ep.add_argument("--output", default="", help="Optional output path for explanation JSON.")

    return parser


def main(argv: list[str] | None = None) -> int:
    raw = list(argv) if argv is not None else list(sys.argv[1:])

    # Backward compatibility: if no explicit command, keep passthrough semantics.
    if not raw:
        return _legacy_passthrough([])

    known_root = {"run-script", "reasoner", "-h", "--help"}
    if raw[0] not in known_root:
        return _legacy_passthrough(raw)

    parser = _reasoner_parser()
    ns = parser.parse_args(raw)

    if ns.command == "run-script":
        return _legacy_passthrough(["--script", ns.script, *list(ns.args or [])])

    if ns.command == "reasoner":
        if ns.reasoner_cmd == "check-compliance":
            return _run_reasoner_check_compliance(ns)
        if ns.reasoner_cmd == "find-violations":
            return _run_reasoner_find_violations(ns)
        if ns.reasoner_cmd == "explain-proof":
            return _run_reasoner_explain_proof(ns)

    raise SystemExit("Unsupported command")


if __name__ == "__main__":
    raise SystemExit(main())
