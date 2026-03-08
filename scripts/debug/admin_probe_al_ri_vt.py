import asyncio
import json
from pathlib import Path

from ipfs_datasets_py.processors.legal_scrapers.state_admin_rules_scraper import (
    _agentic_discover_admin_state_blocks,
)


async def main() -> None:
    result = await _agentic_discover_admin_state_blocks(
        states=["AL", "RI", "VT"],
        max_candidates_per_state=80,
        max_fetch_per_state=12,
        max_results_per_domain=40,
        max_hops=3,
        max_pages=36,
        min_full_text_chars=300,
        require_substantive_text=True,
        fetch_concurrency=10,
    )
    out_dir = Path("artifacts/state_admin_rules")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "admin_probe_al_ri_vt_latest.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    asyncio.run(main())
