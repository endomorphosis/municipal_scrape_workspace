from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
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


if __name__ == "__main__":
    raise SystemExit(main())
