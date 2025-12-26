#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

"$ROOT_DIR/.venv/bin/pip" install --upgrade pip setuptools wheel

# Force ipfs_kit_py from known_good (ZIP) to avoid git submodule issues.
"$ROOT_DIR/.venv/bin/pip" install -c "$ROOT_DIR/constraints.txt" -e .

# Optional: enable Playwright fallbacks (comment in if you want it)
# "$ROOT_DIR/.venv/bin/pip" install -c "$ROOT_DIR/constraints.txt" -e ".[playwright]"
# "$ROOT_DIR/.venv/bin/playwright" install chromium

echo "Bootstrap complete. Activate with: source .venv/bin/activate"
