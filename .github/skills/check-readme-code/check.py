"""Validate README.md python code blocks (Rust-doctest analogue).

Each fenced ```python block is executed in isolation in a fresh temp cwd.
Bails on the first failure. Run via:

    uv run python .github/skills/check-readme-code/check.py

Exit codes:
    0 — all blocks passed
    1 — a block raised
    2 — README not found
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
README = REPO_ROOT / "README.md"
BLOCK_RE = re.compile(r"^```python\n(.*?)^```$", re.MULTILINE | re.DOTALL)


def main() -> int:
    if not README.exists():
        print(f"README not found at {README}", file=sys.stderr)
        return 2

    blocks = BLOCK_RE.findall(README.read_text())
    print(f"Found {len(blocks)} python block(s) in {README.relative_to(REPO_ROOT)}")

    cwd = os.getcwd()
    for i, block in enumerate(blocks, 1):
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            try:
                exec(block, {"__name__": "__main__"})
            except Exception as exc:
                os.chdir(cwd)
                print(f"\nFAIL: block {i}/{len(blocks)}", file=sys.stderr)
                print("---", file=sys.stderr)
                print(block.rstrip(), file=sys.stderr)
                print("---", file=sys.stderr)
                print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
                return 1
            finally:
                os.chdir(cwd)

    print(f"OK: all {len(blocks)} block(s) passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
