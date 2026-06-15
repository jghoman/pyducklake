---
name: check-readme-code
description: Validate every Python code block in README.md by running it as an isolated unit (Rust-doctest analogue). Use when the README has been edited, before a release, or when the user says "check the README examples".
---

# check-readme-code

Validate the Python code blocks in `README.md` by executing each one in isolation. This is the Python analogue of `cargo test --doc`: each ` ```python ` fenced block in `README.md` is treated as a self-contained doctest.

## Strategy

- **Per-block isolation.** Each code block runs in a fresh temp `cwd` with `exec` in a fresh `__main__` namespace. Blocks must not rely on state from previous blocks.
- **DuckDB side effects.** Several README blocks create a catalog file (e.g. `meta.duckdb`) and a `./data` directory. The temp-dir per-block isolation prevents these from leaking into the repo.
- **Skip non-Python fences.** Only ` ```python ` blocks are executed. ` ```bash ` and unlabeled fences are ignored.
- **No external deps.** The check script is stdlib only — regex extraction + `exec`. The skill is fully self-contained inside `.github/skills/check-readme-code/`.

## Implementation

The skill ships a single-file Python script, `check.py`, that lives next to this `SKILL.md`. It extracts every ` ```python ` block from `README.md` and `exec`s each one in a fresh temp cwd, bailing on the first failure with a clear "block N/M" report.

### Steps when this skill is invoked

1. **Verify branch.** Confirm we're not on `main`. If we are, stop and ask the user to switch to a feature branch.

2. **Run the check.**
   ```bash
   uv run python .github/skills/check-readme-code/check.py
   ```

3. **Interpret the output.** The script bails on the first failure, so each run surfaces one failing block at a time:
   - On pass: `OK: all N block(s) passed` (exit 0).
   - On failure: prints `FAIL: block N/M`, the full block source, and the exception (exit 1).

4. **Do not auto-edit `README.md`.** Surface the failure and let the user decide whether the README or the library is wrong. After they fix it, re-run the skill to advance to the next failure.

## Failure-mode catalogue

When blocks fail, the cause is usually one of:

| Symptom | Likely cause |
|---|---|
| `NameError: name 'catalog' is not defined` (or similar) | Block depends on a prior block's state. Inline the setup, or split the README so each section starts from scratch. |
| `ModuleNotFoundError` | Example uses an optional dep not installed in the dev environment (e.g. `polars`, `pandas`). Add it to dev deps or rework the example. |
| `duckdb.IOException: ... already exists` | Two blocks both create the same artifact in the same cwd — should not happen with per-block temp dirs; investigate if it does. |
| `SyntaxError` | The README block is illustrative, not executable (e.g. shows a method signature). Either convert it to executable form or change the fence language to ` ```text `. |

## Non-goals

- Validating `bash` blocks. Out of scope; install commands etc. are not worth the complexity of sandboxing.
- Running examples that hit external services. The README should not contain such examples.
- Linting the README prose. This skill is about code correctness only.
- Enumerating *every* failing block in one run. The script bails on the first failure. Fix and re-run to advance.
