# pyducklake — Claude Code Instructions

## Development Principles
- **TDD**: Write tests first. All new code must have corresponding tests.
- **Property-based testing**: New features should include Hypothesis property-based tests in `tests/test_hypothesis.py` when feasible. Use the existing strategies (`arrow_tables_for_schema`, `boolean_expressions`) and `_fresh_catalog()` helper. Good candidates: any operation with invariants (row counts, round-trips, algebraic laws, accounting identities).
- **Static typing**: Use type annotations everywhere. Code must pass mypy strict mode and pyright strict mode.

## Tooling
- **uv** for dependency management (`uv sync`, `uv run`)
- **just** for task running (`just test`, `just lint`, `just fmt`, `just typecheck`)
- **flox** for environment management
- **ruff** for linting and formatting
- **mypy** + **pyright** for type checking
- **pytest** for testing
- **hypothesis** for property-based testing

## Workflow
- Run `just ci` for the full check suite (format, lint, typecheck, test).
- Use agents for code review when making substantial changes.

## Post-Code Checks (REQUIRED)
After any code task, run ALL of these and fix any issues before reporting done:
1. `uv run ruff check src/ tests/` — must be 0 errors
2. `uv run ruff format --check src/ tests/` — must pass
3. `uv run mypy src/` — must be 0 errors
4. `uv run pyright src/` — must be 0 errors, 0 warnings
5. `uv run python -m pytest tests/ --ignore=tests/integration -q --tb=short` — all tests must pass

If any check fails, fix the issues before returning results. Do not report success with lint or type errors outstanding.
