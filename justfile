# pyducklake — Python SDK for Ducklake

# Default recipe: list all available recipes
default:
    @just --list

# === Dev ===

# Install dependencies and configure git hooks
[group('dev')]
setup:
    uv sync
    git config core.hooksPath .githooks

# Install dependencies
[group('dev')]
sync:
    uv sync

# Format code
[group('dev')]
fmt:
    uv run ruff format src/ tests/

# Check formatting
[group('dev')]
fmt-check:
    uv run ruff format --check src/ tests/

# Lint code
[group('dev')]
lint:
    uv run ruff check src/ tests/

# Lint and fix
[group('dev')]
lint-fix:
    uv run ruff check --fix src/ tests/

# Run all type checkers (mypy + pyright)
[group('dev')]
typecheck: typecheck-mypy typecheck-pyright

# Type check with mypy
[group('dev')]
typecheck-mypy:
    uv run mypy src/

# Type check with pyright
[group('dev')]
typecheck-pyright:
    uv run pyright src/

# Scan for leaked secrets
[group('dev')]
secrets-scan:
    gitleaks detect --verbose

# Audit dependencies for known vulnerabilities
[group('dev')]
audit:
    uv audit
    just secrets-scan

# === Test ===

# Run unit tests (excludes integration)
[group('test')]
test:
    uv run python -m pytest tests/ --ignore=tests/integration

# Run unit tests with coverage
[group('test')]
test-cov:
    uv run python -m pytest tests/ --ignore=tests/integration --cov=pyducklake --cov-report=term-missing

# Run benchmarks
[group('test')]
bench:
    uv run python -m pytest tests/ --ignore=tests/integration -k benchmark --benchmark-only

# Run integration tests (requires Docker)
[group('test')]
test-integration:
    uv run python -m pytest tests/integration -m integration -v

# Run all tests
[group('test')]
test-all: test test-integration

# Full CI check (unit tests only)
[group('test')]
ci: fmt-check lint typecheck test

# === Build ===

# Build wheel and sdist
[group('build')]
build:
    uv build

# Build wheel only
[group('build')]
wheel:
    uv build --wheel

# Clean build artifacts
[group('build')]
clean:
    rm -rf .venv dist *.egg-info __pycache__ src/pyducklake/__pycache__

# === Release ===

# Bump version, commit, tag, and push. Usage: just release patch|minor|major [--yes]
[group('release')]
release bump *flags:
    #!/usr/bin/env bash
    set -euo pipefail

    # Parse current version
    current=$(grep '^version' pyproject.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
    IFS='.' read -r major minor patch <<< "$current"

    # Compute new version
    case "{{bump}}" in
        patch) patch=$((patch + 1)) ;;
        minor) minor=$((minor + 1)); patch=0 ;;
        major) major=$((major + 1)); minor=0; patch=0 ;;
        *) echo "Usage: just release patch|minor|major [--yes]"; exit 1 ;;
    esac
    new="${major}.${minor}.${patch}"

    echo "Current version: $current"
    echo "New version:     $new"
    echo ""

    # Confirm unless --yes passed
    if [[ " {{flags}} " != *" --yes "* ]]; then
        read -rp "Proceed with release v${new}? [y/N] " confirm
        [[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
    fi

    # Bump version in pyproject.toml
    sed -i '' "s/^version = \"${current}\"/version = \"${new}\"/" pyproject.toml

    # Commit, tag, push
    git add pyproject.toml
    git commit -m "Release v${new}"
    git tag "v${new}"
    git push origin main --tags

    echo ""
    echo "Released v${new} — workflow will build and publish to PyPI."

# === Docs ===

# Generate API documentation
[group('docs')]
docs:
    uv run pdoc src/pyducklake -o docs/api

# Serve documentation locally
[group('docs')]
docs-serve:
    uv run pdoc src/pyducklake

# === Examples ===

# Run all local examples (no Docker)
[group('examples')]
examples: example-quickstart example-etl example-time-travel example-transactions example-schema-evolution example-maintenance example-encryption

# Quick start
[group('examples')]
example-quickstart:
    uv run python examples/quickstart/quickstart.py

# ETL pipeline
[group('examples')]
example-etl:
    uv run python examples/etl_pipeline/etl_pipeline.py

# Time travel and CDC
[group('examples')]
example-time-travel:
    uv run python examples/time_travel/time_travel.py

# Multi-table transactions
[group('examples')]
example-transactions:
    uv run python examples/multi_table_transaction/multi_table_transaction.py

# Schema evolution
[group('examples')]
example-schema-evolution:
    uv run python examples/schema_evolution/schema_evolution.py

# Table maintenance
[group('examples')]
example-maintenance:
    uv run python examples/maintenance/maintenance.py

# Encrypted catalog
[group('examples')]
example-encryption:
    uv run python examples/encrypted_catalog/encrypted_catalog.py

# PostgreSQL backend (requires Docker)
[group('examples')]
example-postgres:
    cd examples/postgres_backend && docker compose up --build

# Table replication via CDC (requires Docker)
[group('examples')]
example-replication:
    cd examples/table_replication && docker compose up --build
