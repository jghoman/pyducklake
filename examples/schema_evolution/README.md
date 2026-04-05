# Schema Evolution

Demonstrates all schema evolution operations with data preservation.

## What it demonstrates

- `add_column()` — adding a new nullable column
- `rename_column()` — renaming an existing column
- `update_column()` — widening a column type (INTEGER to BIGINT)
- `drop_column()` — removing a column
- Multiple schema changes in a single commit
- Data preservation through all evolution steps

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/schema_evolution/schema_evolution.py
```
