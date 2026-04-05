# Quickstart

Minimal hello-world: create a catalog, create a table, write data, read it back.

## What it demonstrates

- Creating a Ducklake catalog with DuckDB metadata
- Defining a schema with `Schema.of()`, `required()`, `optional()`
- Creating a table
- Writing data via `table.append()`
- Reading data via `table.scan().to_arrow()`
- Filtered scans and column projection

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/quickstart/quickstart.py
```
