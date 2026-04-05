# Time Travel and CDC

Demonstrates querying historical table state and tracking row-level changes between snapshots.

## What it demonstrates

- Writing data across multiple commits (snapshots)
- Time travel via `scan().with_snapshot(id)`
- Change data capture: `table_insertions()`, `table_deletions()`, `table_changes()`
- Viewing snapshot history via `table.inspect().history()`

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/time_travel/time_travel.py
```
