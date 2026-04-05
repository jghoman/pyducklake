# Table Maintenance

Demonstrates compaction, snapshot expiration, and file cleanup.

## What it demonstrates

- Writing many small batches (creating small files)
- Inspecting file and snapshot metadata
- `compact()` — merging small files into larger ones
- `expire_snapshots()` — removing old snapshots
- `cleanup_files()` — deleting files scheduled for removal
- `delete_orphaned_files()` — removing untracked files
- `checkpoint()` — running all maintenance operations
- Verifying data integrity after maintenance

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/maintenance/maintenance.py
```
