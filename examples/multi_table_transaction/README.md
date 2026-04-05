# Multi-Table Transactions

Demonstrates atomic multi-table writes with automatic rollback on failure.

## What it demonstrates

- `catalog.begin_transaction()` as a context manager
- Writing to multiple tables within a single transaction
- Automatic commit on clean exit
- Automatic rollback when an exception occurs mid-transaction
- Data consistency verification after rollback

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/multi_table_transaction/multi_table_transaction.py
```
