# Encrypted Catalog

Demonstrates Parquet-level encryption for data at rest.

## What it demonstrates

- Creating a catalog with `encrypted=True`
- Writing data (Parquet files are encrypted on disk)
- Transparent decryption when reading through the catalog
- Attempting to read raw Parquet files directly (showing they are protected)

## Prerequisites

- pyducklake installed

## Run

```bash
uv run python examples/encrypted_catalog/encrypted_catalog.py
```
