# PostgreSQL Backend

Demonstrates using PostgreSQL as the metadata backend instead of DuckDB.

## What it demonstrates

- Connecting to PostgreSQL for catalog metadata
- All standard operations (create, write, read, schema evolution, upsert)
- Same API regardless of metadata backend

## Prerequisites

- pyducklake installed
- Docker and Docker Compose

## Run

```bash
# Start PostgreSQL
cd examples/postgres_backend
docker compose up -d

# Run the example
uv run python examples/postgres_backend/postgres_backend.py

# Clean up
docker compose down -v
```
