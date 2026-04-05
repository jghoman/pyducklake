# pyducklake Examples

Self-contained, runnable examples demonstrating pyducklake features.

## Examples

| Example | Description |
|---------|-------------|
| [quickstart/](quickstart/) | Minimal hello-world: create catalog, table, write data, read it back |
| [etl_pipeline/](etl_pipeline/) | Batch loading, upserts, schema evolution, and partitioning |
| [time_travel/](time_travel/) | Query historical state and track row-level changes (CDC) |
| [multi_table_transaction/](multi_table_transaction/) | Atomic multi-table writes with rollback |
| [schema_evolution/](schema_evolution/) | Add, rename, drop columns and widen types |
| [maintenance/](maintenance/) | Compaction, snapshot expiration, file cleanup |
| [encrypted_catalog/](encrypted_catalog/) | Parquet-level encryption for data at rest |
| [postgres_backend/](postgres_backend/) | PostgreSQL as metadata backend (requires Docker) |

## Running

All examples except `postgres_backend` run with zero external dependencies beyond pyducklake:

```bash
# Run a single example
uv run python examples/quickstart/quickstart.py

# Run all examples (except postgres_backend)
just examples
```

The `postgres_backend` example requires Docker:

```bash
cd examples/postgres_backend
docker compose up -d
uv run python examples/postgres_backend/postgres_backend.py
docker compose down -v
```
