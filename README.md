# pyducklake

[![PyPI version](https://img.shields.io/pypi/v/pyducklake)](https://pypi.org/project/pyducklake/)
[![CI](https://img.shields.io/github/actions/workflow/status/jghoman/pyducklake/ci.yml?branch=main&label=CI)](https://github.com/jghoman/pyducklake/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/pyducklake)](https://pypi.org/project/pyducklake/)
[![Downloads](https://static.pepy.tech/badge/pyducklake)](https://pepy.tech/project/pyducklake)

A Python SDK for [Ducklake](https://ducklake.select), providing a [pyiceberg](https://py.iceberg.apache.org/)-like API for the Ducklake lakehouse format.

## What is pyducklake?

pyducklake is to Ducklake what pyiceberg is to Apache Iceberg: a native Python client for catalog management, table operations, schema evolution, and data I/O. It connects to a Ducklake metadata database (DuckDB, PostgreSQL, MySQL, or SQLite) and reads/writes Parquet data files via DuckDB's ducklake extension. The result is a zero-infrastructure lakehouse you can spin up with a single Python import.

## Installation

```bash
# Core (DuckDB + PyArrow)
pip install pyducklake

# With optional output format support
pip install pyducklake[pandas]
pip install pyducklake[polars]
pip install pyducklake[all]
```

**Development install:**

```bash
git clone https://github.com/your-org/pyducklake.git
cd pyducklake
uv sync
```

## Quick Start

```python
import pyarrow as pa
from pyducklake import Catalog, Schema, required, optional, IntegerType, StringType

# 1. Create a catalog (DuckDB file as metadata store)
catalog = Catalog("my_lake", "meta.duckdb", data_path="./data")

# 2. Define a schema
schema = Schema.of(
    required("id", IntegerType()),
    optional("name", StringType()),
    optional("email", StringType()),
)

# 3. Create a table and write data
table = catalog.create_table("users", schema)
table.append(pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
}))

# 4. Read data
table.scan().to_arrow()                  # full table
table.scan("id > 1").to_arrow()          # filtered
table.scan().select("name").to_arrow()   # projected
table.scan().count()                     # row count
```

See [`examples/quickstart/`](examples/quickstart/) for the full runnable version.

## Comparison with pyiceberg

pyducklake follows pyiceberg's API patterns where they make sense, but takes advantage of Ducklake's architecture (SQL metadata database vs file-based manifests) to provide features that are difficult or impossible in pyiceberg.

### Feature Comparison

| Feature | pyducklake | pyiceberg |
|---------|-----------|-----------|
| **Metadata storage** | SQL database (DuckDB, Postgres, MySQL, SQLite) | Files (JSON, Avro manifests) |
| **Catalog backends** | 4 (DuckDB, PostgreSQL, MySQL, SQLite) | 7 (REST, Hive, Glue, DynamoDB, SQL, BigQuery, In-memory) |
| **Schema definition** | `Schema.of()` with `required()`/`optional()` helpers | `Schema()` with `NestedField()` |
| **Read formats** | Arrow, pandas, Polars, DuckDB, RecordBatchReader, PyArrow Dataset | Arrow, pandas, DuckDB, Ray, Polars |
| **Write inputs** | Arrow, Polars, any PyCapsule (`__arrow_c_stream__`), RecordBatchReader | Arrow only |
| **Append** | Yes | Yes |
| **Overwrite** | Yes (full or filtered) | Yes (full or filtered) |
| **Delete** | Yes | Yes |
| **Upsert / Merge** | Yes, with `UpsertResult` counts | Yes (v0.7+) |
| **Streaming writes** | `append_batches()` from RecordBatchReader or iterator | No |
| **Schema evolution** | Add, drop, rename, widen type, set/drop NOT NULL | Add, drop, rename, widen, reorder, union-by-name |
| **Column reordering** | No (Ducklake limitation) | Yes |
| **Identifier fields** | No (not in Ducklake spec) | Yes |
| **Partitioning** | Identity, year, month, day, hour | Identity, bucket, truncate, year, month, day, hour |
| **Bucket/truncate transforms** | No (Ducklake limitation) | Yes |
| **Sort orders** | Yes (applied during writes and compaction) | Spec only (not applied during writes) |
| **Time travel** | By snapshot ID or timestamp | By snapshot ID, ref name, or timestamp |
| **Snapshot branches/tags** | No (not in Ducklake spec) | Yes |
| **Snapshot rollback** | `rollback_to_snapshot()`, `rollback_to_timestamp()` | Not implemented (long-standing request) |
| **Change data capture** | `ChangeSet` with filtering, column projection, timestamp bounds, update pre/post image correlation | Not implemented (long-standing request) |
| **Transactions** | Multi-table atomic commits via SQL transactions | Single-table only |
| **Encryption** | Per-file Parquet encryption (catalog-level) | Not implemented (long-standing request) |
| **Table maintenance** | Compact, expire snapshots, rewrite files, cleanup, checkpoint | Expire snapshots (limited) |
| **Views** | Full CRUD + scannable `View` class | Not implemented |
| **Metadata compaction** | Not needed (SQL database) | Not implemented (long-standing request) |
| **Memory management** | DuckDB handles spilling to disk | Can OOM on large scans |
| **Concurrency** | Serializable isolation via database transactions | Optimistic concurrency (no retry) |
| **External file registration** | `add_files()` with `allow_missing`/`ignore_extra_columns` | `add_files()` |
| **Inspect API** | Snapshots, files, history, partitions | Snapshots, files, manifests, entries, refs, partitions |
| **PyArrow Dataset interface** | `table.to_arrow_dataset()` | Not implemented (long-standing request) |
| **CLI** | 13 commands with text/JSON output | Full CLI |
| **Zero-infrastructure quickstart** | Yes (DuckDB file) | Requires catalog service |
| **Package size** | ~3 deps (duckdb, pyarrow, click) | ~200MB with PyArrow + optional deps |

### API Comparison

| Operation | pyducklake | pyiceberg |
|-----------|-----------|-----------|
| Load catalog | `Catalog("name", "uri")` | `load_catalog("name", **props)` |
| Create table | `catalog.create_table(id, schema)` | `catalog.create_table(id, schema)` |
| Load table | `catalog.load_table(id)` | `catalog.load_table(id)` |
| Scan | `table.scan().filter(...).select(...).to_arrow()` | `table.scan(row_filter=..., selected_fields=...).to_arrow()` |
| Append | `table.append(df)` | `table.append(df)` |
| Schema evolution | `with table.update_schema() as u: u.add_column(...)` | `with table.update_schema() as u: u.add_column(...)` |
| Partitioning | `with table.update_spec() as s: s.add_field(...)` | `with table.update_spec() as s: s.add_field(...)` |
| Transaction | `with catalog.begin_transaction() as txn: ...` | Not available |
| CDC | `table.table_changes(start, end).updates()` | Not available |
| Rollback | `table.rollback_to_snapshot(id)` | Not available |

## Metadata Backends

| Backend    | URI Example                                       |
|------------|---------------------------------------------------|
| DuckDB     | `Catalog("lake", "meta.duckdb")`                  |
| PostgreSQL | `Catalog("lake", "postgres:dbname=mydb host=localhost")` |
| MySQL      | `Catalog("lake", "mysql:host=localhost database=mydb")`  |
| SQLite     | `Catalog("lake", "sqlite:meta.sqlite")`           |

All backends expose the same API. Swap the URI and everything else stays the same.

## Features

### Catalog Management

Create, list, and drop namespaces, tables, and views.

```python
catalog.create_namespace("analytics")
catalog.list_namespaces()            # ["main", "analytics"]
catalog.list_tables("analytics")     # [("analytics", "events"), ...]

table = catalog.create_table(("analytics", "events"), schema)
catalog.rename_table("events", "events_v2")
catalog.drop_table("events_v2")
```

### Views

Full view lifecycle with scannable `View` objects.

```python
view = catalog.create_view("active_users", "SELECT * FROM users WHERE active = true")
view = catalog.load_view("active_users")

# Views are scannable, just like tables
view.scan().to_arrow()
view.scan("id > 100").select("name").to_pandas()
view.to_arrow_dataset()

catalog.create_or_replace_view("active_users", "SELECT * FROM users WHERE status = 'active'")
catalog.rename_view("active_users", "current_users")
catalog.list_views()
catalog.drop_view("current_users")
```

### Schema Definition

Use `Schema.of()` with `required()` / `optional()` helpers for concise schema definitions.

```python
from pyducklake import (
    Schema, required, optional,
    IntegerType, StringType, DoubleType, TimestampType,
    ListType, StructType, MapType, NestedField,
)

schema = Schema.of(
    required("id", IntegerType()),
    optional("name", StringType()),
    optional("score", DoubleType()),
    optional("tags", ListType(element_type=StringType())),
)

# Dict shorthand (all fields optional)
schema = Schema.of({"id": IntegerType(), "name": StringType()})
```

### Reading Data

`DataScan` is an immutable builder. Chain methods and materialize with a terminal call.

```python
scan = table.scan()

# Output formats
scan.to_arrow()                # pyarrow.Table
scan.to_pandas()               # pandas.DataFrame
scan.to_polars()               # polars.DataFrame
scan.to_duckdb()               # duckdb.DuckDBPyRelation
scan.to_arrow_batch_reader()   # pa.RecordBatchReader (streaming)
scan.to_arrow_dataset()        # pyarrow.dataset.Dataset (engine interop)
scan.count()                   # int

# Filtering, projection, limit
table.scan("price > 10.0").select("name", "price").with_limit(100).to_arrow()
```

The `to_arrow_dataset()` method returns a standard PyArrow Dataset, enabling interop with DuckDB, Polars, DataFusion, Dask, and any other engine that consumes the PyArrow dataset API.

### Writing Data

```python
import pyarrow as pa

df = pa.table({"id": [1, 2], "value": [10.0, 20.0]})

table.append(df)                                    # append rows
table.overwrite(df)                                 # replace all data
table.overwrite(df, overwrite_filter="id < 10")     # replace matching rows
table.delete("id = 1")                              # delete matching rows
result = table.upsert(df, join_cols=["id"])          # merge
print(result.rows_updated, result.rows_inserted)
```

**Streaming writes** for memory-efficient ingestion:

```python
reader = pa.RecordBatchReader.from_batches(schema, batch_iterator)
table.append_batches(reader)
```

**Arrow PyCapsule interface** — pass Polars DataFrames, nanoarrow arrays, or any object implementing `__arrow_c_stream__` directly to `append()`, `overwrite()`, and `upsert()`:

```python
import polars as pl
df = pl.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
table.append(df)  # no conversion needed
```

**External file registration:**

```python
table.add_files(
    ["s3://bucket/data.parquet"],
    allow_missing=True,          # fill missing columns with defaults
    ignore_extra_columns=True,   # drop columns not in schema
)
```

See [`examples/etl_pipeline/`](examples/etl_pipeline/) for a full ETL workflow.

### Schema Evolution

Use the `update_schema()` context manager to batch changes.

```python
with table.update_schema() as update:
    update.add_column("email", StringType())
    update.rename_column("score", "points")
    update.update_column("points", BigIntType())  # type widening
    update.drop_column("old_field")
    update.set_nullability("email", required=True)
```

See [`examples/schema_evolution/`](examples/schema_evolution/) for all operations.

### Partitioning

Hidden partitioning with identity and temporal transforms.

```python
from pyducklake import DAY, IDENTITY

with table.update_spec() as spec:
    spec.add_field("event_date", DAY)
    spec.add_field("region", IDENTITY)

table.spec               # current partition spec
table.spec.is_unpartitioned  # False
```

### Sort Orders

Configure sort orders applied during writes and compaction.

```python
from pyducklake import SortDirection, NullOrder

with table.update_sort_order() as sort:
    sort.add_field("timestamp", SortDirection.ASC, NullOrder.NULLS_LAST)
    sort.add_field("id", SortDirection.ASC)

# Writes automatically respect the sort order
table.append(unsorted_data)  # data written in sorted order
```

### Time Travel

Query historical table state by snapshot ID or timestamp.

```python
from datetime import datetime

# By snapshot ID
table.scan().with_snapshot(snap_id).to_arrow()

# By timestamp
table.scan().with_timestamp(datetime(2024, 1, 15)).to_arrow()

# Rollback to a previous state
table.rollback_to_snapshot(snap_id)
table.rollback_to_timestamp(datetime(2024, 1, 15))

# List snapshots
for snap in table.snapshots():
    print(snap.snapshot_id, snap.timestamp)
```

See [`examples/time_travel/`](examples/time_travel/) for a full walkthrough.

### Change Data Capture

Query row-level changes between snapshots or timestamps, with filtering, column projection, and update correlation.

```python
from datetime import datetime, timedelta

# All changes between snapshots
changes = table.table_changes(start_snapshot=2, end_snapshot=5)

# Timestamp-based bounds
changes = table.table_changes(
    start_time=datetime.now() - timedelta(hours=1),
    end_time=datetime.now(),
)

# Column projection and predicate pushdown
changes = table.table_changes(
    start_snapshot=2,
    columns=["id", "status"],
    filter_expr="status = 'active'",
)

# ChangeSet provides structured access
changes.inserts()           # only inserted rows
changes.deletes()           # only deleted rows
changes.update_preimages()  # pre-update row state
changes.update_postimages() # post-update row state
changes.summary()           # {"insert": 5, "delete": 2, ...}

# Correlate update pre/post images by row ID
for old_row, new_row in changes.updates():
    print(f"Changed {old_row['name']} -> {new_row['name']}")

# Convenience methods for insert-only or delete-only queries
inserted = table.table_insertions(start_snapshot=2)
deleted = table.table_deletions(start_snapshot=2)
```

See [`examples/time_travel/`](examples/time_travel/) and [`examples/table_replication/`](examples/table_replication/) for CDC examples.

### Transactions

Atomic multi-table writes with automatic rollback on error.

```python
with catalog.begin_transaction() as txn:
    orders = txn.load_table("orders")
    items = txn.load_table("order_items")

    orders.append(order_data)
    items.append(items_data)
    # Commits on clean exit; rolls back on exception
```

See [`examples/multi_table_transaction/`](examples/multi_table_transaction/).

### Table Maintenance

Compaction, snapshot expiration, and file cleanup.

```python
maint = table.maintenance()

maint.compact()                    # merge small files
maint.expire_snapshots(versions=5) # keep last 5 snapshots
maint.rewrite_data_files()         # rewrite files with current sort order
maint.cleanup_files()              # remove unreferenced files
maint.delete_orphaned_files()      # remove orphaned files
maint.checkpoint()                 # full maintenance pass
```

See [`examples/maintenance/`](examples/maintenance/).

### Encryption

Catalog-level Parquet encryption for data at rest. Keys are auto-generated per file and stored in the catalog metadata.

```python
catalog = Catalog("secure", "meta.duckdb", data_path="./data", encrypted=True)

table = catalog.create_table("pii", schema)
table.append(sensitive_data)

# Reads through the catalog are transparently decrypted
table.scan().to_arrow()

# Raw Parquet files are unreadable without the catalog
```

See [`examples/encrypted_catalog/`](examples/encrypted_catalog/).

### Inspect API

Query table metadata as Arrow tables.

```python
inspect = table.inspect()

inspect.snapshots()                      # snapshot history
inspect.files()                          # data file listing with sizes
inspect.files(snapshot_time="2024-06-01") # files at a point in time
inspect.history()                        # commit history (newest first)
inspect.partitions()                     # partition info
```

## CLI

The `pyducklake` command-line tool provides catalog inspection and maintenance.

```bash
# List tables
pyducklake --uri meta.duckdb list-tables

# Describe a table (schema, partition spec, sort order)
pyducklake --uri meta.duckdb describe users

# Show schema
pyducklake --uri meta.duckdb schema users

# List snapshots
pyducklake --uri meta.duckdb snapshots users

# List data files
pyducklake --uri meta.duckdb files users

# Compact small files
pyducklake --uri meta.duckdb compact users

# Expire old snapshots
pyducklake --uri meta.duckdb expire-snapshots users --versions 5

# JSON output
pyducklake --uri meta.duckdb --output json list-tables

# Show version
pyducklake version
```

## Examples

| Example | Description | Docker |
|---------|-------------|--------|
| [`quickstart/`](examples/quickstart/) | Create catalog, table, write data, read it back | No |
| [`etl_pipeline/`](examples/etl_pipeline/) | Batch loading, upserts, schema evolution, partitioning | No |
| [`time_travel/`](examples/time_travel/) | Historical queries and change data capture | No |
| [`multi_table_transaction/`](examples/multi_table_transaction/) | Atomic multi-table writes with rollback | No |
| [`schema_evolution/`](examples/schema_evolution/) | Add, rename, drop columns and widen types | No |
| [`maintenance/`](examples/maintenance/) | Compaction, snapshot expiration, file cleanup | No |
| [`encrypted_catalog/`](examples/encrypted_catalog/) | Parquet-level encryption for data at rest | No |
| [`postgres_backend/`](examples/postgres_backend/) | PostgreSQL as metadata backend | Yes |
| [`table_replication/`](examples/table_replication/) | CDC-based replication to downstream Ducklakes by team_id | Yes |

Run all local examples:

```bash
just examples
```

Run individual examples:

```bash
just example-quickstart
just example-etl
just example-time-travel
just example-transactions
just example-schema-evolution
just example-maintenance
just example-encryption
just example-postgres          # requires Docker
just example-replication       # requires Docker
```

## API Documentation

Full API documentation is available via pdoc:

```bash
just docs-serve
```

## Development

**Prerequisites:** [uv](https://docs.astral.sh/uv/), [just](https://github.com/casey/just), and [Docker](https://www.docker.com/) (for integration tests).

```bash
just sync              # install dependencies
just test              # unit tests
just test-integration  # integration tests (Docker required)
just ci                # lint + typecheck + test
just fmt               # format code
just typecheck         # mypy strict
just typecheck-pyright # pyright strict
just audit             # dependency vulnerability scan
just build             # build wheel + sdist
just docs              # generate API docs
```

## License

Apache License 2.0
