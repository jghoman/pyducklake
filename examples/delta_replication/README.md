# Delta Lake to Ducklake Replication (POC)

Proof-of-concept for near-real-time, append-only replication from a Delta Lake table to a Ducklake catalog using **transaction log tailing** — no Spark, no JVM, no Change Data Feed (CDF) dependency on the source.

## Architecture

```
┌─────────────────────┐         shared volume          ┌─────────────────────────────────┐
│   delta-producer    │      /data/delta/events/       │          replicator             │
│                     │                                │                                 │
│  generates events   │──▶  _delta_log/               │  1. polls _delta_log/ for new   │
│  writes via         │      000...000.json            │     version JSON files          │
│  deltalake (Rust)   │      000...001.json            │  2. parses NDJSON actions       │
│                     │      ...                       │  3. reads AddFile Parquet files  │
│                     │     part-00000.parquet         │  4. appends to Ducklake table   │
│                     │     part-00001.parquet         │  5. persists checkpoint          │
└─────────────────────┘     ...                        └──────────┬──────────────────────┘
                                                                  │
                                                                  ▼
                                                    ┌──────────────────────────┐
                                                    │      Ducklake Target     │
                                                    │                          │
                                                    │  metadata: PostgreSQL    │
                                                    │  data:     MinIO (S3)    │
                                                    └──────────────────────────┘
```

## How It Works

### Transaction Log Tailing

Delta Lake stores every mutation as a sequentially numbered JSON file in the `_delta_log/` directory. Each file is newline-delimited JSON (NDJSON) where each line is an **action**:

| Action | Key | Description |
|--------|-----|-------------|
| Protocol | `protocol` | Reader/writer version requirements |
| Metadata | `metaData` | Table schema (as Spark StructType JSON), partition columns, configuration |
| Add | `add` | A Parquet file was added to the table |
| Remove | `remove` | A Parquet file was logically deleted |
| CommitInfo | `commitInfo` | Operation type, timestamp, metrics |

The replicator:

1. Lists `_delta_log/*.json` files and sorts by version number
2. Skips versions already processed (tracked via checkpoint file)
3. For each new version, parses the NDJSON and collects `AddFileAction`s with `dataChange=true`
4. Reads the referenced Parquet files directly with PyArrow
5. Appends the Arrow data to the Ducklake target table via `pyducklake`
6. Writes the version number to `.ducklake_checkpoint.json` for crash recovery

### Schema Mapping

On first run, the replicator reads the `metaData` action from version 0, parses the Spark `schemaString`, and maps types to Ducklake:

| Spark Type | Arrow Type | Ducklake Type |
|------------|------------|---------------|
| `integer` | `int32` | `IntegerType` |
| `long` | `int64` | `BigIntType` |
| `short` | `int16` | `SmallIntType` |
| `byte` | `int8` | `TinyIntType` |
| `string` | `large_string` | `StringType` |
| `double` | `float64` | `DoubleType` |
| `float` | `float32` | `FloatType` |
| `boolean` | `bool_` | `BooleanType` |
| `binary` | `binary` | `BinaryType` |
| `date` | `date32` | `DateType` |
| `timestamp` | `timestamp(us)` | `TimestampType` |
| `timestamp` (with tz) | `timestamp(us, tz)` | `TimestampTZType` |
| `decimal(p,s)` | `decimal128(p,s)` | `DecimalType(p,s)` |

The Ducklake target table is created automatically with `create_table_if_not_exists`.

## Services

| Service | Role |
|---------|------|
| `postgres` | Ducklake metadata store |
| `minio` | S3-compatible object storage for Ducklake data files |
| `minio-init` | Creates the `ducklake-target` bucket |
| `delta-producer` | Generates analytics events, writes to a local Delta table via `deltalake` (delta-rs) |
| `replicator` | Tails the Delta log, replicates into Ducklake |

The Delta table lives on a shared Docker volume (`delta-data`) mounted at `/data/delta/` in both the producer and replicator containers. This avoids the complexity of S3-based directory listing in the log tailer.

## Files

| File | Description |
|------|-------------|
| `delta_log.py` | Pure-Python parser for Delta transaction log files. Dataclasses for all action types, NDJSON parsing, Spark-to-Arrow type conversion, schema extraction. No external dependencies beyond PyArrow. |
| `replicator.py` | `DeltaReplicator` class: polls for new versions, reads Parquet, appends to Ducklake, persists checkpoints. |
| `delta_producer.py` | Event generator that writes to a Delta table using the `deltalake` Python package. Same event shape as the `table_replication` example. |
| `replicate_service.py` | Docker entry point: waits for the Delta table to appear, creates the Ducklake catalog, runs the replicator loop. |
| `Dockerfile` | Python 3.14 slim image with `pyducklake` and `deltalake` installed. |
| `docker-compose.yml` | Full orchestration: Postgres, MinIO, producer, replicator. |

## Running

```bash
docker compose up --build
```

### Expected Output

The producer generates events at ~10/sec. The replicator picks up each new Delta version within a few seconds:

```
[delta-producer 12:00:05] flushed 50 events (total: 50, rate: 10.0/s, team-123: 3, team-456: 1)
[replicator 12:00:06] Creating target table 'events' with 7 columns
[replicator 12:00:06]   v0: appended 50 rows from part-00000.parquet
[replicator 12:00:06] run_once complete: 50 total rows replicated
[delta-producer 12:00:10] flushed 50 events (total: 100, rate: 10.0/s, team-123: 2, team-456: 2)
[replicator 12:00:11]   v1: appended 50 rows from part-00001.parquet
```

### Stopping

```bash
docker compose down -v
```

## Tests

Two test files cover the replication pipeline:

```bash
# Delta log parser tests (14 tests)
uv run python -m pytest tests/test_delta_log.py -v

# Replicator integration tests (6 tests)
uv run python -m pytest tests/test_delta_replicator.py -v
```

The tests create synthetic Delta tables on the local filesystem (no Docker required) and verify end-to-end replication into a local DuckDB-backed Ducklake catalog.

| Test | What it verifies |
|------|------------------|
| `test_version_path` | Zero-padded log file path generation |
| `test_parse_action_*` | Parsing each NDJSON action type into typed dataclasses |
| `test_parse_log_file` | Multi-action NDJSON file parsing |
| `test_list_versions` / `test_get_latest_version` | Version discovery and ordering |
| `test_spark_type_to_arrow` | All supported Spark→Arrow type mappings |
| `test_extract_schema` | End-to-end schema extraction from a Delta log |
| `test_delta_schema_to_ducklake` | Arrow→Ducklake type mapping with nullability |
| `test_replicate_single_version` | Single-version table fully replicated with correct data |
| `test_replicate_multiple_versions` | Three versions replicated in one pass |
| `test_checkpoint_persistence` | Checkpoint file written; second replicator resumes correctly |
| `test_replicate_empty_version` | Version with no data files replicates 0 rows without error |
| `test_incremental_replication` | New version appended after initial sync; only new data replicated |

## Limitations

This is a proof-of-concept. Known limitations:

| Limitation | Detail |
|------------|--------|
| **Append-only** | Only `AddFileAction` with `dataChange=true` is replicated. `RemoveFileAction` (deletes, overwrites, compaction) is ignored. |
| **No schema evolution** | Schema changes in subsequent versions are detected and logged as warnings but not applied to the Ducklake target. |
| **No partition mapping** | Delta partition columns (encoded in file paths) are not mapped to Ducklake partition transforms. |
| **Local filesystem only** | The log tailer uses `os.listdir()`, so the Delta table must be on a locally-mounted filesystem. S3-native tailing would require `fsspec`, `obstore`, or the `deltalake` package for listing. |
| **No delete/update replication** | Update and delete operations in Delta appear as remove+add pairs. Detecting and replaying these as Ducklake `upsert()` or `delete()` calls is not implemented. |
| **No CDF support** | Even when Change Data Feed is enabled on the source, the `_change_data/` files are not consumed. CDF would simplify update/delete detection significantly. |
| **Single-table** | Replicates one Delta table to one Ducklake table. Multi-table orchestration is not built in. |
| **No conflict resolution** | If the Ducklake target is also written to by other processes, there is no merge strategy. |
| **Checkpoint is per-table** | Stored as a JSON file alongside the Delta table. Not suitable for distributed or multi-node setups. |
| **No backpressure** | If the producer writes faster than the replicator can consume, versions accumulate without throttling. |

## Next Steps

Potential enhancements to move this from POC to production-grade:

### Delete and Update Replication
- Track `RemoveFileAction` alongside `AddFileAction` within each version
- Diff the removed vs. added file sets to identify updated/deleted rows
- Use `table.upsert(df, join_cols=["primary_key"])` for updates and `table.delete(filter)` for deletes
- Alternatively, consume `_change_data/` files when CDF is enabled — they already contain `_change_type` (insert/delete/update_preimage/update_postimage)

### Schema Evolution
- Detect `MetaDataAction` in versions > 0
- Parse the new `schemaString` and diff against the current Ducklake schema
- Apply changes via `table.update_schema()` context manager (add/drop/rename/widen columns)

### S3-Native Log Tailing
- Replace `os.listdir()` with `fsspec` or `obstore` for direct S3 listing of `_delta_log/`
- Eliminates the shared volume requirement
- Could also use `deltalake.DeltaTable` to list versions via delta-rs, which handles all storage backends

### Partition Mapping
- Parse `partitionColumns` from the `metaData` action
- Map to Ducklake partition transforms via `table.update_spec()`
- Handle partition-encoded columns in file paths (e.g., `year=2024/month=01/part-*.parquet`)

### Checkpoint Externalization
- Move checkpoint state from a local JSON file to the Ducklake metadata database or a dedicated state table
- Enables multi-node replication and recovery without shared filesystem access

### Multi-Table Orchestration
- Configuration-driven: specify a list of Delta tables and their Ducklake targets
- Use Ducklake's `begin_transaction()` for atomic multi-table commits
- Fan-out with per-table replicator instances

### Monitoring and Observability
- Expose replication lag (latest Delta version vs. last replicated version)
- Track rows replicated, bytes transferred, errors per version
- Integrate with Prometheus/Grafana or structured logging

### Backpressure and Rate Limiting
- Configurable batch size limits per replication cycle
- Pause polling when downstream writes are slow
- Dead-letter handling for versions that fail to replicate
