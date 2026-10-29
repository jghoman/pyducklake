"""Tests for the Delta Lake -> Ducklake replicator."""

from __future__ import annotations

import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Add examples dir to path so we can import the replicator module.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "examples", "delta_replication"),
)

from replicator import DeltaReplicator

from pyducklake import Catalog
from pyducklake.types import (
    BigIntType,
    BooleanType,
    DoubleType,
    IntegerType,
    SmallIntType,
    StringType,
    TimestampType,
    TinyIntType,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_ndjson(path: str, actions: list[dict[str, object]]) -> None:
    """Write a list of dicts as newline-delimited JSON."""
    with open(path, "w") as f:
        for action in actions:
            f.write(json.dumps(action) + "\n")


def create_test_delta_table(
    base_path: str,
    schema_fields: list[tuple[str, str, bool]],
    versions: list[pa.Table],
) -> str:
    """Create a minimal Delta table with real Parquet data files.

    Args:
        base_path: Root directory for the table.
        schema_fields: List of ``(name, spark_type, nullable)`` tuples.
        versions: One ``pa.Table`` per version; each becomes a Parquet file
            referenced by an ``add`` action.

    Returns:
        The table path (same as *base_path*).
    """
    table_path = base_path
    log_dir = os.path.join(table_path, "_delta_log")
    os.makedirs(log_dir, exist_ok=True)

    schema_string = json.dumps(
        {
            "type": "struct",
            "fields": [
                {
                    "name": name,
                    "type": spark_type,
                    "nullable": nullable,
                    "metadata": {},
                }
                for name, spark_type, nullable in schema_fields
            ],
        }
    )

    for version, arrow_table in enumerate(versions):
        actions: list[dict[str, object]] = []
        if version == 0:
            actions.append({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
            actions.append(
                {
                    "metaData": {
                        "id": "test-table-id",
                        "format": {"provider": "parquet", "options": {}},
                        "schemaString": schema_string,
                        "partitionColumns": [],
                        "configuration": {},
                        "createdTime": 1700000000000,
                    }
                }
            )

        # Write the actual Parquet data file.
        parquet_filename = f"part-{version:05d}.parquet"
        parquet_path = os.path.join(table_path, parquet_filename)
        pq.write_table(arrow_table, parquet_path)

        file_size = os.path.getsize(parquet_path)
        actions.append(
            {
                "add": {
                    "path": parquet_filename,
                    "partitionValues": {},
                    "size": file_size,
                    "modificationTime": 1700000000000 + version,
                    "dataChange": True,
                }
            }
        )
        actions.append(
            {
                "commitInfo": {
                    "timestamp": 1700000000000 + version,
                    "operation": "WRITE",
                    "operationParameters": {"mode": "Append"},
                }
            }
        )

        write_ndjson(os.path.join(log_dir, f"{version:020d}.json"), actions)

    return table_path


def append_delta_version(
    table_path: str,
    version: int,
    arrow_table: pa.Table,
) -> None:
    """Append a new version to an existing Delta table."""
    log_dir = os.path.join(table_path, "_delta_log")

    parquet_filename = f"part-{version:05d}.parquet"
    parquet_path = os.path.join(table_path, parquet_filename)
    pq.write_table(arrow_table, parquet_path)

    file_size = os.path.getsize(parquet_path)
    actions: list[dict[str, object]] = [
        {
            "add": {
                "path": parquet_filename,
                "partitionValues": {},
                "size": file_size,
                "modificationTime": 1700000000000 + version,
                "dataChange": True,
            }
        },
        {
            "commitInfo": {
                "timestamp": 1700000000000 + version,
                "operation": "WRITE",
                "operationParameters": {"mode": "Append"},
            }
        },
    ]
    write_ndjson(os.path.join(log_dir, f"{version:020d}.json"), actions)


# ---------------------------------------------------------------------------
# Shared schema definition used by most tests
# ---------------------------------------------------------------------------

SCHEMA_FIELDS: list[tuple[str, str, bool]] = [
    ("id", "long", False),
    ("name", "string", True),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def ducklake_catalog(tmp_path: object) -> Catalog:
    """Create a throwaway local Ducklake catalog."""
    path = str(tmp_path)  # type: ignore[arg-type]
    meta_db = os.path.join(path, "ducklake_meta.duckdb")
    data_dir = os.path.join(path, "ducklake_data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_delta_schema_to_ducklake() -> None:
    """Arrow-to-Ducklake type mapping covers the common types."""
    arrow_schema = pa.schema(
        [
            pa.field("a_int8", pa.int8(), nullable=False),
            pa.field("b_int16", pa.int16(), nullable=True),
            pa.field("c_int32", pa.int32(), nullable=False),
            pa.field("d_int64", pa.int64(), nullable=True),
            pa.field("e_str", pa.large_string(), nullable=True),
            pa.field("f_double", pa.float64(), nullable=True),
            pa.field("g_bool", pa.bool_(), nullable=False),
            pa.field("h_ts", pa.timestamp("us"), nullable=True),
        ]
    )

    ducklake_schema = DeltaReplicator._delta_schema_to_ducklake(arrow_schema)

    fields = list(ducklake_schema)
    type_map = {f.name: f.field_type for f in fields}
    nullable_map = {f.name: not f.required for f in fields}

    assert isinstance(type_map["a_int8"], TinyIntType)
    assert isinstance(type_map["b_int16"], SmallIntType)
    assert isinstance(type_map["c_int32"], IntegerType)
    assert isinstance(type_map["d_int64"], BigIntType)
    assert isinstance(type_map["e_str"], StringType)
    assert isinstance(type_map["f_double"], DoubleType)
    assert isinstance(type_map["g_bool"], BooleanType)
    assert isinstance(type_map["h_ts"], TimestampType)

    assert nullable_map["a_int8"] is False
    assert nullable_map["c_int32"] is False
    assert nullable_map["g_bool"] is False
    assert nullable_map["b_int16"] is True
    assert nullable_map["e_str"] is True


def test_replicate_single_version(tmp_path: object, ducklake_catalog: Catalog) -> None:
    """A single-version Delta table is fully replicated."""
    path = str(tmp_path)  # type: ignore[arg-type]
    table_data = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", "c"]})
    delta_path = create_test_delta_table(os.path.join(path, "delta"), SCHEMA_FIELDS, [table_data])

    replicator = DeltaReplicator(delta_path, ducklake_catalog, "replicated_single")
    total = replicator.run_once()

    assert total == 3

    result = ducklake_catalog.load_table("replicated_single").scan().to_arrow()
    assert result.num_rows == 3
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3]
    assert sorted(result.column("name").to_pylist()) == ["a", "b", "c"]


def test_replicate_multiple_versions(tmp_path: object, ducklake_catalog: Catalog) -> None:
    """Three versions (initial + 2 appends) are all replicated in one pass."""
    path = str(tmp_path)  # type: ignore[arg-type]
    v0 = pa.table({"id": pa.array([1, 2], pa.int64()), "name": ["a", "b"]})
    v1 = pa.table({"id": pa.array([3, 4], pa.int64()), "name": ["c", "d"]})
    v2 = pa.table({"id": pa.array([5], pa.int64()), "name": ["e"]})

    delta_path = create_test_delta_table(os.path.join(path, "delta"), SCHEMA_FIELDS, [v0, v1, v2])

    replicator = DeltaReplicator(delta_path, ducklake_catalog, "replicated_multi")
    total = replicator.run_once()

    assert total == 5

    result = ducklake_catalog.load_table("replicated_multi").scan().to_arrow()
    assert result.num_rows == 5
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4, 5]


def test_checkpoint_persistence(tmp_path: object, ducklake_catalog: Catalog) -> None:
    """Checkpoint file is written and a new replicator instance resumes from it."""
    path = str(tmp_path)  # type: ignore[arg-type]
    table_data = pa.table({"id": pa.array([1, 2], pa.int64()), "name": ["x", "y"]})
    delta_path = create_test_delta_table(os.path.join(path, "delta"), SCHEMA_FIELDS, [table_data])

    # First replicator — processes version 0.
    r1 = DeltaReplicator(delta_path, ducklake_catalog, "replicated_ckpt")
    r1.run_once()

    checkpoint_file = os.path.join(delta_path, ".ducklake_checkpoint.json")
    assert os.path.exists(checkpoint_file)

    with open(checkpoint_file) as f:
        ckpt = json.load(f)
    assert ckpt["last_replicated_version"] == 0

    # Second replicator — should resume from checkpoint and replicate nothing.
    r2 = DeltaReplicator(delta_path, ducklake_catalog, "replicated_ckpt")
    assert r2.last_replicated_version == 0

    total = r2.run_once()
    assert total == 0  # no new versions


def test_replicate_empty_version(tmp_path: object, ducklake_catalog: Catalog) -> None:
    """A version with only commitInfo (no AddFile) should replicate 0 rows."""
    path = str(tmp_path)  # type: ignore[arg-type]
    delta_path = os.path.join(path, "delta")
    log_dir = os.path.join(delta_path, "_delta_log")
    os.makedirs(log_dir, exist_ok=True)

    schema_string = json.dumps(
        {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
    )

    # Version 0: protocol + metadata (no add action).
    write_ndjson(
        os.path.join(log_dir, "00000000000000000000.json"),
        [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema_string,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1700000000000,
                }
            },
            {
                "commitInfo": {
                    "timestamp": 1700000000000,
                    "operation": "CREATE TABLE",
                    "operationParameters": {},
                }
            },
        ],
    )

    replicator = DeltaReplicator(delta_path, ducklake_catalog, "replicated_empty")
    total = replicator.run_once()
    assert total == 0


def test_incremental_replication(tmp_path: object, ducklake_catalog: Catalog) -> None:
    """After initial replication, a second run_once picks up only new versions."""
    path = str(tmp_path)  # type: ignore[arg-type]
    v0 = pa.table({"id": pa.array([1, 2], pa.int64()), "name": ["a", "b"]})

    delta_path = create_test_delta_table(os.path.join(path, "delta"), SCHEMA_FIELDS, [v0])

    replicator = DeltaReplicator(delta_path, ducklake_catalog, "replicated_incr")
    total1 = replicator.run_once()
    assert total1 == 2

    # Append a new version to the Delta log.
    v1 = pa.table({"id": pa.array([3, 4, 5], pa.int64()), "name": ["c", "d", "e"]})
    append_delta_version(delta_path, 1, v1)

    total2 = replicator.run_once()
    assert total2 == 3  # only the new version's rows

    result = ducklake_catalog.load_table("replicated_incr").scan().to_arrow()
    assert result.num_rows == 5
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4, 5]
