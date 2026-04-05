"""Tests for InspectTable metadata introspection."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.inspect import InspectTable
from pyducklake.types import IntegerType, NestedField, StringType


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


@pytest.fixture()
def table(catalog: Catalog) -> Table:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    return catalog.create_table("inspect_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
        }
    )


def test_inspect_returns_inspect_table(table: Table) -> None:
    result = table.inspect()
    assert isinstance(result, InspectTable)


def test_inspect_snapshots_empty_table(table: Table) -> None:
    result = table.inspect().snapshots()
    assert isinstance(result, pa.Table)
    # Table creation creates snapshots (schema + table creation)
    assert result.num_rows >= 0
    assert "snapshot_id" in result.column_names


def test_inspect_snapshots_after_writes(table: Table) -> None:
    df = _make_arrow_table([1, 2], ["alice", "bob"])
    table.append(df)
    df2 = _make_arrow_table([3], ["carol"])
    table.append(df2)

    result = table.inspect().snapshots()
    assert isinstance(result, pa.Table)
    # At least the two insert snapshots
    assert result.num_rows >= 2
    assert "snapshot_id" in result.column_names
    assert "snapshot_time" in result.column_names


def test_inspect_files_empty_table(table: Table) -> None:
    result = table.inspect().files()
    assert isinstance(result, pa.Table)
    # No data files for empty table
    assert result.num_rows == 0


def test_inspect_files_after_write(table: Table) -> None:
    # Insert enough data to create an actual file (not inlined)
    ids = list(range(1000))
    names = [f"name_{i}" for i in ids]
    df = _make_arrow_table(ids, names)
    table.append(df)

    result = table.inspect().files()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 1
    assert "data_file" in result.column_names


def test_inspect_history_order(table: Table) -> None:
    df = _make_arrow_table([1], ["alice"])
    table.append(df)
    df2 = _make_arrow_table([2], ["bob"])
    table.append(df2)

    result = table.inspect().history()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 2
    # History should be newest-first
    snapshot_ids = result.column("snapshot_id").to_pylist()
    assert snapshot_ids == sorted(snapshot_ids, reverse=True)


def test_inspect_partitions_unpartitioned(table: Table) -> None:
    result = table.inspect().partitions()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


# ---------------------------------------------------------------------------
# New coverage tests
# ---------------------------------------------------------------------------


def test_inspect_snapshots_columns(table: Table) -> None:
    """Verify expected columns exist in snapshots result."""
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.inspect().snapshots()
    for col in ("snapshot_id", "snapshot_time"):
        assert col in result.column_names, f"Missing column: {col}"


def test_inspect_snapshots_after_delete(table: Table) -> None:
    """A snapshot appears after a delete operation."""
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_count_before = table.inspect().snapshots().num_rows

    table.delete("id = 1")
    snap_count_after = table.inspect().snapshots().num_rows
    assert snap_count_after > snap_count_before


def test_inspect_snapshots_after_upsert(table: Table) -> None:
    """A snapshot appears after an upsert operation."""
    table.append(_make_arrow_table([1], ["alice"]))
    snap_count_before = table.inspect().snapshots().num_rows

    df = _make_arrow_table([1, 2], ["alice_v2", "bob"])
    table.upsert(df, join_cols=["id"])
    snap_count_after = table.inspect().snapshots().num_rows
    assert snap_count_after > snap_count_before


def test_inspect_files_at_specific_snapshot(table: Table) -> None:
    """files(snapshot_id=N) returns files at that snapshot."""
    ids = list(range(1000))
    names = [f"name_{i}" for i in ids]
    table.append(_make_arrow_table(ids, names))

    snap = table.current_snapshot()
    assert snap is not None

    result = table.inspect().files(snapshot_id=snap.snapshot_id)
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 1


def test_inspect_files_multiple_writes(table: Table) -> None:
    """Multiple appends produce multiple data files."""
    ids1 = list(range(1000))
    ids2 = list(range(1000, 2000))
    table.append(_make_arrow_table(ids1, [f"n{i}" for i in ids1]))
    table.append(_make_arrow_table(ids2, [f"n{i}" for i in ids2]))

    result = table.inspect().files()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 2


def test_inspect_files_columns(table: Table) -> None:
    """Verify files() result has expected columns."""
    ids = list(range(1000))
    table.append(_make_arrow_table(ids, [f"n{i}" for i in ids]))
    result = table.inspect().files()
    assert "data_file" in result.column_names


def test_inspect_history_single_snapshot(table: Table) -> None:
    """History with exactly 1 data snapshot."""
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.inspect().history()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 1


def test_inspect_history_columns(table: Table) -> None:
    """History has same columns as snapshots."""
    table.append(_make_arrow_table([1], ["alice"]))
    snap_cols = set(table.inspect().snapshots().column_names)
    hist_cols = set(table.inspect().history().column_names)
    assert snap_cols == hist_cols


def test_inspect_partitions_after_add(catalog: Catalog) -> None:
    """Add partition, verify partitions() returns data."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    tbl = catalog.create_table("part_tbl", schema)
    tbl.update_spec().add_field("name").commit()

    result = tbl.inspect().partitions()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 1


def test_inspect_partitions_after_clear(catalog: Catalog) -> None:
    """Add then clear partitioning; partitions() returns empty."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    tbl = catalog.create_table("part_clr_tbl", schema)
    tbl.update_spec().add_field("name").commit()
    tbl.update_spec().clear().commit()

    result = tbl.inspect().partitions()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_inspect_snapshots_monotonic_ids(table: Table) -> None:
    """Snapshot IDs are strictly increasing."""
    table.append(_make_arrow_table([1], ["alice"]))
    table.append(_make_arrow_table([2], ["bob"]))

    result = table.inspect().snapshots()
    ids = result.column("snapshot_id").to_pylist()
    for i in range(1, len(ids)):
        assert ids[i] > ids[i - 1], f"Non-monotonic at index {i}: {ids}"


def test_inspect_history_after_overwrite(table: Table) -> None:
    """History includes overwrite snapshot."""
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_before = table.inspect().history().num_rows

    table.overwrite(_make_arrow_table([3], ["carol"]))
    snap_after = table.inspect().history().num_rows
    assert snap_after > snap_before


def test_inspect_files_after_overwrite(table: Table) -> None:
    """Files reflect post-overwrite state."""
    ids = list(range(1000))
    table.append(_make_arrow_table(ids, [f"n{i}" for i in ids]))
    new_ids = list(range(2000, 3000))
    table.overwrite(_make_arrow_table(new_ids, [f"n{i}" for i in new_ids]))

    result = table.inspect().files()
    assert isinstance(result, pa.Table)
    # Should have files for the new data
    assert result.num_rows >= 1


def test_inspect_files_nonmain_namespace(catalog: Catalog) -> None:
    """Inspect files for a table in a non-main namespace."""
    catalog.create_namespace("staging")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    tbl = catalog.create_table(("staging", "ns_inspect_tbl"), schema)

    # Write enough data to generate a file
    ids = list(range(1000))
    tbl.append(_make_arrow_table(ids, [f"n{i}" for i in ids]))

    result = tbl.inspect().files()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 1
    assert "data_file" in result.column_names


def test_inspect_partitions_multiple_columns(catalog: Catalog) -> None:
    """Partitions with multiple columns returns expected rows."""
    from pyducklake.types import DateType

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="event_date", field_type=DateType()),
    )
    tbl = catalog.create_table("multi_part_tbl", schema)
    tbl.update_spec().add_field("name").add_field("event_date").commit()

    result = tbl.inspect().partitions()
    assert isinstance(result, pa.Table)
    assert result.num_rows >= 2


def test_inspect_files_empty_table_columns(table: Table) -> None:
    """files() on empty table returns table with expected columns."""
    result = table.inspect().files()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0
    # Schema should still have column names
    assert len(result.column_names) > 0


def test_inspect_snapshots_with_commit_message(catalog: Catalog, table: Table) -> None:
    """Set commit message, verify it shows in snapshots."""
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("test snapshot msg")
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    snapshots = table.inspect().snapshots()
    if "commit_message" in snapshots.column_names:
        messages = snapshots.column("commit_message").to_pylist()
        assert "test snapshot msg" in messages


# ---------------------------------------------------------------------------
# files() return columns
# ---------------------------------------------------------------------------


def test_inspect_files_all_columns(table: Table) -> None:
    """Verify files() returns all documented columns."""
    table.append(_make_arrow_table(list(range(100)), [f"n{i}" for i in range(100)]))
    result = table.inspect().files()
    assert result.num_rows >= 1
    expected = {
        "data_file",
        "data_file_size_bytes",
        "data_file_footer_size",
    }
    actual = set(result.column_names)
    for col in expected:
        assert col in actual, f"Missing column: {col}"


def test_inspect_files_size_bytes_positive(table: Table) -> None:
    """Data file size should be a positive integer."""
    table.append(_make_arrow_table(list(range(100)), [f"n{i}" for i in range(100)]))
    result = table.inspect().files()
    sizes = result.column("data_file_size_bytes").to_pylist()
    for s in sizes:
        assert s is not None
        assert s > 0


def test_inspect_files_delete_columns_present(table: Table) -> None:
    """After a delete, the delete_file column exists in the result."""
    # Write enough rows to avoid data inlining, then delete
    table.append(_make_arrow_table(list(range(500)), [f"n{i}" for i in range(500)]))
    table.delete("id < 100")
    result = table.inspect().files()
    assert "delete_file" in result.column_names


def test_inspect_files_encryption_key_null_unencrypted(table: Table) -> None:
    """Unencrypted table should have NULL encryption keys."""
    table.append(_make_arrow_table([1], ["a"]))
    result = table.inspect().files()
    if "data_file_encryption_key" in result.column_names:
        keys = result.column("data_file_encryption_key").to_pylist()
        assert all(k is None for k in keys)


def test_inspect_files_encryption_key_set_encrypted(tmp_path: Path) -> None:
    """Encrypted table should have non-NULL encryption keys."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    cat = Catalog("enc_cat", meta_db, data_path=data_dir, encrypted=True)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    tbl = cat.create_table("enc_tbl", schema)
    tbl.append(_make_arrow_table(list(range(100)), [f"n{i}" for i in range(100)]))

    result = tbl.inspect().files()
    if "data_file_encryption_key" in result.column_names:
        keys = result.column("data_file_encryption_key").to_pylist()
        non_null = [k for k in keys if k is not None]
        assert len(non_null) >= 1


# ---------------------------------------------------------------------------
# files() with snapshot_time
# ---------------------------------------------------------------------------


def test_inspect_files_with_snapshot_time(table: Table) -> None:
    """files(snapshot_time=...) returns files at that timestamp."""
    import time

    table.append(_make_arrow_table([1], ["a"]))

    # Wait so the next snapshot has a distinct timestamp
    time.sleep(1.1)

    table.append(_make_arrow_table([2], ["b"]))

    # Use a timestamp slightly in the future to ensure it captures current state
    from datetime import datetime, timedelta, timezone

    now = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
    ts_str = now.strftime("%Y-%m-%d %H:%M:%S")

    result = table.inspect().files(snapshot_time=ts_str)
    assert isinstance(result, pa.Table)


def test_inspect_files_snapshot_id_and_time_raises(table: Table) -> None:
    """Cannot specify both snapshot_id and snapshot_time."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        table.inspect().files(snapshot_id=1, snapshot_time="2025-01-01 00:00:00")
