"""Tests for snapshot rollback."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
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
    return catalog.create_table("rollback_tbl", schema)


def _make_df(ids: list[int], names: list[str]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
        }
    )


def test_rollback_to_snapshot(table: Table) -> None:
    """Write v1, write v2, rollback to v1's snapshot, verify v1 data."""
    v1 = _make_df([1, 2], ["alice", "bob"])
    table.append(v1)
    snap_v1 = table.current_snapshot()
    assert snap_v1 is not None

    v2 = _make_df([3], ["carol"])
    table.append(v2)
    assert table.scan().count() == 3

    table.rollback_to_snapshot(snap_v1.snapshot_id)
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("name").to_pylist()) == {"alice", "bob"}


def test_rollback_to_snapshot_preserves_schema(table: Table) -> None:
    """Schema unchanged after rollback."""
    v1 = _make_df([1], ["alice"])
    table.append(v1)
    snap = table.current_snapshot()
    assert snap is not None

    schema_before = table.schema
    table.append(_make_df([2], ["bob"]))
    table.rollback_to_snapshot(snap.snapshot_id)
    table.refresh()
    assert table.schema == schema_before


def test_rollback_creates_new_snapshot(table: Table) -> None:
    """Snapshot count increases after rollback (rollback is a new operation)."""
    table.append(_make_df([1], ["alice"]))
    snap = table.current_snapshot()
    assert snap is not None
    table.append(_make_df([2], ["bob"]))

    snapshots_before = len(table.snapshots())
    table.rollback_to_snapshot(snap.snapshot_id)
    snapshots_after = len(table.snapshots())
    assert snapshots_after > snapshots_before


def test_rollback_to_first_snapshot(table: Table) -> None:
    """Rollback to the very first data snapshot."""
    table.append(_make_df([1], ["alice"]))
    first_snap = table.current_snapshot()
    assert first_snap is not None

    table.append(_make_df([2], ["bob"]))
    table.append(_make_df([3], ["carol"]))
    assert table.scan().count() == 3

    table.rollback_to_snapshot(first_snap.snapshot_id)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["alice"]


def test_rollback_to_invalid_snapshot_raises(table: Table) -> None:
    """Nonexistent snapshot_id raises ValueError."""
    with pytest.raises(ValueError, match="does not exist"):
        table.rollback_to_snapshot(99999)


def test_rollback_to_timestamp(table: Table) -> None:
    """Write data at t1, more at t2, rollback to t1."""
    table.append(_make_df([1], ["alice"]))
    snap1 = table.current_snapshot()
    assert snap1 is not None

    # Small delay so timestamps differ
    time.sleep(0.1)

    table.append(_make_df([2], ["bob"]))
    assert table.scan().count() == 2

    table.rollback_to_timestamp(snap1.timestamp)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["alice"]


def test_rollback_to_timestamp_before_any_data_raises(table: Table) -> None:
    """Timestamp before table creation raises ValueError."""
    table.append(_make_df([1], ["alice"]))
    very_old = datetime(2000, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="No snapshot exists"):
        table.rollback_to_timestamp(very_old)


def test_rollback_then_append(table: Table) -> None:
    """Rollback, then append new data on top."""
    table.append(_make_df([1], ["alice"]))
    snap = table.current_snapshot()
    assert snap is not None

    table.append(_make_df([2], ["bob"]))
    table.rollback_to_snapshot(snap.snapshot_id)

    table.append(_make_df([3], ["carol"]))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("name").to_pylist()) == {"alice", "carol"}


def test_rollback_then_time_travel_to_pre_rollback(table: Table) -> None:
    """Can still time-travel to see pre-rollback state."""
    table.append(_make_df([1], ["alice"]))
    snap_with_alice = table.current_snapshot()
    assert snap_with_alice is not None

    table.append(_make_df([2], ["bob"]))
    snap_with_both = table.current_snapshot()
    assert snap_with_both is not None

    # Rollback to snapshot with only alice
    table.rollback_to_snapshot(snap_with_alice.snapshot_id)

    # Current state: just alice
    assert table.scan().count() == 1

    # Time travel to pre-rollback snapshot should still show both rows
    pre_rollback = table.scan(snapshot_id=snap_with_both.snapshot_id).to_arrow()
    assert pre_rollback.num_rows == 2


def test_rollback_empty_table(table: Table) -> None:
    """Rollback to snapshot when table was empty."""
    # Write data, get a snapshot, then write more
    table.append(_make_df([1], ["alice"]))
    snap_with_data = table.current_snapshot()
    assert snap_with_data is not None

    # Delete all data
    table.delete("1=1")
    empty_snap = table.current_snapshot()
    assert empty_snap is not None

    # Write again
    table.append(_make_df([2], ["bob"]))
    assert table.scan().count() == 1

    # Rollback to when it was empty
    table.rollback_to_snapshot(empty_snap.snapshot_id)
    assert table.scan().count() == 0


# -- P1: rollback_to_timestamp timezone handling --------------------------------


def test_rollback_to_timestamp_naive_ts_with_aware_snapshot(table: Table) -> None:
    """Naive timestamp compared against tz-aware snapshot timestamps."""
    table.append(_make_df([1], ["alice"]))
    snap = table.current_snapshot()
    assert snap is not None

    time.sleep(0.1)
    table.append(_make_df([2], ["bob"]))

    # Use naive timestamp (no tzinfo) — rollback_to_timestamp should handle it
    naive_ts = snap.timestamp.replace(tzinfo=None) if snap.timestamp.tzinfo else snap.timestamp
    table.rollback_to_timestamp(naive_ts)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["alice"]


def test_rollback_to_timestamp_aware_ts(table: Table) -> None:
    """Aware timestamp compared against snapshot timestamps."""
    table.append(_make_df([1], ["alice"]))
    snap = table.current_snapshot()
    assert snap is not None

    time.sleep(0.1)
    table.append(_make_df([2], ["bob"]))

    # Use UTC-aware timestamp
    aware_ts = snap.timestamp.replace(tzinfo=timezone.utc) if snap.timestamp.tzinfo is None else snap.timestamp
    table.rollback_to_timestamp(aware_ts)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["alice"]


def test_rollback_to_snapshot_then_rollback_again(table: Table) -> None:
    """Double rollback: rollback to snap2, then rollback to snap1."""
    table.append(_make_df([1], ["alice"]))
    snap1 = table.current_snapshot()
    assert snap1 is not None

    table.append(_make_df([2], ["bob"]))
    snap2 = table.current_snapshot()
    assert snap2 is not None

    table.append(_make_df([3], ["carol"]))
    assert table.scan().count() == 3

    table.rollback_to_snapshot(snap2.snapshot_id)
    assert table.scan().count() == 2

    table.rollback_to_snapshot(snap1.snapshot_id)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["alice"]


def test_rollback_to_current_snapshot_is_noop(table: Table) -> None:
    """Rollback to current snapshot doesn't change data."""
    table.append(_make_df([1, 2], ["alice", "bob"]))
    snap = table.current_snapshot()
    assert snap is not None

    table.rollback_to_snapshot(snap.snapshot_id)
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("name").to_pylist()) == {"alice", "bob"}
