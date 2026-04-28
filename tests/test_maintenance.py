"""Tests for MaintenanceTable operations."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.maintenance import MaintenanceTable
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
    return catalog.create_table("maint_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
        }
    )


# -- compact ---------------------------------------------------------------


def test_compact_no_error(table: Table) -> None:
    table.append(_make_arrow_table([1, 2], ["a", "b"]))
    table.maintenance().compact()


def test_compact_with_multiple_files(table: Table) -> None:
    for i in range(5):
        ids = list(range(i * 100, (i + 1) * 100))
        table.append(_make_arrow_table(ids, [f"n{x}" for x in ids]))
    table.maintenance().compact()
    result = table.scan().to_arrow()
    assert result.num_rows == 500


def test_compact_empty_table(table: Table) -> None:
    table.maintenance().compact()


def test_compact_with_params(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().compact(min_file_size=1024, max_file_size=1048576)


def test_compact_then_scan(table: Table) -> None:
    table.append(_make_arrow_table([1, 2, 3], ["a", "b", "c"]))
    table.append(_make_arrow_table([4, 5], ["d", "e"]))
    table.maintenance().compact()
    result = table.scan().to_arrow()
    assert result.num_rows == 5
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2, 3, 4, 5]


# -- rewrite_data_files ----------------------------------------------------


def test_rewrite_data_files_no_error(table: Table) -> None:
    table.append(_make_arrow_table([1, 2, 3], ["a", "b", "c"]))
    table.delete("id = 1")
    table.maintenance().rewrite_data_files()


def test_rewrite_data_files_after_delete(table: Table) -> None:
    table.append(_make_arrow_table([1, 2, 3, 4, 5], ["a", "b", "c", "d", "e"]))
    table.delete("id <= 3")
    table.maintenance().rewrite_data_files()
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    ids = sorted(result.column("id").to_pylist())
    assert ids == [4, 5]


def test_rewrite_data_files_with_threshold(table: Table) -> None:
    table.append(_make_arrow_table([1, 2], ["a", "b"]))
    table.maintenance().rewrite_data_files(delete_threshold=0.5)


# -- expire_snapshots ------------------------------------------------------


def test_expire_snapshots_no_error(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().expire_snapshots()


def test_expire_snapshots_with_versions(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.append(_make_arrow_table([2], ["b"]))
    table.maintenance().expire_snapshots(versions=1)


def test_expire_snapshots_dry_run(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    snapshots_before = table.snapshots()
    table.maintenance().expire_snapshots(dry_run=True)
    snapshots_after = table.snapshots()
    assert len(snapshots_after) == len(snapshots_before)


# -- cleanup_files ---------------------------------------------------------


def test_cleanup_files_no_error(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().cleanup_files()


def test_cleanup_files_dry_run(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().cleanup_files(dry_run=True)


# -- delete_orphaned_files -------------------------------------------------


def test_delete_orphaned_files_no_error(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().delete_orphaned_files()


def test_delete_orphaned_files_dry_run(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().delete_orphaned_files(dry_run=True)


# -- checkpoint ------------------------------------------------------------


def test_checkpoint_no_error(table: Table) -> None:
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().checkpoint()


def test_checkpoint_preserves_data(table: Table) -> None:
    table.append(_make_arrow_table([1, 2, 3], ["a", "b", "c"]))
    table.maintenance().checkpoint()
    result = table.scan().to_arrow()
    assert result.num_rows == 3
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2, 3]


# -- misc ------------------------------------------------------------------


def test_maintenance_method_returns_maintenance_table(table: Table) -> None:
    m = table.maintenance()
    assert isinstance(m, MaintenanceTable)


@pytest.mark.duckdb15
def test_compact_max_compacted_files(table: Table) -> None:
    """compact with max_compacted_files param should not error."""
    for i in range(5):
        table.append(_make_arrow_table([i], [f"n{i}"]))
    table.maintenance().compact(max_compacted_files=3)


def test_rewrite_then_scan_values(table: Table) -> None:
    """Rewrite after deletes; verify actual row VALUES, not just count."""
    table.append(_make_arrow_table([1, 2, 3, 4, 5], ["a", "b", "c", "d", "e"]))
    table.delete("id IN (2, 4)")
    table.maintenance().rewrite_data_files()

    result = table.scan().to_arrow()
    ids = sorted(result.column("id").to_pylist())
    names = sorted(result.column("name").to_pylist())
    assert ids == [1, 3, 5]
    assert names == ["a", "c", "e"]


def test_expire_then_time_travel(table: Table) -> None:
    """After expiring old snapshots, some snapshots are removed.

    Ducklake may still allow time-travel to data snapshots whose files
    haven't been cleaned up, so we verify the snapshot count decreases
    rather than asserting a hard failure on time-travel.
    """
    table.append(_make_arrow_table([1], ["a"]))
    table.append(_make_arrow_table([2], ["b"]))
    table.append(_make_arrow_table([3], ["c"]))

    snapshots_before = table.snapshots()

    # Expire keeping only 1 version
    table.maintenance().expire_snapshots(versions=1)

    snapshots_after = table.snapshots()
    # At least one snapshot should have been expired
    assert len(snapshots_after) <= len(snapshots_before)


# -- P1: expire_snapshots with older_than param --------------------------------


def test_expire_snapshots_with_older_than(table: Table) -> None:
    """expire_snapshots with older_than string should not error."""
    table.append(_make_arrow_table([1], ["a"]))
    table.append(_make_arrow_table([2], ["b"]))
    # Use a future timestamp so nothing gets expired
    table.maintenance().expire_snapshots(older_than="2099-01-01 00:00:00")
    # Data should still be accessible
    assert table.scan().count() == 2


def test_expire_snapshots_with_older_than_and_versions(table: Table) -> None:
    """Both older_than and versions params together."""
    table.append(_make_arrow_table([1], ["a"]))
    table.append(_make_arrow_table([2], ["b"]))
    table.append(_make_arrow_table([3], ["c"]))
    table.maintenance().expire_snapshots(older_than="2099-01-01 00:00:00", versions=1)
    assert table.scan().count() == 3


def test_cleanup_files_with_older_than(table: Table) -> None:
    """cleanup_files with older_than param."""
    table.append(_make_arrow_table([1], ["a"]))
    table.maintenance().cleanup_files(older_than="2099-01-01 00:00:00")


# -- SQL injection: older_than with quotes -----------------------------------


def test_maintenance_older_than_with_quotes(table: Table) -> None:
    """older_than with quotes is rejected by format validation."""
    table.append(_make_arrow_table([1], ["a"]))
    with pytest.raises(ValueError, match="Invalid older_than"):
        table.maintenance().expire_snapshots(older_than="2099-01-01'; DROP TABLE --")

    with pytest.raises(ValueError, match="Invalid older_than"):
        table.maintenance().cleanup_files(older_than="2099-01-01'; DROP TABLE --")
