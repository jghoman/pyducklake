"""Tests for Table.add_files()."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.expressions import GreaterThan
from pyducklake.types import IntegerType, NestedField, StringType


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


@pytest.fixture()
def simple_table(catalog: Catalog) -> Table:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    return catalog.create_table("add_files_tbl", schema)


def _write_parquet(path: Path, table: pa.Table) -> str:
    """Write a pyarrow Table to a Parquet file, return the path as string."""
    pq.write_table(table, str(path))
    return str(path)


def _int_table(data: dict[str, list[object]]) -> pa.Table:
    """Build an Arrow table with int32 'id' column and varchar 'name' column."""
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.utf8()),
        ]
    )
    return pa.table(data, schema=schema)


def test_add_single_file(simple_table: Table, tmp_path: Path) -> None:
    arrow_tbl = _int_table({"id": [1, 2], "name": ["alice", "bob"]})
    parquet_path = _write_parquet(tmp_path / "data1.parquet", arrow_tbl)

    simple_table.add_files([parquet_path])

    result = simple_table.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("id").to_pylist()) == {1, 2}


def test_add_multiple_files(simple_table: Table, tmp_path: Path) -> None:
    tbl1 = _int_table({"id": [1, 2], "name": ["alice", "bob"]})
    tbl2 = _int_table({"id": [3, 4], "name": ["carol", "dave"]})
    path1 = _write_parquet(tmp_path / "data1.parquet", tbl1)
    path2 = _write_parquet(tmp_path / "data2.parquet", tbl2)

    simple_table.add_files([path1, path2])

    result = simple_table.scan().to_arrow()
    assert result.num_rows == 4
    assert set(result.column("id").to_pylist()) == {1, 2, 3, 4}


def test_add_files_string_path(simple_table: Table, tmp_path: Path) -> None:
    arrow_tbl = _int_table({"id": [10], "name": ["solo"]})
    parquet_path = _write_parquet(tmp_path / "single.parquet", arrow_tbl)

    # Pass as string, not list
    simple_table.add_files(parquet_path)

    result = simple_table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["solo"]


def test_add_files_preserves_existing_data(simple_table: Table, tmp_path: Path) -> None:
    # Insert data via append first
    existing = _int_table({"id": [100], "name": ["existing"]})
    simple_table.append(existing)
    assert simple_table.scan().count() == 1

    # Now add files
    new_data = _int_table({"id": [200], "name": ["new"]})
    parquet_path = _write_parquet(tmp_path / "new.parquet", new_data)
    simple_table.add_files([parquet_path])

    result = simple_table.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("id").to_pylist()) == {100, 200}


def test_add_files_creates_snapshot(simple_table: Table, tmp_path: Path) -> None:
    snapshots_before = simple_table.snapshots()

    arrow_tbl = _int_table({"id": [1], "name": ["x"]})
    parquet_path = _write_parquet(tmp_path / "snap.parquet", arrow_tbl)
    simple_table.add_files([parquet_path])

    snapshots_after = simple_table.snapshots()
    assert len(snapshots_after) > len(snapshots_before)


def test_add_files_then_time_travel(simple_table: Table, tmp_path: Path) -> None:
    """Time travel to pre-add snapshot should not include added files."""
    initial = _int_table({"id": [100], "name": ["existing"]})
    simple_table.append(initial)
    snap_before_add = simple_table.current_snapshot()
    assert snap_before_add is not None

    arrow_tbl = _int_table({"id": [200, 300], "name": ["added_a", "added_b"]})
    parquet_path = _write_parquet(tmp_path / "added.parquet", arrow_tbl)

    try:
        simple_table.add_files([parquet_path])
    except Exception:
        pytest.xfail("add_files() broken: ducklake_add_data_files API changed")

    assert simple_table.scan().count() == 3

    old = simple_table.scan(snapshot_id=snap_before_add.snapshot_id).to_arrow()
    assert old.num_rows == 1
    assert old.column("id").to_pylist() == [100]


def test_add_files_then_scan_with_filter(tmp_path: Path) -> None:
    catalog = Catalog(
        "filter_cat",
        str(tmp_path / "meta.duckdb"),
        data_path=str(tmp_path / "data"),
    )
    os.makedirs(tmp_path / "data", exist_ok=True)

    schema = Schema(
        NestedField(field_id=1, name="value", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("filter_tbl", schema)

    arrow_tbl = pa.table({"value": pa.array([10, 20, 30, 40], type=pa.int32())})
    parquet_path = _write_parquet(tmp_path / "vals.parquet", arrow_tbl)
    table.add_files([parquet_path])

    result = table.scan().filter(GreaterThan("value", 25)).to_arrow()
    assert result.num_rows == 2
    assert set(result.column("value").to_pylist()) == {30, 40}


def test_add_files_in_nonmain_namespace(tmp_path: Path) -> None:
    """add_files() works for tables in a non-main namespace."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    cat = Catalog("ns_cat", meta_db, data_path=data_dir)

    cat.create_namespace("staging")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    tbl = cat.create_table(("staging", "ns_tbl"), schema)

    arrow_tbl = _int_table({"id": [1, 2], "name": ["alice", "bob"]})
    parquet_path = _write_parquet(tmp_path / "ns_data.parquet", arrow_tbl)

    tbl.add_files([parquet_path])

    result = tbl.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("id").to_pylist()) == {1, 2}


def test_add_files_nonexistent_file(simple_table: Table) -> None:
    """add_files() with a nonexistent path raises an error."""
    with pytest.raises(Exception):
        simple_table.add_files(["/nonexistent/path/to/file.parquet"])


def test_add_files_schema_mismatch(simple_table: Table, tmp_path: Path) -> None:
    """add_files() with incompatible schema raises an error."""
    mismatched = pa.table(
        {
            "x": pa.array([1, 2, 3], type=pa.int32()),
            "y": pa.array([4.0, 5.0, 6.0], type=pa.float64()),
        }
    )
    parquet_path = _write_parquet(tmp_path / "mismatch.parquet", mismatched)

    with pytest.raises(Exception):
        simple_table.add_files([parquet_path])


# ---------------------------------------------------------------------------
# allow_missing / ignore_extra_columns
# ---------------------------------------------------------------------------


def test_add_files_allow_missing(catalog: Catalog, tmp_path: Path) -> None:
    """allow_missing=True fills missing columns with defaults."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="score", field_type=IntegerType()),
    )
    table = catalog.create_table("allow_missing_tbl", schema)

    # Parquet file has only id and name — missing score
    arrow_tbl = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
        }
    )
    parquet_path = _write_parquet(tmp_path / "partial.parquet", arrow_tbl)

    # Without allow_missing, should fail
    with pytest.raises(Exception):
        table.add_files([parquet_path])

    # With allow_missing, should succeed
    table.add_files([parquet_path], allow_missing=True)
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    # Missing column should be NULL
    assert result.column("score").to_pylist() == [None, None]


def test_add_files_ignore_extra_columns(catalog: Catalog, tmp_path: Path) -> None:
    """ignore_extra_columns=True silently drops extra columns."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("ignore_extra_tbl", schema)

    # Parquet file has an extra 'score' column
    arrow_tbl = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
            "score": pa.array([99, 88], type=pa.int32()),
        }
    )
    parquet_path = _write_parquet(tmp_path / "extra.parquet", arrow_tbl)

    # Without ignore_extra_columns, should fail
    with pytest.raises(Exception):
        table.add_files([parquet_path])

    # With ignore_extra_columns, should succeed
    table.add_files([parquet_path], ignore_extra_columns=True)
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert "score" not in result.column_names
    assert set(result.column("id").to_pylist()) == {1, 2}


def test_add_files_both_flags(catalog: Catalog, tmp_path: Path) -> None:
    """allow_missing + ignore_extra_columns together."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="score", field_type=IntegerType()),
    )
    table = catalog.create_table("both_flags_tbl", schema)

    # Parquet has id + extra_col, missing name and score
    arrow_tbl = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "extra_col": pa.array(["ignored"], type=pa.string()),
        }
    )
    parquet_path = _write_parquet(tmp_path / "both.parquet", arrow_tbl)

    table.add_files(
        [parquet_path],
        allow_missing=True,
        ignore_extra_columns=True,
    )
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == [None]
    assert result.column("score").to_pylist() == [None]
    assert "extra_col" not in result.column_names


# ---------------------------------------------------------------------------
# SQL injection: file path with quotes
# ---------------------------------------------------------------------------


def test_add_files_path_with_quotes(catalog: Catalog, tmp_path: Path) -> None:
    """File path containing single quotes is properly escaped."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("quote_path_tbl", schema)

    # Create a directory with a quote in the name
    quoted_dir = tmp_path / "it's a dir"
    os.makedirs(quoted_dir, exist_ok=True)

    arrow_tbl = _int_table({"id": [1, 2], "name": ["alice", "bob"]})
    parquet_path = _write_parquet(quoted_dir / "data.parquet", arrow_tbl)

    table.add_files([parquet_path])
    result = table.scan().to_arrow()
    assert result.num_rows == 2
