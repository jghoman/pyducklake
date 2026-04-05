"""Tests for Table.upsert()."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table, UpsertResult
from pyducklake.expressions import GreaterThan
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
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )
    return catalog.create_table("upsert_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def test_upsert_insert_only(table: Table) -> None:
    df = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    result = table.upsert(df, join_cols=("id",))

    assert isinstance(result, UpsertResult)
    assert result.rows_inserted == 2
    assert result.rows_updated == 0

    data = table.scan().to_arrow()
    assert data.num_rows == 2


def test_upsert_update_only(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)

    df2 = _make_arrow_table([1, 2], ["alice_updated", "bob_updated"], [100, 200])
    result = table.upsert(df2, join_cols=("id",))

    assert result.rows_updated == 2
    assert result.rows_inserted == 0

    data = table.scan().to_arrow()
    assert data.num_rows == 2
    names = sorted(data.column("name").to_pylist())
    assert names == ["alice_updated", "bob_updated"]


def test_upsert_mixed(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)

    # id=2 matches (update), id=3 is new (insert)
    df2 = _make_arrow_table([2, 3], ["bob_updated", "carol"], [200, 30])
    result = table.upsert(df2, join_cols=("id",))

    assert result.rows_updated == 1
    assert result.rows_inserted == 1

    data = table.scan().to_arrow()
    assert data.num_rows == 3
    ids = sorted(data.column("id").to_pylist())
    assert ids == [1, 2, 3]


def test_upsert_multiple_join_cols(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="b", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="val", field_type=StringType()),
    )
    table = catalog.create_table("upsert_multi", schema)

    df1 = pa.table(
        {
            "a": pa.array([1, 1, 2], type=pa.int32()),
            "b": pa.array([10, 20, 10], type=pa.int32()),
            "val": pa.array(["x", "y", "z"], type=pa.string()),
        }
    )
    table.append(df1)

    # (1,10) matches, (2,20) is new
    df2 = pa.table(
        {
            "a": pa.array([1, 2], type=pa.int32()),
            "b": pa.array([10, 20], type=pa.int32()),
            "val": pa.array(["x_updated", "w"], type=pa.string()),
        }
    )
    result = table.upsert(df2, join_cols=("a", "b"))

    assert result.rows_updated == 1
    assert result.rows_inserted == 1

    data = table.scan().to_arrow()
    assert data.num_rows == 4


def test_upsert_preserves_unmatched(table: Table) -> None:
    df1 = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df1)

    # Only upsert id=2
    df2 = _make_arrow_table([2], ["bob_updated"], [200])
    table.upsert(df2, join_cols=("id",))

    data = table.scan().to_arrow()
    assert data.num_rows == 3
    # alice and carol should be unchanged
    id_to_name = dict(zip(data.column("id").to_pylist(), data.column("name").to_pylist()))
    assert id_to_name[1] == "alice"
    assert id_to_name[3] == "carol"
    assert id_to_name[2] == "bob_updated"


# ---------------------------------------------------------------------------
# Additional upsert tests
# ---------------------------------------------------------------------------


def test_upsert_all_rows_match_zero_inserted(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    df2 = _make_arrow_table([1, 2], ["alice_v2", "bob_v2"], [100, 200])
    result = table.upsert(df2, join_cols=("id",))
    assert result.rows_inserted == 0
    assert result.rows_updated == 2


def test_upsert_with_null_in_non_join_col(table: Table) -> None:
    df1 = _make_arrow_table([1], ["alice"], [10])
    table.append(df1)
    # Update name to NULL
    df2 = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array([None], type=pa.string()),
            "value": pa.array([99], type=pa.int32()),
        }
    )
    result = table.upsert(df2, join_cols=("id",))
    assert result.rows_updated == 1
    assert result.rows_inserted == 0
    data = table.scan().to_arrow()
    assert data.column("name").to_pylist() == [None]
    assert data.column("value").to_pylist() == [99]


def test_upsert_result_fields(table: Table) -> None:
    df = _make_arrow_table([1], ["alice"], [10])
    result = table.upsert(df, join_cols=("id",))
    assert hasattr(result, "rows_updated")
    assert hasattr(result, "rows_inserted")
    assert isinstance(result.rows_updated, int)
    assert isinstance(result.rows_inserted, int)


def test_upsert_creates_snapshot(table: Table) -> None:
    df1 = _make_arrow_table([1], ["alice"], [10])
    table.append(df1)
    snapshots_before = len(table.snapshots())
    df2 = _make_arrow_table([1], ["alice_v2"], [100])
    table.upsert(df2, join_cols=("id",))
    snapshots_after = len(table.snapshots())
    assert snapshots_after > snapshots_before


def test_upsert_join_cols_as_list(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    df2 = _make_arrow_table([2, 3], ["bob_v2", "carol"], [200, 30])
    result = table.upsert(df2, join_cols=["id"])
    assert result.rows_updated == 1
    assert result.rows_inserted == 1
    data = table.scan().to_arrow()
    assert data.num_rows == 3


def test_upsert_single_join_col_single_row(table: Table) -> None:
    df1 = _make_arrow_table([1], ["alice"], [10])
    table.append(df1)
    df2 = _make_arrow_table([1], ["alice_updated"], [100])
    result = table.upsert(df2, join_cols=("id",))
    assert result.rows_updated == 1
    assert result.rows_inserted == 0
    data = table.scan().to_arrow()
    assert data.num_rows == 1
    assert data.column("name").to_pylist() == ["alice_updated"]


def test_upsert_preserves_unmatched_values(table: Table) -> None:
    df1 = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df1)
    df2 = _make_arrow_table([2], ["bob_v2"], [200])
    table.upsert(df2, join_cols=("id",))
    data = table.scan().to_arrow()
    id_to_name = dict(zip(data.column("id").to_pylist(), data.column("name").to_pylist()))
    id_to_value = dict(zip(data.column("id").to_pylist(), data.column("value").to_pylist()))
    assert id_to_name[1] == "alice"
    assert id_to_value[1] == 10
    assert id_to_name[3] == "carol"
    assert id_to_value[3] == 30
    assert id_to_name[2] == "bob_v2"
    assert id_to_value[2] == 200


def test_upsert_then_time_travel(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    snap_before = table.current_snapshot()
    assert snap_before is not None
    df2 = _make_arrow_table([1], ["alice_v2"], [100])
    table.upsert(df2, join_cols=("id",))
    # Time-travel to pre-upsert snapshot
    old_data = table.scan(snapshot_id=snap_before.snapshot_id).to_arrow()
    assert old_data.num_rows == 2
    old_names = dict(zip(old_data.column("id").to_pylist(), old_data.column("name").to_pylist()))
    assert old_names[1] == "alice"


def test_upsert_then_delete(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    df2 = _make_arrow_table([2, 3], ["bob_v2", "carol"], [200, 30])
    table.upsert(df2, join_cols=("id",))
    table.delete(GreaterThan("value", 100))
    data = table.scan().to_arrow()
    assert sorted(data.column("id").to_pylist()) == [1, 3]


def test_upsert_updates_only_non_join_cols(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    df2 = _make_arrow_table([1, 2], ["alice_v2", "bob_v2"], [100, 200])
    table.upsert(df2, join_cols=("id",))
    data = table.scan().to_arrow()
    # Join column "id" should be unchanged
    assert sorted(data.column("id").to_pylist()) == [1, 2]
    # Non-join columns should be updated
    id_to_name = dict(zip(data.column("id").to_pylist(), data.column("name").to_pylist()))
    assert id_to_name[1] == "alice_v2"
    assert id_to_name[2] == "bob_v2"


def test_upsert_with_different_column_order(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    # Source table with columns in different order: value, name, id
    df2 = pa.table(
        {
            "value": pa.array([100], type=pa.int32()),
            "name": pa.array(["alice_v2"], type=pa.string()),
            "id": pa.array([1], type=pa.int32()),
        }
    )
    result = table.upsert(df2, join_cols=("id",))
    assert result.rows_updated == 1
    data = table.scan().to_arrow()
    id_to_name = dict(zip(data.column("id").to_pylist(), data.column("name").to_pylist()))
    assert id_to_name[1] == "alice_v2"


def test_upsert_large_batch(table: Table) -> None:
    # Insert 500 rows
    ids_initial = list(range(500))
    names_initial = [f"name_{i}" for i in ids_initial]
    values_initial = [i * 10 for i in ids_initial]
    df1 = pa.table(
        {
            "id": pa.array(ids_initial, type=pa.int32()),
            "name": pa.array(names_initial, type=pa.string()),
            "value": pa.array(values_initial, type=pa.int32()),
        }
    )
    table.append(df1)
    # Upsert 500 rows: 250 update (id 0-249), 250 insert (id 500-749)
    ids_upsert = list(range(250)) + list(range(500, 750))
    names_upsert = [f"updated_{i}" for i in range(250)] + [f"new_{i}" for i in range(500, 750)]
    values_upsert = [i * 100 for i in range(250)] + [i * 10 for i in range(500, 750)]
    df2 = pa.table(
        {
            "id": pa.array(ids_upsert, type=pa.int32()),
            "name": pa.array(names_upsert, type=pa.string()),
            "value": pa.array(values_upsert, type=pa.int32()),
        }
    )
    result = table.upsert(df2, join_cols=("id",))
    assert result.rows_inserted == 250
    assert result.rows_updated == 250
    data = table.scan().to_arrow()
    assert data.num_rows == 750


def test_upsert_empty_source(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    empty_df = pa.table(
        {
            "id": pa.array([], type=pa.int32()),
            "name": pa.array([], type=pa.string()),
            "value": pa.array([], type=pa.int32()),
        }
    )
    result = table.upsert(empty_df, join_cols=("id",))
    assert result.rows_inserted == 0
    assert result.rows_updated == 0
    data = table.scan().to_arrow()
    assert data.num_rows == 2
