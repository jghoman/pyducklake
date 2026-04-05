"""Tests for Table.delete()."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotIn,
    NotNull,
    Or,
)
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
    return catalog.create_table("del_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def _populate(table: Table) -> None:
    df = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df)


def test_delete_with_expression(table: Table) -> None:
    _populate(table)
    table.delete(GreaterThan("value", 15))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [1]


def test_delete_with_string_filter(table: Table) -> None:
    _populate(table)
    table.delete("value > 15")
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [1]


def test_delete_all(table: Table) -> None:
    _populate(table)
    table.delete(AlwaysTrue())
    result = table.scan().to_arrow()
    assert result.num_rows == 0


def test_delete_always_false_noop(table: Table) -> None:
    _populate(table)
    table.delete(AlwaysFalse())
    result = table.scan().to_arrow()
    assert result.num_rows == 3


def test_delete_no_match(table: Table) -> None:
    _populate(table)
    table.delete(GreaterThan("value", 100))
    result = table.scan().to_arrow()
    assert result.num_rows == 3


def test_delete_from_empty_table(table: Table) -> None:
    table.delete(AlwaysTrue())
    result = table.scan().to_arrow()
    assert result.num_rows == 0


# ---------------------------------------------------------------------------
# Additional delete tests
# ---------------------------------------------------------------------------


def _make_nullable_arrow_table(
    ids: list[int],
    names: list[str | None],
    values: list[int | None],
) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def test_delete_with_equalto_expression(table: Table) -> None:
    _populate(table)
    table.delete(EqualTo("name", "bob"))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("name").to_pylist()) == ["alice", "carol"]


def test_delete_with_in_expression(table: Table) -> None:
    _populate(table)
    table.delete(In("id", (1, 3)))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]


def test_delete_with_not_expression(table: Table) -> None:
    _populate(table)
    table.delete(Not(EqualTo("id", 2)))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]


def test_delete_with_or_expression(table: Table) -> None:
    _populate(table)
    table.delete(Or(EqualTo("id", 1), EqualTo("id", 3)))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]


def test_delete_with_and_expression(table: Table) -> None:
    _populate(table)
    table.delete(And(GreaterThan("value", 5), LessThan("value", 25)))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]


def test_delete_with_is_null(table: Table) -> None:
    df = _make_nullable_arrow_table([1, 2, 3], ["alice", None, "carol"], [10, 20, 30])
    table.append(df)
    table.delete(IsNull("name"))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("name").to_pylist()) == ["alice", "carol"]


def test_delete_with_not_null(table: Table) -> None:
    df = _make_nullable_arrow_table([1, 2, 3], ["alice", None, "carol"], [10, 20, 30])
    table.append(df)
    table.delete(NotNull("name"))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == [None]


def test_delete_single_row_table(table: Table) -> None:
    df = _make_arrow_table([1], ["alice"], [10])
    table.append(df)
    table.delete(EqualTo("id", 1))
    result = table.scan().to_arrow()
    assert result.num_rows == 0


def test_delete_creates_new_snapshot(table: Table) -> None:
    _populate(table)
    snapshots_before = len(table.snapshots())
    table.delete(EqualTo("id", 1))
    snapshots_after = len(table.snapshots())
    assert snapshots_after > snapshots_before


def test_delete_preserves_unmatched_row_values(table: Table) -> None:
    _populate(table)
    table.delete(EqualTo("id", 2))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    id_to_name = dict(zip(result.column("id").to_pylist(), result.column("name").to_pylist()))
    id_to_value = dict(zip(result.column("id").to_pylist(), result.column("value").to_pylist()))
    assert id_to_name[1] == "alice"
    assert id_to_name[3] == "carol"
    assert id_to_value[1] == 10
    assert id_to_value[3] == 30


def test_delete_with_null_values_in_data(table: Table) -> None:
    df = _make_nullable_arrow_table([1, 2, 3], ["alice", None, "carol"], [10, None, 30])
    table.append(df)
    table.delete(GreaterThan("id", 2))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [1, 2]


def test_delete_null_not_matched_by_equality(table: Table) -> None:
    df = _make_nullable_arrow_table([1, 2, 3], [None, "bob", "carol"], [10, 20, 30])
    table.append(df)
    table.delete(EqualTo("name", "bob"))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    # NULL row should still be present (not matched by EqualTo)
    names = result.column("name").to_pylist()
    assert None in names
    assert "carol" in names


def test_delete_successive(table: Table) -> None:
    _populate(table)
    table.delete(EqualTo("id", 1))
    table.delete(EqualTo("id", 3))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]


def test_delete_then_append(table: Table) -> None:
    _populate(table)
    table.delete(EqualTo("id", 2))
    new_df = _make_arrow_table([4], ["dave"], [40])
    table.append(new_df)
    result = table.scan().to_arrow()
    assert result.num_rows == 3
    assert sorted(result.column("id").to_pylist()) == [1, 3, 4]


def test_delete_then_time_travel(table: Table) -> None:
    _populate(table)
    snap_before = table.current_snapshot()
    assert snap_before is not None
    table.delete(EqualTo("id", 2))
    # Time-travel to pre-delete snapshot
    old_data = table.scan(snapshot_id=snap_before.snapshot_id).to_arrow()
    assert old_data.num_rows == 3
    assert sorted(old_data.column("id").to_pylist()) == [1, 2, 3]


def test_delete_special_chars_in_string_filter(table: Table) -> None:
    df = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["it's", "normal"], type=pa.string()),
            "value": pa.array([10, 20], type=pa.int32()),
        }
    )
    table.append(df)
    table.delete(EqualTo("name", "it's"))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["normal"]


def test_delete_large_batch(table: Table) -> None:
    ids = list(range(1000))
    names = [f"name_{i}" for i in ids]
    values = list(range(1000))
    df = pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )
    table.append(df)
    # Delete rows with value >= 500
    table.delete(GreaterThanOrEqual("value", 500))
    result = table.scan().to_arrow()
    assert result.num_rows == 500
    assert max(result.column("value").to_pylist()) < 500


def test_delete_all_then_scan_returns_empty_with_schema(table: Table) -> None:
    _populate(table)
    table.delete(AlwaysTrue())
    result = table.scan().to_arrow()
    assert result.num_rows == 0
    assert result.column_names == ["id", "name", "value"]


def test_delete_with_lessequal(table: Table) -> None:
    _populate(table)
    table.delete(LessThanOrEqual("value", 20))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]


def test_delete_with_greaterequal(table: Table) -> None:
    _populate(table)
    table.delete(GreaterThanOrEqual("value", 20))
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [1]


def test_delete_with_not_in(table: Table) -> None:
    _populate(table)
    table.delete(NotIn("id", (1, 2)))
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [1, 2]


def test_delete_idempotent(table: Table) -> None:
    _populate(table)
    table.delete(EqualTo("id", 1))
    result1 = table.scan().to_arrow()
    assert result1.num_rows == 2
    # Second delete with same filter is a no-op
    table.delete(EqualTo("id", 1))
    result2 = table.scan().to_arrow()
    assert result2.num_rows == 2
    assert result2.column("id").to_pylist() == result1.column("id").to_pylist()
