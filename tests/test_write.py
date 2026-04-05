"""Tests for Table.append() and Table.overwrite()."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.expressions import EqualTo, GreaterThan
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
    return catalog.create_table("write_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


# -- Append --------------------------------------------------------------------


def test_append(table: Table) -> None:
    df = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df)
    result = table.scan().to_arrow()
    assert result.num_rows == 2


def test_append_multiple(table: Table) -> None:
    df1 = _make_arrow_table([1], ["alice"], [10])
    df2 = _make_arrow_table([2], ["bob"], [20])
    table.append(df1)
    table.append(df2)
    result = table.scan().to_arrow()
    assert result.num_rows == 2


def test_append_type_mismatch(table: Table) -> None:
    # Wrong column types - string where int expected
    bad_df = pa.table(
        {
            "id": pa.array(["not_an_int"], type=pa.string()),
            "name": pa.array(["alice"], type=pa.string()),
            "value": pa.array(["not_an_int"], type=pa.string()),
        }
    )
    with pytest.raises(Exception):
        table.append(bad_df)


# -- Overwrite -----------------------------------------------------------------


def test_overwrite_all(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    assert table.scan().count() == 2

    # Overwrite all with new data
    df2 = _make_arrow_table([3], ["carol"], [30])
    table.overwrite(df2)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]


def test_overwrite_with_string_filter(table: Table) -> None:
    df1 = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df1)

    # Overwrite only rows where value > 15
    df2 = _make_arrow_table([4], ["dave"], [40])
    table.overwrite(df2, overwrite_filter="value > 15")

    result = table.scan().to_arrow()
    ids = sorted(result.column("id").to_pylist())
    # Row with id=1 (value=10) should be preserved, rows 2,3 deleted, row 4 inserted
    assert ids == [1, 4]


def test_overwrite_with_expression_filter(table: Table) -> None:
    df1 = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df1)

    # Overwrite only rows where value > 15
    df2 = _make_arrow_table([4], ["dave"], [40])
    table.overwrite(df2, overwrite_filter=GreaterThan("value", 15))

    result = table.scan().to_arrow()
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 4]


def test_overwrite_empty_df_with_filter(table: Table) -> None:
    """Overwrite with empty df effectively deletes matching rows."""
    df1 = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df1)

    empty_df = _make_arrow_table([], [], [])
    table.overwrite(empty_df, overwrite_filter=GreaterThan("value", 15))

    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [1]


# -- Round-trip ----------------------------------------------------------------


def test_overwrite_rollback_on_error(table: Table) -> None:
    """Overwrite rolls back if the INSERT fails when wrapped in a transaction."""
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    assert table.scan().count() == 2

    # Attempt overwrite with type-mismatched data inside an explicit transaction.
    # overwrite() itself does not manage transactions; callers must use
    # catalog.begin_transaction() for atomicity across DELETE + INSERT.
    bad_df = pa.table(
        {
            "id": pa.array(["not_an_int"], type=pa.string()),
            "name": pa.array(["carol"], type=pa.string()),
            "value": pa.array(["bad"], type=pa.string()),
        }
    )
    with pytest.raises(Exception):
        with table.catalog.begin_transaction():
            table.overwrite(bad_df)

    # Original data should still be intact
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2]


def test_append_then_scan(table: Table) -> None:
    df = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df)

    # Scan with filter
    result = table.scan().filter(EqualTo("name", "bob")).to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]


# -- P1 tests -----------------------------------------------------------------


def test_append_wrong_column_count_raises(table: Table) -> None:
    bad_df = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["alice"], type=pa.string()),
            "value": pa.array([10], type=pa.int32()),
            "extra": pa.array([99], type=pa.int32()),
        }
    )
    with pytest.raises(Exception):
        table.append(bad_df)


@pytest.mark.xfail(reason="DuckDB INSERT uses positional column matching", strict=True)
def test_append_wrong_column_names_raises(table: Table) -> None:
    bad_df = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "wrong_name": pa.array(["alice"], type=pa.string()),
            "value": pa.array([10], type=pa.int32()),
        }
    )
    table.append(bad_df)
    pytest.fail("Expected DuckDB to reject mismatched column names")


def test_append_empty_table_succeeds(table: Table) -> None:
    empty_df = _make_arrow_table([], [], [])
    table.append(empty_df)
    result = table.scan().to_arrow()
    assert result.num_rows == 0


def test_upsert_with_null_in_join_col(catalog: Catalog) -> None:
    """NULLs in join columns: NULL != NULL so they should be inserted as new rows."""
    # Use a table where id is nullable (required=False)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType()),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )
    tbl = catalog.create_table("nullable_id_tbl", schema)

    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    tbl.append(df1)

    # Upsert with NULL in join col — both rows have NULL id
    upsert_df = pa.table(
        {
            "id": pa.array([None, None], type=pa.int32()),
            "name": pa.array(["null_a", "null_b"], type=pa.string()),
            "value": pa.array([77, 88], type=pa.int32()),
        }
    )
    result = tbl.upsert(upsert_df, join_cols=["id"])
    # NULL != NULL, so both should be inserts (no matches)
    assert result.rows_inserted == 2
    assert result.rows_updated == 0
    assert tbl.scan().count() == 4


def test_overwrite_always_false_filter(table: Table) -> None:
    from pyducklake.expressions import AlwaysFalse

    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)

    # Overwrite with AlwaysFalse — nothing should be deleted, new data inserted
    df2 = _make_arrow_table([3], ["carol"], [30])
    table.overwrite(df2, overwrite_filter=AlwaysFalse())

    result = table.scan().to_arrow()
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2, 3]


def test_append_cleanup_on_error(table: Table) -> None:
    """Verify temp view is unregistered even if INSERT fails."""
    bad_df = pa.table(
        {
            "wrong_col_a": pa.array(["not_int"], type=pa.string()),
            "wrong_col_b": pa.array(["not_int"], type=pa.string()),
            "wrong_col_c": pa.array(["not_int"], type=pa.string()),
            "wrong_col_d": pa.array(["not_int"], type=pa.string()),
        }
    )
    with pytest.raises(Exception):
        table.append(bad_df)

    # Subsequent append should work (temp view name not leaked)
    good_df = _make_arrow_table([1], ["alice"], [10])
    table.append(good_df)
    assert table.scan().count() == 1


def test_overwrite_cleanup_on_error(table: Table) -> None:
    """Verify temp view is unregistered even if overwrite fails."""
    df1 = _make_arrow_table([1], ["alice"], [10])
    table.append(df1)

    bad_df = pa.table(
        {
            "wrong": pa.array(["not_int"], type=pa.string()),
        }
    )
    with pytest.raises(Exception):
        with table.catalog.begin_transaction():
            table.overwrite(bad_df)

    # Subsequent overwrite should work
    df2 = _make_arrow_table([2], ["bob"], [20])
    table.overwrite(df2)
    assert table.scan().count() == 1


def test_overwrite_with_equal_to_filter(table: Table) -> None:
    """Overwrite with EqualTo filter."""
    df1 = _make_arrow_table([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30])
    table.append(df1)

    df2 = _make_arrow_table([4], ["dave"], [40])
    table.overwrite(df2, overwrite_filter=EqualTo("name", "bob"))

    result = table.scan().to_arrow()
    names = sorted(result.column("name").to_pylist())
    assert names == ["alice", "carol", "dave"]


# -- PyCapsule / Arrow C Stream Interface -------------------------------------


def test_append_polars_dataframe(table: Table) -> None:
    """Polars DataFrames implement __arrow_c_stream__ and should work directly."""
    import polars as pl

    pdf = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"], "value": [10, 20]})
    table.append(pdf)
    result = table.scan().to_arrow()
    assert result.num_rows == 2


def test_overwrite_polars_dataframe(table: Table) -> None:
    import polars as pl

    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)

    pdf = pl.DataFrame({"id": [3], "name": ["carol"], "value": [30]})
    table.overwrite(pdf)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]


def test_upsert_polars_dataframe(table: Table) -> None:
    import polars as pl

    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)

    pdf = pl.DataFrame({"id": [2, 3], "name": ["bob_updated", "carol"], "value": [25, 30]})
    result = table.upsert(pdf, join_cols=["id"])
    assert result.rows_updated == 1
    assert result.rows_inserted == 1
    assert table.scan().count() == 3


def test_append_arrow_record_batch_reader(table: Table) -> None:
    """RecordBatchReader implements __arrow_c_stream__."""
    batch = pa.record_batch(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
            "value": pa.array([10, 20], type=pa.int32()),
        }
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    table.append(reader)
    result = table.scan().to_arrow()
    assert result.num_rows == 2


def test_append_invalid_type_raises(table: Table) -> None:
    with pytest.raises(TypeError, match="Expected pyarrow.Table or object implementing __arrow_c_stream__"):
        table.append("not a table")  # type: ignore[arg-type]


def test_append_pycapsule_round_trip(table: Table) -> None:
    """Append via PyCapsule interface and read back to verify data integrity."""
    import polars as pl

    pdf = pl.DataFrame(
        {
            "id": [10, 20, 30],
            "name": ["x", "y", "z"],
            "value": [100, 200, 300],
        }
    )
    table.append(pdf)
    result = table.scan().to_arrow()
    assert result.num_rows == 3
    assert sorted(result.column("id").to_pylist()) == [10, 20, 30]
    assert sorted(result.column("name").to_pylist()) == ["x", "y", "z"]
    assert sorted(result.column("value").to_pylist()) == [100, 200, 300]


# -- Streaming Writes (append_batches) ----------------------------------------


def test_append_batches_from_reader(table: Table) -> None:
    batch = pa.record_batch(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
            "value": pa.array([10, 20], type=pa.int32()),
        }
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    table.append_batches(reader)
    result = table.scan().to_arrow()
    assert result.num_rows == 2


def test_append_batches_from_iterator(table: Table) -> None:
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("value", pa.int32()),
        ]
    )
    batches = [
        pa.record_batch(
            {"id": [1], "name": ["alice"], "value": [10]},
            schema=arrow_schema,
        ),
        pa.record_batch(
            {"id": [2], "name": ["bob"], "value": [20]},
            schema=arrow_schema,
        ),
    ]
    table.append_batches(iter(batches), schema=arrow_schema)
    result = table.scan().to_arrow()
    assert result.num_rows == 2


def test_append_batches_iterator_no_schema_raises(table: Table) -> None:
    batch = pa.record_batch(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["alice"], type=pa.string()),
            "value": pa.array([10], type=pa.int32()),
        }
    )
    with pytest.raises(ValueError, match="schema is required"):
        table.append_batches(iter([batch]))


def test_append_batches_empty_reader(table: Table) -> None:
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("value", pa.int32()),
        ]
    )
    reader = pa.RecordBatchReader.from_batches(arrow_schema, [])
    table.append_batches(reader)
    result = table.scan().to_arrow()
    assert result.num_rows == 0


def test_append_batches_multiple_batches(table: Table) -> None:
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("value", pa.int32()),
        ]
    )
    batches = [
        pa.record_batch({"id": [i], "name": [f"name_{i}"], "value": [i * 10]}, schema=arrow_schema) for i in range(1, 6)
    ]
    reader = pa.RecordBatchReader.from_batches(arrow_schema, batches)
    table.append_batches(reader)
    result = table.scan().to_arrow()
    assert result.num_rows == 5


def test_append_batches_preserves_existing_data(table: Table) -> None:
    df1 = _make_arrow_table([1, 2], ["alice", "bob"], [10, 20])
    table.append(df1)
    assert table.scan().count() == 2

    batch = pa.record_batch(
        {
            "id": pa.array([3, 4], type=pa.int32()),
            "name": pa.array(["carol", "dave"], type=pa.string()),
            "value": pa.array([30, 40], type=pa.int32()),
        }
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    table.append_batches(reader)
    result = table.scan().to_arrow()
    assert result.num_rows == 4
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4]


# -- P1: append_batches error handling ----------------------------------------


def test_append_batches_type_mismatch_raises(table: Table) -> None:
    """append_batches with wrong types raises error."""
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.string()),  # wrong type — table expects int32
            pa.field("name", pa.string()),
            pa.field("value", pa.string()),
        ]
    )
    batch = pa.record_batch(
        {"id": ["not_int"], "name": ["alice"], "value": ["not_int"]},
        schema=arrow_schema,
    )
    reader = pa.RecordBatchReader.from_batches(arrow_schema, [batch])
    with pytest.raises(Exception):
        table.append_batches(reader)


def test_append_batches_cleanup_on_error(table: Table) -> None:
    """Temp view is cleaned up even if INSERT fails."""
    arrow_schema = pa.schema(
        [
            pa.field("wrong_a", pa.string()),
            pa.field("wrong_b", pa.string()),
            pa.field("wrong_c", pa.string()),
            pa.field("wrong_d", pa.string()),
        ]
    )
    batch = pa.record_batch(
        {"wrong_a": ["x"], "wrong_b": ["y"], "wrong_c": ["z"], "wrong_d": ["w"]},
        schema=arrow_schema,
    )
    reader = pa.RecordBatchReader.from_batches(arrow_schema, [batch])
    with pytest.raises(Exception):
        table.append_batches(reader)

    # Subsequent operation should work (temp name not leaked)
    good_df = _make_arrow_table([1], ["alice"], [10])
    table.append(good_df)
    assert table.scan().count() == 1


# -- P1: _to_arrow_table with custom PyCapsule object -------------------------


def test_to_arrow_table_custom_pycapsule(table: Table) -> None:
    """Object implementing __arrow_c_stream__ works."""

    class MockArrowExportable:
        def __init__(self, tbl: pa.Table) -> None:
            self._tbl = tbl

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            return self._tbl.__arrow_c_stream__(requested_schema)

    mock_obj = MockArrowExportable(
        pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "name": pa.array(["alice", "bob"], type=pa.string()),
                "value": pa.array([10, 20], type=pa.int32()),
            }
        )
    )
    table.append(mock_obj)
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [1, 2]


def test_upsert_with_custom_pycapsule(table: Table) -> None:
    """Upsert with an object implementing __arrow_c_stream__."""
    table.append(_make_arrow_table([1], ["alice"], [10]))

    class MockArrowExportable:
        def __init__(self, tbl: pa.Table) -> None:
            self._tbl = tbl

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            return self._tbl.__arrow_c_stream__(requested_schema)

    mock_obj = MockArrowExportable(
        pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "name": pa.array(["alice_v2", "bob"], type=pa.string()),
                "value": pa.array([99, 20], type=pa.int32()),
            }
        )
    )
    result = table.upsert(mock_obj, join_cols=["id"])
    assert result.rows_updated == 1
    assert result.rows_inserted == 1
    assert table.scan().count() == 2


def test_overwrite_with_custom_pycapsule(table: Table) -> None:
    """Overwrite with an object implementing __arrow_c_stream__."""
    table.append(_make_arrow_table([1, 2], ["alice", "bob"], [10, 20]))

    class MockArrowExportable:
        def __init__(self, tbl: pa.Table) -> None:
            self._tbl = tbl

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            return self._tbl.__arrow_c_stream__(requested_schema)

    mock_obj = MockArrowExportable(
        pa.table(
            {
                "id": pa.array([3], type=pa.int32()),
                "name": pa.array(["carol"], type=pa.string()),
                "value": pa.array([30], type=pa.int32()),
            }
        )
    )
    table.overwrite(mock_obj)
    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]
