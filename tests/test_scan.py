"""Tests for pyducklake.scan.DataScan."""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.expressions import EqualTo, GreaterThan, LessThan
from pyducklake.types import IntegerType, NestedField, StringType


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


@pytest.fixture()
def table_with_data(catalog: Catalog) -> Table:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )
    table = catalog.create_table("scan_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice', 10)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'bob', 20)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (3, 'carol', 30)")
    return table


# -- Basic scan ----------------------------------------------------------------


def test_basic_scan(table_with_data: Table) -> None:
    result = table_with_data.scan().to_arrow()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 3


# -- Select --------------------------------------------------------------------


def test_scan_select(table_with_data: Table) -> None:
    result = table_with_data.scan().select("id", "name").to_arrow()
    assert result.num_columns == 2
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 3


# -- Filter with BooleanExpression --------------------------------------------


def test_scan_filter_expression(table_with_data: Table) -> None:
    result = table_with_data.scan().filter(GreaterThan("value", 15)).to_arrow()
    assert result.num_rows == 2


# -- Filter with string -------------------------------------------------------


def test_scan_filter_string(table_with_data: Table) -> None:
    result = table_with_data.scan().filter("value > 15").to_arrow()
    assert result.num_rows == 2


# -- Combined select + filter -------------------------------------------------


def test_scan_combined(table_with_data: Table) -> None:
    result = table_with_data.scan().select("id", "value").filter(GreaterThan("value", 15)).to_arrow()
    assert result.num_rows == 2
    assert result.column_names == ["id", "value"]


# -- Count ---------------------------------------------------------------------


def test_scan_count(table_with_data: Table) -> None:
    count = table_with_data.scan().count()
    assert count == 3


def test_scan_count_with_filter(table_with_data: Table) -> None:
    count = table_with_data.scan().filter(GreaterThan("value", 15)).count()
    assert count == 2


# -- Limit ---------------------------------------------------------------------


def test_scan_limit(table_with_data: Table) -> None:
    result = table_with_data.scan().with_limit(2).to_arrow()
    assert result.num_rows == 2


# -- to_pandas -----------------------------------------------------------------


def test_scan_to_pandas(table_with_data: Table) -> None:
    pytest.importorskip("pandas")
    df = table_with_data.scan().to_pandas()
    assert list(df.columns) == ["id", "name", "value"]
    assert len(df) == 3


# -- Time travel ---------------------------------------------------------------


def test_scan_time_travel(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("tt_tbl", schema)
    fqn = table.fully_qualified_name

    # Insert first batch
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    snapshots_after_first = table.snapshots()

    # Insert second batch
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    # Current should have 2 rows
    assert table.scan().count() == 2

    # Time travel to first snapshot should have 1 row
    if snapshots_after_first:
        first_snap_id = snapshots_after_first[-1].snapshot_id
        result = table.scan().with_snapshot(first_snap_id).to_arrow()
        assert result.num_rows == 1


# -- Chaining ------------------------------------------------------------------


def test_scan_chaining(table_with_data: Table) -> None:
    result = (
        table_with_data.scan().filter(GreaterThan("value", 5)).filter(LessThan("value", 25)).select("name").to_arrow()
    )
    assert result.num_rows == 2
    assert result.column_names == ["name"]


# -- Immutability --------------------------------------------------------------


def test_scan_immutability(table_with_data: Table) -> None:
    scan1 = table_with_data.scan()
    scan2 = scan1.filter(GreaterThan("value", 15))
    scan3 = scan1.select("id")

    # Original scan should still return all rows / all columns
    r1 = scan1.to_arrow()
    assert r1.num_rows == 3
    assert r1.num_columns == 3

    r2 = scan2.to_arrow()
    assert r2.num_rows == 2

    r3 = scan3.to_arrow()
    assert r3.num_columns == 1


# -- Empty table ---------------------------------------------------------------


def test_scan_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
        NestedField(field_id=2, name="b", field_type=StringType()),
    )
    table = catalog.create_table("empty_tbl", schema)
    result = table.scan().to_arrow()
    assert result.num_rows == 0
    assert result.column_names == ["a", "b"]


# -- P1 tests -----------------------------------------------------------------


def test_scan_select_nonexistent_column_raises(table_with_data: Table) -> None:
    with pytest.raises(Exception):
        table_with_data.scan().select("nonexistent_col").to_arrow()


def test_scan_count_with_limit(table_with_data: Table) -> None:
    # count() with a limit should still return count (limited by the LIMIT clause in SQL)
    count_no_limit = table_with_data.scan().count()
    count_with_limit = table_with_data.scan().with_limit(2).count()
    # count() builds SQL with LIMIT, so COUNT(*) with LIMIT 2 still returns <= 3
    # The actual behavior: SELECT COUNT(*) ... LIMIT 2 returns the full count
    # because COUNT(*) returns a single row
    assert count_no_limit == 3
    assert count_with_limit == 3


@pytest.mark.xfail(reason="DuckDB rejects negative LIMIT values", strict=True)
def test_scan_negative_limit(table_with_data: Table) -> None:
    result = table_with_data.scan().with_limit(-1).to_arrow()
    assert result.num_rows == 3


def test_scan_nonexistent_snapshot_raises(table_with_data: Table) -> None:
    with pytest.raises(Exception):
        table_with_data.scan().with_snapshot(999999).to_arrow()


def test_table_scan_with_string_filter_param(table_with_data: Table) -> None:
    result = table_with_data.scan(row_filter="value > 15").to_arrow()
    assert result.num_rows == 2


# -- Phase 2: time travel by timestamp ----------------------------------------


def test_scan_with_timestamp(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("ts_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    # Record local timestamp after first insert (Ducklake stores local time)
    time.sleep(1)
    ts_after_first = datetime.now()
    time.sleep(1)

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    # Current should have 2 rows
    assert table.scan().count() == 2

    # Time travel to timestamp should have 1 row
    result = table.scan().with_timestamp(ts_after_first).to_arrow()
    assert result.num_rows == 1


def test_scan_with_snapshot_then_timestamp_uses_timestamp(catalog: Catalog) -> None:
    """with_timestamp() after with_snapshot() clears snapshot_id so no conflict."""
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("snap_ts_clear_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    time.sleep(1)
    ts_after = datetime.now()
    time.sleep(1)
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    # Chain with_snapshot then with_timestamp — timestamp wins, no error
    result = table.scan().with_snapshot(999).with_timestamp(ts_after).to_arrow()
    assert result.num_rows == 1


# -- Phase 2: to_duckdb -------------------------------------------------------


def test_scan_to_duckdb(table_with_data: Table) -> None:
    rel = table_with_data.scan().to_duckdb()
    assert isinstance(rel, duckdb.DuckDBPyRelation)
    arrow = rel.fetchall()
    assert len(arrow) == 3


# -- Phase 2: to_arrow_batch_reader -------------------------------------------


def test_scan_to_arrow_batch_reader(table_with_data: Table) -> None:
    reader = table_with_data.scan().to_arrow_batch_reader()
    assert isinstance(reader, pa.RecordBatchReader)
    batches = list(reader)
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 3


# -- Phase 2: to_duckdb with filter/select/limit ------------------------------


def test_scan_to_duckdb_with_filter(table_with_data: Table) -> None:
    rel = table_with_data.scan().filter(GreaterThan("value", 15)).to_duckdb()
    rows = rel.fetchall()
    assert len(rows) == 2


def test_scan_to_duckdb_with_select(table_with_data: Table) -> None:
    rel = table_with_data.scan().select("id", "name").to_duckdb()
    rows = rel.fetchall()
    assert len(rows) == 3
    # Each row should have exactly 2 columns
    assert all(len(r) == 2 for r in rows)


def test_scan_to_duckdb_with_limit(table_with_data: Table) -> None:
    rel = table_with_data.scan().with_limit(1).to_duckdb()
    rows = rel.fetchall()
    assert len(rows) == 1


def test_scan_to_duckdb_returns_queryable_relation(table_with_data: Table) -> None:
    rel = table_with_data.scan().to_duckdb()
    # Relation should support further querying via fetchall
    rows = rel.fetchall()
    assert isinstance(rows, list)
    assert len(rows) == 3
    # Verify we can also materialize via fetchdf (requires pandas)
    pytest.importorskip("pandas")
    rel2 = table_with_data.scan().to_duckdb()
    df = rel2.fetchdf()
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "value"]


def test_scan_to_duckdb_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
        NestedField(field_id=2, name="b", field_type=StringType()),
    )
    table = catalog.create_table("empty_duckdb_tbl", schema)
    rel = table.scan().to_duckdb()
    rows = rel.fetchall()
    assert len(rows) == 0


# -- Phase 2: to_arrow_batch_reader with filter/select/empty ------------------


def test_scan_to_arrow_batch_reader_with_filter(table_with_data: Table) -> None:
    reader = table_with_data.scan().filter(EqualTo("name", "alice")).to_arrow_batch_reader()
    tbl = reader.read_all()
    assert tbl.num_rows == 1
    assert tbl.column("name").to_pylist() == ["alice"]


def test_scan_to_arrow_batch_reader_with_select(table_with_data: Table) -> None:
    reader = table_with_data.scan().select("id", "value").to_arrow_batch_reader()
    tbl = reader.read_all()
    assert tbl.num_rows == 3
    assert tbl.column_names == ["id", "value"]


def test_scan_to_arrow_batch_reader_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
        NestedField(field_id=2, name="b", field_type=StringType()),
    )
    table = catalog.create_table("empty_batch_tbl", schema)
    reader = table.scan().to_arrow_batch_reader()
    tbl = reader.read_all()
    assert tbl.num_rows == 0
    assert set(tbl.column_names) == {"a", "b"}


def test_scan_to_arrow_batch_reader_schema(table_with_data: Table) -> None:
    reader = table_with_data.scan().to_arrow_batch_reader()
    schema = reader.schema
    assert len(schema) == 3
    field_names = [schema.field(i).name for i in range(len(schema))]
    assert field_names == ["id", "name", "value"]


def test_scan_to_arrow_batch_reader_read_all_matches_to_arrow(table_with_data: Table) -> None:
    arrow_tbl = table_with_data.scan().to_arrow()
    reader = table_with_data.scan().to_arrow_batch_reader()
    reader_tbl = reader.read_all()
    assert arrow_tbl.equals(reader_tbl)


# -- Phase 2: time travel combined with filter/select -------------------------


def test_scan_with_timestamp_and_filter(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("ts_filter_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (10)")
    time.sleep(1)
    ts_after_first = datetime.now()
    time.sleep(1)

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (100)")

    # At ts_after_first there should be 2 rows; filter x > 5 => 1 row
    result = table.scan().with_timestamp(ts_after_first).filter(GreaterThan("x", 5)).to_arrow()
    assert result.num_rows == 1
    assert result.column("x").to_pylist() == [10]


def test_scan_with_timestamp_and_select(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="y", field_type=StringType()),
    )
    table = catalog.create_table("ts_select_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'a')")
    time.sleep(1)
    ts_after_first = datetime.now()
    time.sleep(1)

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'b')")

    result = table.scan().with_timestamp(ts_after_first).select("x").to_arrow()
    assert result.num_rows == 1
    assert result.column_names == ["x"]


def test_scan_with_snapshot_and_filter(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("snap_filter_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (10)")
    snap = table.snapshots()[-1]

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (100)")

    result = table.scan().with_snapshot(snap.snapshot_id).filter(GreaterThan("x", 5)).to_arrow()
    assert result.num_rows == 1
    assert result.column("x").to_pylist() == [10]


def test_scan_with_snapshot_and_select(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="y", field_type=StringType()),
    )
    table = catalog.create_table("snap_select_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'a')")
    snap = table.snapshots()[-1]

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'b')")

    result = table.scan().with_snapshot(snap.snapshot_id).select("y").to_arrow()
    assert result.num_rows == 1
    assert result.column_names == ["y"]


def test_scan_with_snapshot_and_limit(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("snap_limit_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")
    snap = table.snapshots()[-1]

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (3)")

    result = table.scan().with_snapshot(snap.snapshot_id).with_limit(1).to_arrow()
    assert result.num_rows == 1


# -- Phase 2: to_pandas with filter -------------------------------------------


def test_scan_to_pandas_with_filter(table_with_data: Table) -> None:
    pytest.importorskip("pandas")
    df = table_with_data.scan().filter(EqualTo("name", "bob")).to_pandas()
    assert len(df) == 1
    assert list(df.columns) == ["id", "name", "value"]
    assert df["name"].iloc[0] == "bob"


# -- Phase 2: count, select edge cases ----------------------------------------


def test_scan_count_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
    )
    table = catalog.create_table("count_empty_tbl", schema)
    assert table.scan().count() == 0


def test_scan_select_single_column(table_with_data: Table) -> None:
    result = table_with_data.scan().select("name").to_arrow()
    assert result.num_columns == 1
    assert result.column_names == ["name"]
    assert result.num_rows == 3


def test_scan_select_all_columns_explicit(table_with_data: Table) -> None:
    result = table_with_data.scan().select("id", "name", "value").to_arrow()
    assert result.num_columns == 3
    assert result.column_names == ["id", "name", "value"]
    assert result.num_rows == 3


def test_scan_filter_no_rows_match_has_correct_schema(table_with_data: Table) -> None:
    result = table_with_data.scan().filter(EqualTo("name", "nobody")).to_arrow()
    assert result.num_rows == 0
    assert result.column_names == ["id", "name", "value"]


def test_scan_to_duckdb_external_connection_error(table_with_data: Table) -> None:
    """Passing an external duckdb connection that lacks the catalog should error."""
    external_conn = duckdb.connect()
    with pytest.raises(Exception):
        table_with_data.scan().to_duckdb(connection=external_conn).fetchall()
    external_conn.close()


def test_scan_chaining_preserves_timestamp(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="y", field_type=StringType()),
    )
    table = catalog.create_table("chain_ts_tbl", schema)
    fqn = table.fully_qualified_name

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'a')")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (10, 'b')")
    time.sleep(1)
    ts_after = datetime.now()
    time.sleep(1)

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (100, 'c')")

    # Chain with_timestamp -> filter -> select; timestamp should be preserved
    result = table.scan().with_timestamp(ts_after).filter(GreaterThan("x", 5)).select("y").to_arrow()
    assert result.num_rows == 1
    assert result.column_names == ["y"]
    assert result.column("y").to_pylist() == ["b"]


# -- P0: RawSQL expression ---------------------------------------------------


def test_raw_sql_expression() -> None:
    from pyducklake.scan import RawSQL

    raw = RawSQL("x > 5 AND y = 'hello'")
    assert raw.to_sql() == "x > 5 AND y = 'hello'"
    assert repr(raw) == "RawSQL(sql=\"x > 5 AND y = 'hello'\")"


def test_raw_sql_equality() -> None:
    from pyducklake.scan import RawSQL

    a = RawSQL("x > 5")
    b = RawSQL("x > 5")
    c = RawSQL("x > 6")
    assert a == b
    assert a != c


# -- P1: scan builder preserves all fields ------------------------------------


def test_scan_builder_preserves_filter_on_select(table_with_data: Table) -> None:
    """select() after filter() preserves the filter."""
    scan = table_with_data.scan().filter(GreaterThan("value", 15)).select("id")
    result = scan.to_arrow()
    assert result.num_rows == 2
    assert result.column_names == ["id"]


def test_scan_zero_limit(table_with_data: Table) -> None:
    """LIMIT 0 returns no rows."""
    result = table_with_data.scan().with_limit(0).to_arrow()
    assert result.num_rows == 0


# -- P1: scan constructor with row_filter param --------------------------------


def test_scan_constructor_with_expression_filter(table_with_data: Table) -> None:
    """Table.scan(row_filter=BooleanExpression) works."""
    result = table_with_data.scan(row_filter=GreaterThan("value", 15)).to_arrow()
    assert result.num_rows == 2


def test_scan_constructor_with_selected_fields(table_with_data: Table) -> None:
    """Table.scan(selected_fields=...) works."""
    result = table_with_data.scan(selected_fields=("id", "name")).to_arrow()
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 3


def test_scan_constructor_with_limit(table_with_data: Table) -> None:
    """Table.scan(limit=N) works."""
    result = table_with_data.scan(limit=1).to_arrow()
    assert result.num_rows == 1


def test_scan_constructor_with_snapshot_id(catalog: Catalog) -> None:
    """Table.scan(snapshot_id=N) works."""
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("scan_ctor_snap", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    snap = table.snapshots()[-1]
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    result = table.scan(snapshot_id=snap.snapshot_id).to_arrow()
    assert result.num_rows == 1


# -- P1: count with filter and various expressions ----------------------------


def test_scan_count_with_string_filter(table_with_data: Table) -> None:
    count = table_with_data.scan().filter("value = 20").count()
    assert count == 1


def test_scan_filter_always_false_returns_zero(table_with_data: Table) -> None:
    """Filter with AlwaysFalse() returns 0 rows — but And simplification means
    the expression is AlwaysFalse(), and its to_sql() is 'FALSE'."""
    from pyducklake.expressions import AlwaysFalse

    result = table_with_data.scan().filter(AlwaysFalse()).to_arrow()
    assert result.num_rows == 0


# -- with_snapshot / with_timestamp mutual exclusion ---------------------------


def test_with_snapshot_clears_timestamp(catalog: Catalog) -> None:
    """with_snapshot() after with_timestamp() clears the timestamp."""
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("ws_clears_ts_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    snap = table.snapshots()[-1]
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    ts = datetime.now()
    # Chain with_timestamp then with_snapshot — snapshot wins, no error
    result = table.scan().with_timestamp(ts).with_snapshot(snap.snapshot_id).to_arrow()
    assert result.num_rows == 1


def test_with_timestamp_clears_snapshot(catalog: Catalog) -> None:
    """with_timestamp() after with_snapshot() clears the snapshot_id."""
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("wt_clears_snap_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    time.sleep(1)
    ts_after = datetime.now()
    time.sleep(1)
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    # Chain with_snapshot (bogus id) then with_timestamp — timestamp wins, no error
    result = table.scan().with_snapshot(999999).with_timestamp(ts_after).to_arrow()
    assert result.num_rows == 1
