"""Tests for PyArrow Dataset interface on Table and DataScan."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
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
def table_with_data(catalog: Catalog) -> Table:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )
    table = catalog.create_table("ds_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice', 10)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'bob', 20)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (3, 'carol', 30)")
    return table


# -- Basic conversion ---------------------------------------------------------


def test_table_to_arrow_dataset(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    assert isinstance(dataset, ds.Dataset)


def test_dataset_to_table(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    result = dataset.to_table()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 3
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2, 3]


def test_dataset_schema(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    schema = dataset.schema
    assert "id" in schema.names
    assert "name" in schema.names
    assert "value" in schema.names


# -- Scanner operations -------------------------------------------------------


def test_dataset_scanner(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    scanner = dataset.scanner(filter=pc.field("value") > 15)
    result = scanner.to_table()
    assert result.num_rows == 2
    ids = sorted(result.column("id").to_pylist())
    assert ids == [2, 3]


def test_dataset_scanner_with_projection(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    scanner = dataset.scanner(columns=["id", "name"])
    result = scanner.to_table()
    assert result.num_columns == 2
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 3


def test_dataset_count_rows(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    assert dataset.count_rows() == 3


# -- Time travel ---------------------------------------------------------------


def test_dataset_with_time_travel(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="val", field_type=IntegerType()),
    )
    table = catalog.create_table("tt_ds_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 100)")
    snap1 = table.current_snapshot()
    assert snap1 is not None

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 200)")

    # Current state has 2 rows
    ds_current = table.to_arrow_dataset()
    assert ds_current.count_rows() == 2

    # Historical snapshot has 1 row
    ds_old = table.to_arrow_dataset(snapshot_id=snap1.snapshot_id)
    assert ds_old.count_rows() == 1


# -- Empty table ---------------------------------------------------------------


def test_dataset_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
        NestedField(field_id=2, name="b", field_type=StringType()),
    )
    table = catalog.create_table("empty_ds_tbl", schema)
    dataset = table.to_arrow_dataset()
    assert isinstance(dataset, ds.Dataset)
    assert dataset.count_rows() == 0
    assert "a" in dataset.schema.names
    assert "b" in dataset.schema.names


# -- After mutation ------------------------------------------------------------


def test_dataset_after_mutation(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="val", field_type=IntegerType()),
    )
    table = catalog.create_table("mut_ds_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 10)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 20)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (3, 30)")

    # Delete one row
    table.delete("id = 2")

    dataset = table.to_arrow_dataset()
    assert dataset.count_rows() == 2
    ids = sorted(dataset.to_table().column("id").to_pylist())
    assert ids == [1, 3]


# -- DataScan.to_arrow_dataset -------------------------------------------------


def test_scan_to_arrow_dataset(table_with_data: Table) -> None:
    scan = table_with_data.scan()
    dataset = scan.to_arrow_dataset()
    assert isinstance(dataset, ds.Dataset)
    assert dataset.count_rows() == 3


def test_scan_to_arrow_dataset_with_filter(table_with_data: Table) -> None:
    scan = table_with_data.scan().filter(GreaterThan("value", 15))
    dataset = scan.to_arrow_dataset()
    assert dataset.count_rows() == 2
    ids = sorted(dataset.to_table().column("id").to_pylist())
    assert ids == [2, 3]


# -- Interop with Polars -------------------------------------------------------


def test_dataset_with_polars(table_with_data: Table) -> None:
    pl = pytest.importorskip("polars")
    dataset = table_with_data.to_arrow_dataset()
    lf = pl.scan_pyarrow_dataset(dataset)
    df = lf.collect()
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3


# -- Interop with DuckDB -------------------------------------------------------


def test_dataset_with_duckdb(table_with_data: Table) -> None:
    dataset = table_with_data.to_arrow_dataset()
    arrow_tbl = dataset.to_table()
    conn = duckdb.connect()
    conn.register("test_tbl", arrow_tbl)
    result = conn.execute("SELECT count(*) FROM test_tbl").fetchone()
    assert result is not None
    assert result[0] == 3


# -- P1: DataScan.to_arrow_dataset edge cases ---------------------------------


def test_scan_to_arrow_dataset_with_select(table_with_data: Table) -> None:
    scan = table_with_data.scan().select("id", "name")
    dataset = scan.to_arrow_dataset()
    result = dataset.to_table()
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 3


def test_scan_to_arrow_dataset_with_limit(table_with_data: Table) -> None:
    scan = table_with_data.scan().with_limit(2)
    dataset = scan.to_arrow_dataset()
    assert dataset.count_rows() == 2


def test_scan_to_arrow_dataset_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
        NestedField(field_id=2, name="b", field_type=StringType()),
    )
    table = catalog.create_table("empty_scan_ds_tbl", schema)
    dataset = table.scan().to_arrow_dataset()
    assert isinstance(dataset, ds.Dataset)
    assert dataset.count_rows() == 0


def test_table_to_arrow_dataset_after_delete(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("ds_del_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice')")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'bob')")
    table.delete("id = 1")
    dataset = table.to_arrow_dataset()
    assert dataset.count_rows() == 1
    assert dataset.to_table().column("name").to_pylist() == ["bob"]


def test_scan_to_arrow_dataset_with_time_travel(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("ds_tt_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1)")
    snap1 = table.current_snapshot()
    assert snap1 is not None

    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2)")

    dataset = table.scan().with_snapshot(snap1.snapshot_id).to_arrow_dataset()
    assert dataset.count_rows() == 1
