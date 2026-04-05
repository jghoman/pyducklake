"""Tests for pyducklake.schema_evolution.UpdateSchema."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.types import (
    BigIntType,
    BooleanType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
)


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
    return catalog.create_table("evo_tbl", schema)


# -- Add column ----------------------------------------------------------------


def test_add_column(table: Table) -> None:
    table.update_schema().add_column("score", IntegerType()).commit()
    assert "score" in table.schema.column_names()


def test_add_column_with_various_types(table: Table) -> None:
    table.update_schema().add_column("flag", BooleanType()).commit()
    assert "flag" in table.schema.column_names()

    table.update_schema().add_column("ratio", DoubleType()).commit()
    assert "ratio" in table.schema.column_names()


# -- Drop column ---------------------------------------------------------------


def test_drop_column(table: Table) -> None:
    table.update_schema().drop_column("name").commit()
    assert "name" not in table.schema.column_names()
    assert "id" in table.schema.column_names()


# -- Rename column -------------------------------------------------------------


def test_rename_column(table: Table) -> None:
    table.update_schema().rename_column("name", "full_name").commit()
    cols = table.schema.column_names()
    assert "name" not in cols
    assert "full_name" in cols


# -- Update column type --------------------------------------------------------


def test_update_column_type(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType()),
    )
    table = catalog.create_table("type_tbl", schema)
    table.update_schema().update_column("x", BigIntType()).commit()
    field = table.schema.find_field("x")
    assert isinstance(field.field_type, BigIntType)


# -- Set NOT NULL --------------------------------------------------------------


def test_set_not_null(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("notnull_tbl", schema)
    # Insert data so ducklake has stats (required for SET NOT NULL)
    df = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["alice"], type=pa.string()),
        }
    )
    table.append(df)

    table.update_schema().set_nullability("name", required=True).commit()
    field = table.schema.find_field("name")
    assert field.required is True


# -- Drop NOT NULL -------------------------------------------------------------


def test_drop_not_null(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType()),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("dropnull_tbl", schema)
    # Insert data so ducklake has stats
    df = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["alice"], type=pa.string()),
        }
    )
    table.append(df)

    # First SET NOT NULL, then DROP it
    table.update_schema().set_nullability("name", required=True).commit()
    assert table.schema.find_field("name").required is True

    table.update_schema().set_nullability("name", required=False).commit()
    field = table.schema.find_field("name")
    assert field.required is False


# -- Chained operations --------------------------------------------------------


def test_chained_operations(table: Table) -> None:
    (table.update_schema().add_column("score", IntegerType()).rename_column("name", "full_name").commit())
    cols = table.schema.column_names()
    assert "score" in cols
    assert "full_name" in cols
    assert "name" not in cols


# -- Context manager -----------------------------------------------------------


def test_context_manager(table: Table) -> None:
    with table.update_schema() as update:
        update.add_column("via_ctx", StringType())

    assert "via_ctx" in table.schema.column_names()


# -- Data preserved after add column -------------------------------------------


def test_data_preserved_after_add_column(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("preserve_tbl", schema)

    # Insert data
    df = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
        }
    )
    table.append(df)

    # Add column
    table.update_schema().add_column("score", IntegerType()).commit()

    # Existing data should have NULL for new column
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert result.column("score").to_pylist() == [None, None]
    assert result.column("name").to_pylist() == ["alice", "bob"]


# -- Error cases ---------------------------------------------------------------


def test_set_not_null_with_null_data_raises(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("null_data_tbl", schema)

    # Insert row with NULL in 'name'
    df = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array([None], type=pa.string()),
        }
    )
    table.append(df)

    with pytest.raises(Exception):
        table.update_schema().set_nullability("name", required=True).commit()


def test_add_existing_column_raises(table: Table) -> None:
    with pytest.raises(Exception):
        table.update_schema().add_column("id", IntegerType()).commit()


# -- P1 tests -----------------------------------------------------------------


def test_drop_nonexistent_column_raises(table: Table) -> None:
    with pytest.raises(Exception):
        table.update_schema().drop_column("nonexistent_col").commit()


def test_rename_column_to_existing_name_raises(table: Table) -> None:
    with pytest.raises(Exception):
        table.update_schema().rename_column("name", "id").commit()


def test_update_column_incompatible_type_raises(table: Table) -> None:
    with pytest.raises(Exception):
        table.update_schema().update_column("name", IntegerType()).commit()


def test_commit_no_changes_is_noop(table: Table) -> None:
    schema_before = table.schema
    table.update_schema().commit()
    assert table.schema.column_names() == schema_before.column_names()


def test_add_required_column_to_nonempty_table(catalog: Catalog) -> None:
    """Adding a NOT NULL column to a table with data and no default should fail."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("nonempty_req_tbl", schema)
    df = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
        }
    )
    table.append(df)

    # Adding a required (NOT NULL) column without a default should fail
    # because existing rows would have NULL for the new column
    with pytest.raises(Exception):
        table.update_schema().add_column("score", IntegerType(), required=True).commit()


def test_context_manager_exception_skips_commit(table: Table) -> None:
    cols_before = table.schema.column_names()
    with pytest.raises(RuntimeError):
        with table.update_schema() as update:
            update.add_column("should_not_appear", StringType())
            raise RuntimeError("simulated error")
    # refresh to be sure
    table.refresh()
    assert table.schema.column_names() == cols_before


# -- SQL injection: column names with embedded quotes --------------------------


def test_schema_evolution_column_name_with_quotes(catalog: Catalog) -> None:
    """Add and rename columns with embedded double-quotes in the name."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    )
    table = catalog.create_table("quote_col_tbl", schema)

    # Add a column with embedded double-quote
    table.update_schema().add_column('col"quoted', StringType()).commit()
    assert 'col"quoted' in table.schema.column_names()

    # Rename it
    table.update_schema().rename_column('col"quoted', 'new"name').commit()
    cols = table.schema.column_names()
    assert 'col"quoted' not in cols
    assert 'new"name' in cols
