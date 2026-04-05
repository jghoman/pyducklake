"""Tests for scan output formats (to_polars, to_ray)."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

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
    table = catalog.create_table("fmt_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice', 10)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'bob', 20)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (3, 'carol', 30)")
    return table


# -- to_polars -----------------------------------------------------------------


def test_to_polars_basic(table_with_data: Table) -> None:
    pl = pytest.importorskip("polars")
    df = table_with_data.scan().to_polars()
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name", "value"}


def test_to_polars_with_filter(table_with_data: Table) -> None:
    pl = pytest.importorskip("polars")
    df = table_with_data.scan().filter(GreaterThan("value", 15)).to_polars()
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2


def test_to_polars_with_select(table_with_data: Table) -> None:
    pl = pytest.importorskip("polars")
    df = table_with_data.scan().select("id", "name").to_polars()
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) == {"id", "name"}
    assert len(df) == 3


def test_to_polars_empty_table(catalog: Catalog) -> None:
    pl = pytest.importorskip("polars")
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType()),
        NestedField(field_id=2, name="b", field_type=StringType()),
    )
    table = catalog.create_table("empty_polars_tbl", schema)
    df = table.scan().to_polars()
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


def test_to_polars_not_installed(table_with_data: Table) -> None:
    import builtins

    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "polars":
            raise ImportError("No module named 'polars'")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="polars is required"):
            table_with_data.scan().to_polars()


def test_to_ray_not_installed(table_with_data: Table) -> None:
    import builtins

    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "ray.data" or name == "ray":
            raise ImportError("No module named 'ray'")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="ray is required"):
            table_with_data.scan().to_ray()


def test_to_pandas_not_installed(table_with_data: Table) -> None:
    """to_pandas() raises ImportError when pandas is not importable."""
    with patch("pyducklake.scan.importlib.util.find_spec", return_value=None):
        with pytest.raises(ImportError, match="pandas is required"):
            table_with_data.scan().to_pandas()


def test_to_polars_with_limit(table_with_data: Table) -> None:
    pl = pytest.importorskip("polars")
    df = table_with_data.scan().with_limit(1).to_polars()
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 1
