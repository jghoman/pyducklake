"""Tests for catalog view operations and the View class."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import (
    Catalog,
    NoSuchViewError,
    Schema,
    View,
    ViewAlreadyExistsError,
)
from pyducklake.types import IntegerType, NestedField, StringType


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


@pytest.fixture()
def table_with_data(catalog: Catalog) -> None:
    """Create a table with data for views to reference."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    table = catalog.create_table("base_tbl", schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice')")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'bob')")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (3, 'carol')")


# ---------------------------------------------------------------------------
# View CRUD
# ---------------------------------------------------------------------------


def test_create_view_returns_view_object(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("my_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert isinstance(view, View)
    assert view.name == "my_view"


def test_create_view_schema(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("schema_view", 'SELECT id, name FROM "test_cat"."main"."base_tbl"')
    col_names = view.schema.column_names()
    assert "id" in col_names
    assert "name" in col_names


def test_create_view_sql_text(catalog: Catalog, table_with_data: None) -> None:
    sql = 'SELECT * FROM "test_cat"."main"."base_tbl"'
    view = catalog.create_view("sql_view", sql)
    # The sql_text should be non-empty (DuckDB may normalize the SQL)
    assert len(view.sql_text) > 0


def test_create_view_with_namespace(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_namespace("analytics")
    view = catalog.create_view(
        ("analytics", "agg_view"),
        'SELECT COUNT(*) AS cnt FROM "test_cat"."main"."base_tbl"',
    )
    assert view.namespace == "analytics"
    assert view.name == "agg_view"
    views = catalog.list_views("analytics")
    assert ("analytics", "agg_view") in views


def test_create_view_duplicate_raises(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("dup_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    with pytest.raises(ViewAlreadyExistsError):
        catalog.create_view("dup_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')


def test_create_or_replace_view(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("replace_me", 'SELECT id FROM "test_cat"."main"."base_tbl"')
    view = catalog.create_or_replace_view("replace_me", 'SELECT name FROM "test_cat"."main"."base_tbl"')
    assert isinstance(view, View)
    col_names = view.schema.column_names()
    assert "name" in col_names
    assert "id" not in col_names


def test_create_or_replace_view_new(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_or_replace_view("brand_new", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert isinstance(view, View)
    assert view.name == "brand_new"


def test_drop_view(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("drop_me", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert catalog.view_exists("drop_me")
    catalog.drop_view("drop_me")
    assert not catalog.view_exists("drop_me")


def test_drop_nonexistent_raises(catalog: Catalog) -> None:
    with pytest.raises(NoSuchViewError):
        catalog.drop_view("no_such_view")


def test_load_view(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("load_me", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    view = catalog.load_view("load_me")
    assert isinstance(view, View)
    assert view.name == "load_me"
    assert view.namespace == "main"
    assert len(view.sql_text) > 0


def test_load_nonexistent_raises(catalog: Catalog) -> None:
    with pytest.raises(NoSuchViewError):
        catalog.load_view("no_such_view")


def test_rename_view(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("old_name", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    view = catalog.rename_view("old_name", "new_name")
    assert view.name == "new_name"
    assert not catalog.view_exists("old_name")
    assert catalog.view_exists("new_name")


def test_rename_view_nonexistent_raises(catalog: Catalog) -> None:
    with pytest.raises(NoSuchViewError):
        catalog.rename_view("ghost", "also_ghost")


def test_list_views_empty(catalog: Catalog) -> None:
    views = catalog.list_views("main")
    assert views == []


def test_list_views_multiple(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("view_a", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    catalog.create_view("view_b", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    views = catalog.list_views("main")
    view_names = [v[1] for v in views]
    assert "view_a" in view_names
    assert "view_b" in view_names
    assert len(view_names) >= 2


def test_view_exists(catalog: Catalog, table_with_data: None) -> None:
    assert not catalog.view_exists("nonexistent")
    catalog.create_view("exists_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert catalog.view_exists("exists_view")


def test_view_not_in_list_tables(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("hidden_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    tables = catalog.list_tables("main")
    table_names = [t[1] for t in tables]
    assert "hidden_view" not in table_names
    assert "base_tbl" in table_names


# ---------------------------------------------------------------------------
# View scanning
# ---------------------------------------------------------------------------


def test_view_scan_to_arrow(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("arrow_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    result = view.scan().to_arrow()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 3


def test_view_scan_with_filter(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("filter_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    result = view.scan(row_filter="id > 1").to_arrow()
    assert result.num_rows == 2


def test_view_scan_with_select(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("select_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    result = view.scan(selected_fields=("name",)).to_arrow()
    assert result.column_names == ["name"]
    assert result.num_rows == 3


def test_view_scan_with_limit(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("limit_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    result = view.scan(limit=2).to_arrow()
    assert result.num_rows == 2


def test_view_scan_to_pandas(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("pandas_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    df = view.scan().to_pandas()
    assert len(df) == 3


def test_view_scan_to_duckdb(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("duckdb_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    rel = view.scan().to_duckdb()
    result = rel.fetchall()
    assert len(result) == 3


def test_view_scan_count(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("count_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.scan().count() == 3


def test_view_to_arrow_shorthand(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("arrow_sh", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    result = view.to_arrow()
    assert isinstance(result, pa.Table)
    assert result.num_rows == 3


def test_view_to_pandas_shorthand(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("pandas_sh", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    df = view.to_pandas()
    assert len(df) == 3


def test_view_to_arrow_dataset(catalog: Catalog, table_with_data: None) -> None:
    import pyarrow.dataset as ds

    view = catalog.create_view("ds_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    dataset = view.to_arrow_dataset()
    assert isinstance(dataset, ds.Dataset)
    assert dataset.count_rows() == 3


# ---------------------------------------------------------------------------
# View properties
# ---------------------------------------------------------------------------


def test_view_name_property(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("prop_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.name == "prop_view"


def test_view_namespace_property(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("ns_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.namespace == "main"


def test_view_identifier_property(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("id_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.identifier == ("main", "id_view")


def test_view_fully_qualified_name(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("fqn_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert "test_cat" in view.fully_qualified_name
    assert "main" in view.fully_qualified_name
    assert "fqn_view" in view.fully_qualified_name


def test_view_repr(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("repr_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    r = repr(view)
    assert "View" in r
    assert "repr_view" in r


def test_view_equality(catalog: Catalog, table_with_data: None) -> None:
    catalog.create_view("eq_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    v1 = catalog.load_view("eq_view")
    v2 = catalog.load_view("eq_view")
    assert v1 == v2


# ---------------------------------------------------------------------------
# View + base table interactions
# ---------------------------------------------------------------------------


def test_view_reflects_table_data_changes(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("live_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.scan().count() == 3
    # Insert more data into the base table
    table = catalog.load_table("base_tbl")
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (4, 'dave')")
    assert view.scan().count() == 4


def test_view_after_base_table_schema_evolution(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("schema_evo_view", 'SELECT id, name FROM "test_cat"."main"."base_tbl"')
    # Add a column to the base table
    base_tbl = catalog.load_table("base_tbl")
    base_tbl.update_schema().add_column("score", IntegerType()).commit()
    # View should still work (it selects specific columns)
    result = view.scan().to_arrow()
    assert result.num_rows == 3


def test_view_with_aggregation(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view(
        "agg_view", 'SELECT COUNT(*) AS cnt, MIN(id) AS min_id FROM "test_cat"."main"."base_tbl"'
    )
    result = view.to_arrow()
    assert result.num_rows == 1
    assert result.column("cnt")[0].as_py() == 3
    assert result.column("min_id")[0].as_py() == 1


def test_view_with_join(catalog: Catalog, table_with_data: None) -> None:
    # Create a second table
    schema2 = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="dept", field_type=StringType()),
    )
    t2 = catalog.create_table("dept_tbl", schema2)
    fqn2 = t2.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn2} VALUES (1, 'eng')")
    catalog.connection.execute(f"INSERT INTO {fqn2} VALUES (2, 'sales')")

    view = catalog.create_view(
        "join_view",
        "SELECT b.id, b.name, d.dept "
        'FROM "test_cat"."main"."base_tbl" b '
        'JOIN "test_cat"."main"."dept_tbl" d ON b.id = d.id',
    )
    result = view.to_arrow()
    assert result.num_rows == 2
    assert "dept" in result.column_names


def test_view_with_special_chars_in_name(catalog: Catalog, table_with_data: None) -> None:
    view_name = 'my "special view'
    view = catalog.create_view(view_name, 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.name == view_name
    assert catalog.view_exists(view_name)
    result = view.to_arrow()
    assert result.num_rows == 3
    catalog.drop_view(view_name)
    assert not catalog.view_exists(view_name)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_view_over_empty_table(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    )
    catalog.create_table("empty_tbl", schema)
    view = catalog.create_view("empty_view", 'SELECT * FROM "test_cat"."main"."empty_tbl"')
    result = view.to_arrow()
    assert result.num_rows == 0


def test_rename_view_cross_namespace_raises(catalog: Catalog, table_with_data: None) -> None:
    """Renaming a view across namespaces raises DucklakeError."""
    from pyducklake import DucklakeError

    catalog.create_namespace("ns_a")
    catalog.create_namespace("ns_b")
    catalog.create_view(("ns_a", "xns_view"), 'SELECT * FROM "test_cat"."main"."base_tbl"')
    with pytest.raises(DucklakeError, match="Cross-namespace rename is not supported"):
        catalog.rename_view(("ns_a", "xns_view"), ("ns_b", "xns_view"))


def test_view_scan_with_boolean_expression(catalog: Catalog, table_with_data: None) -> None:
    """Scan view with a GreaterThan expression (not string filter)."""
    from pyducklake.expressions import GreaterThan

    view = catalog.create_view("gt_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    result = view.scan(row_filter=GreaterThan("id", 1)).to_arrow()
    assert result.num_rows == 2
    assert all(v > 1 for v in result.column("id").to_pylist())


def test_view_scan_to_arrow_batch_reader(catalog: Catalog, table_with_data: None) -> None:
    """Batch reader on view returns all rows."""
    view = catalog.create_view("br_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    reader = view.scan().to_arrow_batch_reader()
    combined = pa.Table.from_batches(list(reader))
    assert combined.num_rows == 3


def test_view_eq_non_view(catalog: Catalog, table_with_data: None) -> None:
    """view == 'string' returns NotImplemented."""
    view = catalog.create_view("ne_view", 'SELECT * FROM "test_cat"."main"."base_tbl"')
    assert view.__eq__("string") is NotImplemented


def test_view_refresh(catalog: Catalog, table_with_data: None) -> None:
    view = catalog.create_view("refresh_view", 'SELECT id, name FROM "test_cat"."main"."base_tbl"')
    original_cols = view.schema.column_names()
    assert "id" in original_cols
    # refresh should succeed and return self
    result = view.refresh()
    assert result is view
