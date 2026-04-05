"""Tests for pyducklake.catalog.Catalog."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pyducklake import (
    Catalog,
    DucklakeError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    Schema,
    TableAlreadyExistsError,
)
from pyducklake.types import (
    BigIntType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    SmallIntType,
    StringType,
    StructType,
    TimestampType,
)


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    """Create a catalog backed by a local DuckDB file."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


@pytest.fixture()
def simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="active", field_type=BooleanType()),
    )


# -- Catalog creation -------------------------------------------------------


def test_create_catalog_with_local_file(tmp_path: Path) -> None:
    meta_db = str(tmp_path / "meta.duckdb")
    cat = Catalog("mycat", meta_db)
    assert cat.name == "mycat"
    cat.close()


def test_create_catalog_with_data_path(tmp_path: Path) -> None:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    cat = Catalog("mycat", meta_db, data_path=data_dir)
    assert cat.name == "mycat"
    cat.close()


# -- Namespace operations ---------------------------------------------------


def test_list_namespaces_default(catalog: Catalog) -> None:
    namespaces = catalog.list_namespaces()
    assert "main" in namespaces


def test_create_namespace(catalog: Catalog) -> None:
    catalog.create_namespace("test_ns")
    assert "test_ns" in catalog.list_namespaces()


def test_create_namespace_duplicate_raises(catalog: Catalog) -> None:
    catalog.create_namespace("dup_ns")
    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace("dup_ns")


def test_create_namespace_if_not_exists(catalog: Catalog) -> None:
    catalog.create_namespace("ns1")
    # Should not raise
    catalog.create_namespace_if_not_exists("ns1")
    assert "ns1" in catalog.list_namespaces()


def test_drop_namespace(catalog: Catalog) -> None:
    catalog.create_namespace("drop_me")
    assert catalog.namespace_exists("drop_me")
    catalog.drop_namespace("drop_me")
    assert not catalog.namespace_exists("drop_me")


def test_drop_namespace_nonempty_raises(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_namespace("nonempty_ns")
    catalog.create_table(("nonempty_ns", "tbl"), simple_schema)
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace("nonempty_ns")


def test_drop_namespace_nonexistent_raises(catalog: Catalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.drop_namespace("no_such_ns")


def test_namespace_exists(catalog: Catalog) -> None:
    assert catalog.namespace_exists("main")
    assert not catalog.namespace_exists("nonexistent")


# -- Table operations -------------------------------------------------------


def test_create_table(catalog: Catalog, simple_schema: Schema) -> None:
    table = catalog.create_table("my_table", simple_schema)
    assert table.name == "my_table"
    assert table.namespace == "main"
    assert len(table.schema) == 3
    col_names = table.schema.column_names()
    assert col_names == ["id", "name", "active"]


def test_create_table_with_various_types(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="col_int", field_type=IntegerType()),
        NestedField(field_id=2, name="col_str", field_type=StringType()),
        NestedField(field_id=3, name="col_bool", field_type=BooleanType()),
        NestedField(field_id=4, name="col_decimal", field_type=DecimalType(10, 2)),
        NestedField(field_id=5, name="col_float", field_type=FloatType()),
        NestedField(field_id=6, name="col_double", field_type=DoubleType()),
        NestedField(field_id=7, name="col_bigint", field_type=BigIntType()),
        NestedField(field_id=8, name="col_smallint", field_type=SmallIntType()),
        NestedField(field_id=9, name="col_date", field_type=DateType()),
        NestedField(field_id=10, name="col_ts", field_type=TimestampType()),
    )
    table = catalog.create_table("typed_table", schema)
    assert len(table.schema) == 10

    # Verify types round-trip
    assert isinstance(table.schema.find_type("col_int"), IntegerType)
    assert isinstance(table.schema.find_type("col_str"), StringType)
    assert isinstance(table.schema.find_type("col_bool"), BooleanType)
    assert isinstance(table.schema.find_type("col_decimal"), DecimalType)
    assert isinstance(table.schema.find_type("col_float"), FloatType)
    assert isinstance(table.schema.find_type("col_double"), DoubleType)
    assert isinstance(table.schema.find_type("col_bigint"), BigIntType)
    assert isinstance(table.schema.find_type("col_smallint"), SmallIntType)
    assert isinstance(table.schema.find_type("col_date"), DateType)
    assert isinstance(table.schema.find_type("col_ts"), TimestampType)


def test_create_table_not_null_constraint(catalog: Catalog) -> None:
    """required=True on NestedField emits NOT NULL in CREATE TABLE."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),  # optional
    )
    table = catalog.create_table("not_null_tbl", schema)

    # id should be required, name should be optional
    id_field = table.schema.find_field("id")
    name_field = table.schema.find_field("name")
    assert id_field.required is True
    assert name_field.required is False

    # Inserting NULL into required column should fail
    import pyarrow as pa

    with pytest.raises(Exception):
        table.append(
            pa.table(
                {
                    "id": pa.array([None], type=pa.int32()),
                    "name": pa.array(["alice"], type=pa.string()),
                }
            )
        )


def test_create_table_not_null_round_trip(catalog: Catalog) -> None:
    """NOT NULL constraint survives create -> load round trip."""
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="b", field_type=StringType(), required=True),
        NestedField(field_id=3, name="c", field_type=IntegerType()),  # optional
    )
    catalog.create_table("nn_round_trip", schema)
    loaded = catalog.load_table("nn_round_trip")

    assert loaded.schema.find_field("a").required is True
    assert loaded.schema.find_field("b").required is True
    assert loaded.schema.find_field("c").required is False


def test_create_table_duplicate_raises(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("dup_table", simple_schema)
    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table("dup_table", simple_schema)


def test_create_table_if_not_exists(catalog: Catalog, simple_schema: Schema) -> None:
    t1 = catalog.create_table("maybe_table", simple_schema)
    t2 = catalog.create_table_if_not_exists("maybe_table", simple_schema)
    assert t1 == t2


def test_load_table(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("load_me", simple_schema)
    table = catalog.load_table("load_me")
    assert table.name == "load_me"
    assert table.schema.column_names() == ["id", "name", "active"]


def test_load_table_nonexistent_raises(catalog: Catalog) -> None:
    with pytest.raises(NoSuchTableError):
        catalog.load_table("no_such_table")


def test_drop_table(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("drop_me", simple_schema)
    assert catalog.table_exists("drop_me")
    catalog.drop_table("drop_me")
    assert not catalog.table_exists("drop_me")


def test_drop_table_nonexistent_raises(catalog: Catalog) -> None:
    with pytest.raises(NoSuchTableError):
        catalog.drop_table("no_such_table")


def test_rename_table(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("old_name", simple_schema)
    renamed = catalog.rename_table("old_name", "new_name")
    assert renamed.name == "new_name"
    assert not catalog.table_exists("old_name")
    assert catalog.table_exists("new_name")


def test_table_exists(catalog: Catalog, simple_schema: Schema) -> None:
    assert not catalog.table_exists("nope")
    catalog.create_table("yep", simple_schema)
    assert catalog.table_exists("yep")


def test_list_tables(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("t1", simple_schema)
    catalog.create_table("t2", simple_schema)
    tables = catalog.list_tables("main")
    names = [t[1] for t in tables]
    assert "t1" in names
    assert "t2" in names


def test_list_tables_with_namespace(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_namespace("ns1")
    catalog.create_table(("ns1", "tbl_a"), simple_schema)
    catalog.create_table("main_tbl", simple_schema)

    ns1_tables = catalog.list_tables("ns1")
    assert len(ns1_tables) == 1
    assert ns1_tables[0] == ("ns1", "tbl_a")

    main_tables = catalog.list_tables("main")
    main_names = [t[1] for t in main_tables]
    assert "main_tbl" in main_names


# -- Identifier resolution --------------------------------------------------


def test_identifier_string(catalog: Catalog) -> None:
    ns, name = catalog._resolve_identifier("my_table")
    assert ns == "main"
    assert name == "my_table"


def test_identifier_dotted_string(catalog: Catalog) -> None:
    ns, name = catalog._resolve_identifier("my_schema.my_table")
    assert ns == "my_schema"
    assert name == "my_table"


def test_resolve_identifier_dotted_table_name(catalog: Catalog) -> None:
    """'a.b.c' resolves to namespace='a', table='b.c'."""
    ns, name = catalog._resolve_identifier("a.b.c")
    assert ns == "a"
    assert name == "b.c"


def test_identifier_tuple(catalog: Catalog) -> None:
    ns, name = catalog._resolve_identifier(("my_schema", "my_table"))
    assert ns == "my_schema"
    assert name == "my_table"


# -- Context manager --------------------------------------------------------


def test_context_manager(tmp_path: Path) -> None:
    meta_db = str(tmp_path / "meta.duckdb")
    with Catalog("ctx_cat", meta_db) as cat:
        assert cat.name == "ctx_cat"
    # Connection should be closed after exiting context


# -- Nested types -----------------------------------------------------------


def test_create_table_with_nested_types(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="tags", field_type=ListType(element_id=10, element_type=StringType())),
        NestedField(
            field_id=2,
            name="metadata",
            field_type=StructType(
                fields=(
                    NestedField(field_id=20, name="key", field_type=StringType()),
                    NestedField(field_id=21, name="value", field_type=IntegerType()),
                )
            ),
        ),
        NestedField(
            field_id=3,
            name="scores",
            field_type=MapType(key_id=30, key_type=StringType(), value_id=31, value_type=DoubleType()),
        ),
    )
    table = catalog.create_table("nested_table", schema)
    assert len(table.schema) == 3

    tags_type = table.schema.find_type("tags")
    assert isinstance(tags_type, ListType)
    assert isinstance(tags_type.element_type, StringType)

    meta_type = table.schema.find_type("metadata")
    assert isinstance(meta_type, StructType)
    assert len(meta_type.fields) == 2

    scores_type = table.schema.find_type("scores")
    assert isinstance(scores_type, MapType)
    assert isinstance(scores_type.key_type, StringType)
    assert isinstance(scores_type.value_type, DoubleType)


# -- Identifier quoting (SQL injection prevention) -------------------------


def test_namespace_name_with_special_chars(catalog: Catalog) -> None:
    name = 'my"schema'
    catalog.create_namespace(name)
    assert catalog.namespace_exists(name)
    assert name in catalog.list_namespaces()
    catalog.drop_namespace(name)
    assert not catalog.namespace_exists(name)


def test_table_name_with_special_chars(catalog: Catalog, simple_schema: Schema) -> None:
    ns = 'ns"special'
    catalog.create_namespace(ns)
    tbl_name = 'tbl "name'
    table = catalog.create_table((ns, tbl_name), simple_schema)
    assert table.name == tbl_name

    loaded = catalog.load_table((ns, tbl_name))
    assert loaded.name == tbl_name

    catalog.drop_table((ns, tbl_name))
    assert not catalog.table_exists((ns, tbl_name))


# -- Cross-namespace rename ------------------------------------------------


def test_rename_table_cross_namespace_raises(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_namespace("ns_a")
    catalog.create_namespace("ns_b")
    catalog.create_table(("ns_a", "tbl"), simple_schema)
    with pytest.raises(DucklakeError, match="Cross-namespace rename is not supported"):
        catalog.rename_table(("ns_a", "tbl"), ("ns_b", "tbl"))


# -- P1 tests ---------------------------------------------------------------


def test_rename_table_to_existing_name_raises(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("tbl_src", simple_schema)
    catalog.create_table("tbl_dst", simple_schema)
    with pytest.raises(Exception):
        catalog.rename_table("tbl_src", "tbl_dst")


@pytest.mark.xfail(reason="DuckDB rejects zero-column tables", strict=True)
def test_create_table_empty_schema(catalog: Catalog) -> None:
    empty = Schema()
    catalog.create_table("empty_schema_tbl", empty)


def test_operations_after_close_raise(tmp_path: Path) -> None:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    cat = Catalog("close_cat", meta_db, data_path=data_dir)
    cat.close()
    with pytest.raises(Exception):
        cat.list_namespaces()


def test_same_table_name_different_namespaces(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_namespace("ns_x")
    catalog.create_namespace("ns_y")
    t1 = catalog.create_table(("ns_x", "shared_name"), simple_schema)
    t2 = catalog.create_table(("ns_y", "shared_name"), simple_schema)
    assert t1.namespace == "ns_x"
    assert t2.namespace == "ns_y"
    assert t1 != t2
    # Each table is independent
    assert catalog.table_exists(("ns_x", "shared_name"))
    assert catalog.table_exists(("ns_y", "shared_name"))


# -- P1 phase 3 tests ---------------------------------------------------------


def test_drop_namespace_containing_views(catalog: Catalog, simple_schema: Schema) -> None:
    """Namespace with only views (no tables): drop_namespace should raise NamespaceNotEmptyError."""
    catalog.create_namespace("views_only")
    # Create a base table in main so the view SQL is valid
    catalog.create_table("base_for_view", simple_schema)
    catalog.create_view(
        ("views_only", "v1"),
        'SELECT * FROM "test_cat"."main"."base_for_view"',
    )
    assert catalog.view_exists(("views_only", "v1"))

    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace("views_only")


def test_drop_namespace_with_only_views_raises(catalog: Catalog, simple_schema: Schema) -> None:
    """Namespace with only views should be treated as non-empty."""
    catalog.create_namespace("view_only_ns")
    catalog.create_table("base_for_vonly", simple_schema)
    catalog.create_view(
        ("view_only_ns", "v1"),
        'SELECT * FROM "test_cat"."main"."base_for_vonly"',
    )
    assert catalog.view_exists(("view_only_ns", "v1"))

    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace("view_only_ns")


def test_catalog_properties_key_injection_rejected(tmp_path: Path) -> None:
    """Properties dict with invalid key is rejected."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    with pytest.raises(ValueError, match="Invalid property key"):
        Catalog(
            "inj_cat",
            meta_db,
            data_path=data_dir,
            properties={"bad key; DROP TABLE --": "1"},
        )


def test_catalog_properties_applied(tmp_path: Path) -> None:
    """Catalog properties dict is applied as SET statements on the connection."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    cat = Catalog(
        "prop_cat",
        meta_db,
        data_path=data_dir,
        properties={"threads": "1"},
    )
    rows = cat.fetchall("SELECT current_setting('threads')")
    assert rows[0][0] in ("1", 1)
    cat.close()


def test_catalog_reopen_data_persists(tmp_path: Path) -> None:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    import pyarrow as pa

    cat1 = Catalog("reopen_cat", meta_db, data_path=data_dir)
    tbl = cat1.create_table("persist_tbl", schema)
    df = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
        }
    )
    tbl.append(df)
    cat1.close()

    cat2 = Catalog("reopen_cat", meta_db, data_path=data_dir)
    tbl2 = cat2.load_table("persist_tbl")
    result = tbl2.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [1, 2]
    cat2.close()


# -- P0: _duckdb_type_to_ducklake parser tests ---------------------------------


class TestDuckdbTypeToDucklake:
    """Unit tests for the DuckDB type string parser."""

    def test_direct_primitive_types(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        assert isinstance(_duckdb_type_to_ducklake("BOOLEAN"), BooleanType)
        assert isinstance(_duckdb_type_to_ducklake("INTEGER"), IntegerType)
        assert isinstance(_duckdb_type_to_ducklake("BIGINT"), BigIntType)
        assert isinstance(_duckdb_type_to_ducklake("VARCHAR"), StringType)
        assert isinstance(_duckdb_type_to_ducklake("DOUBLE"), DoubleType)
        assert isinstance(_duckdb_type_to_ducklake("FLOAT"), FloatType)
        assert isinstance(_duckdb_type_to_ducklake("DATE"), DateType)
        assert isinstance(_duckdb_type_to_ducklake("TIMESTAMP"), TimestampType)

    def test_type_aliases(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        # These aliases must map correctly or schema loading silently breaks
        assert isinstance(_duckdb_type_to_ducklake("INT"), IntegerType)
        assert isinstance(_duckdb_type_to_ducklake("TEXT"), StringType)
        assert isinstance(_duckdb_type_to_ducklake("STRING"), StringType)
        assert isinstance(_duckdb_type_to_ducklake("REAL"), FloatType)
        assert isinstance(_duckdb_type_to_ducklake("BYTEA"), BinaryType)
        assert isinstance(_duckdb_type_to_ducklake("DATETIME"), TimestampType)

    def test_case_insensitive(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        assert isinstance(_duckdb_type_to_ducklake("integer"), IntegerType)
        assert isinstance(_duckdb_type_to_ducklake("varchar"), StringType)
        assert isinstance(_duckdb_type_to_ducklake("Boolean"), BooleanType)

    def test_decimal_type(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("DECIMAL(10, 2)")
        assert isinstance(result, DecimalType)
        assert result.precision == 10
        assert result.scale == 2

    def test_list_type(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("INTEGER[]")
        assert isinstance(result, ListType)
        assert isinstance(result.element_type, IntegerType)

    def test_nested_list_type(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("VARCHAR[][]")
        assert isinstance(result, ListType)
        assert isinstance(result.element_type, ListType)
        inner = result.element_type
        assert isinstance(inner, ListType)
        assert isinstance(inner.element_type, StringType)

    def test_map_type(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("MAP(VARCHAR, INTEGER)")
        assert isinstance(result, MapType)
        assert isinstance(result.key_type, StringType)
        assert isinstance(result.value_type, IntegerType)

    def test_struct_type(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("STRUCT(a INTEGER, b VARCHAR)")
        assert isinstance(result, StructType)
        assert len(result.fields) == 2
        assert result.fields[0].name == "a"
        assert isinstance(result.fields[0].field_type, IntegerType)
        assert result.fields[1].name == "b"
        assert isinstance(result.fields[1].field_type, StringType)

    def test_struct_with_quoted_field_name(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake('STRUCT("my_field" INTEGER)')
        assert isinstance(result, StructType)
        assert result.fields[0].name == "my_field"

    def test_nested_struct_in_list(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("STRUCT(a INTEGER, b VARCHAR)[]")
        assert isinstance(result, ListType)
        assert isinstance(result.element_type, StructType)

    def test_map_with_nested_value(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("MAP(VARCHAR, INTEGER[])")
        assert isinstance(result, MapType)
        assert isinstance(result.key_type, StringType)
        assert isinstance(result.value_type, ListType)

    def test_unknown_type_raises(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        with pytest.raises(ValueError, match="Cannot parse DuckDB type"):
            _duckdb_type_to_ducklake("NOSUCHTYPE")

    def test_whitespace_stripped(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        assert isinstance(_duckdb_type_to_ducklake("  INTEGER  "), IntegerType)

    def test_field_id_counter_tracks_nested(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        counter: list[int] = [100]
        result = _duckdb_type_to_ducklake("INTEGER[]", counter)
        assert isinstance(result, ListType)
        # Counter should have been incremented for the element_id
        assert result.element_id == 100
        assert counter[0] == 101

    def test_field_id_counter_none(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        result = _duckdb_type_to_ducklake("INTEGER[]", None)
        assert isinstance(result, ListType)
        # Without counter, element_id defaults to 0
        assert result.element_id == 0

    def test_map_bad_parts_raises(self) -> None:
        from pyducklake.catalog import _duckdb_type_to_ducklake

        with pytest.raises(ValueError, match="Cannot parse MAP type"):
            _duckdb_type_to_ducklake("MAP(VARCHAR)")


# -- P0: _split_top_level tests -----------------------------------------------


class TestSplitTopLevel:
    def test_simple_split(self) -> None:
        from pyducklake.catalog import _split_top_level

        assert _split_top_level("a, b, c") == ["a", " b", " c"]

    def test_nested_parens(self) -> None:
        from pyducklake.catalog import _split_top_level

        result = _split_top_level("DECIMAL(10, 2), VARCHAR")
        assert len(result) == 2
        assert "DECIMAL(10, 2)" in result[0]

    def test_nested_brackets(self) -> None:
        from pyducklake.catalog import _split_top_level

        result = _split_top_level("INTEGER[], VARCHAR")
        assert len(result) == 2

    def test_empty_string(self) -> None:
        from pyducklake.catalog import _split_top_level

        assert _split_top_level("") == []

    def test_deeply_nested(self) -> None:
        from pyducklake.catalog import _split_top_level

        result = _split_top_level("MAP(VARCHAR, MAP(VARCHAR, INTEGER)), BOOLEAN")
        assert len(result) == 2


# -- P0: escape_string_literal tests ------------------------------------------


class TestEscapeStringLiteral:
    def test_no_quotes(self) -> None:
        from pyducklake.catalog import escape_string_literal

        assert escape_string_literal("hello") == "hello"

    def test_single_quote(self) -> None:
        from pyducklake.catalog import escape_string_literal

        assert escape_string_literal("it's") == "it''s"

    def test_multiple_quotes(self) -> None:
        from pyducklake.catalog import escape_string_literal

        assert escape_string_literal("it''s a 'test'") == "it''''s a ''test''"

    def test_empty_string(self) -> None:
        from pyducklake.catalog import escape_string_literal

        assert escape_string_literal("") == ""


# -- P0: quote_identifier tests ----------------------------------------------


class TestQuoteIdentifier:
    def test_simple(self) -> None:
        from pyducklake.catalog import quote_identifier

        assert quote_identifier("my_table") == '"my_table"'

    def test_embedded_quotes(self) -> None:
        from pyducklake.catalog import quote_identifier

        assert quote_identifier('my"table') == '"my""table"'

    def test_empty_string(self) -> None:
        from pyducklake.catalog import quote_identifier

        assert quote_identifier("") == '""'
