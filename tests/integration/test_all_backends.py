"""Parameterized integration tests: core test suite runs against all metadata backends.

Backends: DuckDB (local file), PostgreSQL, SQLite, MySQL.
Requires Docker for PostgreSQL and MySQL.

Run with: uv run python -m pytest tests/integration -m integration -v --tb=short
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from pyducklake import (
    Catalog,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    Schema,
    TableAlreadyExistsError,
)
from pyducklake.cdc import ChangeSet
from pyducklake.expressions import AlwaysTrue, EqualTo, GreaterThan
from pyducklake.partitioning import IDENTITY, UNPARTITIONED, YEAR
from pyducklake.types import (
    BigIntType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

pytestmark = pytest.mark.integration


def _backend(request: pytest.FixtureRequest) -> str:
    """Extract the backend name from the parameterized catalog fixture."""
    return request.node.callspec.params.get("catalog", "")


def _mysql_xfail(request: pytest.FixtureRequest) -> None:
    """Mark test as xfail when running on the MySQL backend (HASH_JOIN limitation)."""
    if _backend(request) == "mysql":
        pytest.xfail("DuckDB MySQL connector does not support HASH_JOIN in UPDATE statements")


def _skip_duckdb_snapshots(request: pytest.FixtureRequest) -> None:
    """Skip test on DuckDB backend — Table.snapshots() returns empty on DuckDB local files."""
    if _backend(request) == "duckdb":
        pytest.skip("Table.snapshots() not supported on DuckDB local file backend")


# ---------------------------------------------------------------------------
# Namespace operations
# ---------------------------------------------------------------------------


class TestNamespaces:
    def test_default_namespace_exists(self, catalog: Catalog) -> None:
        assert "main" in catalog.list_namespaces()

    def test_create_and_list(self, catalog: Catalog) -> None:
        catalog.create_namespace("test_ns")
        assert "test_ns" in catalog.list_namespaces()

    def test_create_duplicate_raises(self, catalog: Catalog) -> None:
        catalog.create_namespace("dup_ns")
        with pytest.raises(NamespaceAlreadyExistsError):
            catalog.create_namespace("dup_ns")

    def test_drop_namespace(self, catalog: Catalog) -> None:
        catalog.create_namespace("drop_me")
        assert catalog.namespace_exists("drop_me")
        catalog.drop_namespace("drop_me")
        assert not catalog.namespace_exists("drop_me")

    def test_drop_nonempty_raises(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_namespace("nonempty")
        catalog.create_table(("nonempty", "tbl"), simple_schema)
        with pytest.raises(NamespaceNotEmptyError):
            catalog.drop_namespace("nonempty")

    def test_drop_nonexistent_raises(self, catalog: Catalog) -> None:
        with pytest.raises(NoSuchNamespaceError):
            catalog.drop_namespace("ghost")

    def test_namespace_exists(self, catalog: Catalog) -> None:
        assert not catalog.namespace_exists("nope")
        catalog.create_namespace("yep")
        assert catalog.namespace_exists("yep")


# ---------------------------------------------------------------------------
# Table CRUD
# ---------------------------------------------------------------------------


class TestTableCRUD:
    def test_create_and_load(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("crud_test", simple_schema)
        assert tbl.name == "crud_test"
        assert tbl.namespace == "main"

        loaded = catalog.load_table("crud_test")
        assert len(loaded.schema.fields) == len(simple_schema.fields)

    def test_create_duplicate_raises(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("dup_tbl", simple_schema)
        with pytest.raises(TableAlreadyExistsError):
            catalog.create_table("dup_tbl", simple_schema)

    def test_create_if_not_exists(self, catalog: Catalog, simple_schema: Schema) -> None:
        t1 = catalog.create_table("idempotent", simple_schema)
        t2 = catalog.create_table_if_not_exists("idempotent", simple_schema)
        assert t1 == t2

    def test_drop_table(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("drop_tbl", simple_schema)
        assert catalog.table_exists("drop_tbl")
        catalog.drop_table("drop_tbl")
        assert not catalog.table_exists("drop_tbl")

    def test_drop_nonexistent_raises(self, catalog: Catalog) -> None:
        with pytest.raises(NoSuchTableError):
            catalog.drop_table("ghost_tbl")

    def test_load_nonexistent_raises(self, catalog: Catalog) -> None:
        with pytest.raises(NoSuchTableError):
            catalog.load_table("ghost_tbl")

    def test_list_tables(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("list_a", simple_schema)
        catalog.create_table("list_b", simple_schema)
        tables = catalog.list_tables()
        names = [t[1] for t in tables]
        assert "list_a" in names
        assert "list_b" in names

    def test_table_in_custom_namespace(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_namespace("custom")
        tbl = catalog.create_table(("custom", "ns_tbl"), simple_schema)
        assert tbl.namespace == "custom"
        assert catalog.table_exists(("custom", "ns_tbl"))

    def test_rename_table(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("old_name", simple_schema)
        renamed = catalog.rename_table("old_name", "new_name")
        assert renamed.name == "new_name"
        assert not catalog.table_exists("old_name")
        assert catalog.table_exists("new_name")

    def test_various_types(self, catalog: Catalog) -> None:
        schema = Schema(
            NestedField(field_id=1, name="i", field_type=IntegerType()),
            NestedField(field_id=2, name="b", field_type=BigIntType()),
            NestedField(field_id=3, name="s", field_type=StringType()),
            NestedField(field_id=4, name="f", field_type=BooleanType()),
            NestedField(field_id=5, name="d", field_type=DecimalType(10, 2)),
            NestedField(field_id=6, name="dt", field_type=DateType()),
            NestedField(field_id=7, name="ts", field_type=TimestampType()),
        )
        catalog.create_table("typed", schema)
        loaded = catalog.load_table("typed")
        assert len(loaded.schema.fields) == 7


# ---------------------------------------------------------------------------
# Write + Read round-trip
# ---------------------------------------------------------------------------


class TestWriteRead:
    def test_append_and_scan(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("append_scan", simple_schema)
        df = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]})
        tbl.append(df)

        result = tbl.scan().to_arrow()
        assert result.num_rows == 3
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_multiple_appends(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("multi_append", simple_schema)
        for i in range(3):
            df = pa.table({"id": [i], "name": [f"row_{i}"], "value": [float(i)]})
            tbl.append(df)

        assert tbl.scan().count() == 3

    def test_scan_with_filter(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("filter_scan", simple_schema)
        df = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
                "value": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )
        tbl.append(df)

        result = tbl.scan().filter(GreaterThan("id", 3)).to_arrow()
        assert result.num_rows == 2
        assert set(result.column("id").to_pylist()) == {4, 5}

    def test_scan_with_select(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("select_scan", simple_schema)
        df = pa.table({"id": [1], "name": ["x"], "value": [9.9]})
        tbl.append(df)

        result = tbl.scan().select("id", "name").to_arrow()
        assert result.column_names == ["id", "name"]

    def test_scan_to_pandas(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("pandas_scan", simple_schema)
        df = pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]})
        tbl.append(df)

        pdf = tbl.scan().to_pandas()
        assert len(pdf) == 2
        assert list(pdf.columns) == ["id", "name", "value"]

    def test_overwrite_all(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("overwrite_all", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        assert tbl.scan().count() == 2

        tbl.overwrite(pa.table({"id": [10], "name": ["z"], "value": [99.0]}))
        result = tbl.scan().to_arrow()
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [10]

    def test_overwrite_with_filter(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("overwrite_filter", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))

        tbl.overwrite(
            pa.table({"id": [20], "name": ["B"], "value": [22.0]}),
            overwrite_filter=EqualTo("id", 2),
        )
        result = tbl.scan().to_arrow()
        ids = sorted(result.column("id").to_pylist())
        assert ids == [1, 3, 20]

    def test_scan_with_limit(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("limit_scan", simple_schema)
        df = pa.table(
            {
                "id": list(range(100)),
                "name": [f"n{i}" for i in range(100)],
                "value": [float(i) for i in range(100)],
            }
        )
        tbl.append(df)

        result = tbl.scan().with_limit(5).to_arrow()
        assert result.num_rows == 5


# ---------------------------------------------------------------------------
# Snapshots & time travel
# ---------------------------------------------------------------------------


class TestSnapshots:
    def test_empty_table_no_crash(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("no_snap", simple_schema)
        tbl.current_snapshot()  # should not raise

    def test_snapshots_after_writes(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("snap_writes", simple_schema)
        snap_before = len(tbl.snapshots())

        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))

        snaps = tbl.snapshots()
        assert len(snaps) >= snap_before + 2

    def test_time_travel(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("time_travel", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))

        snaps = tbl.snapshots()
        first_data_snap = snaps[-1]

        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))

        assert tbl.scan().count() == 2

        result = tbl.scan().with_snapshot(first_data_snap.snapshot_id).to_arrow()
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [1]


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


class TestSchemaEvolution:
    def test_add_column(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("evo_add", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))

        tbl.update_schema().add_column("extra", StringType()).commit()
        tbl.refresh()

        assert "extra" in tbl.schema.column_names()

        result = tbl.scan().to_arrow()
        assert result.column("extra").to_pylist() == [None]

    def test_drop_column(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("evo_drop", simple_schema)
        tbl.update_schema().drop_column("value").commit()
        tbl.refresh()

        assert "value" not in tbl.schema.column_names()

    def test_rename_column(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("evo_rename", simple_schema)
        tbl.update_schema().rename_column("name", "label").commit()
        tbl.refresh()

        assert "label" in tbl.schema.column_names()
        assert "name" not in tbl.schema.column_names()

    def test_widen_type(self, catalog: Catalog, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        schema = Schema(NestedField(field_id=1, name="x", field_type=IntegerType()))
        tbl = catalog.create_table("evo_widen", schema)
        tbl.update_schema().update_column("x", BigIntType()).commit()
        tbl.refresh()

        loaded_type = tbl.schema.find_type("x")
        assert isinstance(loaded_type, BigIntType)

    def test_chained_evolution(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("evo_chain", simple_schema)
        tbl.update_schema().add_column("col_a", IntegerType()).add_column("col_b", StringType()).commit()
        tbl.refresh()

        names = tbl.schema.column_names()
        assert "col_a" in names
        assert "col_b" in names

    def test_context_manager(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("evo_ctx", simple_schema)
        with tbl.update_schema() as us:
            us.add_column("ctx_col", DoubleType())
        tbl.refresh()

        assert "ctx_col" in tbl.schema.column_names()

    def test_data_preserved(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("evo_data", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))

        tbl.update_schema().add_column("tag", StringType()).commit()
        tbl.refresh()

        result = tbl.scan().to_arrow()
        assert result.num_rows == 2
        assert sorted(result.column("id").to_pylist()) == [1, 2]
        assert result.column("tag").to_pylist() == [None, None]


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_with_expression(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("del_expr", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3, 4, 5], "name": list("abcde"), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}))
        tbl.delete(GreaterThan("id", 3))
        result = tbl.scan().to_arrow()
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_delete_with_string_filter(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("del_str", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        tbl.delete("id = 2")
        result = tbl.scan().to_arrow()
        assert sorted(result.column("id").to_pylist()) == [1, 3]

    def test_delete_all(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("del_all", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        tbl.delete(AlwaysTrue())
        assert tbl.scan().count() == 0

    def test_delete_then_time_travel(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("del_tt", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        snap_before = tbl.snapshots()[-1]
        tbl.delete(GreaterThan("id", 1))
        assert tbl.scan().count() == 1
        result = tbl.scan().with_snapshot(snap_before.snapshot_id).to_arrow()
        assert result.num_rows == 3


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------


class TestUpsert:
    def test_upsert_insert_only(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("ups_ins", simple_schema)
        result = tbl.upsert(
            pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}),
            join_cols=["id"],
        )
        assert result.rows_inserted == 2
        assert result.rows_updated == 0
        assert tbl.scan().count() == 2

    def test_upsert_mixed(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("ups_mix", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        result = tbl.upsert(
            pa.table({"id": [2, 3], "name": ["B", "C"], "value": [22.0, 33.0]}),
            join_cols=["id"],
        )
        assert result.rows_updated == 1
        assert result.rows_inserted == 1
        data = tbl.scan().to_arrow()
        assert data.num_rows == 3
        row2 = data.filter(pa.compute.equal(data.column("id"), 2)).to_pydict()
        assert row2["name"] == ["B"]
        assert row2["value"] == [22.0]

    def test_upsert_preserves_unmatched(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("ups_keep", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        tbl.upsert(
            pa.table({"id": [2], "name": ["B"], "value": [22.0]}),
            join_cols=["id"],
        )
        data = tbl.scan().to_arrow()
        row1 = data.filter(pa.compute.equal(data.column("id"), 1)).to_pydict()
        assert row1["name"] == ["a"]
        assert row1["value"] == [1.0]
        row3 = data.filter(pa.compute.equal(data.column("id"), 3)).to_pydict()
        assert row3["name"] == ["c"]
        assert row3["value"] == [3.0]

    def test_upsert_then_time_travel(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("ups_tt", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        snap_before = tbl.snapshots()[-1]
        tbl.upsert(
            pa.table({"id": [2], "name": ["B"], "value": [22.0]}),
            join_cols=["id"],
        )
        old = tbl.scan().with_snapshot(snap_before.snapshot_id).to_arrow()
        row2 = old.filter(pa.compute.equal(old.column("id"), 2)).to_pydict()
        assert row2["name"] == ["b"]
        assert row2["value"] == [2.0]


# ---------------------------------------------------------------------------
# Scan features (Phase 2)
# ---------------------------------------------------------------------------


class TestScanPhase2:
    def test_scan_with_timestamp(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _skip_duckdb_snapshots(request)
        if _backend(request) == "sqlite":
            pytest.skip("SQLite backend returns epoch-zero snapshot timestamps")
        _mysql_xfail(request)
        from datetime import timedelta

        tbl = catalog.create_table("scan_ts", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        snap2 = tbl.snapshots()[-1]
        ts = snap2.timestamp + timedelta(seconds=1)
        result_all = tbl.scan().with_timestamp(ts).to_arrow()
        assert result_all.num_rows == 2

    def test_scan_to_duckdb(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("scan_ddb", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        rel = tbl.scan().to_duckdb()
        arrow = rel.arrow()
        if isinstance(arrow, pa.Table):
            assert arrow.num_rows == 3
        else:
            assert arrow.read_all().num_rows == 3

    def test_scan_to_arrow_batch_reader(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("scan_abr", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        reader = tbl.scan().to_arrow_batch_reader()
        assert isinstance(reader, pa.RecordBatchReader)
        result = reader.read_all()
        expected = tbl.scan().to_arrow()
        assert result.num_rows == expected.num_rows
        assert sorted(result.column("id").to_pylist()) == sorted(expected.column("id").to_pylist())

    def test_scan_combined_filter_select_limit(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("scan_combo", simple_schema)
        tbl.append(
            pa.table(
                {
                    "id": list(range(1, 11)),
                    "name": [f"n{i}" for i in range(1, 11)],
                    "value": [float(i) for i in range(1, 11)],
                }
            )
        )
        result = tbl.scan().filter(GreaterThan("id", 3)).select("id", "value").with_limit(2).to_arrow()
        assert result.column_names == ["id", "value"]
        assert result.num_rows == 2
        for v in result.column("id").to_pylist():
            assert v > 3


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


class TestTransaction:
    def test_transaction_commit(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("txn_commit", simple_schema)
        with catalog.begin_transaction() as txn:
            t = txn.load_table("txn_commit")
            t.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        assert tbl.scan().count() == 1

    def test_transaction_rollback(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("txn_rb", simple_schema)
        txn = catalog.begin_transaction()
        t = txn.load_table("txn_rb")
        t.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        txn.rollback()
        assert tbl.scan().count() == 0

    def test_transaction_multi_table(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("txn_mt1", simple_schema)
        catalog.create_table("txn_mt2", simple_schema)
        with catalog.begin_transaction() as txn:
            t1 = txn.load_table("txn_mt1")
            t2 = txn.load_table("txn_mt2")
            t1.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
            t2.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        assert catalog.load_table("txn_mt1").scan().count() == 1
        assert catalog.load_table("txn_mt2").scan().count() == 1

    def test_transaction_context_manager_rollback_on_exception(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("txn_exc", simple_schema)
        with pytest.raises(RuntimeError):
            with catalog.begin_transaction() as txn:
                t = txn.load_table("txn_exc")
                t.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
                raise RuntimeError("boom")
        assert catalog.load_table("txn_exc").scan().count() == 0


# ---------------------------------------------------------------------------
# Inspect
# ---------------------------------------------------------------------------


class TestInspect:
    def test_inspect_snapshots(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("insp_snap", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        snaps = tbl.inspect().snapshots()
        assert isinstance(snaps, pa.Table)
        assert snaps.num_rows >= 2

    def test_inspect_files(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("insp_files", simple_schema)
        tbl.append(
            pa.table(
                {
                    "id": list(range(1000)),
                    "name": [f"name_{i}" for i in range(1000)],
                    "value": [float(i) for i in range(1000)],
                }
            )
        )
        files = tbl.inspect().files()
        assert isinstance(files, pa.Table)
        assert files.num_rows >= 1

    def test_inspect_history(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("insp_hist", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        history = tbl.inspect().history()
        assert isinstance(history, pa.Table)
        assert history.num_rows >= 2
        ids = history.column("snapshot_id").to_pylist()
        assert ids == sorted(ids, reverse=True)


# ---------------------------------------------------------------------------
# CDC
# ---------------------------------------------------------------------------


class TestCDC:
    def test_table_insertions(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("cdc_ins", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        snap1 = tbl.snapshots()[-1].snapshot_id
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        snap2 = tbl.snapshots()[-1].snapshot_id
        insertions = tbl.table_insertions(snap1, snap2)
        assert isinstance(insertions, ChangeSet)
        assert insertions.num_rows >= 1

    def test_table_deletions(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("cdc_del", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        snap1 = tbl.snapshots()[-1].snapshot_id
        tbl.delete(EqualTo("id", 2))
        snap2 = tbl.snapshots()[-1].snapshot_id
        deletions = tbl.table_deletions(snap1, snap2)
        assert isinstance(deletions, ChangeSet)
        assert deletions.num_rows >= 1

    def test_table_changes(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("cdc_chg", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        snap1 = tbl.snapshots()[-1].snapshot_id
        tbl.append(pa.table({"id": [3], "name": ["c"], "value": [3.0]}))
        tbl.delete(EqualTo("id", 1))
        snap2 = tbl.snapshots()[-1].snapshot_id
        changes = tbl.table_changes(snap1, snap2)
        assert isinstance(changes, ChangeSet)
        assert changes.num_rows >= 1


# ---------------------------------------------------------------------------
# Commit Metadata
# ---------------------------------------------------------------------------


class TestCommitMetadata:
    def test_commit_message_visible(
        self,
        catalog: Catalog,
        simple_schema: Schema,
        request: pytest.FixtureRequest,
    ) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("cmeta", simple_schema)
        with catalog.begin_transaction() as txn:
            catalog.set_commit_message("test commit message", author="test_author")
            t = txn.load_table("cmeta")
            t.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        snaps = tbl.inspect().snapshots()
        messages = snaps.column("commit_message").to_pylist()
        assert any("test commit message" in str(m) for m in messages if m is not None)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_get_options(self, catalog: Catalog) -> None:
        opts = catalog.get_options()
        assert isinstance(opts, pa.Table)
        assert opts.num_rows > 0

    def test_set_and_get_option(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("cfg_tbl", simple_schema)
        catalog.set_option("target_file_size", "10MB")
        opts = catalog.get_options()
        assert isinstance(opts, pa.Table)
        assert opts.num_rows > 0


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


class TestPartitioning:
    def test_set_partition_identity(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("part_id", simple_schema)
        tbl.update_spec().add_field("name").commit()
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        result = tbl.scan().to_arrow()
        assert result.num_rows == 2

    def test_set_partition_year(self, catalog: Catalog) -> None:
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="ts", field_type=TimestampType()),
            NestedField(field_id=3, name="value", field_type=DoubleType()),
        )
        tbl = catalog.create_table("part_year", schema)
        tbl.update_spec().add_field("ts", YEAR).commit()
        import datetime as dt

        tbl.append(
            pa.table(
                {
                    "id": [1, 2],
                    "ts": [dt.datetime(2023, 1, 15), dt.datetime(2024, 6, 20)],
                    "value": [1.0, 2.0],
                }
            )
        )
        result = tbl.scan().to_arrow()
        assert result.num_rows == 2

    def test_clear_partition(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("part_clr", simple_schema)
        tbl.update_spec().add_field("name").commit()
        assert not tbl.spec.is_unpartitioned
        tbl.update_spec().clear().commit()
        assert tbl.spec.is_unpartitioned

    def test_spec_property(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("part_spec", simple_schema)
        assert tbl.spec == UNPARTITIONED
        tbl.update_spec().add_field("name", IDENTITY).commit()
        spec = tbl.spec
        assert not spec.is_unpartitioned
        assert len(spec.fields) == 1
        assert spec.fields[0].source_column == "name"
        assert spec.fields[0].transform == IDENTITY


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------


class TestSorting:
    def test_set_sort_order(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("sort_single", simple_schema)
        tbl.update_sort_order().add_field("name").commit()
        so = tbl.sort_order
        assert not so.is_unsorted
        assert len(so.fields) == 1
        assert so.fields[0].source_column == "name"
        assert so.fields[0].direction.value == "ASC"

    def test_set_sort_order_multiple_fields(self, catalog: Catalog, simple_schema: Schema) -> None:
        from pyducklake.sorting import SortDirection

        tbl = catalog.create_table("sort_multi", simple_schema)
        tbl.update_sort_order().add_field("name", SortDirection.ASC).add_field("value", SortDirection.DESC).commit()
        so = tbl.sort_order
        assert len(so.fields) == 2
        assert so.fields[0].source_column == "name"
        assert so.fields[0].direction == SortDirection.ASC
        assert so.fields[1].source_column == "value"
        assert so.fields[1].direction == SortDirection.DESC

    def test_clear_sort_order(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("sort_clear", simple_schema)
        tbl.update_sort_order().add_field("name").commit()
        assert not tbl.sort_order.is_unsorted
        tbl.update_sort_order().clear().commit()
        assert tbl.sort_order.is_unsorted

    def test_sorted_table_write_read(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("sort_wr", simple_schema)
        tbl.update_sort_order().add_field("id").commit()
        tbl.append(pa.table({"id": [3, 1, 2], "name": ["c", "a", "b"], "value": [3.0, 1.0, 2.0]}))
        result = tbl.scan().to_arrow()
        assert result.num_rows == 3
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


class TestMaintenance:
    def test_compact(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("maint_compact", simple_schema)
        for i in range(5):
            tbl.append(pa.table({"id": [i], "name": [f"n{i}"], "value": [float(i)]}))
        tbl.maintenance().compact()
        result = tbl.scan().to_arrow()
        assert result.num_rows == 5
        assert sorted(result.column("id").to_pylist()) == [0, 1, 2, 3, 4]

    def test_expire_snapshots(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _skip_duckdb_snapshots(request)
        _mysql_xfail(request)
        tbl = catalog.create_table("maint_expire", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        snaps_before = len(tbl.snapshots())
        assert snaps_before >= 2
        tbl.maintenance().expire_snapshots(versions=1)
        assert tbl.scan().count() == 2

    def test_rewrite_data_files(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("maint_rewrite", simple_schema)
        tbl.append(
            pa.table(
                {
                    "id": list(range(10)),
                    "name": [f"n{i}" for i in range(10)],
                    "value": [float(i) for i in range(10)],
                }
            )
        )
        tbl.delete(GreaterThan("id", 5))
        tbl.maintenance().rewrite_data_files(delete_threshold=0.0)
        result = tbl.scan().to_arrow()
        assert sorted(result.column("id").to_pylist()) == [0, 1, 2, 3, 4, 5]

    def test_checkpoint(self, catalog: Catalog, simple_schema: Schema, request: pytest.FixtureRequest) -> None:
        _mysql_xfail(request)
        tbl = catalog.create_table("maint_ckpt", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.maintenance().checkpoint()
        assert tbl.scan().count() == 1


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


class TestViews:
    def test_create_and_list_views(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("view_base_list", simple_schema)
        fqn = catalog.fully_qualified_name("main", "view_base_list")
        catalog.create_view("v_list", f"SELECT * FROM {fqn}")
        views = catalog.list_views()
        view_names = [v[1] for v in views]
        assert "v_list" in view_names

    def test_drop_view(self, catalog: Catalog, simple_schema: Schema) -> None:
        catalog.create_table("view_base_drop", simple_schema)
        fqn = catalog.fully_qualified_name("main", "view_base_drop")
        catalog.create_view("v_drop", f"SELECT * FROM {fqn}")
        assert catalog.view_exists("v_drop")
        catalog.drop_view("v_drop")
        assert not catalog.view_exists("v_drop")

    def test_view_exists(self, catalog: Catalog, simple_schema: Schema) -> None:
        assert not catalog.view_exists("v_ghost")
        catalog.create_table("view_base_exists", simple_schema)
        fqn = catalog.fully_qualified_name("main", "view_base_exists")
        catalog.create_view("v_exists", f"SELECT * FROM {fqn}")
        assert catalog.view_exists("v_exists")

    def test_view_query(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("view_base_query", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        fqn = catalog.fully_qualified_name("main", "view_base_query")
        view_fqn = catalog.fully_qualified_name("main", "v_query")
        catalog.create_view("v_query", f"SELECT id, name FROM {fqn} WHERE id > 1")
        result = catalog.connection.execute(f"SELECT * FROM {view_fqn}").fetchall()
        assert len(result) == 2
        ids = sorted(r[0] for r in result)
        assert ids == [2, 3]


# ---------------------------------------------------------------------------
# Nested types
# ---------------------------------------------------------------------------


class TestNestedTypes:
    def test_write_read_nested_types(self, catalog: Catalog, request: pytest.FixtureRequest) -> None:
        if _backend(request) == "mysql":
            pytest.xfail("MySQL does not support composite/STRUCT types")
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(
                field_id=2,
                name="info",
                field_type=StructType(
                    fields=(
                        NestedField(field_id=10, name="key", field_type=StringType()),
                        NestedField(field_id=11, name="val", field_type=IntegerType()),
                    )
                ),
            ),
        )
        tbl = catalog.create_table("nested_tbl", schema)

        struct_type = pa.struct(
            [
                pa.field("key", pa.string()),
                pa.field("val", pa.int32()),
            ]
        )
        df = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "info": pa.array(
                    [{"key": "a", "val": 1}, {"key": "b", "val": 2}],
                    type=struct_type,
                ),
            }
        )
        tbl.append(df)

        result = tbl.scan().to_arrow()
        assert result.num_rows == 2
        info_col = result.column("info").to_pylist()
        assert info_col[0]["key"] == "a"
        assert info_col[1]["val"] == 2


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_append_type_mismatch_raises(self, catalog: Catalog, simple_schema: Schema) -> None:
        tbl = catalog.create_table("type_err", simple_schema)
        bad_df = pa.table(
            {
                "id": pa.array(["not_an_int"], type=pa.string()),
                "name": pa.array(["alice"], type=pa.string()),
                "value": pa.array(["not_a_double"], type=pa.string()),
            }
        )
        with pytest.raises(Exception):
            tbl.append(bad_df)
