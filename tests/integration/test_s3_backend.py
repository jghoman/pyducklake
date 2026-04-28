"""Integration tests: full Phase 1 workflow with PostgreSQL metadata + S3 (MinIO) data storage.

Requires Docker. Run with: just test-integration
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from pyducklake import (
    Catalog,
    Schema,
)
from pyducklake.expressions import EqualTo, GreaterThan
from pyducklake.partitioning import YEAR
from pyducklake.types import (
    BigIntType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Table CRUD with S3 data
# ---------------------------------------------------------------------------


class TestS3TableCRUD:
    def test_create_and_load(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_crud", simple_schema)
        assert tbl.name == "s3_crud"

        loaded = s3_catalog.load_table("s3_crud")
        assert len(loaded.schema.fields) == len(simple_schema.fields)

    def test_list_tables(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        s3_catalog.create_table("s3_list_a", simple_schema)
        s3_catalog.create_table("s3_list_b", simple_schema)
        names = [t[1] for t in s3_catalog.list_tables()]
        assert "s3_list_a" in names
        assert "s3_list_b" in names

    def test_drop_table(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        s3_catalog.create_table("s3_drop", simple_schema)
        assert s3_catalog.table_exists("s3_drop")
        s3_catalog.drop_table("s3_drop")
        assert not s3_catalog.table_exists("s3_drop")


# ---------------------------------------------------------------------------
# Write + Read round-trip via S3
# ---------------------------------------------------------------------------


class TestS3WriteRead:
    def test_append_and_scan(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_append", simple_schema)
        df = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]})
        tbl.append(df)

        result = tbl.scan().to_arrow()
        assert result.num_rows == 3
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_multiple_appends(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_multi", simple_schema)
        for i in range(3):
            tbl.append(pa.table({"id": [i], "name": [f"r{i}"], "value": [float(i)]}))

        assert tbl.scan().count() == 3

    def test_scan_with_filter(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_filter", simple_schema)
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

    def test_scan_with_select(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_select", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["x"], "value": [9.9]}))

        result = tbl.scan().select("id", "name").to_arrow()
        assert result.column_names == ["id", "name"]

    def test_scan_to_pandas(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_pandas", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))

        pdf = tbl.scan().to_pandas()
        assert len(pdf) == 2

    def test_overwrite_all(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_ow_all", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))

        tbl.overwrite(pa.table({"id": [10], "name": ["z"], "value": [99.0]}))
        result = tbl.scan().to_arrow()
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [10]

    def test_overwrite_with_filter(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_ow_filt", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))

        tbl.overwrite(
            pa.table({"id": [20], "name": ["B"], "value": [22.0]}),
            overwrite_filter=EqualTo("id", 2),
        )
        ids = sorted(tbl.scan().to_arrow().column("id").to_pylist())
        assert ids == [1, 3, 20]

    def test_scan_with_limit(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_limit", simple_schema)
        tbl.append(
            pa.table(
                {
                    "id": list(range(10)),
                    "name": [f"n{i}" for i in range(10)],
                    "value": [float(i) for i in range(10)],
                }
            )
        )

        result = tbl.scan().with_limit(5).to_arrow()
        assert result.num_rows == 5


# ---------------------------------------------------------------------------
# Snapshots & time travel via S3
# ---------------------------------------------------------------------------


class TestS3Snapshots:
    def test_snapshots_after_writes(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_snaps", simple_schema)
        snap_before = len(tbl.snapshots())

        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))

        assert len(tbl.snapshots()) >= snap_before + 2

    def test_time_travel(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_tt", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        first_snap = tbl.snapshots()[-1]

        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        assert tbl.scan().count() == 2

        result = tbl.scan().with_snapshot(first_snap.snapshot_id).to_arrow()
        assert result.num_rows == 1


# ---------------------------------------------------------------------------
# Schema evolution via S3
# ---------------------------------------------------------------------------


class TestS3SchemaEvolution:
    def test_add_column(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_evo_add", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))

        tbl.update_schema().add_column("extra", StringType()).commit()
        tbl.refresh()

        assert "extra" in tbl.schema.column_names()
        result = tbl.scan().to_arrow()
        assert result.column("extra").to_pylist() == [None]

    def test_drop_column(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_evo_drop", simple_schema)
        tbl.update_schema().drop_column("value").commit()
        tbl.refresh()
        assert "value" not in tbl.schema.column_names()

    def test_rename_column(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_evo_ren", simple_schema)
        tbl.update_schema().rename_column("name", "label").commit()
        tbl.refresh()
        assert "label" in tbl.schema.column_names()
        assert "name" not in tbl.schema.column_names()

    def test_widen_type(self, s3_catalog: Catalog) -> None:
        schema = Schema(NestedField(field_id=1, name="x", field_type=IntegerType()))
        tbl = s3_catalog.create_table("s3_evo_widen", schema)
        tbl.update_schema().update_column("x", BigIntType()).commit()
        tbl.refresh()
        assert isinstance(tbl.schema.find_type("x"), BigIntType)

    def test_data_preserved(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_evo_data", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))

        tbl.update_schema().add_column("tag", StringType()).commit()
        tbl.refresh()

        result = tbl.scan().to_arrow()
        assert result.num_rows == 2
        assert result.column("tag").to_pylist() == [None, None]


# ---------------------------------------------------------------------------
# P1: Extended S3 tests
# ---------------------------------------------------------------------------


class TestS3Extended:
    def test_s3_rename_table(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        s3_catalog.create_table("s3_old_name", simple_schema)
        renamed = s3_catalog.rename_table("s3_old_name", "s3_new_name")
        assert renamed.name == "s3_new_name"
        assert not s3_catalog.table_exists("s3_old_name")
        assert s3_catalog.table_exists("s3_new_name")

    def test_s3_namespace_crud(self, s3_catalog: Catalog) -> None:
        s3_catalog.create_namespace("s3_test_ns")
        assert s3_catalog.namespace_exists("s3_test_ns")
        assert "s3_test_ns" in s3_catalog.list_namespaces()
        s3_catalog.drop_namespace("s3_test_ns")
        assert not s3_catalog.namespace_exists("s3_test_ns")


# ---------------------------------------------------------------------------
# Phase 2: Delete (S3)
# ---------------------------------------------------------------------------


class TestS3Delete:
    def test_s3_delete_with_filter(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_del_filt", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3, 4, 5], "name": list("abcde"), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}))
        tbl.delete(GreaterThan("id", 3))
        result = tbl.scan().to_arrow()
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_s3_delete_then_time_travel(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_del_tt", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        snap_before = tbl.snapshots()[-1]
        tbl.delete(GreaterThan("id", 1))
        assert tbl.scan().count() == 1
        result = tbl.scan().with_snapshot(snap_before.snapshot_id).to_arrow()
        assert result.num_rows == 3


# ---------------------------------------------------------------------------
# Phase 2: Upsert (S3)
# ---------------------------------------------------------------------------


class TestS3Upsert:
    @pytest.mark.xfail(reason="DuckDB MERGE with S3 storage may fail with HTTP 403", strict=False)
    def test_s3_upsert_mixed(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_ups_mix", simple_schema)
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

    @pytest.mark.xfail(reason="DuckDB MERGE with S3 storage may fail with HTTP 403", strict=False)
    def test_s3_upsert_preserves_unmatched(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_ups_keep", simple_schema)
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


# ---------------------------------------------------------------------------
# Phase 2: Scan features (S3)
# ---------------------------------------------------------------------------


class TestS3ScanPhase2:
    def test_s3_scan_to_duckdb(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_scan_ddb", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        rel = tbl.scan().to_duckdb()
        arrow = rel.arrow()
        if isinstance(arrow, pa.Table):
            assert arrow.num_rows == 3
        else:
            assert arrow.read_all().num_rows == 3

    def test_s3_scan_to_arrow_batch_reader(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_scan_abr", simple_schema)
        tbl.append(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}))
        reader = tbl.scan().to_arrow_batch_reader()
        assert isinstance(reader, pa.RecordBatchReader)
        result = reader.read_all()
        assert result.num_rows == 2

    def test_s3_scan_with_timestamp(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        from datetime import timedelta

        tbl = s3_catalog.create_table("s3_scan_ts", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        snap2 = tbl.snapshots()[-1]
        ts = snap2.timestamp + timedelta(seconds=1)
        result_all = tbl.scan().with_timestamp(ts).to_arrow()
        assert result_all.num_rows == 2


# ---------------------------------------------------------------------------
# Phase 2: Transaction (S3)
# ---------------------------------------------------------------------------


class TestS3Transaction:
    def test_s3_transaction_commit(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_txn_c", simple_schema)
        with s3_catalog.begin_transaction() as txn:
            t = txn.load_table("s3_txn_c")
            t.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        assert tbl.scan().count() == 1

    def test_s3_transaction_rollback(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_txn_rb", simple_schema)
        txn = s3_catalog.begin_transaction()
        t = txn.load_table("s3_txn_rb")
        t.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        txn.rollback()
        assert tbl.scan().count() == 0


# ---------------------------------------------------------------------------
# Phase 2: Inspect (S3)
# ---------------------------------------------------------------------------


class TestS3Inspect:
    def test_s3_inspect_snapshots(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_insp_snap", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.append(pa.table({"id": [2], "name": ["b"], "value": [2.0]}))
        snaps = tbl.inspect().snapshots()
        assert isinstance(snaps, pa.Table)
        assert snaps.num_rows >= 2

    def test_s3_inspect_files(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_insp_files", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        files = tbl.inspect().files()
        assert isinstance(files, pa.Table)
        # Small writes may be inlined by ducklake, so we just verify the API returns a valid table
        assert "data_file" in files.column_names


# ---------------------------------------------------------------------------
# Phase 2: Partitioning (S3)
# ---------------------------------------------------------------------------


class TestS3Partitioning:
    def test_s3_partition_write_read(self, s3_catalog: Catalog) -> None:
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="ts", field_type=TimestampType()),
            NestedField(field_id=3, name="value", field_type=DoubleType()),
        )
        tbl = s3_catalog.create_table("s3_part_wr", schema)
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


# ---------------------------------------------------------------------------
# Phase 3: Sorting (S3)
# ---------------------------------------------------------------------------


@pytest.mark.duckdb15
class TestS3Sorting:
    def test_s3_set_sort_order(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_sort_set", simple_schema)
        tbl.update_sort_order().add_field("name").commit()
        so = tbl.sort_order
        assert not so.is_unsorted
        assert len(so.fields) == 1
        assert so.fields[0].source_column == "name"

    def test_s3_sorted_write_read(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_sort_wr", simple_schema)
        tbl.update_sort_order().add_field("id").commit()
        tbl.append(pa.table({"id": [3, 1, 2], "name": ["c", "a", "b"], "value": [3.0, 1.0, 2.0]}))
        result = tbl.scan().to_arrow()
        assert result.num_rows == 3
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Phase 3: Maintenance (S3)
# ---------------------------------------------------------------------------


class TestS3Maintenance:
    def test_s3_compact(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_maint_compact", simple_schema)
        for i in range(5):
            tbl.append(pa.table({"id": [i], "name": [f"n{i}"], "value": [float(i)]}))
        tbl.maintenance().compact()
        result = tbl.scan().to_arrow()
        assert result.num_rows == 5
        assert sorted(result.column("id").to_pylist()) == [0, 1, 2, 3, 4]

    @pytest.mark.xfail(reason="CHECKPOINT with S3 storage may fail with HTTP 403", strict=False)
    def test_s3_checkpoint(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_maint_ckpt", simple_schema)
        tbl.append(pa.table({"id": [1], "name": ["a"], "value": [1.0]}))
        tbl.maintenance().checkpoint()
        assert tbl.scan().count() == 1


# ---------------------------------------------------------------------------
# Phase 3: Views (S3)
# ---------------------------------------------------------------------------


class TestS3Views:
    def test_s3_create_and_list_views(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        s3_catalog.create_table("s3_vbase_list", simple_schema)
        fqn = s3_catalog.fully_qualified_name("main", "s3_vbase_list")
        s3_catalog.create_view("s3_v_list", f"SELECT * FROM {fqn}")
        views = s3_catalog.list_views()
        view_names = [v[1] for v in views]
        assert "s3_v_list" in view_names

    def test_s3_view_query(self, s3_catalog: Catalog, simple_schema: Schema) -> None:
        tbl = s3_catalog.create_table("s3_vbase_query", simple_schema)
        tbl.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]}))
        fqn = s3_catalog.fully_qualified_name("main", "s3_vbase_query")
        view_fqn = s3_catalog.fully_qualified_name("main", "s3_v_query")
        s3_catalog.create_view("s3_v_query", f"SELECT id, name FROM {fqn} WHERE id > 1")
        result = s3_catalog.connection.execute(f"SELECT * FROM {view_fqn}").fetchall()
        assert len(result) == 2
        ids = sorted(r[0] for r in result)
        assert ids == [2, 3]
