"""Cross-feature interaction tests for pyducklake.

Tests that verify correct behavior when multiple features are used together.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyducklake import Catalog, Schema
from pyducklake.expressions import EqualTo, GreaterThan
from pyducklake.types import (
    DateType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
    StructType,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


def _simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )


def _dated_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="region", field_type=StringType()),
        NestedField(field_id=4, name="event_date", field_type=DateType()),
    )


def _simple_df(ids: list[int], names: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def _dated_df(
    ids: list[int],
    names: list[str],
    regions: list[str],
    dates: list[date],
) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "region": pa.array(regions, type=pa.string()),
            "event_date": pa.array(dates, type=pa.date32()),
        }
    )


# ---------------------------------------------------------------------------
# Schema evolution + read/write
# ---------------------------------------------------------------------------


class TestSchemaEvolutionWithData:
    def test_append_after_add_column(self, catalog: Catalog) -> None:
        table = catalog.create_table("evo_append", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))

        table.update_schema().add_column("score", IntegerType()).commit()

        df2 = pa.table(
            {
                "id": pa.array([2], type=pa.int32()),
                "name": pa.array(["bob"], type=pa.string()),
                "value": pa.array([20], type=pa.int32()),
                "score": pa.array([99], type=pa.int32()),
            }
        )
        table.append(df2)

        result = table.scan().to_arrow()
        assert result.num_rows == 2
        assert result.column("score").to_pylist()[0] is None
        assert result.column("score").to_pylist()[1] == 99

    def test_overwrite_filter_uses_renamed_column(self, catalog: Catalog) -> None:
        table = catalog.create_table("evo_rename_ow", _simple_schema())
        table.append(_simple_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))

        table.update_schema().rename_column("value", "amount").commit()

        df2 = pa.table(
            {
                "id": pa.array([4], type=pa.int32()),
                "name": pa.array(["dave"], type=pa.string()),
                "amount": pa.array([40], type=pa.int32()),
            }
        )
        table.overwrite(df2, overwrite_filter=GreaterThan("amount", 15))

        ids = sorted(table.scan().to_arrow().column("id").to_pylist())
        assert ids == [1, 4]

    def test_scan_select_dropped_column_raises(self, catalog: Catalog) -> None:
        table = catalog.create_table("evo_drop_scan", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))

        table.update_schema().drop_column("value").commit()

        with pytest.raises(Exception):
            table.scan().select("value").to_arrow()

    def test_schema_evolution_on_partitioned_table(self, catalog: Catalog) -> None:
        table = catalog.create_table("evo_part", _dated_schema())
        table.update_spec().add_field("region").commit()

        table.append(
            _dated_df(
                [1, 2],
                ["alice", "bob"],
                ["us", "eu"],
                [date(2024, 1, 15), date(2024, 2, 20)],
            )
        )

        table.update_schema().add_column("score", IntegerType()).commit()
        assert "score" in table.schema.column_names()

        result = table.scan().to_arrow()
        assert result.num_rows == 2
        assert result.column("score").to_pylist() == [None, None]

        table.update_schema().drop_column("score").commit()
        assert "score" not in table.schema.column_names()
        assert table.scan().count() == 2


# ---------------------------------------------------------------------------
# Transactions + mutations
# ---------------------------------------------------------------------------


class TestTransactionInteractions:
    def test_upsert_in_transaction_rollback(self, catalog: Catalog) -> None:
        table = catalog.create_table("txn_upsert_rb", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))

        with pytest.raises(RuntimeError):
            with catalog.begin_transaction() as txn:
                tbl = txn.load_table("txn_upsert_rb")
                tbl.upsert(
                    _simple_df([1, 2], ["alice_v2", "bob"], [11, 20]),
                    join_cols=["id"],
                )
                raise RuntimeError("force rollback")

        assert table.scan().count() == 1
        assert table.scan().to_arrow().column("name").to_pylist() == ["alice"]

    def test_delete_in_transaction(self, catalog: Catalog) -> None:
        table = catalog.create_table("txn_del", _simple_schema())
        table.append(_simple_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))

        with catalog.begin_transaction() as txn:
            tbl = txn.load_table("txn_del")
            tbl.delete(EqualTo("name", "bob"))

        ids = sorted(table.scan().to_arrow().column("id").to_pylist())
        assert ids == [1, 3]

    def test_multi_table_transaction(self, catalog: Catalog) -> None:
        t1 = catalog.create_table("txn_t1", _simple_schema())
        t2 = catalog.create_table("txn_t2", _simple_schema())

        with catalog.begin_transaction() as txn:
            txn.load_table("txn_t1").append(_simple_df([1], ["alice"], [10]))
            txn.load_table("txn_t2").append(_simple_df([2], ["bob"], [20]))

        assert t1.scan().count() == 1
        assert t2.scan().count() == 1

    def test_single_snapshot_for_multi_op_transaction(self, catalog: Catalog) -> None:
        table = catalog.create_table("txn_snap", _simple_schema())
        snaps_before = table.inspect().snapshots().num_rows

        with catalog.begin_transaction() as txn:
            tbl = txn.load_table("txn_snap")
            tbl.append(_simple_df([1], ["alice"], [10]))
            tbl.append(_simple_df([2], ["bob"], [20]))

        assert table.inspect().snapshots().num_rows == snaps_before + 1

    def test_overwrite_with_commit_message(self, catalog: Catalog) -> None:
        table = catalog.create_table("txn_ow_msg", _simple_schema())
        table.append(_simple_df([1, 2], ["alice", "bob"], [10, 20]))

        with catalog.begin_transaction() as txn:
            catalog.set_commit_message("overwrite in txn", author="tester")
            txn.load_table("txn_ow_msg").overwrite(_simple_df([3], ["carol"], [30]))

        assert table.scan().to_arrow().column("id").to_pylist() == [3]
        snapshots = table.inspect().snapshots()
        if "commit_message" in snapshots.column_names:
            assert "overwrite in txn" in snapshots.column("commit_message").to_pylist()


# ---------------------------------------------------------------------------
# Time travel after mutations
# ---------------------------------------------------------------------------


class TestTimeTravelAfterMutations:
    def test_time_travel_after_upsert(self, catalog: Catalog) -> None:
        table = catalog.create_table("tt_upsert", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))
        snap = table.current_snapshot()
        assert snap is not None

        table.upsert(_simple_df([1], ["alice_v2"], [99]), join_cols=["id"])

        assert table.scan().to_arrow().column("name").to_pylist() == ["alice_v2"]

        old = table.scan(snapshot_id=snap.snapshot_id).to_arrow()
        assert old.column("name").to_pylist() == ["alice"]

    def test_time_travel_after_delete(self, catalog: Catalog) -> None:
        table = catalog.create_table("tt_del", _simple_schema())
        table.append(_simple_df([1, 2], ["alice", "bob"], [10, 20]))
        snap = table.current_snapshot()
        assert snap is not None

        table.delete(EqualTo("name", "bob"))
        assert table.scan().count() == 1

        old = table.scan(snapshot_id=snap.snapshot_id).to_arrow()
        assert old.num_rows == 2


# ---------------------------------------------------------------------------
# Scan output formats after mutations
# ---------------------------------------------------------------------------


class TestScanOutputAfterMutations:
    def test_to_duckdb_after_delete(self, catalog: Catalog) -> None:
        table = catalog.create_table("scan_del_ddb", _simple_schema())
        table.append(_simple_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))
        table.delete(EqualTo("name", "bob"))

        rel = table.scan().to_duckdb()
        ids = sorted(r[0] for r in rel.fetchall())
        assert ids == [1, 3]

    def test_batch_reader_after_upsert(self, catalog: Catalog) -> None:
        table = catalog.create_table("scan_ups_br", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))
        table.upsert(_simple_df([1], ["alice_v2"], [99]), join_cols=["id"])

        reader = table.scan().to_arrow_batch_reader()
        combined = pa.Table.from_batches(list(reader))
        assert combined.num_rows == 1
        assert combined.column("name").to_pylist() == ["alice_v2"]


# ---------------------------------------------------------------------------
# Metadata after mutations
# ---------------------------------------------------------------------------


class TestMetadataAfterMutations:
    def test_commit_message_on_delete(self, catalog: Catalog) -> None:
        table = catalog.create_table("meta_del_msg", _simple_schema())
        table.append(_simple_df([1, 2], ["alice", "bob"], [10, 20]))

        conn = catalog.connection
        conn.execute("BEGIN TRANSACTION")
        catalog.set_commit_message("deleting bob", author="test")
        table.delete(EqualTo("name", "bob"))
        conn.execute("COMMIT")

        snapshots = table.inspect().snapshots()
        if "commit_message" in snapshots.column_names:
            assert "deleting bob" in snapshots.column("commit_message").to_pylist()

    def test_inspect_files_after_upsert(self, catalog: Catalog) -> None:
        table = catalog.create_table("meta_ups_files", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))
        n_before = table.inspect().files().num_rows

        table.upsert(_simple_df([1, 2], ["alice_v2", "bob"], [11, 20]), join_cols=["id"])
        assert table.inspect().files().num_rows >= n_before

    def test_inspect_history_after_upsert(self, catalog: Catalog) -> None:
        table = catalog.create_table("meta_ups_hist", _simple_schema())
        table.append(_simple_df([1], ["alice"], [10]))
        n_before = table.inspect().history().num_rows

        table.upsert(_simple_df([1], ["alice_v2"], [99]), join_cols=["id"])
        assert table.inspect().history().num_rows == n_before + 1

    def test_config_then_write(self, catalog: Catalog) -> None:
        table = catalog.create_table("meta_cfg_write", _simple_schema())
        catalog.set_option("target_file_size", "10MB")
        table.append(_simple_df([1, 2], ["alice", "bob"], [10, 20]))
        assert table.scan().count() == 2


# ---------------------------------------------------------------------------
# Delete + upsert lifecycle
# ---------------------------------------------------------------------------


class TestDeleteUpsertLifecycle:
    def test_delete_then_upsert_back(self, catalog: Catalog) -> None:
        table = catalog.create_table("del_ups_cycle", _simple_schema())
        table.append(_simple_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))

        table.delete(EqualTo("name", "bob"))
        assert table.scan().count() == 2

        result = table.upsert(_simple_df([2, 4], ["bob", "dave"], [25, 40]), join_cols=["id"])
        assert result.rows_inserted == 2
        assert result.rows_updated == 0

        ids = sorted(table.scan().to_arrow().column("id").to_pylist())
        assert ids == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Partitioning + other features
# ---------------------------------------------------------------------------


class TestPartitioningInteractions:
    @pytest.mark.duckdb15
    def test_sort_plus_partition(self, catalog: Catalog) -> None:
        table = catalog.create_table("part_sort", _dated_schema())
        table.update_spec().add_field("region").commit()
        table.update_sort_order().add_field("name").commit()

        assert not table.spec.is_unpartitioned
        assert not table.sort_order.is_unsorted

        table.append(
            _dated_df(
                [3, 1, 2],
                ["carol", "alice", "bob"],
                ["us", "eu", "us"],
                [date(2024, 3, 10), date(2024, 1, 15), date(2024, 2, 20)],
            )
        )

        result = table.scan().to_arrow()
        assert result.num_rows == 3
        assert set(result.column("region").to_pylist()) == {"us", "eu"}

    @pytest.mark.xfail(
        reason="ducklake_add_data_files rejects files for partitioned tables",
        strict=True,
    )
    def test_add_files_to_partitioned_table(self, tmp_path: Path, catalog: Catalog) -> None:
        table = catalog.create_table("part_add_files", _dated_schema())
        table.update_spec().add_field("region").commit()

        arrow_tbl = pa.table(
            {
                "id": pa.array([10, 20], type=pa.int32()),
                "name": pa.array(["ext_a", "ext_b"], type=pa.string()),
                "region": pa.array(["us", "eu"], type=pa.string()),
                "event_date": pa.array([date(2024, 5, 1), date(2024, 6, 1)], type=pa.date32()),
            }
        )
        pq.write_table(arrow_tbl, str(tmp_path / "external.parquet"))
        table.add_files([str(tmp_path / "external.parquet")])
        assert table.scan().count() == 2

    def test_compact_partitioned_table(self, catalog: Catalog) -> None:
        table = catalog.create_table("part_compact", _dated_schema())
        table.update_spec().add_field("region").commit()

        for i in range(5):
            table.append(
                _dated_df(
                    [i * 10 + j for j in range(3)],
                    [f"n{i}_{j}" for j in range(3)],
                    ["us", "eu", "us"],
                    [date(2024, 1, 1)] * 3,
                )
            )

        assert table.scan().count() == 15
        table.maintenance().compact()
        assert table.scan().count() == 15

    def test_views_not_blocking_drop_namespace(self, catalog: Catalog) -> None:
        base_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType()),
        )
        catalog.create_table("base_tbl", base_schema)
        catalog.create_namespace("viewns")
        catalog.create_view(("viewns", "v1"), 'SELECT * FROM "test_cat"."main"."base_tbl"')

        # DuckDB blocks dropping namespace that contains views
        with pytest.raises(Exception):
            catalog.drop_namespace("viewns")


# ---------------------------------------------------------------------------
# CDC after mutations
# ---------------------------------------------------------------------------


class TestCDCAfterMutations:
    def test_cdc_after_upsert(self, catalog: Catalog) -> None:
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType()),
        )
        table = catalog.create_table("cdc_upsert", schema)

        df = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "name": pa.array(["alice", "bob"], type=pa.string()),
            }
        )
        table.append(df)
        snap_after_insert = table.current_snapshot()
        assert snap_after_insert is not None

        table.upsert(
            pa.table(
                {
                    "id": pa.array([1, 3], type=pa.int32()),
                    "name": pa.array(["alice_v2", "carol"], type=pa.string()),
                }
            ),
            join_cols=["id"],
        )
        snap_after_upsert = table.current_snapshot()
        assert snap_after_upsert is not None

        changes = table.table_changes(
            snap_after_insert.snapshot_id,
            snap_after_upsert.snapshot_id,
        )
        assert changes.num_rows >= 1
        assert "insert" in set(changes.to_arrow().column("change_type").to_pylist())


# ---------------------------------------------------------------------------
# Nested type round-trips
# ---------------------------------------------------------------------------


class TestRollbackEncrypted:
    def test_rollback_encrypted_table(self, tmp_path: Path) -> None:
        """Rollback on an encrypted table works correctly."""
        meta_db = str(tmp_path / "meta.duckdb")
        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir, exist_ok=True)
        cat = Catalog("enc_cat", meta_db, data_path=data_dir, encrypted=True)
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType()),
        )
        tbl = cat.create_table("enc_rb", schema)
        tbl.append(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int32()),
                    "name": pa.array(["alice", "bob"], type=pa.string()),
                }
            )
        )
        snap_v1 = tbl.current_snapshot()
        assert snap_v1 is not None

        tbl.append(
            pa.table(
                {
                    "id": pa.array([3], type=pa.int32()),
                    "name": pa.array(["carol"], type=pa.string()),
                }
            )
        )
        assert tbl.scan().count() == 3

        tbl.rollback_to_snapshot(snap_v1.snapshot_id)
        result = tbl.scan().to_arrow()
        assert result.num_rows == 2
        assert sorted(result.column("id").to_pylist()) == [1, 2]
        cat.close()


class TestCDCAcrossSchemaEvolution:
    def test_cdc_across_schema_evolution(self, catalog: Catalog) -> None:
        """Add column, then check table_changes across the schema change boundary."""
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType()),
        )
        table = catalog.create_table("cdc_evo", schema)
        snap_before = table.current_snapshot()
        start = snap_before.snapshot_id if snap_before else 0

        table.append(
            pa.table(
                {
                    "id": pa.array([1], type=pa.int32()),
                    "name": pa.array(["alice"], type=pa.string()),
                }
            )
        )

        table.update_schema().add_column("score", IntegerType()).commit()

        table.append(
            pa.table(
                {
                    "id": pa.array([2], type=pa.int32()),
                    "name": pa.array(["bob"], type=pa.string()),
                    "score": pa.array([99], type=pa.int32()),
                }
            )
        )

        snap_after = table.current_snapshot()
        assert snap_after is not None

        result = table.table_changes(start, snap_after.snapshot_id)
        assert result.num_rows >= 2
        change_types = set(result.to_arrow().column("change_type").to_pylist())
        assert "insert" in change_types


class TestUpsertAllJoinColumns:
    def test_upsert_all_join_columns(self, catalog: Catalog) -> None:
        """Upsert where every column is a join column (empty update_set in MERGE)."""
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType()),
        )
        table = catalog.create_table("upsert_allkeys", schema)
        table.append(
            pa.table(
                {
                    "id": pa.array([1], type=pa.int32()),
                    "name": pa.array(["alice"], type=pa.string()),
                }
            )
        )

        result = table.upsert(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int32()),
                    "name": pa.array(["alice", "bob"], type=pa.string()),
                }
            ),
            join_cols=["id", "name"],
        )
        # id=1,name=alice already exists; id=2,name=bob is new
        assert result.rows_inserted >= 1
        assert table.scan().count() >= 2


class TestViewOverPartitionedTable:
    def test_view_over_partitioned_table(self, catalog: Catalog) -> None:
        """Create view over partitioned table, scan it."""
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType()),
            NestedField(field_id=3, name="region", field_type=StringType()),
        )
        table = catalog.create_table("part_view_tbl", schema)
        table.update_spec().add_field("region").commit()
        table.append(
            pa.table(
                {
                    "id": pa.array([1, 2, 3], type=pa.int32()),
                    "name": pa.array(["a", "b", "c"], type=pa.string()),
                    "region": pa.array(["us", "eu", "us"], type=pa.string()),
                }
            )
        )

        view = catalog.create_view(
            "part_view",
            'SELECT * FROM "test_cat"."main"."part_view_tbl"',
        )
        result = view.to_arrow()
        assert result.num_rows == 3
        assert set(result.column("region").to_pylist()) == {"us", "eu"}


class TestNestedTypeRoundTrips:
    def test_struct_and_list(self, catalog: Catalog) -> None:
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(
                field_id=2,
                name="metadata",
                field_type=StructType(
                    fields=(
                        NestedField(field_id=10, name="key", field_type=StringType()),
                        NestedField(field_id=11, name="val", field_type=IntegerType()),
                    )
                ),
            ),
            NestedField(
                field_id=3,
                name="tags",
                field_type=ListType(element_id=20, element_type=StringType()),
            ),
        )
        table = catalog.create_table("nested_tbl", schema)

        struct_type = pa.struct([pa.field("key", pa.string()), pa.field("val", pa.int32())])
        df = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "metadata": pa.array(
                    [{"key": "a", "val": 1}, {"key": "b", "val": 2}],
                    type=struct_type,
                ),
                "tags": pa.array([["x", "y"], ["z"]], type=pa.list_(pa.string())),
            }
        )
        table.append(df)

        result = table.scan().to_arrow()
        assert result.num_rows == 2
        assert result.column("metadata").to_pylist()[0]["key"] == "a"
        assert result.column("tags").to_pylist()[0] == ["x", "y"]
