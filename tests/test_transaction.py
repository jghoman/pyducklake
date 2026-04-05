"""Tests for pyducklake.transaction.Transaction."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, DucklakeError, Schema, Table
from pyducklake.expressions import EqualTo
from pyducklake.types import IntegerType, NestedField, StringType


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("test_cat", meta_db, data_path=data_dir)


@pytest.fixture()
def simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )


@pytest.fixture()
def table(catalog: Catalog, simple_schema: Schema) -> Table:
    return catalog.create_table("txn_tbl", simple_schema)


def _make_df(ids: list[int], names: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


# ---------------------------------------------------------------------------


def test_transaction_commit(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    tbl = txn.load_table("txn_tbl")
    tbl.append(_make_df([1], ["alice"], [10]))
    txn.commit()

    assert table.scan().count() == 1


def test_transaction_rollback(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    tbl = txn.load_table("txn_tbl")
    tbl.append(_make_df([1], ["alice"], [10]))
    txn.rollback()

    assert table.scan().count() == 0


def test_transaction_context_manager_commit(catalog: Catalog, table: Table) -> None:
    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.append(_make_df([1], ["alice"], [10]))

    assert table.scan().count() == 1


def test_transaction_context_manager_rollback(catalog: Catalog, table: Table) -> None:
    with pytest.raises(RuntimeError):
        with catalog.begin_transaction() as txn:
            tbl = txn.load_table("txn_tbl")
            tbl.append(_make_df([1], ["alice"], [10]))
            raise RuntimeError("force rollback")

    assert table.scan().count() == 0


def test_transaction_multi_table(catalog: Catalog, simple_schema: Schema, table: Table) -> None:
    table2 = catalog.create_table("txn_tbl2", simple_schema)

    with catalog.begin_transaction() as txn:
        t1 = txn.load_table("txn_tbl")
        t1.append(_make_df([1], ["alice"], [10]))

        t2 = txn.load_table("txn_tbl2")
        t2.append(_make_df([2], ["bob"], [20]))

    assert table.scan().count() == 1
    assert table2.scan().count() == 1


def test_transaction_multi_table_rollback(catalog: Catalog, simple_schema: Schema, table: Table) -> None:
    table2 = catalog.create_table("txn_tbl2", simple_schema)

    txn = catalog.begin_transaction()
    t1 = txn.load_table("txn_tbl")
    t1.append(_make_df([1], ["alice"], [10]))
    t2 = txn.load_table("txn_tbl2")
    t2.append(_make_df([2], ["bob"], [20]))
    txn.rollback()

    assert table.scan().count() == 0
    assert table2.scan().count() == 0


def test_transaction_delete_in_txn(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.delete(EqualTo("name", "bob"))

    result = table.scan().to_arrow()
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 3]


def test_transaction_overwrite_in_txn(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1, 2], ["alice", "bob"], [10, 20]))

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.overwrite(_make_df([3], ["carol"], [30]))

    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]


def test_transaction_double_commit_raises(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    txn.commit()

    with pytest.raises(DucklakeError, match="already finalized"):
        txn.commit()


def test_transaction_commit_then_rollback_raises(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    txn.commit()

    with pytest.raises(DucklakeError, match="already finalized"):
        txn.rollback()


def test_transaction_is_active(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    assert txn.is_active is True

    txn.commit()
    assert txn.is_active is False


# -- Phase 2: additional transaction tests ------------------------------------


def test_transaction_rollback_then_commit_raises(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    txn.rollback()

    with pytest.raises(DucklakeError, match="already finalized"):
        txn.commit()


def test_transaction_double_rollback_raises(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    txn.rollback()

    with pytest.raises(DucklakeError, match="already finalized"):
        txn.rollback()


def test_transaction_is_active_after_rollback(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    assert txn.is_active is True

    txn.rollback()
    assert txn.is_active is False


def test_transaction_append_and_delete_same_table(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.append(_make_df([4], ["dave"], [40]))
        tbl.delete(EqualTo("name", "bob"))

    result = table.scan().to_arrow()
    names = sorted(result.column("name").to_pylist())
    assert names == ["alice", "carol", "dave"]


def test_transaction_upsert_in_txn(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1, 2], ["alice", "bob"], [10, 20]))

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        # Update id=1 value, insert id=3
        upsert_df = _make_df([1, 3], ["alice", "carol"], [99, 30])
        result = tbl.upsert(upsert_df, join_cols=["id"])

    assert result.rows_updated == 1
    assert result.rows_inserted == 1

    arrow = table.scan().to_arrow()
    rows = {r["id"]: r for r in arrow.to_pylist()}
    assert rows[1]["value"] == 99
    assert rows[3]["name"] == "carol"


def test_transaction_overwrite_partial_filter_in_txn(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1, 2, 3], ["alice", "bob", "carol"], [10, 20, 30]))

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        # Overwrite only rows where name = 'bob'
        tbl.overwrite(
            _make_df([2], ["robert"], [25]),
            overwrite_filter=EqualTo("name", "bob"),
        )

    result = table.scan().to_arrow()
    names = sorted(result.column("name").to_pylist())
    assert names == ["alice", "carol", "robert"]


def test_transaction_context_manager_already_committed(catalog: Catalog, table: Table) -> None:
    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.append(_make_df([1], ["alice"], [10]))
        txn.commit()  # manually commit inside the block

    # __exit__ should be a no-op; data should still be committed
    assert table.scan().count() == 1


def test_transaction_context_manager_already_rolled_back(catalog: Catalog, table: Table) -> None:
    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.append(_make_df([1], ["alice"], [10]))
        txn.rollback()  # manually rollback inside the block

    # __exit__ should be a no-op; data should not be visible
    assert table.scan().count() == 0


def test_transaction_empty_commit(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    txn.commit()
    # No error, no data change
    assert table.scan().count() == 0


def test_transaction_scan_within_txn(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1], ["alice"], [10]))

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.append(_make_df([2], ["bob"], [20]))
        # Read inside the transaction should see uncommitted data
        count = tbl.scan().count()
        assert count == 2

    # After commit, count is still 2
    assert table.scan().count() == 2


def test_transaction_schema_evolution_in_txn(catalog: Catalog, table: Table) -> None:
    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.update_schema().add_column("extra", IntegerType()).commit()
        # Append with new schema
        df = pa.table(
            {
                "id": pa.array([1], type=pa.int32()),
                "name": pa.array(["alice"], type=pa.string()),
                "value": pa.array([10], type=pa.int32()),
                "extra": pa.array([42], type=pa.int32()),
            }
        )
        tbl.append(df)

    result = table.refresh().scan().to_arrow()
    assert result.num_rows == 1
    assert "extra" in result.column_names
    assert result.column("extra").to_pylist() == [42]


def test_transaction_exception_during_operation_rolls_back(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1], ["alice"], [10]))

    with pytest.raises(Exception):
        with catalog.begin_transaction() as txn:
            tbl = txn.load_table("txn_tbl")
            # This should fail: wrong number of columns / type mismatch
            bad_df = pa.table({"wrong_col": pa.array(["not_an_int"])})
            tbl.append(bad_df)

    # Original data should still be intact
    assert table.scan().count() == 1


def test_transaction_load_nonexistent_table_raises(catalog: Catalog, table: Table) -> None:
    txn = catalog.begin_transaction()
    try:
        with pytest.raises(Exception):
            txn.load_table("nope")
    finally:
        txn.rollback()


def test_transaction_multiple_appends_same_table(catalog: Catalog, table: Table) -> None:
    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.append(_make_df([1], ["alice"], [10]))
        tbl.append(_make_df([2], ["bob"], [20]))

    result = table.scan().to_arrow()
    assert result.num_rows == 2
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2]


def test_transaction_delete_all_then_append_in_txn(catalog: Catalog, table: Table) -> None:
    table.append(_make_df([1, 2], ["alice", "bob"], [10, 20]))
    assert table.scan().count() == 2

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table("txn_tbl")
        tbl.delete("1=1")  # delete everything
        tbl.append(_make_df([3], ["carol"], [30]))

    result = table.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["carol"]
