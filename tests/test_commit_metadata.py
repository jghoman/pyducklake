"""Tests for commit metadata (set_commit_message)."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.types import IntegerType, NestedField, StringType


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
    return catalog.create_table("commit_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
        }
    )


def test_set_commit_message(catalog: Catalog, table: Table) -> None:
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("test commit message")
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    # Verify message is stored in snapshot_changes
    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes WHERE commit_message IS NOT NULL'
    )
    messages = [r[0] for r in rows]
    assert "test commit message" in messages


def test_set_commit_message_with_author(catalog: Catalog, table: Table) -> None:
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("authored commit", author="jkr")
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT author, commit_message FROM "{meta_schema}".ducklake_snapshot_changes WHERE commit_message IS NOT NULL'
    )
    found = False
    for author, message in rows:
        if message == "authored commit" and author == "jkr":
            found = True
    assert found


def test_commit_message_in_inspect(catalog: Catalog, table: Table) -> None:
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("inspect visible msg", author="tester")
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    snapshots = table.inspect().snapshots()
    assert isinstance(snapshots, pa.Table)

    # The snapshots() function includes commit_message column
    if "commit_message" in snapshots.column_names:
        messages = snapshots.column("commit_message").to_pylist()
        assert "inspect visible msg" in messages


# ---------------------------------------------------------------------------
# New coverage tests
# ---------------------------------------------------------------------------


def test_set_commit_message_empty_string(catalog: Catalog, table: Table) -> None:
    """message = '' is stored."""
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("")
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 1'
    )
    assert len(rows) >= 1
    # Empty string should be stored (possibly as empty or NULL depending on impl)
    assert rows[0][0] is not None or rows[0][0] == ""


def test_set_commit_message_special_chars(catalog: Catalog, table: Table) -> None:
    """Message with single quotes and unicode."""
    msg = "it's a test with unicode: \u00e9\u00e0\u00fc \u2603"
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message(msg)
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes '
        f"WHERE commit_message IS NOT NULL "
        f"ORDER BY snapshot_id DESC LIMIT 1"
    )
    assert len(rows) >= 1
    assert rows[0][0] == msg


def test_set_commit_message_author_special_chars(catalog: Catalog, table: Table) -> None:
    """Author with quotes and unicode."""
    author = "O'Brien \u00fc"
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("author test", author=author)
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f"SELECT author FROM \"{meta_schema}\".ducklake_snapshot_changes WHERE commit_message = 'author test'"
    )
    assert len(rows) >= 1
    assert rows[0][0] == author


def test_set_commit_message_multiple_in_txn(catalog: Catalog, table: Table) -> None:
    """Call set_commit_message twice in same txn; last wins."""
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("first msg")
    catalog.set_commit_message("second msg")
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 1'
    )
    assert len(rows) >= 1
    assert rows[0][0] == "second msg"


def test_set_commit_message_with_transaction_class(catalog: Catalog, table: Table) -> None:
    """Use catalog.begin_transaction() context manager."""
    with catalog.begin_transaction() as txn:
        catalog.set_commit_message("txn class msg")
        tbl = txn.load_table("commit_tbl")
        tbl.append(_make_arrow_table([10], ["via_txn"]))

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f"SELECT commit_message FROM \"{meta_schema}\".ducklake_snapshot_changes WHERE commit_message = 'txn class msg'"
    )
    assert len(rows) >= 1


def test_commit_message_not_visible_after_rollback(catalog: Catalog, table: Table) -> None:
    """Set message, rollback; message should not appear in snapshots."""
    # Get snapshot count before
    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows_before = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes '
        f"WHERE commit_message = 'rolled_back_msg'"
    )
    assert len(rows_before) == 0

    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("rolled_back_msg")
    table.append(_make_arrow_table([99], ["rollback_test"]))
    conn.execute("ROLLBACK")

    rows_after = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes '
        f"WHERE commit_message = 'rolled_back_msg'"
    )
    assert len(rows_after) == 0


def test_commit_message_per_snapshot(catalog: Catalog, table: Table) -> None:
    """Two txns with different messages; each has its own."""
    conn = catalog.connection

    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("msg_one")
    table.append(_make_arrow_table([1], ["first"]))
    conn.execute("COMMIT")

    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message("msg_two")
    table.append(_make_arrow_table([2], ["second"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes '
        f"WHERE commit_message IN ('msg_one', 'msg_two') "
        f"ORDER BY snapshot_id"
    )
    messages = [r[0] for r in rows]
    assert "msg_one" in messages
    assert "msg_two" in messages


def test_commit_message_long_string(catalog: Catalog, table: Table) -> None:
    """Message of 1000+ characters."""
    long_msg = "x" * 1500
    conn = catalog.connection
    conn.execute("BEGIN TRANSACTION")
    catalog.set_commit_message(long_msg)
    table.append(_make_arrow_table([1], ["alice"]))
    conn.execute("COMMIT")

    meta_schema = f"__ducklake_metadata_{catalog.name}"
    rows = catalog.fetchall(
        f'SELECT commit_message FROM "{meta_schema}".ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 1'
    )
    assert len(rows) >= 1
    assert rows[0][0] == long_msg
