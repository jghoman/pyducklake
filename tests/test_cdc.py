"""Tests for CDC (Change Data Capture) methods on Table."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.cdc import ChangeSet
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
    return catalog.create_table("cdc_tbl", schema)


def _make_arrow_table(ids: list[int], names: list[str | None]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
        }
    )


# ---------------------------------------------------------------------------
# 1-8: Basic snapshot-based tests
# ---------------------------------------------------------------------------


def test_table_insertions_returns_changeset(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    assert isinstance(result, ChangeSet)


def test_table_insertions_to_arrow(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    arrow = result.to_arrow()
    assert isinstance(arrow, pa.Table)
    assert arrow.num_rows == 1


def test_table_insertions_data(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_insertions(start, snap_after.snapshot_id)
    assert result.num_rows == 2
    assert sorted(result.to_arrow().column("id").to_pylist()) == [1, 2]


def test_table_deletions_data(table: Table) -> None:
    table.append(_make_arrow_table([1, 2, 3], ["alice", "bob", "carol"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.delete("id = 2")
    snap_after_delete = table.current_snapshot()
    assert snap_after_delete is not None

    result = table.table_deletions(snap_after_insert.snapshot_id, snap_after_delete.snapshot_id)
    assert result.num_rows == 1
    assert result.to_arrow().column("id").to_pylist() == [2]


def test_table_changes_data(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    table.delete("id = 1")
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(start, snap_after.snapshot_id)
    assert result.num_rows >= 2
    assert "change_type" in result.column_names
    change_types = set(result.to_arrow().column("change_type").to_pylist())
    assert "insert" in change_types
    assert "delete" in change_types


def test_table_changes_default_end(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_changes(start)
    assert result.num_rows >= 1


def test_table_insertions_default_end(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    assert result.num_rows == 1


def test_table_deletions_default_end(table: Table) -> None:
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None
    table.delete("id = 2")
    result = table.table_deletions(snap_after_insert.snapshot_id)
    assert result.num_rows == 1
    assert result.to_arrow().column("id").to_pylist() == [2]


# ---------------------------------------------------------------------------
# 9-12: Timestamp-based bounds
# ---------------------------------------------------------------------------


def test_table_changes_timestamp_bounds(table: Table) -> None:
    # Get the create-table snapshot as start
    snap_start = table.current_snapshot()
    assert snap_start is not None
    start_time = snap_start.timestamp

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    table.delete("id = 1")

    snap_after = table.current_snapshot()
    assert snap_after is not None
    end_time = snap_after.timestamp

    result = table.table_changes(start_time=start_time, end_time=end_time)
    assert result.num_rows >= 2
    change_types = set(result.to_arrow().column("change_type").to_pylist())
    assert "insert" in change_types
    assert "delete" in change_types


def test_table_insertions_timestamp_bounds(table: Table) -> None:
    snap_start = table.current_snapshot()
    assert snap_start is not None
    start_time = snap_start.timestamp

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))

    snap_after = table.current_snapshot()
    assert snap_after is not None
    end_time = snap_after.timestamp

    result = table.table_insertions(start_time=start_time, end_time=end_time)
    assert result.num_rows == 2


def test_table_changes_mixed_bounds_raises(table: Table) -> None:
    with pytest.raises(ValueError, match="Cannot mix"):
        table.table_changes(
            start_snapshot=1,
            end_time=datetime.now(tz=timezone.utc),
        )


def test_table_changes_no_bounds_raises(table: Table) -> None:
    with pytest.raises(ValueError, match="Must provide"):
        table.table_changes()


# ---------------------------------------------------------------------------
# 13-14: Column projection
# ---------------------------------------------------------------------------


def test_table_changes_column_projection(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    result = table.table_changes(start, columns=["id"])

    cols = result.column_names
    # Must have metadata cols + projected col
    assert "snapshot_id" in cols
    assert "rowid" in cols
    assert "change_type" in cols
    assert "id" in cols
    assert "name" not in cols


def test_table_insertions_column_projection(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    result = table.table_insertions(start, columns=["id"])

    cols = result.column_names
    # insertions have no metadata columns, just the projected data column
    assert "id" in cols
    assert "name" not in cols


# ---------------------------------------------------------------------------
# 15-16: Predicate pushdown
# ---------------------------------------------------------------------------


def test_table_changes_filter_expr(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2, 3], ["alice", "bob", "carol"]))
    result = table.table_changes(start, filter_expr="id > 2")

    ids = result.to_arrow().column("id").to_pylist()
    assert all(i > 2 for i in ids)
    assert len(ids) >= 1


def test_table_insertions_filter_expr(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2, 3], ["alice", "bob", "carol"]))
    result = table.table_insertions(start, filter_expr="id > 2")

    ids = result.to_arrow().column("id").to_pylist()
    assert ids == [3]


# ---------------------------------------------------------------------------
# 17-21: ChangeSet filtering
# ---------------------------------------------------------------------------


def test_changeset_inserts(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    table.delete("id = 1")

    result = table.table_changes(start)
    ins = result.inserts()
    assert all(ct == "insert" for ct in ins.column("change_type").to_pylist())


def test_changeset_deletes(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    table.delete("id = 1")

    result = table.table_changes(start)
    dels = result.deletes()
    assert all(ct == "delete" for ct in dels.column("change_type").to_pylist())
    assert dels.num_rows >= 1


def test_changeset_update_preimages(table: Table) -> None:
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(_make_arrow_table([1], ["alice_v2"]), join_cols=["id"])
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after.snapshot_id)
    pre = result.update_preimages()
    assert pre.num_rows >= 1
    assert all(ct == "update_preimage" for ct in pre.column("change_type").to_pylist())


def test_changeset_update_postimages(table: Table) -> None:
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(_make_arrow_table([1], ["alice_v2"]), join_cols=["id"])
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after.snapshot_id)
    post = result.update_postimages()
    assert post.num_rows >= 1
    assert all(ct == "update_postimage" for ct in post.column("change_type").to_pylist())


def test_changeset_summary(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    table.delete("id = 1")

    result = table.table_changes(start)
    summary = result.summary()
    assert isinstance(summary, dict)
    assert "insert" in summary
    assert "delete" in summary
    assert summary["insert"] >= 1
    assert summary["delete"] >= 1


# ---------------------------------------------------------------------------
# 22-26: Update correlation
# ---------------------------------------------------------------------------


def test_changeset_updates_paired(table: Table) -> None:
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(_make_arrow_table([1], ["alice_v2"]), join_cols=["id"])
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after.snapshot_id)
    pairs = result.updates()
    assert len(pairs) >= 1
    for pre, post in pairs:
        assert pre["rowid"] == post["rowid"]


def test_changeset_updates_multiple(table: Table) -> None:
    table.append(_make_arrow_table([1, 2, 3], ["alice", "bob", "carol"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(
        _make_arrow_table([1, 2], ["alice_v2", "bob_v2"]),
        join_cols=["id"],
    )
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after.snapshot_id)
    pairs = result.updates()
    assert len(pairs) >= 2
    for pre, post in pairs:
        assert pre["rowid"] == post["rowid"]


def test_changeset_updates_empty(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))

    result = table.table_changes(start)
    pairs = result.updates()
    assert pairs == []


def test_changeset_has_updates_true(table: Table) -> None:
    table.append(_make_arrow_table([1], ["alice"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(_make_arrow_table([1], ["alice_v2"]), join_cols=["id"])
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after.snapshot_id)
    assert result.has_updates() is True


def test_changeset_has_updates_false(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))

    result = table.table_changes(start)
    assert result.has_updates() is False


# ---------------------------------------------------------------------------
# 27-30: ChangeSet on insertions/deletions (no change_type col)
# ---------------------------------------------------------------------------


def test_insertions_changeset_inserts_raises(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))

    result = table.table_insertions(start)
    with pytest.raises(ValueError, match="no change_type column"):
        result.inserts()


def test_changeset_num_rows(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1, 2, 3], ["a", "b", "c"]))

    result = table.table_insertions(start)
    assert result.num_rows == 3


def test_changeset_column_names(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))

    # table_insertions returns only data columns (no metadata cols)
    result = table.table_insertions(start)
    cols = result.column_names
    assert "id" in cols
    assert "name" in cols

    # table_changes returns metadata cols + data cols
    result2 = table.table_changes(start)
    cols2 = result2.column_names
    assert "snapshot_id" in cols2
    assert "rowid" in cols2
    assert "change_type" in cols2
    assert "id" in cols2
    assert "name" in cols2


def test_changeset_to_pandas(table: Table) -> None:
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))

    result = table.table_insertions(start)
    df = result.to_pandas()
    import pandas as pd  # type: ignore[import-untyped]

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


# ---------------------------------------------------------------------------
# 31-35: Edge cases
# ---------------------------------------------------------------------------


def test_cdc_empty_range(table: Table) -> None:
    """Deletions between two insert-only snapshots returns empty."""
    table.append(_make_arrow_table([1], ["alice"]))
    snap_after_first = table.current_snapshot()
    assert snap_after_first is not None

    table.append(_make_arrow_table([2], ["bob"]))
    snap_after_second = table.current_snapshot()
    assert snap_after_second is not None

    result = table.table_deletions(snap_after_first.snapshot_id, snap_after_second.snapshot_id)
    assert result.num_rows == 0


def test_cdc_across_many_snapshots(table: Table) -> None:
    """5+ snapshots; CDC range spans all."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    for i in range(6):
        table.append(_make_arrow_table([i * 10 + j for j in range(3)], [f"n{i}_{j}" for j in range(3)]))

    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_insertions(start, snap_after.snapshot_id)
    assert result.num_rows == 18  # 6 * 3


def test_cdc_with_null_values(table: Table) -> None:
    """Insert rows with NULLs, delete them; CDC captures NULLs."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    table.append(_make_arrow_table([1, 2], [None, "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    inserts = table.table_insertions(start, snap_after_insert.snapshot_id)
    names = inserts.to_arrow().column("name").to_pylist()
    assert None in names

    table.delete("id = 1")
    deletions = table.table_deletions(snap_after_insert.snapshot_id)
    del_names = deletions.to_arrow().column("name").to_pylist()
    assert None in del_names


def test_cdc_after_upsert(table: Table) -> None:
    """Upsert produces update pre/post images in changes."""
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(_make_arrow_table([1, 3], ["alice_v2", "carol"]), join_cols=["id"])
    snap_after_upsert = table.current_snapshot()
    assert snap_after_upsert is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after_upsert.snapshot_id)
    assert result.num_rows >= 1
    change_types = set(result.to_arrow().column("change_type").to_pylist())
    assert "insert" in change_types


@pytest.mark.xfail(
    reason="CURRENT_TIMESTAMP not supported as ducklake table function parameter — source bug",
    strict=True,
)
def test_table_changes_end_time_none(table: Table) -> None:
    """Call table_changes(start_time=ts) without end_time; defaults to current."""
    snap_start = table.current_snapshot()
    assert snap_start is not None
    start_time = snap_start.timestamp

    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))

    result = table.table_changes(start_time=start_time)
    assert result.num_rows >= 2
    assert "change_type" in result.column_names


def test_table_changes_end_snapshot_only_raises(table: Table) -> None:
    """Calling table_changes(end_snapshot=5) without start raises ValueError."""
    with pytest.raises(ValueError, match="start_snapshot is required"):
        table.table_changes(end_snapshot=5)


def test_cdc_on_empty_table(table: Table) -> None:
    """CDC query on table with no data returns empty ChangeSet."""
    snap = table.current_snapshot()
    start = snap.snapshot_id if snap else 0
    result = table.table_changes(start)
    assert result.num_rows == 0


def test_changeset_updates_orphan_postimages(table: Table) -> None:
    """Postimage with no matching preimage is excluded from updates() pairs."""
    table.append(_make_arrow_table([1, 2], ["alice", "bob"]))
    snap_after_insert = table.current_snapshot()
    assert snap_after_insert is not None

    table.upsert(_make_arrow_table([1], ["alice_v2"]), join_cols=["id"])
    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(snap_after_insert.snapshot_id, snap_after.snapshot_id)
    pairs = result.updates()
    # Every pair must have matching rowids; no orphan postimages
    for pre, post in pairs:
        assert pre["rowid"] == post["rowid"]


def test_cdc_single_row_lifecycle(table: Table) -> None:
    """Insert, update, delete same row across snapshots."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    # Insert
    table.append(_make_arrow_table([42], ["the_answer"]))

    # Update
    table.upsert(_make_arrow_table([42], ["the_answer_v2"]), join_cols=["id"])

    # Delete
    table.delete("id = 42")

    snap_after = table.current_snapshot()
    assert snap_after is not None

    result = table.table_changes(start, snap_after.snapshot_id)
    change_types = set(result.to_arrow().column("change_type").to_pylist())
    assert "insert" in change_types
    assert "delete" in change_types


# ---------------------------------------------------------------------------
# SQL injection: catalog name with special chars in CDC
# ---------------------------------------------------------------------------


def test_cdc_catalog_name_with_special_chars(tmp_path: Path) -> None:
    """Catalog name with quotes doesn't break CDC queries."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)
    cat = Catalog("cat'quote", meta_db, data_path=data_dir)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )
    tbl = cat.create_table("cdc_special", schema)

    snap_before = tbl.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    tbl.append(_make_arrow_table([1], ["alice"]))

    result = tbl.table_insertions(start)
    assert result.num_rows == 1


def test_table_changes_end_time_only_raises(table: Table) -> None:
    """Calling table_changes(end_time=ts) without start_time raises ValueError."""
    with pytest.raises(ValueError, match="start_time is required"):
        table.table_changes(end_time=datetime.now(tz=timezone.utc))


def test_changeset_summary_on_insertions_raises(table: Table) -> None:
    """summary() on a ChangeSet from table_insertions() raises ValueError."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    with pytest.raises(ValueError, match="no change_type column"):
        result.summary()


def test_changeset_has_updates_on_insertions_raises(table: Table) -> None:
    """has_updates() on a ChangeSet from table_insertions() raises ValueError."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    with pytest.raises(ValueError, match="no change_type column"):
        result.has_updates()


def test_changeset_deletes_on_insertions_raises(table: Table) -> None:
    """deletes() on a ChangeSet from table_insertions() raises ValueError."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    with pytest.raises(ValueError, match="no change_type column"):
        result.deletes()


def test_changeset_to_pandas_not_installed(table: Table) -> None:
    """to_pandas() raises ImportError when pandas is not importable."""
    from unittest.mock import patch

    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)

    with patch("pyducklake.cdc.importlib.util.find_spec", return_value=None):
        with pytest.raises(ImportError, match="pandas is required"):
            result.to_pandas()


def test_changeset_repr(table: Table) -> None:
    """repr(ChangeSet) contains expected info."""
    snap_before = table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0
    table.append(_make_arrow_table([1], ["alice"]))
    result = table.table_insertions(start)
    r = repr(result)
    assert "ChangeSet" in r
    assert "num_rows=1" in r
