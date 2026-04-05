"""Tests for pyducklake.table.Table."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pyducklake import Catalog, Schema, Snapshot, Table
from pyducklake.types import (
    BooleanType,
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
def simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="active", field_type=BooleanType()),
    )


@pytest.fixture()
def table(catalog: Catalog, simple_schema: Schema) -> Table:
    return catalog.create_table("test_tbl", simple_schema)


# -- Properties --------------------------------------------------------------


def test_table_name(table: Table) -> None:
    assert table.name == "test_tbl"


def test_table_namespace(table: Table) -> None:
    assert table.namespace == "main"


def test_table_identifier(table: Table) -> None:
    assert table.identifier == ("main", "test_tbl")


def test_table_fully_qualified_name(table: Table) -> None:
    fqn = table.fully_qualified_name
    assert "test_cat" in fqn
    assert "main" in fqn
    assert "test_tbl" in fqn


def test_table_schema(table: Table) -> None:
    assert table.schema.column_names() == ["id", "name", "active"]


# -- Snapshots ---------------------------------------------------------------


def test_current_snapshot_empty_table(table: Table) -> None:
    snapshot = table.current_snapshot()
    # A newly created table with no data may have no snapshots,
    # or may have an initial snapshot depending on ducklake version.
    # We just verify it doesn't crash and returns Snapshot or None.
    assert snapshot is None or isinstance(snapshot, Snapshot)


def test_current_snapshot_after_insert(catalog: Catalog, table: Table) -> None:
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice', true)")
    snapshot = table.current_snapshot()
    # After insert, there should be at least one snapshot
    assert snapshot is not None
    assert isinstance(snapshot, Snapshot)
    assert snapshot.snapshot_id >= 0


def test_snapshots_list(catalog: Catalog, table: Table) -> None:
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'alice', true)")
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (2, 'bob', false)")
    snapshots = table.snapshots()
    assert len(snapshots) >= 1
    for s in snapshots:
        assert isinstance(s, Snapshot)


# -- Refresh -----------------------------------------------------------------


def test_table_refresh(catalog: Catalog, simple_schema: Schema) -> None:
    table = catalog.create_table("refresh_tbl", simple_schema)
    orig_cols = table.schema.column_names()
    assert "extra" not in orig_cols

    # Add column via raw SQL
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"ALTER TABLE {fqn} ADD COLUMN extra INTEGER")
    table.refresh()
    assert "extra" in table.schema.column_names()


# -- Equality ----------------------------------------------------------------


def test_table_equality(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("eq_table", simple_schema)
    t1 = catalog.load_table("eq_table")
    t2 = catalog.load_table("eq_table")
    assert t1 == t2


def test_table_inequality(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_table("tbl_a", simple_schema)
    catalog.create_table("tbl_b", simple_schema)
    t1 = catalog.load_table("tbl_a")
    t2 = catalog.load_table("tbl_b")
    assert t1 != t2


# -- Repr --------------------------------------------------------------------


def test_table_repr(table: Table) -> None:
    r = repr(table)
    assert "Table" in r
    assert "test_tbl" in r


# -- P1 tests ----------------------------------------------------------------


def test_table_eq_non_table_returns_not_implemented(table: Table) -> None:
    result = table.__eq__("string")
    assert result is NotImplemented
    # Also verify via != operator
    assert table != "string"


def test_table_catalog_property(catalog: Catalog, table: Table) -> None:
    assert table.catalog is catalog


def test_table_current_snapshot_none_for_new_table(table: Table) -> None:
    """current_snapshot() may return None when no data snapshots exist."""
    snap = table.current_snapshot()
    # May be None or Snapshot depending on ducklake version
    assert snap is None or isinstance(snap, Snapshot)


def test_table_snapshots_empty_table(table: Table) -> None:
    snaps = table.snapshots()
    assert isinstance(snaps, list)


def test_table_equality_different_namespace(catalog: Catalog, simple_schema: Schema) -> None:
    catalog.create_namespace("ns_eq")
    catalog.create_table(("ns_eq", "same_name"), simple_schema)
    catalog.create_table("same_name", simple_schema)
    t1 = catalog.load_table(("ns_eq", "same_name"))
    t2 = catalog.load_table("same_name")
    assert t1 != t2


# -- P1: Snapshot dataclass fields -------------------------------------------


def test_snapshot_fields() -> None:
    """Verify all Snapshot fields are accessible and defaults are correct."""
    from datetime import datetime, timezone

    ts = datetime.now(tz=timezone.utc)
    s = Snapshot(
        snapshot_id=42,
        timestamp=ts,
        schema_version=3,
        changes="insert",
        author="tester",
        commit_message="test msg",
    )
    assert s.snapshot_id == 42
    assert s.timestamp == ts
    assert s.schema_version == 3
    assert s.changes == "insert"
    assert s.author == "tester"
    assert s.commit_message == "test msg"


def test_snapshot_defaults() -> None:
    from datetime import datetime, timezone

    ts = datetime.now(tz=timezone.utc)
    s = Snapshot(snapshot_id=1, timestamp=ts)
    assert s.schema_version is None
    assert s.changes is None
    assert s.author is None
    assert s.commit_message is None


def test_snapshot_immutable() -> None:
    from datetime import datetime, timezone

    ts = datetime.now(tz=timezone.utc)
    s = Snapshot(snapshot_id=1, timestamp=ts)
    with pytest.raises(AttributeError):
        s.snapshot_id = 2  # type: ignore[misc]


def test_snapshots_docstring_accuracy(catalog: Catalog, simple_schema: Schema) -> None:
    """Verify snapshots() returns catalog-level snapshots and docstring is accurate."""
    table = catalog.create_table("doc_tbl", simple_schema)
    fqn = table.fully_qualified_name
    catalog.connection.execute(f"INSERT INTO {fqn} VALUES (1, 'a', true)")

    snapshots = table.snapshots()
    assert isinstance(snapshots, list)
    assert len(snapshots) >= 1
    # Verify the docstring mentions catalog-level semantics
    assert "catalog" in (table.snapshots.__doc__ or "").lower()
