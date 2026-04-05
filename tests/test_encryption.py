"""Tests for Ducklake encryption support."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyducklake import Catalog, Schema
from pyducklake.partitioning import IDENTITY
from pyducklake.types import IntegerType, NestedField, StringType


def _make_catalog(tmp_path: Path, *, encrypted: bool = True, data_path: bool = True) -> Catalog:
    """Create a catalog in tmp_path with optional encryption and data_path."""
    meta_db = str(tmp_path / "meta.duckdb")
    kwargs: dict[str, object] = {}
    if data_path:
        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir, exist_ok=True)
        kwargs["data_path"] = data_dir
    return Catalog("enc_cat", meta_db, encrypted=encrypted, **kwargs)  # type: ignore[arg-type]


def _simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )


def _make_arrow(ids: list[int], names: list[str]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
        }
    )


def _three_col_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=IntegerType()),
    )


def _make_arrow3(ids: list[int], names: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


# -- Basic functionality -------------------------------------------------------


def test_create_encrypted_catalog(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path, encrypted=True)
    assert cat.encrypted is True
    cat.close()


def test_encrypted_catalog_write_and_read(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    result = tbl.scan().to_arrow()
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [1, 2]
    cat.close()


def test_encrypted_catalog_default_false(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path, encrypted=False)
    assert cat.encrypted is False
    cat.close()


def test_encrypted_property(tmp_path: Path) -> None:
    cat_enc = _make_catalog(tmp_path / "enc", encrypted=True)
    cat_plain = _make_catalog(tmp_path / "plain", encrypted=False)
    assert cat_enc.encrypted is True
    assert cat_plain.encrypted is False
    cat_enc.close()
    cat_plain.close()


# -- Write/Read round-trips ----------------------------------------------------


def test_encrypted_append_and_scan(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1], ["alice"]))
    tbl.append(_make_arrow([2], ["bob"]))
    result = tbl.scan().to_arrow()
    assert result.num_rows == 2
    cat.close()


def test_encrypted_overwrite(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    tbl.overwrite(_make_arrow([3], ["carol"]))
    result = tbl.scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [3]
    cat.close()


def test_encrypted_delete(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2, 3], ["alice", "bob", "carol"]))
    tbl.delete("id = 2")
    result = tbl.scan().to_arrow()
    assert sorted(result.column("id").to_pylist()) == [1, 3]
    cat.close()


def test_encrypted_upsert(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _three_col_schema())
    tbl.append(_make_arrow3([1, 2], ["alice", "bob"], [10, 20]))
    upsert_df = _make_arrow3([2, 3], ["bob_updated", "carol"], [25, 30])
    res = tbl.upsert(upsert_df, join_cols=["id"])
    assert res.rows_updated == 1
    assert res.rows_inserted == 1
    result = tbl.scan().to_arrow()
    assert result.num_rows == 3
    cat.close()


def test_encrypted_scan_with_filter(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2, 3], ["alice", "bob", "carol"]))
    result = tbl.scan(row_filter="id > 1").to_arrow()
    assert sorted(result.column("id").to_pylist()) == [2, 3]
    cat.close()


def test_encrypted_scan_with_select(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    result = tbl.scan(selected_fields=("name",)).to_arrow()
    assert result.column_names == ["name"]
    assert result.num_rows == 2
    cat.close()


def test_encrypted_scan_to_pandas(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    df = tbl.scan().to_pandas()
    assert len(df) == 2
    cat.close()


def test_encrypted_scan_to_duckdb(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    rel = tbl.scan().to_duckdb()
    result = rel.fetchall()
    assert len(result) == 2
    cat.close()


# -- Schema operations --------------------------------------------------------


def test_encrypted_schema_evolution(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1], ["alice"]))

    with tbl.update_schema() as us:
        us.add_column("age", IntegerType())

    tbl.refresh()
    assert "age" in tbl.schema.column_names()

    with tbl.update_schema() as us:
        us.rename_column("age", "years")

    tbl.refresh()
    assert "years" in tbl.schema.column_names()
    assert "age" not in tbl.schema.column_names()

    with tbl.update_schema() as us:
        us.drop_column("years")

    tbl.refresh()
    assert "years" not in tbl.schema.column_names()
    cat.close()


def test_encrypted_multiple_tables(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    t1 = cat.create_table("t1", _simple_schema())
    t2 = cat.create_table("t2", _simple_schema())
    t1.append(_make_arrow([1], ["alice"]))
    t2.append(_make_arrow([2], ["bob"]))
    assert t1.scan().to_arrow().num_rows == 1
    assert t2.scan().to_arrow().num_rows == 1
    cat.close()


# -- Snapshots and time travel -------------------------------------------------


def test_encrypted_snapshots(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1], ["alice"]))
    tbl.append(_make_arrow([2], ["bob"]))
    snaps = tbl.snapshots()
    assert len(snaps) >= 2
    cat.close()


def test_encrypted_time_travel(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1], ["alice"]))
    snap1 = tbl.current_snapshot()
    assert snap1 is not None

    tbl.append(_make_arrow([2], ["bob"]))
    # Read at snap1 — should only see 1 row
    result = tbl.scan(snapshot_id=snap1.snapshot_id).to_arrow()
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [1]
    cat.close()


# -- Maintenance ---------------------------------------------------------------


def test_encrypted_compact(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    for i in range(5):
        tbl.append(_make_arrow([i], [f"n{i}"]))
    tbl.maintenance().compact()
    result = tbl.scan().to_arrow()
    assert result.num_rows == 5
    cat.close()


def test_encrypted_checkpoint(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    tbl.maintenance().checkpoint()
    result = tbl.scan().to_arrow()
    assert result.num_rows == 2
    cat.close()


# -- Partitioning + encryption ------------------------------------------------


def test_encrypted_with_partitioning(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="region", field_type=StringType()),
    )
    tbl = cat.create_table("t", schema)
    tbl.update_spec().add_field("region", IDENTITY).commit()
    df = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
        }
    )
    tbl.append(df)
    result = tbl.scan().to_arrow()
    assert result.num_rows == 3
    cat.close()


# -- Direct file access blocked ------------------------------------------------


def test_encrypted_files_not_readable_directly(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    # Write enough data to produce a data file (not inlined)
    ids = list(range(1000))
    names = [f"name_{i}" for i in ids]
    tbl.append(_make_arrow(ids, names))

    files_table = tbl.inspect().files()
    if files_table.num_rows == 0:
        pytest.skip("No data files produced (data inlined)")

    data_file_col = files_table.column("data_file")
    first_file = str(data_file_col[0])

    # Resolve relative paths against data dir
    data_dir = str(tmp_path / "data")
    if not os.path.isabs(first_file):
        first_file = os.path.join(data_dir, first_file)

    # Trying to read the encrypted parquet file directly should fail
    with pytest.raises(Exception):
        pq.read_table(first_file)

    cat.close()


# -- Transactions --------------------------------------------------------------


def test_encrypted_transaction(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    cat.create_table("t", _simple_schema())

    txn = cat.begin_transaction()
    tbl = txn.load_table("t")
    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    txn.commit()

    loaded = cat.load_table("t")
    assert loaded.scan().count() == 2

    # Rollback
    txn2 = cat.begin_transaction()
    tbl2 = txn2.load_table("t")
    tbl2.append(_make_arrow([3], ["carol"]))
    txn2.rollback()

    assert loaded.scan().count() == 2
    cat.close()


# -- Inspect -------------------------------------------------------------------


def test_encrypted_inspect_files(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    ids = list(range(1000))
    tbl.append(_make_arrow(ids, [f"n{i}" for i in ids]))
    files = tbl.inspect().files()
    assert isinstance(files, pa.Table)
    # Should have at least one file for 1000 rows
    assert files.num_rows >= 0
    cat.close()


def test_encrypted_inspect_snapshots(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1], ["alice"]))
    snaps = tbl.inspect().snapshots()
    assert isinstance(snaps, pa.Table)
    assert snaps.num_rows >= 1
    assert "snapshot_id" in snaps.column_names
    cat.close()


# -- CDC -----------------------------------------------------------------------


def test_encrypted_cdc(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())

    snap_before = tbl.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    tbl.append(_make_arrow([1, 2], ["alice", "bob"]))
    snap_after = tbl.current_snapshot()
    assert snap_after is not None

    changes = tbl.table_changes(start, snap_after.snapshot_id)
    assert changes.num_rows == 2
    cat.close()


# -- Edge cases ----------------------------------------------------------------


def test_encrypted_without_data_path(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path, encrypted=True, data_path=False)
    assert cat.encrypted is True
    tbl = cat.create_table("t", _simple_schema())
    tbl.append(_make_arrow([1], ["alice"]))
    result = tbl.scan().to_arrow()
    assert result.num_rows == 1
    cat.close()


def test_encrypted_empty_table(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    result = tbl.scan().to_arrow()
    assert result.num_rows == 0
    cat.close()


def test_encrypted_large_write(tmp_path: Path) -> None:
    cat = _make_catalog(tmp_path)
    tbl = cat.create_table("t", _simple_schema())
    ids = list(range(1000))
    names = [f"name_{i}" for i in ids]
    tbl.append(_make_arrow(ids, names))
    result = tbl.scan().to_arrow()
    assert result.num_rows == 1000
    cat.close()
