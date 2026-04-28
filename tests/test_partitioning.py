"""Tests for pyducklake.partitioning."""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.partitioning import (
    DAY,
    HOUR,
    IDENTITY,
    MONTH,
    UNPARTITIONED,
    YEAR,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    PartitionField,
    PartitionSpec,
    YearTransform,
)
from pyducklake.types import (
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)


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
        NestedField(field_id=3, name="region", field_type=StringType()),
        NestedField(field_id=4, name="event_date", field_type=DateType()),
        NestedField(field_id=5, name="event_ts", field_type=TimestampType()),
    )
    return catalog.create_table("part_tbl", schema)


# -- PartitionField -----------------------------------------------------------


def test_partition_field_identity() -> None:
    field = PartitionField(source_column="region", transform=IDENTITY)
    assert field.source_column == "region"
    assert field.transform == IDENTITY


def test_partition_field_year() -> None:
    field = PartitionField(source_column="event_date", transform=YEAR)
    assert field.source_column == "event_date"
    assert isinstance(field.transform, YearTransform)


# -- PartitionSpec ------------------------------------------------------------


def test_partition_spec_unpartitioned() -> None:
    spec = UNPARTITIONED
    assert spec.is_unpartitioned is True
    assert spec.fields == ()


def test_partition_spec_single_field() -> None:
    spec = PartitionSpec(PartitionField("region", IDENTITY))
    assert spec.is_unpartitioned is False
    assert len(spec.fields) == 1
    assert spec.fields[0].source_column == "region"


def test_partition_spec_multiple_fields() -> None:
    spec = PartitionSpec(
        PartitionField("event_date", YEAR),
        PartitionField("region", IDENTITY),
    )
    assert len(spec.fields) == 2
    assert spec.fields[0].transform == YEAR
    assert spec.fields[1].transform == IDENTITY


def test_partition_spec_equality() -> None:
    spec1 = PartitionSpec(
        PartitionField("event_date", YEAR),
        PartitionField("region", IDENTITY),
    )
    spec2 = PartitionSpec(
        PartitionField("event_date", YEAR),
        PartitionField("region", IDENTITY),
    )
    assert spec1 == spec2


# -- Transform.to_sql() -------------------------------------------------------


def test_transform_to_sql() -> None:
    assert IdentityTransform().to_sql() == ""
    assert YearTransform().to_sql() == "year"
    assert MonthTransform().to_sql() == "month"
    assert DayTransform().to_sql() == "day"
    assert HourTransform().to_sql() == "hour"


# -- UpdateSpec: identity partition -------------------------------------------


def test_update_spec_add_identity(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert len(spec.fields) == 1
    assert spec.fields[0].source_column == "region"
    assert spec.fields[0].transform == IDENTITY


# -- UpdateSpec: year partition -----------------------------------------------


def test_update_spec_add_year(table: Table) -> None:
    table.update_spec().add_field("event_date", YEAR).commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert spec.fields[0].transform == YEAR


# -- UpdateSpec: clear --------------------------------------------------------


def test_update_spec_clear(table: Table) -> None:
    # First add partitioning
    table.update_spec().add_field("region").commit()
    assert not table.spec.is_unpartitioned

    # Then clear it
    table.update_spec().clear().commit()
    assert table.spec.is_unpartitioned


# -- UpdateSpec: context manager ----------------------------------------------


def test_update_spec_context_manager(table: Table) -> None:
    with table.update_spec() as spec:
        spec.add_field("region")

    result = table.spec
    assert not result.is_unpartitioned
    assert result.fields[0].source_column == "region"


# -- Partitioned write and read -----------------------------------------------


def test_partitioned_write_and_read(table: Table) -> None:
    table.update_spec().add_field("region").commit()

    df = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alice", "bob", "carol"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
            "event_date": pa.array([date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)], type=pa.date32()),
            "event_ts": pa.array(
                [
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                    datetime(2024, 3, 10, 14, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    assert result.num_rows == 3
    # All data should be readable
    assert set(result.column("region").to_pylist()) == {"us", "eu"}


# -- Partition after data already exists --------------------------------------


def test_partition_after_data(table: Table) -> None:
    # Insert data first (unpartitioned)
    df1 = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
            "region": pa.array(["us", "eu"], type=pa.string()),
            "event_date": pa.array([date(2024, 1, 15), date(2024, 2, 20)], type=pa.date32()),
            "event_ts": pa.array(
                [
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df1)

    # Add partitioning after data exists
    table.update_spec().add_field("region").commit()

    # Write more data after partitioning
    df2 = pa.table(
        {
            "id": pa.array([3], type=pa.int32()),
            "name": pa.array(["carol"], type=pa.string()),
            "region": pa.array(["us"], type=pa.string()),
            "event_date": pa.array([date(2024, 3, 10)], type=pa.date32()),
            "event_ts": pa.array(
                [
                    datetime(2024, 3, 10, 14, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df2)

    # All 3 rows should be readable
    result = table.scan().to_arrow()
    assert result.num_rows == 3


# -- Helper to build Arrow tables for partitioned tests -----------------------


def _make_partitioned_df(
    ids: list[int],
    names: list[str],
    regions: list[str],
    dates: list[date],
    timestamps: list[datetime],
) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(names, type=pa.string()),
            "region": pa.array(regions, type=pa.string()),
            "event_date": pa.array(dates, type=pa.date32()),
            "event_ts": pa.array(timestamps, type=pa.timestamp("us")),
        }
    )


# -- UpdateSpec: month partition -----------------------------------------------


def test_update_spec_add_month(table: Table) -> None:
    table.update_spec().add_field("event_date", MONTH).commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert len(spec.fields) == 1
    assert spec.fields[0].source_column == "event_date"
    assert spec.fields[0].transform == MONTH


# -- UpdateSpec: day partition -------------------------------------------------


def test_update_spec_add_day(table: Table) -> None:
    table.update_spec().add_field("event_date", DAY).commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert len(spec.fields) == 1
    assert spec.fields[0].source_column == "event_date"
    assert spec.fields[0].transform == DAY


# -- UpdateSpec: hour partition ------------------------------------------------


def test_update_spec_add_hour(table: Table) -> None:
    table.update_spec().add_field("event_ts", HOUR).commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert len(spec.fields) == 1
    assert spec.fields[0].source_column == "event_ts"
    assert spec.fields[0].transform == HOUR


# -- UpdateSpec: multiple fields in single call --------------------------------


def test_update_spec_multiple_fields(table: Table) -> None:
    table.update_spec().add_field("event_date", YEAR).add_field("region").commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert len(spec.fields) == 2
    assert spec.fields[0].source_column == "event_date"
    assert spec.fields[0].transform == YEAR
    assert spec.fields[1].source_column == "region"
    assert spec.fields[1].transform == IDENTITY


# -- UpdateSpec: replace partitioning (clear + add in same builder) -----------


def test_update_spec_replace_partitioning(table: Table) -> None:
    # Set initial partitioning
    table.update_spec().add_field("region").commit()
    assert table.spec.fields[0].source_column == "region"

    # clear() wipes pending fields, so add_field after clear sets new partitioning
    # But clear() sets _clear=True which takes precedence in commit()
    # So we need two commits: one to clear, one to add new
    table.update_spec().clear().commit()
    assert table.spec.is_unpartitioned

    table.update_spec().add_field("event_date", YEAR).commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert spec.fields[0].source_column == "event_date"
    assert spec.fields[0].transform == YEAR


# -- UpdateSpec: clear on unpartitioned table is a no-op ----------------------


def test_update_spec_clear_unpartitioned_noop(table: Table) -> None:
    assert table.spec.is_unpartitioned
    # Should not raise
    table.update_spec().clear().commit()
    assert table.spec.is_unpartitioned


# -- Table.spec property: default is UNPARTITIONED ----------------------------


def test_table_spec_property_default(table: Table) -> None:
    spec = table.spec
    assert spec == UNPARTITIONED
    assert spec.is_unpartitioned


# -- Table.spec after add ----------------------------------------------------


def test_table_spec_after_add(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    spec = table.spec
    assert not spec.is_unpartitioned
    assert spec.fields[0].source_column == "region"


# -- Table.spec after clear --------------------------------------------------


def test_table_spec_after_clear(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    assert not table.spec.is_unpartitioned
    table.update_spec().clear().commit()
    assert table.spec == UNPARTITIONED


# -- PartitionSpec inequality -------------------------------------------------


def test_partition_spec_inequality() -> None:
    spec_a = PartitionSpec(PartitionField("region", IDENTITY))
    spec_b = PartitionSpec(PartitionField("event_date", YEAR))
    assert spec_a != spec_b


# -- PartitionSpec repr: UNPARTITIONED ----------------------------------------


def test_partition_spec_repr_unpartitioned() -> None:
    r = repr(UNPARTITIONED)
    assert "UNPARTITIONED" in r


# -- PartitionSpec repr: with fields ------------------------------------------


def test_partition_spec_repr_with_fields() -> None:
    spec = PartitionSpec(
        PartitionField("region", IDENTITY),
        PartitionField("event_date", YEAR),
    )
    r = repr(spec)
    assert "region" in r
    assert "year" in r


# -- UpdateSpec chaining returns self -----------------------------------------


def test_update_spec_chaining(table: Table) -> None:
    builder = table.update_spec()
    ret = builder.add_field("region").add_field("event_date", YEAR)
    assert ret is builder


# -- Partitioned delete -------------------------------------------------------


def test_partitioned_delete(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    df = _make_partitioned_df(
        [1, 2, 3],
        ["alice", "bob", "carol"],
        ["us", "eu", "us"],
        [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)],
        [datetime(2024, 1, 15, 10), datetime(2024, 2, 20, 12), datetime(2024, 3, 10, 14)],
    )
    table.append(df)
    table.delete("region = 'eu'")
    result = table.scan().to_arrow()
    assert result.num_rows == 2
    assert set(result.column("region").to_pylist()) == {"us"}


# -- Partitioned upsert -------------------------------------------------------


def test_partitioned_upsert(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    df = _make_partitioned_df(
        [1, 2],
        ["alice", "bob"],
        ["us", "eu"],
        [date(2024, 1, 15), date(2024, 2, 20)],
        [datetime(2024, 1, 15, 10), datetime(2024, 2, 20, 12)],
    )
    table.append(df)

    upsert_df = _make_partitioned_df(
        [2, 3],
        ["bob_updated", "carol"],
        ["eu", "us"],
        [date(2024, 2, 20), date(2024, 3, 10)],
        [datetime(2024, 2, 20, 12), datetime(2024, 3, 10, 14)],
    )
    result = table.upsert(upsert_df, join_cols=["id"])
    assert result.rows_updated == 1
    assert result.rows_inserted == 1

    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 3


# -- Partitioned overwrite ----------------------------------------------------


def test_partitioned_overwrite(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    df = _make_partitioned_df(
        [1, 2, 3],
        ["alice", "bob", "carol"],
        ["us", "eu", "us"],
        [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)],
        [datetime(2024, 1, 15, 10), datetime(2024, 2, 20, 12), datetime(2024, 3, 10, 14)],
    )
    table.append(df)

    new_df = _make_partitioned_df(
        [4],
        ["dave"],
        ["us"],
        [date(2024, 4, 1)],
        [datetime(2024, 4, 1, 8)],
    )
    table.overwrite(new_df, overwrite_filter="region = 'us'")
    result = table.scan().to_arrow()
    ids = sorted(result.column("id").to_pylist())
    # id=2 (eu) kept, id=1,3 (us) deleted, id=4 inserted
    assert ids == [2, 4]


# -- Partitioned scan filter on partition column ------------------------------


def test_partitioned_scan_filter_on_partition_col(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    df = _make_partitioned_df(
        [1, 2, 3],
        ["alice", "bob", "carol"],
        ["us", "eu", "us"],
        [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)],
        [datetime(2024, 1, 15, 10), datetime(2024, 2, 20, 12), datetime(2024, 3, 10, 14)],
    )
    table.append(df)
    result = table.scan(row_filter="region = 'eu'").to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["bob"]


# -- Partitioned time travel --------------------------------------------------


def test_partitioned_time_travel(table: Table) -> None:
    table.update_spec().add_field("region").commit()
    df1 = _make_partitioned_df(
        [1],
        ["alice"],
        ["us"],
        [date(2024, 1, 15)],
        [datetime(2024, 1, 15, 10)],
    )
    table.append(df1)
    snap1 = table.current_snapshot()
    assert snap1 is not None

    df2 = _make_partitioned_df(
        [2],
        ["bob"],
        ["eu"],
        [date(2024, 2, 20)],
        [datetime(2024, 2, 20, 12)],
    )
    table.append(df2)

    # Time travel to snapshot 1: should only have 1 row
    result = table.scan(snapshot_id=snap1.snapshot_id).to_arrow()
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["alice"]


# -- PartitionField equality --------------------------------------------------


def test_partition_field_equality() -> None:
    f1 = PartitionField(source_column="region", transform=IDENTITY)
    f2 = PartitionField(source_column="region", transform=IDENTITY)
    assert f1 == f2


# -- PartitionField inequality ------------------------------------------------


def test_partition_field_inequality() -> None:
    f1 = PartitionField(source_column="region", transform=IDENTITY)
    f2 = PartitionField(source_column="event_date", transform=IDENTITY)
    f3 = PartitionField(source_column="region", transform=YEAR)
    assert f1 != f2
    assert f1 != f3


# -- Transform equality -------------------------------------------------------


def test_transform_equality() -> None:
    assert YEAR == YearTransform()
    assert MONTH == MonthTransform()
    assert DAY == DayTransform()
    assert HOUR == HourTransform()
    assert IDENTITY == IdentityTransform()


# -- Transform inequality -----------------------------------------------------


def test_transform_inequality() -> None:
    assert YEAR != MONTH
    assert DAY != HOUR
    assert IDENTITY != YEAR


# -- Context manager rollback on exception ------------------------------------


def test_context_manager_rollback_on_exception(table: Table) -> None:
    with pytest.raises(ValueError):
        with table.update_spec() as spec:
            spec.add_field("region")
            raise ValueError("abort")

    # Partitioning should not have been applied
    assert table.spec.is_unpartitioned


# -- UpdateSpec: add field for nonexistent column -----------------------------


def test_update_spec_add_replaces_existing(table: Table) -> None:
    """Setting partition to col A, then a new update_spec to col B; B should replace A."""
    table.update_spec().add_field("region").commit()
    spec = table.spec
    assert len(spec.fields) == 1
    assert spec.fields[0].source_column == "region"

    # New update_spec with different column
    table.update_spec().add_field("event_date", YEAR).commit()
    spec2 = table.spec
    assert len(spec2.fields) == 1
    assert spec2.fields[0].source_column == "event_date"
    assert spec2.fields[0].transform == YEAR


@pytest.mark.duckdb15
def test_sorting_plus_partitioning(table: Table) -> None:
    """Setting both sort order and partition spec on the same table works."""
    table.update_spec().add_field("region").commit()
    table.update_sort_order().add_field("name").commit()

    assert not table.spec.is_unpartitioned
    assert table.spec.fields[0].source_column == "region"
    assert not table.sort_order.is_unsorted
    assert table.sort_order.fields[0].source_column == "name"

    df = _make_partitioned_df(
        [3, 1, 2],
        ["carol", "alice", "bob"],
        ["us", "eu", "us"],
        [date(2024, 3, 10), date(2024, 1, 15), date(2024, 2, 20)],
        [datetime(2024, 3, 10, 14), datetime(2024, 1, 15, 10), datetime(2024, 2, 20, 12)],
    )
    table.append(df)

    result = table.scan().to_arrow()
    assert result.num_rows == 3
    assert set(result.column("name").to_pylist()) == {"alice", "bob", "carol"}
    assert set(result.column("region").to_pylist()) == {"us", "eu"}


def test_update_spec_add_field_nonexistent_column(table: Table) -> None:
    with pytest.raises(Exception):
        table.update_spec().add_field("nonexistent_col").commit()
