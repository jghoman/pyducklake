"""Tests for pyducklake.sorting."""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema, Table
from pyducklake.sorting import (
    UNSORTED,
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyducklake.types import (
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

pytestmark = pytest.mark.duckdb15


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
    return catalog.create_table("sort_tbl", schema)


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_sort_direction_values() -> None:
    assert SortDirection.ASC.value == "ASC"
    assert SortDirection.DESC.value == "DESC"


def test_null_order_values() -> None:
    assert NullOrder.NULLS_FIRST.value == "NULLS FIRST"
    assert NullOrder.NULLS_LAST.value == "NULLS LAST"


def test_sort_field_defaults() -> None:
    f = SortField(source_column="col")
    assert f.direction == SortDirection.ASC
    assert f.null_order == NullOrder.NULLS_LAST


def test_sort_field_to_sql_asc() -> None:
    f = SortField(source_column="col")
    assert f.to_sql() == '"col" ASC NULLS LAST'


def test_sort_field_to_sql_desc_nulls_first() -> None:
    f = SortField(
        source_column="col",
        direction=SortDirection.DESC,
        null_order=NullOrder.NULLS_FIRST,
    )
    assert f.to_sql() == '"col" DESC NULLS FIRST'


def test_sort_field_equality() -> None:
    f1 = SortField(source_column="col", direction=SortDirection.ASC)
    f2 = SortField(source_column="col", direction=SortDirection.ASC)
    assert f1 == f2


def test_sort_field_inequality() -> None:
    f1 = SortField(source_column="col", direction=SortDirection.ASC)
    f2 = SortField(source_column="col", direction=SortDirection.DESC)
    f3 = SortField(source_column="other", direction=SortDirection.ASC)
    assert f1 != f2
    assert f1 != f3


def test_sort_order_unsorted() -> None:
    assert UNSORTED.is_unsorted is True


def test_sort_order_with_fields() -> None:
    order = SortOrder(fields=(SortField(source_column="col"),))
    assert order.is_unsorted is False


def test_sort_order_equality() -> None:
    o1 = SortOrder(fields=(SortField(source_column="col"),))
    o2 = SortOrder(fields=(SortField(source_column="col"),))
    assert o1 == o2


def test_sort_order_repr() -> None:
    r = repr(UNSORTED)
    assert "UNSORTED" in r

    order = SortOrder(fields=(SortField(source_column="col"),))
    r2 = repr(order)
    assert "col" in r2
    assert "ASC" in r2


# ---------------------------------------------------------------------------
# Integration tests (DuckDB)
# ---------------------------------------------------------------------------


def test_update_sort_order_single_field(table: Table) -> None:
    table.update_sort_order().add_field("name").commit()
    order = table.sort_order
    assert not order.is_unsorted
    assert len(order.fields) == 1
    assert order.fields[0].source_column == "name"
    assert order.fields[0].direction == SortDirection.ASC
    assert order.fields[0].null_order == NullOrder.NULLS_LAST


def test_update_sort_order_multiple_fields(table: Table) -> None:
    table.update_sort_order().add_field("name").add_field("region").commit()
    order = table.sort_order
    assert not order.is_unsorted
    assert len(order.fields) == 2
    assert order.fields[0].source_column == "name"
    assert order.fields[1].source_column == "region"


def test_update_sort_order_desc(table: Table) -> None:
    table.update_sort_order().add_field("name", direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST).commit()
    order = table.sort_order
    assert not order.is_unsorted
    assert order.fields[0].direction == SortDirection.DESC
    assert order.fields[0].null_order == NullOrder.NULLS_FIRST


def test_update_sort_order_clear(table: Table) -> None:
    table.update_sort_order().add_field("name").commit()
    assert not table.sort_order.is_unsorted

    table.update_sort_order().clear().commit()
    assert table.sort_order.is_unsorted


def test_update_sort_order_chaining(table: Table) -> None:
    builder = table.update_sort_order()
    ret = builder.add_field("name").add_field("region")
    assert ret is builder


def test_update_sort_order_context_manager(table: Table) -> None:
    with table.update_sort_order() as sort:
        sort.add_field("name")

    order = table.sort_order
    assert not order.is_unsorted
    assert order.fields[0].source_column == "name"


def test_sort_order_property_default(table: Table) -> None:
    order = table.sort_order
    assert order == UNSORTED
    assert order.is_unsorted


def test_sort_order_property_after_set(table: Table) -> None:
    table.update_sort_order().add_field("name").commit()
    order = table.sort_order
    assert not order.is_unsorted
    assert order.fields[0].source_column == "name"


def test_sort_order_property_after_clear(table: Table) -> None:
    table.update_sort_order().add_field("name").commit()
    assert not table.sort_order.is_unsorted

    table.update_sort_order().clear().commit()
    assert table.sort_order == UNSORTED


def test_sorted_table_write_and_read(table: Table) -> None:
    table.update_sort_order().add_field("name").commit()

    df = pa.table(
        {
            "id": pa.array([3, 1, 2], type=pa.int32()),
            "name": pa.array(["carol", "alice", "bob"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 3, 10), date(2024, 1, 15), date(2024, 2, 20)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    datetime(2024, 3, 10, 14, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    assert result.num_rows == 3
    assert set(result.column("name").to_pylist()) == {"alice", "bob", "carol"}


def test_sort_order_nonexistent_column(table: Table) -> None:
    """Setting sort on a column that doesn't exist in the table."""
    try:
        table.update_sort_order().add_field("nonexistent_col").commit()
    except Exception:
        pass  # expected: ducklake rejects nonexistent column
    else:
        pytest.xfail("Ducklake does not validate column existence in SET SORTED BY")


def test_sort_order_replace(table: Table) -> None:
    """Setting a new sort order replaces the previous one."""
    table.update_sort_order().add_field("name").commit()
    assert table.sort_order.fields[0].source_column == "name"

    table.update_sort_order().add_field("region", SortDirection.DESC).commit()
    order = table.sort_order
    assert len(order.fields) == 1
    assert order.fields[0].source_column == "region"
    assert order.fields[0].direction == SortDirection.DESC


def test_context_manager_exception_no_commit(table: Table) -> None:
    with pytest.raises(ValueError):
        with table.update_sort_order() as sort:
            sort.add_field("name")
            raise ValueError("abort")

    assert table.sort_order.is_unsorted


# ---------------------------------------------------------------------------
# Sorted write tests
# ---------------------------------------------------------------------------


def test_sorted_write_order_applied(table: Table) -> None:
    """Set sort order ASC, write unsorted data, read back and verify sorted."""
    table.update_sort_order().add_field("name").commit()

    df = pa.table(
        {
            "id": pa.array([3, 1, 2], type=pa.int32()),
            "name": pa.array(["carol", "alice", "bob"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 3, 10), date(2024, 1, 15), date(2024, 2, 20)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    datetime(2024, 3, 10, 14, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    names = result.column("name").to_pylist()
    assert names == ["alice", "bob", "carol"]


def test_sorted_write_desc(table: Table) -> None:
    """Set sort order DESC, verify descending order."""
    table.update_sort_order().add_field("name", direction=SortDirection.DESC).commit()

    df = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alice", "carol", "bob"], type=pa.string()),
            "region": pa.array(["eu", "us", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 1, 15), date(2024, 3, 10), date(2024, 2, 20)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 3, 10, 14, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    names = result.column("name").to_pylist()
    assert names == ["carol", "bob", "alice"]


def test_sorted_write_multiple_fields(table: Table) -> None:
    """Sort by region ASC, name DESC."""
    table.update_sort_order().add_field("region").add_field("name", direction=SortDirection.DESC).commit()

    df = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], type=pa.int32()),
            "name": pa.array(["bob", "alice", "carol", "dave"], type=pa.string()),
            "region": pa.array(["us", "eu", "eu", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 1, 1)] * 4,
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [datetime(2024, 1, 1)] * 4,
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    regions = result.column("region").to_pylist()
    names = result.column("name").to_pylist()
    # eu before us (ASC), within eu: carol before alice (DESC)
    assert regions == ["eu", "eu", "us", "us"]
    assert names == ["carol", "alice", "dave", "bob"]


def test_sorted_write_nulls_ordering(table: Table) -> None:
    """NULLS FIRST vs NULLS LAST."""
    table.update_sort_order().add_field("name", null_order=NullOrder.NULLS_FIRST).commit()

    df = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["bob", None, "alice"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 1, 1)] * 3,
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [datetime(2024, 1, 1)] * 3,
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    names = result.column("name").to_pylist()
    assert names[0] is None
    assert names[1:] == ["alice", "bob"]


def test_unsorted_table_write_preserves_order(table: Table) -> None:
    """No sort order = insertion order preserved."""
    df = pa.table(
        {
            "id": pa.array([3, 1, 2], type=pa.int32()),
            "name": pa.array(["carol", "alice", "bob"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 3, 10), date(2024, 1, 15), date(2024, 2, 20)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    datetime(2024, 3, 10, 14, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    table.append(df)

    result = table.scan().to_arrow()
    names = result.column("name").to_pylist()
    assert names == ["carol", "alice", "bob"]


def test_sorted_overwrite_applies_sort(table: Table) -> None:
    """Overwrite respects sort order."""
    table.update_sort_order().add_field("name").commit()

    initial = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["zebra"], type=pa.string()),
            "region": pa.array(["us"], type=pa.string()),
            "event_date": pa.array([date(2024, 1, 1)], type=pa.date32()),
            "event_ts": pa.array([datetime(2024, 1, 1)], type=pa.timestamp("us")),
        }
    )
    table.append(initial)

    replacement = pa.table(
        {
            "id": pa.array([2, 3], type=pa.int32()),
            "name": pa.array(["carol", "alice"], type=pa.string()),
            "region": pa.array(["eu", "us"], type=pa.string()),
            "event_date": pa.array([date(2024, 2, 1), date(2024, 3, 1)], type=pa.date32()),
            "event_ts": pa.array([datetime(2024, 2, 1), datetime(2024, 3, 1)], type=pa.timestamp("us")),
        }
    )
    table.overwrite(replacement)

    result = table.scan().to_arrow()
    names = result.column("name").to_pylist()
    assert names == ["alice", "carol"]


def test_sorted_append_batches(table: Table) -> None:
    """append_batches respects sort order."""
    table.update_sort_order().add_field("name").commit()

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("region", pa.string()),
            pa.field("event_date", pa.date32()),
            pa.field("event_ts", pa.timestamp("us")),
        ]
    )
    batch = pa.record_batch(
        {
            "id": pa.array([3, 1, 2], type=pa.int32()),
            "name": pa.array(["carol", "alice", "bob"], type=pa.string()),
            "region": pa.array(["us", "eu", "us"], type=pa.string()),
            "event_date": pa.array(
                [date(2024, 3, 10), date(2024, 1, 15), date(2024, 2, 20)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    datetime(2024, 3, 10, 14, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 2, 20, 12, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
        },
        schema=arrow_schema,
    )
    reader = pa.RecordBatchReader.from_batches(arrow_schema, [batch])
    table.append_batches(reader)

    result = table.scan().to_arrow()
    names = result.column("name").to_pylist()
    assert names == ["alice", "bob", "carol"]
