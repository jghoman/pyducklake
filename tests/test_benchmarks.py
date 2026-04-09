"""Benchmarks for core pyducklake operations using pytest-benchmark."""

from __future__ import annotations

import os
import tempfile

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField, StringType

_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType()),
    NestedField(field_id=3, name="value", field_type=IntegerType()),
)


def _fresh_catalog() -> Catalog:
    tmp = tempfile.mkdtemp()
    meta_db = os.path.join(tmp, "meta.duckdb")
    data_dir = os.path.join(tmp, "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog("bench", meta_db, data_path=data_dir)


def _make_data(n: int) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int32()),
            "name": pa.array([f"name_{i}" for i in range(n)], type=pa.string()),
            "value": pa.array([i * 10 for i in range(n)], type=pa.int32()),
        }
    )


@pytest.fixture()
def populated_table():  # type: ignore[no-untyped-def]
    cat = _fresh_catalog()
    table = cat.create_table("t", _SCHEMA)
    table.append(_make_data(1000))
    return table


# -- Write benchmarks -------------------------------------------------------


def test_benchmark_append_100(benchmark):  # type: ignore[no-untyped-def]
    cat = _fresh_catalog()
    table = cat.create_table("t", _SCHEMA)
    data = _make_data(100)
    benchmark(table.append, data)


def test_benchmark_append_1000(benchmark):  # type: ignore[no-untyped-def]
    cat = _fresh_catalog()
    table = cat.create_table("t", _SCHEMA)
    data = _make_data(1000)
    benchmark(table.append, data)


def test_benchmark_upsert_100(benchmark):  # type: ignore[no-untyped-def]
    cat = _fresh_catalog()
    table = cat.create_table("t", _SCHEMA)
    table.append(_make_data(100))
    upsert_data = _make_data(100)
    benchmark(table.upsert, upsert_data, ["id"])


# -- Read benchmarks --------------------------------------------------------


def test_benchmark_scan_to_arrow_1000(benchmark, populated_table):  # type: ignore[no-untyped-def]
    benchmark(lambda: populated_table.scan().to_arrow())


def test_benchmark_scan_with_filter_1000(benchmark, populated_table):  # type: ignore[no-untyped-def]
    benchmark(lambda: populated_table.scan('"value" > 5000').to_arrow())


def test_benchmark_scan_count_1000(benchmark, populated_table):  # type: ignore[no-untyped-def]
    benchmark(lambda: populated_table.scan().count())


def test_benchmark_scan_select_1000(benchmark, populated_table):  # type: ignore[no-untyped-def]
    benchmark(lambda: populated_table.scan().select("id", "name").to_arrow())
