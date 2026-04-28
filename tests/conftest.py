"""Shared pytest fixtures for pyducklake tests."""

import duckdb
import pytest

_DUCKDB_VERSION = tuple(int(x) for x in duckdb.__version__.split(".")[:2])


def pytest_collection_modifyitems(config, items):
    if _DUCKDB_VERSION >= (1, 5):
        return
    skip = pytest.mark.skip(reason=f"requires DuckDB >= 1.5 (have {duckdb.__version__})")
    for item in items:
        if "duckdb15" in item.keywords:
            item.add_marker(skip)
