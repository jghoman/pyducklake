"""Tests for opt-in DuckDB append profiling."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake import Catalog, Schema
from pyducklake.profiling import AppendProfiler
from pyducklake.types import IntegerType, NestedField


def _catalog(tmp_path: Path, *, properties: dict[str, str] | None = None) -> Catalog:
    return Catalog(
        "profile_catalog",
        str(tmp_path / "metadata.duckdb"),
        data_path=str(tmp_path / "data"),
        properties=properties,
    )


def _table(catalog: Catalog):
    return catalog.create_table(
        "events",
        Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True)),
    )


@pytest.mark.duckdb15
def test_append_profiling_logs_a_redacted_insert_profile(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    catalog = _catalog(tmp_path, properties={"pyducklake_profile_append": "true"})
    table = _table(catalog)

    with caplog.at_level(logging.INFO, logger="pyducklake.profiling"):
        table.append(pa.table({"id": [1, 2, 3]}))

    records = [record for record in caplog.records if record.name == "pyducklake.profiling"]
    assert len(records) == 1
    assert records[0].message.startswith("pyducklake_append_profile ")

    event = json.loads(records[0].message.removeprefix("pyducklake_append_profile "))
    assert event["catalog"] == "profile_catalog"
    assert event["table"] == "main.events"
    assert event["profile"]["latency"] >= 0
    assert "query_name" not in json.dumps(event)
    assert "extra_info" not in json.dumps(event)


@pytest.mark.duckdb15
def test_append_profiling_is_off_by_default(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    table = _table(_catalog(tmp_path))

    with caplog.at_level(logging.INFO, logger="pyducklake.profiling"):
        table.append(pa.table({"id": [1]}))

    assert not [record for record in caplog.records if record.name == "pyducklake.profiling"]


def test_append_profiling_property_must_be_boolean(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="pyducklake_profile_append"):
        _catalog(tmp_path, properties={"pyducklake_profile_append": "sometimes"})


@pytest.mark.parametrize("key", ["enable_profiling", "ENABLE_PROFILING", "profile_output", "CUSTOM_PROFILING_SETTINGS"])
def test_append_profiling_rejects_conflicting_duckdb_settings(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        _catalog(
            tmp_path,
            properties={
                "pyducklake_profile_append": "true",
                key: "json",
            },
        )


def test_append_profile_capture_failure_does_not_raise() -> None:
    class BrokenProfileConnection:
        def __init__(self) -> None:
            self.executed: list[str] = []

        def get_profiling_information(self, format: str) -> str:
            raise RuntimeError("profile unavailable")

        def execute(self, sql: str) -> None:
            self.executed.append(sql)

    connection = BrokenProfileConnection()
    AppendProfiler(enabled=True).capture(connection, catalog="catalog", table="main.events")  # type: ignore[arg-type]

    assert connection.executed == []


def test_append_profile_partial_setup_is_disabled() -> None:
    class PartiallyBrokenConnection:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.coverage_calls = 0

        def execute(self, sql: str) -> None:
            self.executed.append(sql)
            if sql == "SET profiling_coverage = 'ALL'":
                self.coverage_calls += 1
                raise RuntimeError("coverage unavailable")

    connection = PartiallyBrokenConnection()
    assert not AppendProfiler(enabled=True).start(connection)  # type: ignore[arg-type]
    assert connection.executed == [
        "SET enable_profiling = 'no_output'",
        "SET profiling_coverage = 'ALL'",
        "PRAGMA disable_profiling",
        "SET profiling_coverage = 'SELECT'",
    ]


def test_append_profile_failed_initial_setup_does_not_reset_existing_state() -> None:
    class InitiallyBrokenConnection:
        def __init__(self) -> None:
            self.executed: list[str] = []

        def execute(self, sql: str) -> None:
            self.executed.append(sql)
            raise RuntimeError("profiling unavailable")

    connection = InitiallyBrokenConnection()
    assert not AppendProfiler(enabled=True).start(connection)  # type: ignore[arg-type]
    assert connection.executed == ["SET enable_profiling = 'no_output'"]


@pytest.mark.duckdb15
def test_failed_append_does_not_log_a_success_profile(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    catalog = _catalog(tmp_path, properties={"pyducklake_profile_append": "true"})
    table = _table(catalog)
    bad_table = pa.table({"id": ["not-an-integer"]})

    with caplog.at_level(logging.INFO, logger="pyducklake.profiling"):
        with pytest.raises(Exception):
            table.append(bad_table)

    assert not [record for record in caplog.records if record.message.startswith("pyducklake_append_profile ")]
    assert catalog.connection.execute("SELECT current_setting('enable_profiling')").fetchone() == (None,)
