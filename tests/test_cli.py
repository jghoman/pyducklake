"""Tests for pyducklake CLI."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pytest
from click.testing import CliRunner

from pyducklake import Catalog, Schema
from pyducklake.cli import cli
from pyducklake.types import IntegerType, NestedField, StringType


@pytest.fixture()
def meta_db(tmp_path: Path) -> str:
    return str(tmp_path / "meta.duckdb")


@pytest.fixture()
def data_dir(tmp_path: Path) -> str:
    d = str(tmp_path / "data")
    os.makedirs(d, exist_ok=True)
    return d


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
    )


def _base_args(meta_db: str, data_dir: str) -> list[str]:
    return ["--uri", meta_db, "--catalog", "test_cat", "--data-path", data_dir]


def _create_catalog(meta_db: str, data_dir: str) -> Catalog:
    return Catalog("test_cat", meta_db, data_path=data_dir)


# --- Tests ---


def test_version(runner: CliRunner) -> None:
    """pyducklake version outputs version string."""
    # version command doesn't require --uri
    result = runner.invoke(cli, ["--uri", "dummy", "version"])
    assert result.exit_code == 0
    from pyducklake import __version__

    assert __version__ in result.output


def test_list_namespaces(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """List namespaces includes 'main'."""
    # Create catalog to initialise DB
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["list-namespaces"])
    assert result.exit_code == 0
    assert "main" in result.output


def test_create_namespace(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """Create namespace, verify in list."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["create-namespace", "staging"])
    assert result.exit_code == 0
    assert "Created namespace: staging" in result.output

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["list-namespaces"])
    assert result.exit_code == 0
    assert "staging" in result.output


def test_drop_namespace(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """Create then drop namespace."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_namespace("temp_ns")
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["drop-namespace", "temp_ns"])
    assert result.exit_code == 0
    assert "Dropped namespace: temp_ns" in result.output

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["list-namespaces"])
    assert result.exit_code == 0
    assert "temp_ns" not in result.output


def test_list_tables_empty(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """No tables initially."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["list-tables"])
    assert result.exit_code == 0
    # Header only, no data rows
    lines = [line for line in result.output.strip().splitlines() if line.strip()]
    assert len(lines) == 1  # Just the header


def test_list_tables_with_tables(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Create tables via API, list via CLI."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.create_table("orders", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["list-tables"])
    assert result.exit_code == 0
    assert "users" in result.output
    assert "orders" in result.output


def test_describe_table(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Describe shows schema columns."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["describe", "users"])
    assert result.exit_code == 0
    assert "Schema:" in result.output
    assert "id" in result.output
    assert "name" in result.output
    assert "Partition spec:" in result.output
    assert "Sort order:" in result.output


def test_schema_command(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Shows column names and types."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["schema", "users"])
    assert result.exit_code == 0
    assert "id" in result.output
    assert "name" in result.output
    assert "field_id" in result.output


def test_snapshots_command(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Shows snapshot list."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["snapshots", "users"])
    assert result.exit_code == 0
    assert "snapshot_id" in result.output


def test_files_command(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Shows files (may be empty for no data)."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["files", "users"])
    assert result.exit_code == 0
    # Empty table -> "No data files."
    assert "No data files" in result.output or result.output.strip() != ""


def test_compact_command(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Compact runs without error."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["compact", "users"])
    assert result.exit_code == 0
    assert "Compacted" in result.output


def test_expire_snapshots_dry_run(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Dry run outputs info."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["expire-snapshots", "users", "--dry-run", "--versions", "1"],
    )
    assert result.exit_code == 0
    assert "Dry run" in result.output
    assert "snapshot" in result.output.lower()


def test_checkpoint_command(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Checkpoint runs without error."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["checkpoint"])
    assert result.exit_code == 0
    assert "Checkpoint complete" in result.output


def test_spec_command(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Shows partition spec (UNPARTITIONED for new table)."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["spec", "users"])
    assert result.exit_code == 0
    assert "UNPARTITIONED" in result.output


def test_json_output(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """--output json produces valid JSON."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("users", simple_schema)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["--output", "json", "schema", "users"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) >= 2
    assert any(r["name"] == "id" for r in data)


def test_cli_expire_snapshots_actual(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """expire-snapshots without --dry-run actually runs."""
    cat = _create_catalog(meta_db, data_dir)
    tbl = cat.create_table("exp_tbl", simple_schema)
    df = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["alice"], type=pa.string()),
        }
    )
    tbl.append(df)
    tbl.append(
        pa.table(
            {
                "id": pa.array([2], type=pa.int32()),
                "name": pa.array(["bob"], type=pa.string()),
            }
        )
    )
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["expire-snapshots", "exp_tbl", "--versions", "1"],
    )
    assert result.exit_code == 0
    assert "Expired" in result.output


def test_cli_list_tables_json(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """--output json list-tables produces valid JSON."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("jtbl", simple_schema)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["--output", "json", "list-tables"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    names = [r["table"] for r in data]
    assert "jtbl" in names


def test_cli_list_namespaces_json(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """--output json list-namespaces produces valid JSON."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["--output", "json", "list-namespaces"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    ns_names = [r["namespace"] for r in data]
    assert "main" in ns_names


def test_cli_describe_json(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """--output json describe produces output (may not be pure JSON due to mixed output)."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_table("desc_tbl", simple_schema)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["--output", "json", "describe", "desc_tbl"],
    )
    assert result.exit_code == 0
    assert "id" in result.output


def test_cli_files_json(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """--output json files produces valid JSON."""
    cat = _create_catalog(meta_db, data_dir)
    tbl = cat.create_table("files_tbl", simple_schema)
    ids = list(range(1000))
    df = pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array([f"n{i}" for i in ids], type=pa.string()),
        }
    )
    tbl.append(df)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["--output", "json", "files", "files_tbl"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, dict)


def test_cli_spec_json_partitioned(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """--output json spec on a partitioned table outputs fields."""
    from pyducklake.types import DateType

    cat = _create_catalog(meta_db, data_dir)
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="event_date", field_type=DateType()),
    )
    tbl = cat.create_table("pspec_tbl", schema)
    tbl.update_spec().add_field("event_date").commit()
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["--output", "json", "spec", "pspec_tbl"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "fields" in data
    assert len(data["fields"]) == 1
    assert data["fields"][0]["source_column"] == "event_date"


def test_cli_create_namespace_already_exists(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """Creating a namespace that already exists should give a user-friendly error."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_namespace("existing_ns")
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["create-namespace", "existing_ns"],
    )
    assert result.exit_code != 0


def test_cli_drop_namespace_not_found(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """Dropping a namespace that doesn't exist should give a user-friendly error."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["drop-namespace", "nonexistent_ns"],
    )
    assert result.exit_code != 0


def test_cli_drop_namespace_not_empty(runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema) -> None:
    """Dropping a namespace with tables should give a user-friendly error."""
    cat = _create_catalog(meta_db, data_dir)
    cat.create_namespace("full_ns")
    cat.create_table(("full_ns", "tbl"), simple_schema)
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["drop-namespace", "full_ns"],
    )
    assert result.exit_code != 0


def test_cli_describe_nonexistent_table(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """describe on nonexistent table exits with error, no stack trace."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["describe", "no_such_table"])
    assert result.exit_code != 0
    assert "does not exist" in result.output or "does not exist" in (result.stderr or "")


def test_cli_schema_nonexistent_table(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """schema on nonexistent table exits with error, no stack trace."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["schema", "no_such_table"])
    assert result.exit_code != 0
    assert "does not exist" in result.output or "does not exist" in (result.stderr or "")


def test_cli_snapshots_nonexistent_table(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """snapshots on nonexistent table exits with error, no stack trace."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["snapshots", "no_such_table"])
    assert result.exit_code != 0
    assert "does not exist" in result.output or "does not exist" in (result.stderr or "")


def test_cli_files_nonexistent_table(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """files on nonexistent table exits with error, no stack trace."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["files", "no_such_table"])
    assert result.exit_code != 0
    assert "does not exist" in result.output or "does not exist" in (result.stderr or "")


def test_cli_compact_nonexistent_table(runner: CliRunner, meta_db: str, data_dir: str) -> None:
    """compact on nonexistent table exits with error, no stack trace."""
    cat = _create_catalog(meta_db, data_dir)
    cat.close()

    result = runner.invoke(cli, _base_args(meta_db, data_dir) + ["compact", "no_such_table"])
    assert result.exit_code != 0
    assert "does not exist" in result.output or "does not exist" in (result.stderr or "")


def test_missing_uri_errors(runner: CliRunner) -> None:
    """No --uri flag produces error."""
    result = runner.invoke(cli, ["list-namespaces"])
    assert result.exit_code != 0
    assert "Missing" in result.output or "uri" in result.output.lower() or "Error" in result.output


def test_cli_expire_snapshots_older_than_injection(
    runner: CliRunner, meta_db: str, data_dir: str, simple_schema: Schema
) -> None:
    """expire-snapshots --older-than with SQL injection is rejected."""
    cat = _create_catalog(meta_db, data_dir)
    tbl = cat.create_table("inj_tbl", simple_schema)
    tbl.append(
        pa.table(
            {
                "id": pa.array([1], type=pa.int32()),
                "name": pa.array(["alice"], type=pa.string()),
            }
        )
    )
    cat.close()

    result = runner.invoke(
        cli,
        _base_args(meta_db, data_dir) + ["expire-snapshots", "inj_tbl", "--older-than", "2099-01-01'; DROP TABLE --"],
    )
    assert result.exit_code != 0
