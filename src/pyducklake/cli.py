"""pyducklake CLI — command-line interface for Ducklake catalogs."""

from __future__ import annotations

import json
from typing import Any

import click

from pyducklake import __version__
from pyducklake.catalog import Catalog
from pyducklake.exceptions import NoSuchTableError

__all__ = ["cli"]


def _open_catalog(ctx: click.Context) -> Catalog:
    """Create a Catalog from context options."""
    obj: dict[str, Any] = ctx.obj
    kwargs: dict[str, Any] = {}
    if obj["data_path"] is not None:
        kwargs["data_path"] = obj["data_path"]
    return Catalog(obj["catalog_name"], obj["uri"], **kwargs)


def _output_format(ctx: click.Context) -> str:
    obj: dict[str, Any] = ctx.obj
    fmt: str = obj["output_format"]
    return fmt


def _echo_table(headers: list[str], rows: list[list[str]], ctx: click.Context) -> None:
    """Output tabular data as text or JSON."""
    if _output_format(ctx) == "json":
        records = [dict(zip(headers, row)) for row in rows]
        click.echo(json.dumps(records, indent=2, default=str))
        return

    if not rows:
        # Print headers only
        click.echo("  ".join(headers))
        return

    # Compute column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    # Header
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    click.echo(header_line)

    # Rows
    for row in rows:
        line = "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))
        click.echo(line)


def _resolve_table_identifier(table: str) -> str | tuple[str, str]:
    """Parse 'namespace.table' or just 'table'."""
    parts = table.split(".", 1)
    if len(parts) == 2:
        return (parts[0], parts[1])
    return table


@click.group()
@click.option("--uri", required=True, help="Metadata database URI (e.g., 'meta.duckdb', 'postgres:dbname=mydb')")
@click.option("--catalog", "catalog_name", default="default", help="Catalog name")
@click.option("--data-path", default=None, help="Data file storage path")
@click.option("--output", "output_format", type=click.Choice(["text", "json"]), default="text")
@click.pass_context
def cli(ctx: click.Context, uri: str, catalog_name: str, data_path: str | None, output_format: str) -> None:
    """pyducklake — CLI for Ducklake catalogs."""
    ctx.ensure_object(dict)
    ctx.obj["uri"] = uri
    ctx.obj["catalog_name"] = catalog_name
    ctx.obj["data_path"] = data_path
    ctx.obj["output_format"] = output_format


# --- Namespace commands ---


@cli.command("list-namespaces")
@click.pass_context
def list_namespaces(ctx: click.Context) -> None:
    """List all namespaces."""
    cat = _open_catalog(ctx)
    try:
        namespaces = cat.list_namespaces()
        _echo_table(["namespace"], [[ns] for ns in namespaces], ctx)
    finally:
        cat.close()


@cli.command("create-namespace")
@click.argument("namespace")
@click.pass_context
def create_namespace(ctx: click.Context, namespace: str) -> None:
    """Create a namespace."""
    cat = _open_catalog(ctx)
    try:
        cat.create_namespace(namespace)
        click.echo(f"Created namespace: {namespace}")
    finally:
        cat.close()


@cli.command("drop-namespace")
@click.argument("namespace")
@click.pass_context
def drop_namespace(ctx: click.Context, namespace: str) -> None:
    """Drop a namespace."""
    cat = _open_catalog(ctx)
    try:
        cat.drop_namespace(namespace)
        click.echo(f"Dropped namespace: {namespace}")
    finally:
        cat.close()


# --- Table commands ---


@cli.command("list-tables")
@click.option("--namespace", "-n", default="main")
@click.pass_context
def list_tables(ctx: click.Context, namespace: str) -> None:
    """List tables in a namespace."""
    cat = _open_catalog(ctx)
    try:
        tables = cat.list_tables(namespace)
        _echo_table(["namespace", "table"], [[ns, name] for ns, name in tables], ctx)
    finally:
        cat.close()


@cli.command("describe")
@click.argument("table")
@click.pass_context
def describe(ctx: click.Context, table: str) -> None:
    """Describe a table (show schema, partition spec, sort order)."""
    cat = _open_catalog(ctx)
    try:
        tbl = cat.load_table(_resolve_table_identifier(table))

        # Schema
        click.echo("Schema:")
        headers = ["field_id", "name", "type", "required"]
        rows = [[str(f.field_id), f.name, repr(f.field_type), str(f.required)] for f in tbl.schema.fields]
        _echo_table(headers, rows, ctx)

        click.echo("")

        # Partition spec
        click.echo(f"Partition spec: {tbl.spec!r}")

        # Sort order
        click.echo(f"Sort order: {tbl.sort_order!r}")
    except NoSuchTableError:
        click.echo(f"Error: table '{table}' does not exist.", err=True)
        ctx.exit(1)
    finally:
        cat.close()


@cli.command("schema")
@click.argument("table")
@click.pass_context
def show_schema(ctx: click.Context, table: str) -> None:
    """Show table schema."""
    cat = _open_catalog(ctx)
    try:
        tbl = cat.load_table(_resolve_table_identifier(table))
        headers = ["field_id", "name", "type", "required"]
        rows = [[str(f.field_id), f.name, repr(f.field_type), str(f.required)] for f in tbl.schema.fields]
        _echo_table(headers, rows, ctx)
    except NoSuchTableError:
        click.echo(f"Error: table '{table}' does not exist.", err=True)
        ctx.exit(1)
    finally:
        cat.close()


@cli.command("spec")
@click.argument("table")
@click.pass_context
def show_spec(ctx: click.Context, table: str) -> None:
    """Show partition spec."""
    cat = _open_catalog(ctx)
    try:
        tbl = cat.load_table(_resolve_table_identifier(table))
        spec = tbl.spec
        if _output_format(ctx) == "json":
            if spec.is_unpartitioned:
                click.echo(json.dumps({"spec": "UNPARTITIONED"}))
            else:
                fields = [
                    {"source_column": f.source_column, "transform": f.transform.to_sql() or "identity"}
                    for f in spec.fields
                ]
                click.echo(json.dumps({"fields": fields}, indent=2))
        else:
            click.echo(repr(spec))
    except NoSuchTableError:
        click.echo(f"Error: table '{table}' does not exist.", err=True)
        ctx.exit(1)
    finally:
        cat.close()


@cli.command("snapshots")
@click.argument("table")
@click.pass_context
def show_snapshots(ctx: click.Context, table: str) -> None:
    """List table snapshots."""
    cat = _open_catalog(ctx)
    try:
        tbl = cat.load_table(_resolve_table_identifier(table))
        snapshots = tbl.snapshots()
        headers = ["snapshot_id", "timestamp", "schema_version"]
        rows = [[str(s.snapshot_id), str(s.timestamp), str(s.schema_version)] for s in snapshots]
        _echo_table(headers, rows, ctx)
    except NoSuchTableError:
        click.echo(f"Error: table '{table}' does not exist.", err=True)
        ctx.exit(1)
    finally:
        cat.close()


@cli.command("files")
@click.argument("table")
@click.pass_context
def show_files(ctx: click.Context, table: str) -> None:
    """List data files for a table."""
    cat = _open_catalog(ctx)
    try:
        tbl = cat.load_table(_resolve_table_identifier(table))
        files_arrow = tbl.inspect().files()
        if _output_format(ctx) == "json":
            records = files_arrow.to_pydict()
            click.echo(json.dumps(records, indent=2, default=str))
        else:
            if files_arrow.num_rows == 0:
                click.echo("No data files.")
            else:
                col_names = files_arrow.column_names
                click.echo("  ".join(col_names))
                for i in range(files_arrow.num_rows):
                    row_vals = [str(files_arrow.column(c)[i].as_py()) for c in col_names]
                    click.echo("  ".join(row_vals))
    except NoSuchTableError:
        click.echo(f"Error: table '{table}' does not exist.", err=True)
        ctx.exit(1)
    finally:
        cat.close()


# --- Maintenance commands ---


@cli.command("compact")
@click.argument("table")
@click.pass_context
def compact(ctx: click.Context, table: str) -> None:
    """Compact small files."""
    cat = _open_catalog(ctx)
    try:
        from pyducklake.catalog import escape_string_literal

        # Validate the table exists
        cat.load_table(_resolve_table_identifier(table))
        cat_name = escape_string_literal(cat.name)
        cat.connection.execute(f"CALL ducklake_merge_adjacent_files('{cat_name}')")
        click.echo(f"Compacted: {table}")
    except NoSuchTableError:
        click.echo(f"Error: table '{table}' does not exist.", err=True)
        ctx.exit(1)
    finally:
        cat.close()


@cli.command("expire-snapshots")
@click.argument("table")
@click.option("--versions", type=int, default=None, help="Number of versions to keep")
@click.option("--older-than", default=None, help="Expire snapshots older than this timestamp")
@click.option("--dry-run", is_flag=True, help="Show what would be expired without doing it")
@click.pass_context
def expire_snapshots(
    ctx: click.Context,
    table: str,
    versions: int | None,
    older_than: str | None,
    dry_run: bool,
) -> None:
    """Expire old snapshots."""
    cat = _open_catalog(ctx)
    try:
        tbl = cat.load_table(_resolve_table_identifier(table))
        cat_name = cat.name

        if dry_run:
            snapshots = tbl.snapshots()
            click.echo(f"Table {table} has {len(snapshots)} snapshot(s).")
            if versions is not None:
                would_expire = max(0, len(snapshots) - versions)
                click.echo(f"Would expire {would_expire} snapshot(s) (keeping {versions}).")
            if older_than is not None:
                click.echo(f"Would expire snapshots older than {older_than}.")
            click.echo("Dry run — no changes made.")
        else:
            from pyducklake.catalog import escape_string_literal
            from pyducklake.maintenance import validate_older_than

            esc_name = escape_string_literal(cat_name)
            params: list[str] = [f"'{esc_name}'"]
            if older_than is not None:
                validate_older_than(older_than)
                params.append(f"older_than := '{escape_string_literal(older_than)}'")
            if versions is not None:
                params.append(f"versions := [{versions}::UBIGINT]")

            cat.connection.execute(f"CALL ducklake_expire_snapshots({', '.join(params)})")
            click.echo(f"Expired snapshots for: {table}")
    finally:
        cat.close()


@cli.command("checkpoint")
@click.pass_context
def checkpoint(ctx: click.Context) -> None:
    """Run all maintenance operations."""
    cat = _open_catalog(ctx)
    try:
        from pyducklake.catalog import escape_string_literal

        cat_name = escape_string_literal(cat.name)
        cat.connection.execute(f"CALL ducklake_merge_adjacent_files('{cat_name}')")
        click.echo("Checkpoint complete.")
    finally:
        cat.close()


# --- Version ---


@cli.command("version")
def show_version() -> None:
    """Show pyducklake version."""
    click.echo(__version__)
