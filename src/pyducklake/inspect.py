"""Metadata introspection for Ducklake tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa

if TYPE_CHECKING:
    from pyducklake.table import Table

__all__ = ["InspectTable"]


def _to_arrow_table(result: Any) -> pa.Table:
    """Convert a DuckDB result to a PyArrow Table.

    DuckDB's ``.arrow()`` may return a ``RecordBatchReader`` or a ``Table``
    depending on the version. This helper normalises the output.
    """
    arrow_obj: Any = result.arrow()
    if isinstance(arrow_obj, pa.Table):
        return arrow_obj
    # RecordBatchReader
    tbl: pa.Table = arrow_obj.read_all()
    return tbl


class InspectTable:
    """Metadata introspection for a Ducklake table.

    Obtained via ``table.inspect()``.
    """

    def __init__(self, table: Table) -> None:
        self._table = table

    def snapshots(self) -> pa.Table:
        """Return all snapshots as an Arrow table.

        Columns: snapshot_id, snapshot_time, schema_version, changes,
                 author, commit_message, commit_extra_info
        """
        catalog_name = self._table.catalog.name
        result = self._table.catalog.connection.execute(
            f'SELECT * FROM "{catalog_name}".snapshots() ORDER BY snapshot_id'
        )
        return _to_arrow_table(result)

    def files(
        self,
        snapshot_id: int | None = None,
        snapshot_time: str | None = None,
    ) -> pa.Table:
        """Return data files for the table, optionally at a specific snapshot.

        Columns returned: data_file, data_file_size_bytes,
        data_file_footer_size, data_file_encryption_key, delete_file,
        delete_file_size_bytes, delete_file_footer_size,
        delete_file_encryption_key.

        Args:
            snapshot_id: Fetch files at a specific snapshot version.
            snapshot_time: Fetch files at a specific timestamp
                (e.g. ``'2025-06-16 15:24:30'``).

        Raises:
            ValueError: If both ``snapshot_id`` and ``snapshot_time``
                are provided.

        Uses ``ducklake_list_files()`` function.
        """
        if snapshot_id is not None and snapshot_time is not None:
            raise ValueError("Cannot specify both snapshot_id and snapshot_time")

        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        table_name = self._table.name
        schema_name = self._table.namespace
        sql = (
            f"SELECT * FROM ducklake_list_files('{escape_string_literal(catalog_name)}', "
            f"'{escape_string_literal(table_name)}', schema := '{escape_string_literal(schema_name)}'"
        )
        if snapshot_id is not None:
            sql += f", snapshot_version := {snapshot_id}::BIGINT"
        if snapshot_time is not None:
            sql += f", snapshot_time := '{escape_string_literal(snapshot_time)}'"
        sql += ")"
        result = self._table.catalog.connection.execute(sql)
        return _to_arrow_table(result)

    def history(self) -> pa.Table:
        """Return snapshot history as an Arrow table, newest-first."""
        catalog_name = self._table.catalog.name
        result = self._table.catalog.connection.execute(
            f'SELECT * FROM "{catalog_name}".snapshots() ORDER BY snapshot_id DESC'
        )
        return _to_arrow_table(result)

    def partitions(self) -> pa.Table:
        """Return partition info as an Arrow table.

        Columns: partition_id, column_id, transform
        If no partitioning, returns an empty table.
        """
        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        meta_schema = f"__ducklake_metadata_{catalog_name}"

        # Get table_id first
        esc_name = escape_string_literal(self._table.name)
        esc_ns = escape_string_literal(self._table.namespace)
        table_rows = self._table.catalog.fetchall(
            f'SELECT t.table_id FROM "{meta_schema}".ducklake_table t'
            f' JOIN "{meta_schema}".ducklake_schema s ON t.schema_id = s.schema_id'
            f" WHERE t.table_name = '{esc_name}'"
            f" AND s.schema_name = '{esc_ns}'"
        )
        if not table_rows:
            return pa.table(
                {
                    "partition_id": pa.array([], type=pa.int64()),
                    "column_id": pa.array([], type=pa.int64()),
                    "transform": pa.array([], type=pa.string()),
                }
            )

        table_id = table_rows[0][0]

        try:
            rows = self._table.catalog.fetchall(
                f"SELECT pc.partition_id, pc.column_id, pc.transform "
                f'FROM "{meta_schema}".ducklake_partition_column pc '
                f'JOIN "{meta_schema}".ducklake_partition_info pi '
                f"ON pc.partition_id = pi.partition_id "
                f"AND pc.table_id = pi.table_id "
                f"WHERE pc.table_id = {table_id} "
                f"AND pi.end_snapshot IS NULL"
            )
        except (duckdb.CatalogException, duckdb.BinderException):
            rows = []

        if not rows:
            return pa.table(
                {
                    "partition_id": pa.array([], type=pa.int64()),
                    "column_id": pa.array([], type=pa.int64()),
                    "transform": pa.array([], type=pa.string()),
                }
            )

        return pa.table(
            {
                "partition_id": pa.array([r[0] for r in rows], type=pa.int64()),
                "column_id": pa.array([r[1] for r in rows], type=pa.int64()),
                "transform": pa.array([str(r[2]) for r in rows], type=pa.string()),
            }
        )
