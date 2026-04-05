"""Ducklake table representation."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import duckdb
import pyarrow as pa

from pyducklake.expressions import AlwaysFalse, AlwaysTrue, BooleanExpression
from pyducklake.partitioning import PartitionSpec
from pyducklake.schema import Schema
from pyducklake.snapshot import Snapshot
from pyducklake.sorting import SortOrder

if TYPE_CHECKING:
    import pyarrow.dataset as ds

    from pyducklake.catalog import Catalog
    from pyducklake.cdc import ChangeSet
    from pyducklake.inspect import InspectTable
    from pyducklake.maintenance import MaintenanceTable
    from pyducklake.partitioning import UpdateSpec
    from pyducklake.scan import DataScan
    from pyducklake.schema_evolution import UpdateSchema
    from pyducklake.sorting import UpdateSortOrder


@runtime_checkable
class ArrowStreamExportable(Protocol):
    """Protocol for objects implementing the Arrow PyCapsule interface."""

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any: ...


ArrowCompatible = pa.Table | ArrowStreamExportable

__all__ = ["ArrowCompatible", "ArrowStreamExportable", "Table", "UpsertResult"]


@dataclass(frozen=True)
class UpsertResult:
    """Result of an upsert operation."""

    rows_updated: int
    rows_inserted: int


class Table:
    """Represents a loaded Ducklake table."""

    def __init__(
        self,
        identifier: tuple[str, str],
        schema: Schema,
        catalog: Catalog,
    ) -> None:
        self._identifier = identifier
        self._schema = schema
        self._catalog = catalog

    @property
    def name(self) -> str:
        """Table name (without namespace)."""
        return self._identifier[1]

    @property
    def namespace(self) -> str:
        """Namespace (schema) name."""
        return self._identifier[0]

    @property
    def identifier(self) -> tuple[str, str]:
        """(namespace, table_name) tuple."""
        return self._identifier

    @property
    def schema(self) -> Schema:
        """Current table schema."""
        return self._schema

    @property
    def catalog(self) -> Catalog:
        """The catalog this table belongs to."""
        return self._catalog

    @property
    def fully_qualified_name(self) -> str:
        """catalog.namespace.table_name"""
        return self._catalog.fully_qualified_name(self._identifier[0], self._identifier[1])

    def current_snapshot(self) -> Snapshot | None:
        """Get current snapshot info. Returns None if table has no data."""
        snapshots = self.snapshots()
        if not snapshots:
            return None
        return snapshots[-1]

    def snapshots(self) -> list[Snapshot]:
        """List all catalog-level snapshots.

        Ducklake snapshots are catalog-wide (not per-table). Each snapshot
        represents a transaction that may have modified any table in the
        catalog. The table may not have changed in every returned snapshot.
        """
        catalog_name = self._catalog.name
        meta_schema = f"__ducklake_metadata_{catalog_name}"
        try:
            rows: list[tuple[Any, ...]] = self._catalog.fetchall(
                f"SELECT snapshot_id, snapshot_time, schema_version "
                f'FROM "{meta_schema}".ducklake_snapshot '
                f"ORDER BY snapshot_id"
            )
        except (duckdb.CatalogException, duckdb.BinderException):
            return []

        snapshots: list[Snapshot] = []
        for row in rows:
            snapshot_id = int(row[0])
            ts = row[1]
            if isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.fromtimestamp(0, tz=timezone.utc)

            schema_version = int(row[2]) if len(row) > 2 and row[2] is not None else None

            snapshots.append(
                Snapshot(
                    snapshot_id=snapshot_id,
                    timestamp=timestamp,
                    schema_version=schema_version,
                )
            )
        return snapshots

    def refresh(self) -> Table:
        """Reload schema and metadata from the catalog. Returns self."""
        self._schema = self._catalog.build_schema_from_describe(self._identifier[0], self._identifier[1])
        return self

    # -- Rollback --------------------------------------------------------------

    def rollback_to_snapshot(self, snapshot_id: int) -> None:
        """Roll back the table to a previous snapshot.

        This creates a new snapshot that matches the state at the given snapshot_id.
        Data written after that snapshot becomes inaccessible (but files aren't
        deleted until maintenance operations run).

        Args:
            snapshot_id: The snapshot ID to roll back to.

        Raises:
            ValueError: If snapshot_id doesn't exist.
        """
        known_ids = {s.snapshot_id for s in self.snapshots()}
        if snapshot_id not in known_ids:
            raise ValueError(f"Snapshot {snapshot_id} does not exist")

        fqn = self.fully_qualified_name
        conn = self._catalog.connection
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE _pyducklake_rollback AS SELECT * FROM {fqn} AT (VERSION => {snapshot_id})"
        )
        conn.execute(f"DELETE FROM {fqn}")
        conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_rollback")
        conn.execute("DROP TABLE _pyducklake_rollback")

    def rollback_to_timestamp(self, timestamp: datetime) -> None:
        """Roll back the table to the state at a given timestamp.

        Finds the latest snapshot at or before the timestamp and rolls back to it.

        Args:
            timestamp: The point in time to roll back to.

        Raises:
            ValueError: If no snapshot exists at or before the timestamp.
        """
        snapshots = self.snapshots()
        # Normalize timestamp once to avoid mutating across iterations
        ts = timestamp
        candidate: Snapshot | None = None
        for snap in snapshots:
            snap_ts = snap.timestamp
            # Make both offset-aware or both naive for comparison
            if ts.tzinfo is not None and snap_ts.tzinfo is None:
                snap_ts = snap_ts.replace(tzinfo=ts.tzinfo)
            elif ts.tzinfo is None and snap_ts.tzinfo is not None:
                ts = ts.replace(tzinfo=snap_ts.tzinfo)
            if snap_ts <= ts:
                candidate = snap
        if candidate is None:
            raise ValueError(f"No snapshot exists at or before {timestamp}")
        self.rollback_to_snapshot(candidate.snapshot_id)

    # -- Scan ------------------------------------------------------------------

    def scan(
        self,
        row_filter: BooleanExpression | str = AlwaysTrue(),
        selected_fields: tuple[str, ...] = ("*",),
        snapshot_id: int | None = None,
        limit: int | None = None,
    ) -> DataScan:
        """Create a scan on this table."""
        from pyducklake.scan import DataScan, RawSQL

        if isinstance(row_filter, str):
            actual_filter: BooleanExpression = RawSQL(row_filter)
        else:
            actual_filter = row_filter

        return DataScan(
            table=self,
            row_filter=actual_filter,
            selected_fields=selected_fields,
            snapshot_id=snapshot_id,
            limit=limit,
        )

    # -- Add Files -------------------------------------------------------------

    def add_files(
        self,
        file_paths: list[str] | str,
        *,
        allow_missing: bool = False,
        ignore_extra_columns: bool = False,
    ) -> None:
        """Register external Parquet files with this table.

        Args:
            file_paths: Path(s) to Parquet files to register.
            allow_missing: If True, columns present in the table but
                missing from the file are filled with their initial
                default value.
            ignore_extra_columns: If True, extra columns in the file
                that aren't in the table are silently ignored.

        The files must be valid Parquet files with a compatible schema.
        """
        from pyducklake.catalog import escape_string_literal

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        catalog_name = self._catalog.name
        namespace = self._identifier[0]
        table_name = self._identifier[1]

        opts = ""
        if namespace != "main":
            opts += f", schema := '{escape_string_literal(namespace)}'"
        if allow_missing:
            opts += ", allow_missing := true"
        if ignore_extra_columns:
            opts += ", ignore_extra_columns := true"

        esc_cat = escape_string_literal(catalog_name)
        esc_tbl = escape_string_literal(table_name)
        for path in file_paths:
            esc_path = escape_string_literal(path)
            self._catalog.connection.execute(
                f"CALL ducklake_add_data_files('{esc_cat}', '{esc_tbl}', '{esc_path}'{opts})"
            )

    # -- Write -----------------------------------------------------------------

    @staticmethod
    def _to_arrow_table(df: ArrowCompatible) -> pa.Table:
        """Convert input to pyarrow.Table, supporting the Arrow PyCapsule interface.

        Accepts ``pyarrow.Table`` directly, or any object implementing
        ``__arrow_c_stream__`` (e.g. Polars DataFrame, nanoarrow array,
        arro3 table).
        """
        if isinstance(df, pa.Table):
            return df
        if hasattr(df, "__arrow_c_stream__"):
            return pa.RecordBatchReader.from_stream(df).read_all()
        raise TypeError(f"Expected pyarrow.Table or object implementing __arrow_c_stream__, got {type(df).__name__}")

    def _sort_order_clause(self) -> str | None:
        """Return an ORDER BY clause if the table has a sort order, else None."""
        sort = self.sort_order
        if sort.is_unsorted:
            return None
        return ", ".join(f.to_sql() for f in sort.fields)

    def append(self, df: ArrowCompatible) -> None:
        """Append data to the table.

        Accepts ``pyarrow.Table`` or any object implementing the Arrow
        PyCapsule interface (``__arrow_c_stream__``).

        If the table has a sort order, data is sorted before insertion.
        """
        arrow_table = self._to_arrow_table(df)
        conn = self._catalog.connection
        conn.register("_pyducklake_tmp_append", arrow_table)
        try:
            fqn = self.fully_qualified_name
            order = self._sort_order_clause()
            if order:
                conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_tmp_append ORDER BY {order}")
            else:
                conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_tmp_append")
        finally:
            conn.unregister("_pyducklake_tmp_append")

    def append_batches(
        self,
        batches: pa.RecordBatchReader | Iterator[pa.RecordBatch],
        *,
        schema: pa.Schema | None = None,
    ) -> None:
        """Append data from a stream of record batches.

        Memory-efficient alternative to :meth:`append` — processes batches
        without materializing the full dataset in memory.

        Args:
            batches: RecordBatchReader or iterator of RecordBatch.
            schema: Required if passing an iterator (not needed for RecordBatchReader).
        """
        if isinstance(batches, pa.RecordBatchReader):
            reader = batches
        else:
            if schema is None:
                raise ValueError("schema is required when passing an iterator of RecordBatch")
            reader = pa.RecordBatchReader.from_batches(schema, batches)

        conn = self._catalog.connection
        conn.register("_pyducklake_tmp_batches", reader)
        try:
            fqn = self.fully_qualified_name
            order = self._sort_order_clause()
            if order:
                conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_tmp_batches ORDER BY {order}")
            else:
                conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_tmp_batches")
        finally:
            conn.unregister("_pyducklake_tmp_batches")

    def overwrite(
        self,
        df: ArrowCompatible,
        overwrite_filter: BooleanExpression | str = AlwaysTrue(),
    ) -> None:
        """Overwrite data matching the filter, then insert new data.

        Accepts ``pyarrow.Table`` or any object implementing the Arrow
        PyCapsule interface (``__arrow_c_stream__``).

        If overwrite_filter is AlwaysTrue or not provided, truncates and inserts.
        Otherwise, deletes matching rows then inserts.

        Does not manage its own transaction — relies on the caller (or DuckDB
        auto-commit) for atomicity.  This allows overwrite to be used inside
        an explicit :class:`Transaction` without nesting conflicts.
        """
        arrow_table = self._to_arrow_table(df)
        conn = self._catalog.connection
        conn.register("_pyducklake_tmp_overwrite", arrow_table)
        try:
            if isinstance(overwrite_filter, str):
                where: str | None = overwrite_filter
            elif overwrite_filter == AlwaysTrue():
                where = None
            else:
                where = overwrite_filter.to_sql()

            fqn = self.fully_qualified_name
            if where:
                conn.execute(f"DELETE FROM {fqn} WHERE {where}")
            else:
                conn.execute(f"DELETE FROM {fqn}")

            order = self._sort_order_clause()
            if order:
                conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_tmp_overwrite ORDER BY {order}")
            else:
                conn.execute(f"INSERT INTO {fqn} SELECT * FROM _pyducklake_tmp_overwrite")
        finally:
            conn.unregister("_pyducklake_tmp_overwrite")

    # -- Delete ----------------------------------------------------------------

    def delete(self, delete_filter: BooleanExpression | str) -> None:
        """Delete rows matching the filter.

        Args:
            delete_filter: Rows matching this filter are deleted.
        """
        if isinstance(delete_filter, str):
            filter_sql = delete_filter
        elif delete_filter == AlwaysFalse():
            return
        elif delete_filter == AlwaysTrue():
            filter_sql = None
        else:
            filter_sql = delete_filter.to_sql()

        fqn = self.fully_qualified_name
        if filter_sql is None:
            self._catalog.connection.execute(f"DELETE FROM {fqn}")
        else:
            self._catalog.connection.execute(f"DELETE FROM {fqn} WHERE {filter_sql}")

    # -- Upsert ----------------------------------------------------------------

    def upsert(
        self,
        df: ArrowCompatible,
        join_cols: tuple[str, ...] | list[str],
    ) -> UpsertResult:
        """Upsert data: update existing rows matching on join_cols, insert new rows.

        Accepts ``pyarrow.Table`` or any object implementing the Arrow
        PyCapsule interface (``__arrow_c_stream__``).

        Uses DuckDB's MERGE statement.
        """
        arrow_table = self._to_arrow_table(df)
        conn = self._catalog.connection
        fqn = self.fully_qualified_name
        join_col_set = set(join_cols)

        # Count rows before to determine updated vs inserted
        count_before = self.scan().count()

        conn.register("_pyducklake_tmp_upsert", arrow_table)
        try:
            on_clause = " AND ".join(f'target."{col}" = source."{col}"' for col in join_cols)

            all_cols = [field.name for field in arrow_table.schema]  # pyright: ignore[reportUnknownVariableType]
            non_join_cols = [c for c in all_cols if c not in join_col_set]

            update_set = ", ".join(f'"{col}" = source."{col}"' for col in non_join_cols)

            insert_cols = ", ".join(f'source."{col}"' for col in all_cols)

            merge_sql = f"MERGE INTO {fqn} AS target USING _pyducklake_tmp_upsert AS source ON {on_clause}"
            if update_set:
                merge_sql += f" WHEN MATCHED THEN UPDATE SET {update_set}"
            merge_sql += f" WHEN NOT MATCHED THEN INSERT VALUES ({insert_cols})"

            conn.execute(merge_sql)
        finally:
            conn.unregister("_pyducklake_tmp_upsert")

        count_after = self.scan().count()
        rows_inserted = count_after - count_before
        rows_updated = arrow_table.num_rows - rows_inserted

        return UpsertResult(rows_updated=rows_updated, rows_inserted=rows_inserted)

    # -- Partitioning ----------------------------------------------------------

    @property
    def spec(self) -> PartitionSpec:
        """Current partition spec. Returns UNPARTITIONED if not partitioned."""
        from pyducklake.catalog import escape_string_literal
        from pyducklake.partitioning import (
            DAY,
            HOUR,
            IDENTITY,
            MONTH,
            UNPARTITIONED,
            YEAR,
            PartitionField,
            PartitionSpec,
        )

        catalog_name = self._catalog.name
        meta_schema = f"__ducklake_metadata_{catalog_name}"
        try:
            rows: list[tuple[Any, ...]] = self._catalog.fetchall(
                f"SELECT c.column_name, pc.transform "
                f'FROM "{meta_schema}".ducklake_partition_column pc '
                f'JOIN "{meta_schema}".ducklake_partition_info pi '
                f"ON pc.partition_id = pi.partition_id "
                f"AND pc.table_id = pi.table_id "
                f'JOIN "{meta_schema}".ducklake_table t '
                f"ON pi.table_id = t.table_id "
                f'JOIN "{meta_schema}".ducklake_schema s '
                f"ON t.schema_id = s.schema_id "
                f'JOIN "{meta_schema}".ducklake_column c '
                f"ON pc.column_id = c.column_id "
                f"AND pc.table_id = c.table_id "
                f"WHERE t.table_name = '{escape_string_literal(self._identifier[1])}' "
                f"AND s.schema_name = '{escape_string_literal(self._identifier[0])}' "
                f"AND pi.end_snapshot IS NULL "
                f"ORDER BY pc.partition_key_index"
            )
        except (duckdb.CatalogException, duckdb.BinderException):
            return UNPARTITIONED

        if not rows:
            return UNPARTITIONED

        _transform_map = {
            "identity": IDENTITY,
            "year": YEAR,
            "month": MONTH,
            "day": DAY,
            "hour": HOUR,
        }

        fields: list[PartitionField] = []
        for col_name, transform_name in rows:
            transform = _transform_map.get(str(transform_name).lower(), IDENTITY)
            fields.append(PartitionField(source_column=str(col_name), transform=transform))

        return PartitionSpec(*fields)

    def update_spec(self) -> UpdateSpec:
        """Begin partition spec evolution. Returns an UpdateSpec builder."""
        from pyducklake.partitioning import UpdateSpec

        return UpdateSpec(self)

    # -- Sorting ---------------------------------------------------------------

    @property
    def sort_order(self) -> SortOrder:
        """Current sort order. Returns UNSORTED if not sorted."""
        from pyducklake.catalog import escape_string_literal
        from pyducklake.sorting import (
            UNSORTED,
            NullOrder,
            SortDirection,
            SortField,
            SortOrder,
        )

        catalog_name = self._catalog.name
        meta_schema = f"__ducklake_metadata_{catalog_name}"
        try:
            rows: list[tuple[Any, ...]] = self._catalog.fetchall(
                f"SELECT se.expression, se.sort_direction, se.null_order "
                f'FROM "{meta_schema}".ducklake_sort_expression se '
                f'JOIN "{meta_schema}".ducklake_sort_info si '
                f"ON se.sort_id = si.sort_id "
                f"AND se.table_id = si.table_id "
                f'JOIN "{meta_schema}".ducklake_table t '
                f"ON si.table_id = t.table_id "
                f'JOIN "{meta_schema}".ducklake_schema s '
                f"ON t.schema_id = s.schema_id "
                f"WHERE t.table_name = '{escape_string_literal(self._identifier[1])}' "
                f"AND s.schema_name = '{escape_string_literal(self._identifier[0])}' "
                f"AND si.end_snapshot IS NULL "
                f"ORDER BY se.sort_key_index"
            )
        except (duckdb.CatalogException, duckdb.BinderException):
            return UNSORTED

        if not rows:
            return UNSORTED

        fields: list[SortField] = []
        for expr, sort_dir, null_ord in rows:
            # expression is a quoted identifier like '"name"' — strip quotes
            col_name = str(expr).strip('"')
            direction = SortDirection.DESC if str(sort_dir).upper() == "DESC" else SortDirection.ASC
            null_order = NullOrder.NULLS_FIRST if "FIRST" in str(null_ord).upper() else NullOrder.NULLS_LAST
            fields.append(
                SortField(
                    source_column=col_name,
                    direction=direction,
                    null_order=null_order,
                )
            )

        return SortOrder(fields=tuple(fields))

    def update_sort_order(self) -> UpdateSortOrder:
        """Begin sort order evolution. Returns an UpdateSortOrder builder."""
        from pyducklake.sorting import UpdateSortOrder

        return UpdateSortOrder(self)

    # -- Arrow Dataset ---------------------------------------------------------

    def to_arrow_dataset(self, *, snapshot_id: int | None = None) -> ds.Dataset:
        """Return this table as a PyArrow Dataset.

        Enables interop with engines that consume the PyArrow dataset API
        (DuckDB, Polars, DataFusion, Dask, etc.).

        Args:
            snapshot_id: Optional snapshot for time travel.

        Returns:
            A pyarrow.dataset.Dataset wrapping this table's data.
        """
        import pyarrow.dataset as ds

        scan = self.scan(snapshot_id=snapshot_id)
        return ds.dataset(scan.to_arrow())  # pyright: ignore[reportUnknownMemberType]

    # -- Inspect ---------------------------------------------------------------

    def inspect(self) -> InspectTable:
        """Return an :class:`InspectTable` for metadata introspection."""
        from pyducklake.inspect import InspectTable

        return InspectTable(self)

    # -- Maintenance -----------------------------------------------------------

    def maintenance(self) -> MaintenanceTable:
        """Get maintenance operations for this table."""
        from pyducklake.maintenance import MaintenanceTable

        return MaintenanceTable(self)

    # -- CDC (Change Data Capture) ---------------------------------------------

    def table_changes(
        self,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        columns: tuple[str, ...] | list[str] | None = None,
        filter_expr: str | None = None,
    ) -> ChangeSet:
        """Query all changes between two snapshots or timestamps.

        Returns a ChangeSet with inserts, deletes, and update pre/post images.
        """
        from pyducklake.cdc import ChangeSet

        return ChangeSet(
            self._cdc_query(
                "ducklake_table_changes",
                start_snapshot,
                end_snapshot,
                start_time=start_time,
                end_time=end_time,
                columns=columns,
                filter_expr=filter_expr,
                meta_cols=("snapshot_id", "rowid", "change_type"),
            ),
            change_type_col="change_type",
        )

    def table_insertions(
        self,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        columns: tuple[str, ...] | list[str] | None = None,
        filter_expr: str | None = None,
    ) -> ChangeSet:
        """Query inserted rows between two snapshots or timestamps."""
        from pyducklake.cdc import ChangeSet

        return ChangeSet(
            self._cdc_query(
                "ducklake_table_insertions",
                start_snapshot,
                end_snapshot,
                start_time=start_time,
                end_time=end_time,
                columns=columns,
                filter_expr=filter_expr,
                meta_cols=(),
            ),
            change_type_col=None,
        )

    def table_deletions(
        self,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        columns: tuple[str, ...] | list[str] | None = None,
        filter_expr: str | None = None,
    ) -> ChangeSet:
        """Query deleted rows between two snapshots or timestamps."""
        from pyducklake.cdc import ChangeSet

        return ChangeSet(
            self._cdc_query(
                "ducklake_table_deletions",
                start_snapshot,
                end_snapshot,
                start_time=start_time,
                end_time=end_time,
                columns=columns,
                filter_expr=filter_expr,
                meta_cols=(),
            ),
            change_type_col=None,
        )

    @staticmethod
    def _validate_cdc_bounds(
        start_snapshot: int | None,
        end_snapshot: int | None,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> None:
        """Validate CDC bound arguments."""
        has_snapshot = start_snapshot is not None or end_snapshot is not None
        has_time = start_time is not None or end_time is not None

        if has_snapshot and has_time:
            raise ValueError(
                "Cannot mix snapshot and timestamp bounds. "
                "Use either (start_snapshot, end_snapshot) or (start_time, end_time)."
            )

        if not has_snapshot and not has_time:
            raise ValueError("Must provide either (start_snapshot, end_snapshot) or (start_time, end_time).")

        if has_snapshot and start_snapshot is None:
            raise ValueError("start_snapshot is required when using snapshot bounds.")

        if has_time and start_time is None:
            raise ValueError("start_time is required when using timestamp bounds.")

    def _cdc_query(
        self,
        func_name: str,
        start_snapshot: int | None,
        end_snapshot: int | None,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        columns: tuple[str, ...] | list[str] | None = None,
        filter_expr: str | None = None,
        meta_cols: tuple[str, ...] = (),
    ) -> pa.Table:
        """Execute a CDC function and return the result as an Arrow table."""
        self._validate_cdc_bounds(start_snapshot, end_snapshot, start_time, end_time)

        catalog_name = self._catalog.name
        namespace = self._identifier[0]
        table_name = self._identifier[1]

        # Build bound arguments
        if start_time is not None:
            start_arg = f"'{start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}'::TIMESTAMP"
            if end_time is not None:
                end_arg = f"'{end_time.strftime('%Y-%m-%d %H:%M:%S.%f')}'::TIMESTAMP"
            else:
                end_arg = "CURRENT_TIMESTAMP"
        else:
            assert start_snapshot is not None  # validated above
            start_arg = f"{start_snapshot}::BIGINT"
            if end_snapshot is not None:
                end_arg = f"{end_snapshot}::BIGINT"
            else:
                snap = self.current_snapshot()
                resolved_end = snap.snapshot_id if snap is not None else start_snapshot
                end_arg = f"{resolved_end}::BIGINT"

        # Build column list
        if columns is not None:
            col_list = ", ".join([f'"{c}"' for c in meta_cols] + [f'"{c}"' for c in columns])
        else:
            col_list = "*"

        from pyducklake.catalog import escape_string_literal

        esc_cat = escape_string_literal(catalog_name)
        esc_ns = escape_string_literal(namespace)
        esc_tbl = escape_string_literal(table_name)
        sql = f"SELECT {col_list} FROM {func_name}('{esc_cat}', '{esc_ns}', '{esc_tbl}', {start_arg}, {end_arg})"

        if filter_expr is not None:
            sql += f" WHERE {filter_expr}"

        result: Any = self._catalog.connection.execute(sql)
        arrow_obj: Any = result.arrow()
        if isinstance(arrow_obj, pa.Table):
            return arrow_obj
        tbl: pa.Table = arrow_obj.read_all()
        return tbl

    # -- Schema Evolution ------------------------------------------------------

    def update_schema(self) -> UpdateSchema:
        """Begin schema evolution. Returns an UpdateSchema builder."""
        from pyducklake.schema_evolution import UpdateSchema

        return UpdateSchema(self)

    def __repr__(self) -> str:
        return f"Table(identifier={self._identifier!r}, schema={self._schema!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Table):
            return NotImplemented
        return self._identifier == other._identifier and self._catalog.name == other._catalog.name
