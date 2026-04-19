"""DataScan for reading Ducklake tables with filtering, projection, and time travel."""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownParameterType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from pyducklake.expressions import AlwaysTrue, And, BooleanExpression

if TYPE_CHECKING:
    import pandas as pd  # type: ignore[import-untyped]
    import polars as pl
    import pyarrow.dataset as ds
    import ray.data  # type: ignore[import-not-found]

    from pyducklake.table import Table
    from pyducklake.view import View

__all__ = ["DataScan", "RawSQL"]


@dataclass(frozen=True)
class RawSQL(BooleanExpression):
    """Wraps a raw SQL string as a BooleanExpression."""

    sql: str

    def to_sql(self) -> str:
        return self.sql

    def __repr__(self) -> str:
        return f"RawSQL(sql={self.sql!r})"


def _is_always_true(expr: BooleanExpression) -> bool:
    """Check if an expression is AlwaysTrue without importing private class."""
    return expr == AlwaysTrue()


class DataScan:
    """Scans a Ducklake table with optional filtering, column selection, and time travel.

    Immutable builder: each method returns a new DataScan instance.
    """

    __slots__ = ("_table", "_row_filter", "_selected_fields", "_snapshot_id", "_timestamp", "_limit")

    def __init__(
        self,
        table: Table | View,
        row_filter: BooleanExpression = AlwaysTrue(),
        selected_fields: tuple[str, ...] = ("*",),
        snapshot_id: int | None = None,
        limit: int | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        self._table = table
        self._row_filter = row_filter
        self._selected_fields = selected_fields
        self._snapshot_id = snapshot_id
        self._timestamp = timestamp
        self._limit = limit

    # -- Builder methods (return new DataScan) ---------------------------------

    def filter(self, expr: BooleanExpression | str) -> DataScan:
        """Add a row filter.

        If string, wrap in a raw SQL expression.
        If BooleanExpression, combine with existing filter via And.
        """
        if isinstance(expr, str):
            new_filter: BooleanExpression = RawSQL(expr)
        else:
            new_filter = expr

        combined = And(self._row_filter, new_filter)
        return DataScan(
            table=self._table,
            row_filter=combined,
            selected_fields=self._selected_fields,
            snapshot_id=self._snapshot_id,
            limit=self._limit,
            timestamp=self._timestamp,
        )

    def select(self, *fields: str) -> DataScan:
        """Select specific columns. Replaces any previous selection."""
        return DataScan(
            table=self._table,
            row_filter=self._row_filter,
            selected_fields=fields,
            snapshot_id=self._snapshot_id,
            limit=self._limit,
            timestamp=self._timestamp,
        )

    def with_snapshot(self, snapshot_id: int) -> DataScan:
        """Time travel to a specific snapshot version."""
        return DataScan(
            table=self._table,
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=snapshot_id,
            limit=self._limit,
            timestamp=None,
        )

    def with_timestamp(self, timestamp: datetime) -> DataScan:
        """Time travel to data as of a specific timestamp.

        Uses AT (TIMESTAMP => ...) syntax.
        """
        return DataScan(
            table=self._table,
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=None,
            limit=self._limit,
            timestamp=timestamp,
        )

    def with_limit(self, limit: int) -> DataScan:
        """Limit number of rows returned."""
        return DataScan(
            table=self._table,
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=self._snapshot_id,
            limit=limit,
            timestamp=self._timestamp,
        )

    # -- Terminal methods ------------------------------------------------------

    def to_arrow(self) -> pa.Table:
        """Execute scan and return PyArrow Table."""
        sql = self._build_sql()
        result = self._table.catalog.connection.execute(sql)
        return result.fetch_arrow_table()

    def to_pandas(self) -> pd.DataFrame:
        """Execute scan and return pandas DataFrame.

        Requires pandas to be installed separately.
        """
        if importlib.util.find_spec("pandas") is None:
            raise ImportError("pandas is required for to_pandas(). Install it with: pip install pandas")
        sql = self._build_sql()
        result = self._table.catalog.connection.execute(sql)
        df: pd.DataFrame = result.fetchdf()
        return df

    def to_duckdb(self, *, connection: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyRelation:
        """Execute scan and return a DuckDB relation.

        If connection is None, uses the catalog's connection.
        """
        sql = self._build_sql()
        conn = connection if connection is not None else self._table.catalog.connection
        return conn.sql(sql)

    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
        """Execute scan and return a streaming Arrow RecordBatchReader."""
        sql = self._build_sql()
        result = self._table.catalog.connection.execute(sql)
        return result.to_arrow_reader()

    def to_polars(self) -> pl.DataFrame:
        """Execute scan and return a Polars DataFrame.

        Converts via Arrow (to_arrow() then pl.from_arrow()).
        Requires polars to be installed separately.
        """
        try:
            import polars as pl_mod
        except ImportError:
            raise ImportError("polars is required for to_polars(). Install it with: pip install polars") from None
        arrow_table = self.to_arrow()
        result = pl_mod.from_arrow(arrow_table)
        assert isinstance(result, pl_mod.DataFrame)
        return result

    def to_ray(self) -> ray.data.Dataset:
        """Execute scan and return a Ray Dataset.

        Converts via Arrow.
        Requires ray to be installed separately.
        """
        try:
            import ray.data  # pyright: ignore[reportMissingImports]
        except ImportError:
            raise ImportError("ray is required for to_ray(). Install it with: pip install 'ray[data]'") from None
        arrow_table = self.to_arrow()
        dataset: ray.data.Dataset = ray.data.from_arrow(arrow_table)
        return dataset

    def to_arrow_dataset(self) -> ds.Dataset:
        """Return scan results as a PyArrow Dataset.

        Preserves any filters, projections, and time travel settings.
        """
        import pyarrow.dataset as ds

        return ds.dataset(self.to_arrow())

    def count(self) -> int:
        """Return row count (executes SELECT COUNT(*))."""
        sql = self._build_count_sql()
        rows = self._table.catalog.fetchall(sql)
        return int(rows[0][0])

    # -- SQL generation --------------------------------------------------------

    def _build_sql(self) -> str:
        """Build the SELECT SQL statement."""
        columns = self._format_columns()
        table_ref = self._format_table_ref()
        sql = f"SELECT {columns} FROM {table_ref}"
        sql = self._append_where(sql)
        sql = self._append_limit(sql)
        return sql

    def _build_count_sql(self) -> str:
        """Build a SELECT COUNT(*) SQL statement."""
        table_ref = self._format_table_ref()
        sql = f"SELECT COUNT(*) FROM {table_ref}"
        sql = self._append_where(sql)
        sql = self._append_limit(sql)
        return sql

    def _format_columns(self) -> str:
        if self._selected_fields == ("*",):
            return "*"
        return ", ".join(f'"{col}"' for col in self._selected_fields)

    def _format_table_ref(self) -> str:
        fqn = self._table.fully_qualified_name
        if self._snapshot_id is not None and self._timestamp is not None:
            raise ValueError("Cannot set both snapshot_id and timestamp for time travel")
        if self._snapshot_id is not None:
            return f"{fqn} AT (VERSION => {self._snapshot_id})"
        if self._timestamp is not None:
            ts_str = self._timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
            return f"{fqn} AT (TIMESTAMP => '{ts_str}')"
        return fqn

    def _append_where(self, sql: str) -> str:
        if not _is_always_true(self._row_filter):
            sql += f" WHERE {self._row_filter.to_sql()}"
        return sql

    def _append_limit(self, sql: str) -> str:
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        return sql
