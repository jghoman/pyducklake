"""Ducklake view representation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyducklake.expressions import AlwaysTrue, BooleanExpression
from pyducklake.schema import Schema

if TYPE_CHECKING:
    import pandas as pd  # type: ignore[import-untyped]
    import pyarrow as pa
    import pyarrow.dataset as ds

    from pyducklake.catalog import Catalog
    from pyducklake.scan import DataScan

__all__ = ["View"]


class View:
    """Represents a Ducklake view."""

    def __init__(
        self,
        identifier: tuple[str, str],
        schema: Schema,
        sql: str,
        catalog: Catalog,
    ) -> None:
        self._identifier = identifier
        self._schema = schema
        self._sql = sql
        self._catalog = catalog

    @property
    def name(self) -> str:
        """View name (without namespace)."""
        return self._identifier[1]

    @property
    def namespace(self) -> str:
        """Namespace (schema) name."""
        return self._identifier[0]

    @property
    def identifier(self) -> tuple[str, str]:
        """(namespace, view_name) tuple."""
        return self._identifier

    @property
    def schema(self) -> Schema:
        """View's output schema."""
        return self._schema

    @property
    def sql_text(self) -> str:
        """The SQL definition of the view."""
        return self._sql

    @property
    def fully_qualified_name(self) -> str:
        """catalog.namespace.view_name"""
        return self._catalog.fully_qualified_name(self._identifier[0], self._identifier[1])

    @property
    def catalog(self) -> Catalog:
        """The catalog this view belongs to."""
        return self._catalog

    def scan(
        self,
        row_filter: BooleanExpression | str = AlwaysTrue(),
        selected_fields: tuple[str, ...] = ("*",),
        limit: int | None = None,
    ) -> DataScan:
        """Create a scan on this view.

        Works exactly like Table.scan() -- the view is queryable
        with filters, projections, and limits.
        """
        from pyducklake.scan import DataScan, RawSQL

        if isinstance(row_filter, str):
            actual_filter: BooleanExpression = RawSQL(row_filter)
        else:
            actual_filter = row_filter

        return DataScan(
            table=self,
            row_filter=actual_filter,
            selected_fields=selected_fields,
            limit=limit,
        )

    def to_arrow(self) -> pa.Table:
        """Read the entire view as an Arrow table. Shorthand for scan().to_arrow()."""
        return self.scan().to_arrow()

    def to_pandas(self) -> pd.DataFrame:
        """Read the entire view as a pandas DataFrame."""
        return self.scan().to_pandas()

    def to_arrow_dataset(self) -> ds.Dataset:
        """Return view results as a PyArrow Dataset."""
        return self.scan().to_arrow_dataset()

    def refresh(self) -> View:
        """Reload schema from catalog. Returns self."""
        self._schema = self._catalog.build_schema_from_describe(self._identifier[0], self._identifier[1])
        return self

    def __repr__(self) -> str:
        return f"View(identifier={self._identifier!r}, sql={self._sql!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, View):
            return NotImplemented
        return self._identifier == other._identifier and self._catalog.name == other._catalog.name
