"""Schema evolution builder for Ducklake tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyducklake.catalog import quote_identifier
from pyducklake.types import DucklakeType, ducklake_type_to_sql

if TYPE_CHECKING:
    from pyducklake.table import Table

__all__ = ["UpdateSchema"]


@dataclass
class _AddColumn:
    name: str
    field_type: DucklakeType
    doc: str | None
    required: bool


@dataclass
class _DropColumn:
    name: str


@dataclass
class _RenameColumn:
    name: str
    new_name: str


@dataclass
class _UpdateColumnType:
    name: str
    new_type: DucklakeType


@dataclass
class _SetNullability:
    name: str
    required: bool


_SchemaChange = _AddColumn | _DropColumn | _RenameColumn | _UpdateColumnType | _SetNullability


class UpdateSchema:
    """Builder for schema evolution operations.

    Obtained via table.update_schema(). Collects changes, applies on commit().
    """

    __slots__ = ("_table", "_changes")

    def __init__(self, table: Table) -> None:
        self._table = table
        self._changes: list[_SchemaChange] = []

    def add_column(
        self,
        name: str,
        field_type: DucklakeType,
        doc: str | None = None,
        required: bool = False,
    ) -> UpdateSchema:
        """Add a column. Returns self for chaining."""
        self._changes.append(_AddColumn(name=name, field_type=field_type, doc=doc, required=required))
        return self

    def drop_column(self, name: str) -> UpdateSchema:
        """Drop a column. Returns self for chaining."""
        self._changes.append(_DropColumn(name=name))
        return self

    def rename_column(self, name: str, new_name: str) -> UpdateSchema:
        """Rename a column. Returns self for chaining."""
        self._changes.append(_RenameColumn(name=name, new_name=new_name))
        return self

    def update_column(self, name: str, new_type: DucklakeType) -> UpdateSchema:
        """Change column type (lossless promotions only). Returns self for chaining."""
        self._changes.append(_UpdateColumnType(name=name, new_type=new_type))
        return self

    def set_nullability(self, name: str, required: bool) -> UpdateSchema:
        """Set or drop NOT NULL. Returns self for chaining."""
        self._changes.append(_SetNullability(name=name, required=required))
        return self

    def commit(self) -> None:
        """Execute all pending schema changes as ALTER TABLE statements.

        Refreshes the table schema afterward.
        """
        fqn = self._table.fully_qualified_name
        conn = self._table.catalog.connection

        for change in self._changes:
            sql = self._change_to_sql(fqn, change)
            conn.execute(sql)

        self._changes.clear()
        self._table.refresh()

    def _change_to_sql(self, fqn: str, change: _SchemaChange) -> str:
        if isinstance(change, _AddColumn):
            type_sql = ducklake_type_to_sql(change.field_type)
            sql = f"ALTER TABLE {fqn} ADD COLUMN {quote_identifier(change.name)} {type_sql}"
            if change.required:
                sql += " NOT NULL"
            return sql
        if isinstance(change, _DropColumn):
            return f"ALTER TABLE {fqn} DROP COLUMN {quote_identifier(change.name)}"
        if isinstance(change, _RenameColumn):
            return f"ALTER TABLE {fqn} RENAME {quote_identifier(change.name)} TO {quote_identifier(change.new_name)}"
        if isinstance(change, _UpdateColumnType):
            type_sql = ducklake_type_to_sql(change.new_type)
            return f"ALTER TABLE {fqn} ALTER {quote_identifier(change.name)} SET TYPE {type_sql}"
        # At this point, change must be _SetNullability (exhaustive match)
        if change.required:
            return f"ALTER TABLE {fqn} ALTER {quote_identifier(change.name)} SET NOT NULL"
        return f"ALTER TABLE {fqn} ALTER {quote_identifier(change.name)} DROP NOT NULL"

    # -- Context manager -------------------------------------------------------

    def __enter__(self) -> UpdateSchema:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Auto-commit on clean exit."""
        if exc_type is None:
            self.commit()
