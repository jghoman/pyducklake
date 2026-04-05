"""Sort order management for Ducklake tables."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyducklake.table import Table

__all__ = [
    "SortDirection",
    "NullOrder",
    "SortField",
    "SortOrder",
    "UNSORTED",
    "UpdateSortOrder",
]


class SortDirection(Enum):
    """Sort direction for a sort field."""

    ASC = "ASC"
    DESC = "DESC"


class NullOrder(Enum):
    """Null ordering for a sort field."""

    NULLS_FIRST = "NULLS FIRST"
    NULLS_LAST = "NULLS LAST"


def _quote_identifier(name: str) -> str:
    """Double-quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class SortField:
    """A single sort field."""

    source_column: str
    direction: SortDirection = SortDirection.ASC
    null_order: NullOrder = NullOrder.NULLS_LAST

    def to_sql(self) -> str:
        """E.g. '"col_name" ASC NULLS LAST'"""
        return f"{_quote_identifier(self.source_column)} {self.direction.value} {self.null_order.value}"


@dataclass(frozen=True)
class SortOrder:
    """Sort order specification for a table."""

    fields: tuple[SortField, ...] = ()

    @property
    def is_unsorted(self) -> bool:
        return len(self.fields) == 0

    def __repr__(self) -> str:
        if self.is_unsorted:
            return "SortOrder(UNSORTED)"
        fields_repr = ", ".join(f.to_sql() for f in self.fields)
        return f"SortOrder({fields_repr})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SortOrder):
            return NotImplemented
        return self.fields == other.fields

    def __hash__(self) -> int:
        return hash(self.fields)


UNSORTED = SortOrder()


class UpdateSortOrder:
    """Builder for sort order changes. Obtained via table.update_sort_order()."""

    __slots__ = ("_table", "_fields", "_clear")

    def __init__(self, table: Table) -> None:
        self._table = table
        self._fields: list[SortField] = []
        self._clear = False

    def add_field(
        self,
        source_column: str,
        direction: SortDirection = SortDirection.ASC,
        null_order: NullOrder = NullOrder.NULLS_LAST,
    ) -> UpdateSortOrder:
        """Add a sort field. Returns self for chaining."""
        self._fields.append(
            SortField(
                source_column=source_column,
                direction=direction,
                null_order=null_order,
            )
        )
        return self

    def clear(self) -> UpdateSortOrder:
        """Remove all sorting (RESET SORTED BY). Returns self."""
        self._clear = True
        self._fields.clear()
        return self

    def commit(self) -> None:
        """Apply sort order changes via ALTER TABLE."""
        fqn = self._table.fully_qualified_name
        conn = self._table.catalog.connection

        if self._clear:
            conn.execute(f"ALTER TABLE {fqn} RESET SORTED BY")
        elif self._fields:
            parts = [f.to_sql() for f in self._fields]
            sort_expr = ", ".join(parts)
            conn.execute(f"ALTER TABLE {fqn} SET SORTED BY ({sort_expr})")

        self._fields.clear()
        self._clear = False
        self._table.refresh()

    # -- Context manager -------------------------------------------------------

    def __enter__(self) -> UpdateSortOrder:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Auto-commit on clean exit."""
        if exc_type is None:
            self.commit()
