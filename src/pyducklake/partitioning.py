"""Partition transforms and spec management for Ducklake tables."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyducklake.table import Table

__all__ = [
    "Transform",
    "IdentityTransform",
    "YearTransform",
    "MonthTransform",
    "DayTransform",
    "HourTransform",
    "IDENTITY",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "PartitionField",
    "PartitionSpec",
    "UNPARTITIONED",
    "UpdateSpec",
]


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


class Transform(ABC):
    """Base for partition transforms."""

    __slots__ = ()

    @abstractmethod
    def to_sql(self) -> str:
        """Return SQL function name for use in SET PARTITIONED BY.

        Identity returns empty string (no function wrapper).
        """

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __hash__(self) -> int:
        return hash(type(self))

    @abstractmethod
    def __repr__(self) -> str: ...


class IdentityTransform(Transform):
    """Identity transform -- partition by raw column value."""

    __slots__ = ()

    def to_sql(self) -> str:
        return ""

    def __repr__(self) -> str:
        return "IdentityTransform()"


class YearTransform(Transform):
    """Extract year from a date/timestamp column."""

    __slots__ = ()

    def to_sql(self) -> str:
        return "year"

    def __repr__(self) -> str:
        return "YearTransform()"


class MonthTransform(Transform):
    """Extract month from a date/timestamp column."""

    __slots__ = ()

    def to_sql(self) -> str:
        return "month"

    def __repr__(self) -> str:
        return "MonthTransform()"


class DayTransform(Transform):
    """Extract day from a date/timestamp column."""

    __slots__ = ()

    def to_sql(self) -> str:
        return "day"

    def __repr__(self) -> str:
        return "DayTransform()"


class HourTransform(Transform):
    """Extract hour from a timestamp column."""

    __slots__ = ()

    def to_sql(self) -> str:
        return "hour"

    def __repr__(self) -> str:
        return "HourTransform()"


# Convenience singletons
IDENTITY = IdentityTransform()
YEAR = YearTransform()
MONTH = MonthTransform()
DAY = DayTransform()
HOUR = HourTransform()


# ---------------------------------------------------------------------------
# PartitionField / PartitionSpec
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PartitionField:
    """A single partition field: a source column plus a transform."""

    source_column: str
    transform: Transform


class PartitionSpec:
    """Partition specification for a table."""

    __slots__ = ("_fields",)

    def __init__(self, *fields: PartitionField) -> None:
        self._fields = fields

    @property
    def fields(self) -> tuple[PartitionField, ...]:
        return self._fields

    @property
    def is_unpartitioned(self) -> bool:
        return len(self._fields) == 0

    def __repr__(self) -> str:
        if self.is_unpartitioned:
            return "PartitionSpec(UNPARTITIONED)"
        fields_repr = ", ".join(
            f"{f.transform.to_sql()}({f.source_column!r})" if f.transform.to_sql() else f.source_column
            for f in self._fields
        )
        return f"PartitionSpec({fields_repr})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PartitionSpec):
            return NotImplemented
        return self._fields == other._fields

    def __hash__(self) -> int:
        return hash(self._fields)


UNPARTITIONED = PartitionSpec()


# ---------------------------------------------------------------------------
# UpdateSpec builder
# ---------------------------------------------------------------------------


def _quote_identifier(name: str) -> str:
    """Double-quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


class UpdateSpec:
    """Builder for partition spec changes.

    Obtained via table.update_spec().
    """

    __slots__ = ("_table", "_fields", "_clear")

    def __init__(self, table: Table) -> None:
        self._table = table
        self._fields: list[PartitionField] = []
        self._clear = False

    def add_field(
        self,
        source_column: str,
        transform: Transform = IDENTITY,
    ) -> UpdateSpec:
        """Add a partition field."""
        self._fields.append(PartitionField(source_column=source_column, transform=transform))
        return self

    def clear(self) -> UpdateSpec:
        """Remove all partitioning (RESET PARTITIONED BY)."""
        self._clear = True
        self._fields.clear()
        return self

    def commit(self) -> None:
        """Apply partition changes via ALTER TABLE."""
        fqn = self._table.fully_qualified_name
        conn = self._table.catalog.connection

        if self._clear:
            conn.execute(f"ALTER TABLE {fqn} RESET PARTITIONED BY")
        elif self._fields:
            parts: list[str] = []
            for field in self._fields:
                col = _quote_identifier(field.source_column)
                func = field.transform.to_sql()
                if func:
                    parts.append(f"{func}({col})")
                else:
                    parts.append(col)
            partition_expr = ", ".join(parts)
            conn.execute(f"ALTER TABLE {fqn} SET PARTITIONED BY ({partition_expr})")

        self._fields.clear()
        self._clear = False
        self._table.refresh()

    # -- Context manager -------------------------------------------------------

    def __enter__(self) -> UpdateSpec:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Auto-commit on clean exit."""
        if exc_type is None:
            self.commit()
