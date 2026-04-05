"""Expression system for row-level filters, deletes, and overwrites.

Expressions are immutable, composable, and convertible to DuckDB SQL WHERE clauses.
Follows pyiceberg's expression pattern.
"""

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

__all__ = [
    "AlwaysFalse",
    "AlwaysTrue",
    "And",
    "BooleanExpression",
    "EqualTo",
    "GreaterThan",
    "GreaterThanOrEqual",
    "In",
    "IsNaN",
    "IsNull",
    "LessThan",
    "LessThanOrEqual",
    "Not",
    "NotEqualTo",
    "NotIn",
    "NotNaN",
    "NotNull",
    "Or",
    "Reference",
]


def _format_value(value: Any) -> str:
    """Format a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(value, datetime.date):
        return f"'{value.isoformat()}'"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise TypeError(f"Unsupported type for SQL literal: {type(value).__name__}")


def _quote_column(name: str) -> str:
    """Double-quote a column name, escaping embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


class BooleanExpression(ABC):
    """Base for all filter expressions."""

    @abstractmethod
    def to_sql(self) -> str: ...

    def __and__(self, other: BooleanExpression) -> BooleanExpression:
        return And(self, other)

    def __or__(self, other: BooleanExpression) -> BooleanExpression:
        return Or(self, other)

    def __invert__(self) -> BooleanExpression:
        return Not(self)


class _AlwaysTrue(BooleanExpression):
    """Singleton expression that always evaluates to TRUE."""

    _instance: _AlwaysTrue | None = None

    def __new__(cls) -> _AlwaysTrue:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def to_sql(self) -> str:
        return "TRUE"

    def __repr__(self) -> str:
        return "AlwaysTrue()"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _AlwaysTrue)

    def __hash__(self) -> int:
        return hash(type(self))


class _AlwaysFalse(BooleanExpression):
    """Singleton expression that always evaluates to FALSE."""

    _instance: _AlwaysFalse | None = None

    def __new__(cls) -> _AlwaysFalse:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def to_sql(self) -> str:
        return "FALSE"

    def __repr__(self) -> str:
        return "AlwaysFalse()"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _AlwaysFalse)

    def __hash__(self) -> int:
        return hash(type(self))


def AlwaysTrue() -> _AlwaysTrue:  # noqa: N802
    """Return the AlwaysTrue singleton."""
    return _AlwaysTrue()


def AlwaysFalse() -> _AlwaysFalse:  # noqa: N802
    """Return the AlwaysFalse singleton."""
    return _AlwaysFalse()


@dataclass(frozen=True)
class Reference:
    """Column reference."""

    name: str

    def __repr__(self) -> str:
        return f"Reference('{self.name}')"


@dataclass(frozen=True)
class Not(BooleanExpression):
    """Logical NOT."""

    child: BooleanExpression

    def __new__(cls, child: BooleanExpression) -> BooleanExpression:  # type: ignore[misc]
        if isinstance(child, _AlwaysTrue):
            return AlwaysFalse()
        if isinstance(child, _AlwaysFalse):
            return AlwaysTrue()
        if isinstance(child, Not):
            return child.child
        instance = super().__new__(cls)
        return instance

    def to_sql(self) -> str:
        return f"(NOT {self.child.to_sql()})"

    def __repr__(self) -> str:
        return f"Not(child={self.child!r})"


@dataclass(frozen=True)
class And(BooleanExpression):
    """Logical AND with short-circuit simplification."""

    left: BooleanExpression
    right: BooleanExpression

    def __new__(cls, left: BooleanExpression, right: BooleanExpression) -> BooleanExpression:  # type: ignore[misc]
        if isinstance(left, _AlwaysTrue):
            return right
        if isinstance(right, _AlwaysTrue):
            return left
        if isinstance(left, _AlwaysFalse) or isinstance(right, _AlwaysFalse):
            return AlwaysFalse()
        instance = super().__new__(cls)
        return instance

    def to_sql(self) -> str:
        return f"({self.left.to_sql()} AND {self.right.to_sql()})"

    def __repr__(self) -> str:
        return f"And(left={self.left!r}, right={self.right!r})"


@dataclass(frozen=True)
class Or(BooleanExpression):
    """Logical OR with short-circuit simplification."""

    left: BooleanExpression
    right: BooleanExpression

    def __new__(cls, left: BooleanExpression, right: BooleanExpression) -> BooleanExpression:  # type: ignore[misc]
        if isinstance(left, _AlwaysTrue) or isinstance(right, _AlwaysTrue):
            return AlwaysTrue()
        if isinstance(left, _AlwaysFalse):
            return right
        if isinstance(right, _AlwaysFalse):
            return left
        instance = super().__new__(cls)
        return instance

    def to_sql(self) -> str:
        return f"({self.left.to_sql()} OR {self.right.to_sql()})"

    def __repr__(self) -> str:
        return f"Or(left={self.left!r}, right={self.right!r})"


# --- Comparison predicates ---


@dataclass(frozen=True)
class EqualTo(BooleanExpression):
    """Column equals value."""

    term: str
    value: Any

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} = {_format_value(self.value)}"

    def __repr__(self) -> str:
        return f"EqualTo(term={self.term!r}, value={self.value!r})"


@dataclass(frozen=True)
class NotEqualTo(BooleanExpression):
    """Column not equals value."""

    term: str
    value: Any

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} != {_format_value(self.value)}"

    def __repr__(self) -> str:
        return f"NotEqualTo(term={self.term!r}, value={self.value!r})"


@dataclass(frozen=True)
class GreaterThan(BooleanExpression):
    """Column greater than value."""

    term: str
    value: Any

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} > {_format_value(self.value)}"

    def __repr__(self) -> str:
        return f"GreaterThan(term={self.term!r}, value={self.value!r})"


@dataclass(frozen=True)
class GreaterThanOrEqual(BooleanExpression):
    """Column greater than or equal to value."""

    term: str
    value: Any

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} >= {_format_value(self.value)}"

    def __repr__(self) -> str:
        return f"GreaterThanOrEqual(term={self.term!r}, value={self.value!r})"


@dataclass(frozen=True)
class LessThan(BooleanExpression):
    """Column less than value."""

    term: str
    value: Any

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} < {_format_value(self.value)}"

    def __repr__(self) -> str:
        return f"LessThan(term={self.term!r}, value={self.value!r})"


@dataclass(frozen=True)
class LessThanOrEqual(BooleanExpression):
    """Column less than or equal to value."""

    term: str
    value: Any

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} <= {_format_value(self.value)}"

    def __repr__(self) -> str:
        return f"LessThanOrEqual(term={self.term!r}, value={self.value!r})"


# --- Set predicates ---


@dataclass(frozen=True)
class In(BooleanExpression):
    """Column value in a set of values."""

    term: str
    values: tuple[Any, ...]

    def __new__(cls, term: str, values: tuple[Any, ...]) -> BooleanExpression:  # type: ignore[misc]
        if not values:
            return AlwaysFalse()
        instance = super().__new__(cls)
        return instance

    def to_sql(self) -> str:
        vals = ", ".join(_format_value(v) for v in self.values)
        return f"{_quote_column(self.term)} IN ({vals})"

    def __repr__(self) -> str:
        return f"In(term={self.term!r}, values={self.values!r})"


@dataclass(frozen=True)
class NotIn(BooleanExpression):
    """Column value not in a set of values."""

    term: str
    values: tuple[Any, ...]

    def __new__(cls, term: str, values: tuple[Any, ...]) -> BooleanExpression:  # type: ignore[misc]
        if not values:
            return AlwaysTrue()
        instance = super().__new__(cls)
        return instance

    def to_sql(self) -> str:
        vals = ", ".join(_format_value(v) for v in self.values)
        return f"{_quote_column(self.term)} NOT IN ({vals})"

    def __repr__(self) -> str:
        return f"NotIn(term={self.term!r}, values={self.values!r})"


# --- Unary predicates ---


@dataclass(frozen=True)
class IsNull(BooleanExpression):
    """Column is NULL."""

    term: str

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} IS NULL"

    def __repr__(self) -> str:
        return f"IsNull(term={self.term!r})"


@dataclass(frozen=True)
class NotNull(BooleanExpression):
    """Column is not NULL."""

    term: str

    def to_sql(self) -> str:
        return f"{_quote_column(self.term)} IS NOT NULL"

    def __repr__(self) -> str:
        return f"NotNull(term={self.term!r})"


@dataclass(frozen=True)
class IsNaN(BooleanExpression):
    """Column is NaN."""

    term: str

    def to_sql(self) -> str:
        return f"isnan({_quote_column(self.term)})"

    def __repr__(self) -> str:
        return f"IsNaN(term={self.term!r})"


@dataclass(frozen=True)
class NotNaN(BooleanExpression):
    """Column is not NaN."""

    term: str

    def to_sql(self) -> str:
        return f"NOT isnan({_quote_column(self.term)})"

    def __repr__(self) -> str:
        return f"NotNaN(term={self.term!r})"
