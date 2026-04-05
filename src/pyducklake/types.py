"""Ducklake type system.

Maps between Python type representations, DuckDB SQL type strings, and PyArrow types.
"""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownArgumentType=false

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

import pyarrow as pa

__all__ = [
    "DucklakeType",
    "BooleanType",
    "TinyIntType",
    "SmallIntType",
    "IntegerType",
    "BigIntType",
    "HugeIntType",
    "UTinyIntType",
    "USmallIntType",
    "UIntegerType",
    "UBigIntType",
    "FloatType",
    "DoubleType",
    "StringType",
    "BinaryType",
    "DateType",
    "TimeType",
    "TimestampType",
    "TimestampTZType",
    "UUIDType",
    "JSONType",
    "IntervalType",
    "DecimalType",
    "NestedField",
    "StructType",
    "ListType",
    "MapType",
    "ducklake_type_to_arrow",
    "arrow_type_to_ducklake",
    "ducklake_type_to_sql",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class DucklakeType(ABC):
    """Abstract base class for all Ducklake types."""

    __slots__ = ()

    @abstractmethod
    def __repr__(self) -> str: ...

    def __str__(self) -> str:
        return repr(self)


# ---------------------------------------------------------------------------
# Primitive types (singletons via __new__)
# ---------------------------------------------------------------------------

_primitive_instances: dict[type[PrimitiveType], PrimitiveType] = {}


class PrimitiveType(DucklakeType):
    """Base for singleton primitive types."""

    __slots__ = ()

    def __new__(cls) -> PrimitiveType:
        inst = _primitive_instances.get(cls)
        if inst is None:
            inst = super().__new__(cls)
            _primitive_instances[cls] = inst
        return inst

    @property
    @abstractmethod
    def _name(self) -> str: ...

    def __repr__(self) -> str:
        return f"{self._name}()"

    def __eq__(self, other: object) -> bool:
        return self is other or type(self) is type(other)

    def __hash__(self) -> int:
        return hash(type(self))


class BooleanType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "BooleanType"


class TinyIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "TinyIntType"


class SmallIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "SmallIntType"


class IntegerType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "IntegerType"


class BigIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "BigIntType"


class HugeIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "HugeIntType"


class UTinyIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "UTinyIntType"


class USmallIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "USmallIntType"


class UIntegerType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "UIntegerType"


class UBigIntType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "UBigIntType"


class FloatType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "FloatType"


class DoubleType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "DoubleType"


class StringType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "StringType"


class BinaryType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "BinaryType"


class DateType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "DateType"


class TimeType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "TimeType"


class TimestampType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "TimestampType"


class TimestampTZType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "TimestampTZType"


class UUIDType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "UUIDType"


class JSONType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "JSONType"


class IntervalType(PrimitiveType):
    __slots__ = ()

    @property
    def _name(self) -> str:
        return "IntervalType"


# ---------------------------------------------------------------------------
# Parameterized types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecimalType(DucklakeType):
    """DECIMAL(precision, scale)."""

    precision: int
    scale: int

    def __post_init__(self) -> None:
        if self.precision < 1 or self.precision > 38:
            raise ValueError(f"Decimal precision must be between 1 and 38, got {self.precision}")
        if self.scale < 0:
            raise ValueError(f"Decimal scale must be non-negative, got {self.scale}")
        if self.scale > self.precision:
            raise ValueError(f"Decimal scale ({self.scale}) cannot exceed precision ({self.precision})")

    def __repr__(self) -> str:
        return f"DecimalType(precision={self.precision}, scale={self.scale})"


# ---------------------------------------------------------------------------
# Nested types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NestedField:
    """A named field within a struct, with an ID and optional documentation."""

    field_id: int
    name: str
    field_type: DucklakeType
    required: bool = False
    doc: str | None = None

    def __repr__(self) -> str:
        opt = "required" if self.required else "optional"
        return f"NestedField(field_id={self.field_id}, name={self.name!r}, type={self.field_type}, {opt})"


@dataclass(frozen=True, slots=True)
class StructType(DucklakeType):
    """STRUCT type containing named fields."""

    fields: tuple[NestedField, ...]

    def __iter__(self) -> Iterator[NestedField]:
        return iter(self.fields)

    def __repr__(self) -> str:
        field_strs = ", ".join(repr(f) for f in self.fields)
        return f"StructType(fields=({field_strs}))"


@dataclass(frozen=True, slots=True)
class ListType(DucklakeType):
    """LIST type with a typed element."""

    element_id: int
    element_type: DucklakeType
    element_required: bool = True

    def __repr__(self) -> str:
        return (
            f"ListType(element_id={self.element_id}, element_type={self.element_type}, "
            f"element_required={self.element_required})"
        )


@dataclass(frozen=True, slots=True)
class MapType(DucklakeType):
    """MAP type with typed key and value."""

    key_id: int
    key_type: DucklakeType
    value_id: int
    value_type: DucklakeType
    value_required: bool = True

    def __repr__(self) -> str:
        return (
            f"MapType(key_id={self.key_id}, key_type={self.key_type}, "
            f"value_id={self.value_id}, value_type={self.value_type}, "
            f"value_required={self.value_required})"
        )


# ---------------------------------------------------------------------------
# Conversion: DucklakeType → PyArrow
# ---------------------------------------------------------------------------

_PRIMITIVE_TO_ARROW: dict[type[PrimitiveType], pa.DataType] = {
    BooleanType: pa.bool_(),
    TinyIntType: pa.int8(),
    SmallIntType: pa.int16(),
    IntegerType: pa.int32(),
    BigIntType: pa.int64(),
    HugeIntType: pa.decimal128(38, 0),
    UTinyIntType: pa.uint8(),
    USmallIntType: pa.uint16(),
    UIntegerType: pa.uint32(),
    UBigIntType: pa.uint64(),
    FloatType: pa.float32(),
    DoubleType: pa.float64(),
    StringType: pa.string(),
    BinaryType: pa.binary(),
    DateType: pa.date32(),
    TimeType: pa.time64("us"),
    TimestampType: pa.timestamp("us"),
    TimestampTZType: pa.timestamp("us", tz="UTC"),
    UUIDType: pa.string(),
    JSONType: pa.string(),
    IntervalType: pa.month_day_nano_interval(),
}


def ducklake_type_to_arrow(t: DucklakeType) -> pa.DataType:
    """Convert a DucklakeType to a PyArrow DataType."""
    if isinstance(t, PrimitiveType):
        arrow_type = _PRIMITIVE_TO_ARROW.get(type(t))
        if arrow_type is not None:
            return arrow_type

    if isinstance(t, DecimalType):
        return pa.decimal128(t.precision, t.scale)

    if isinstance(t, ListType):
        element_arrow = ducklake_type_to_arrow(t.element_type)
        return pa.list_(pa.field("element", element_arrow, nullable=not t.element_required))

    if isinstance(t, MapType):
        key_arrow = ducklake_type_to_arrow(t.key_type)
        value_arrow = ducklake_type_to_arrow(t.value_type)
        return pa.map_(key_arrow, pa.field("value", value_arrow, nullable=not t.value_required))  # type: ignore[call-overload, no-any-return]

    if isinstance(t, StructType):
        fields = [pa.field(f.name, ducklake_type_to_arrow(f.field_type), nullable=not f.required) for f in t.fields]
        return pa.struct(fields)

    raise TypeError(f"Cannot convert {t} to Arrow type")


# ---------------------------------------------------------------------------
# Conversion: PyArrow → DucklakeType
# ---------------------------------------------------------------------------

_ARROW_TO_PRIMITIVE: dict[pa.DataType, DucklakeType] = {  # pyright: ignore[reportAssignmentType]
    pa.bool_(): BooleanType(),
    pa.int8(): TinyIntType(),
    pa.int16(): SmallIntType(),
    pa.int32(): IntegerType(),
    pa.int64(): BigIntType(),
    pa.uint8(): UTinyIntType(),
    pa.uint16(): USmallIntType(),
    pa.uint32(): UIntegerType(),
    pa.uint64(): UBigIntType(),
    pa.float32(): FloatType(),
    pa.float64(): DoubleType(),
    pa.string(): StringType(),
    pa.binary(): BinaryType(),
    pa.date32(): DateType(),
    pa.time64("us"): TimeType(),
    pa.month_day_nano_interval(): IntervalType(),
}


def arrow_type_to_ducklake(t: pa.DataType) -> DucklakeType:
    """Convert a PyArrow DataType to a DucklakeType."""
    direct = _ARROW_TO_PRIMITIVE.get(t)
    if direct is not None:
        return direct

    if isinstance(t, pa.Decimal128Type):
        prec: int = cast(int, t.precision)
        sc: int = cast(int, t.scale)
        if prec == 38 and sc == 0:
            return HugeIntType()
        return DecimalType(prec, sc)

    if isinstance(t, pa.TimestampType):
        unit: str = cast(str, t.unit)
        tz: str | None = cast("str | None", t.tz)
        if unit != "us":
            raise TypeError(f"Unsupported timestamp unit '{unit}': only 'us' (microsecond) is supported")
        if tz is not None and tz != "UTC":
            raise TypeError(f"Unsupported timezone '{tz}': only UTC is supported")
        if tz is None:
            return TimestampType()
        return TimestampTZType()

    if isinstance(t, pa.ListType):
        elem_type = arrow_type_to_ducklake(t.value_type)
        return ListType(element_id=0, element_type=elem_type, element_required=not t.value_field.nullable)

    if isinstance(t, pa.MapType):
        key_type = arrow_type_to_ducklake(t.key_type)
        value_type = arrow_type_to_ducklake(t.item_type)
        return MapType(
            key_id=0,
            key_type=key_type,
            value_id=0,
            value_type=value_type,
            value_required=not t.item_field.nullable,
        )

    if isinstance(t, pa.StructType):
        fields = tuple(
            NestedField(
                field_id=i,
                name=t.field(i).name,
                field_type=arrow_type_to_ducklake(t.field(i).type),
                required=not t.field(i).nullable,
            )
            for i in range(t.num_fields)
        )
        return StructType(fields)

    raise TypeError(f"Cannot convert Arrow type {t} to DucklakeType")


# ---------------------------------------------------------------------------
# Conversion: DucklakeType → SQL string
# ---------------------------------------------------------------------------

_PRIMITIVE_TO_SQL: dict[type[PrimitiveType], str] = {
    BooleanType: "BOOLEAN",
    TinyIntType: "TINYINT",
    SmallIntType: "SMALLINT",
    IntegerType: "INTEGER",
    BigIntType: "BIGINT",
    HugeIntType: "HUGEINT",
    UTinyIntType: "UTINYINT",
    USmallIntType: "USMALLINT",
    UIntegerType: "UINTEGER",
    UBigIntType: "UBIGINT",
    FloatType: "FLOAT",
    DoubleType: "DOUBLE",
    StringType: "VARCHAR",
    BinaryType: "BLOB",
    DateType: "DATE",
    TimeType: "TIME",
    TimestampType: "TIMESTAMP",
    TimestampTZType: "TIMESTAMPTZ",
    UUIDType: "UUID",
    JSONType: "JSON",
    IntervalType: "INTERVAL",
}


def ducklake_type_to_sql(t: DucklakeType) -> str:
    """Convert a DucklakeType to a DuckDB SQL type string."""
    if isinstance(t, PrimitiveType):
        sql = _PRIMITIVE_TO_SQL.get(type(t))
        if sql is not None:
            return sql

    if isinstance(t, DecimalType):
        return f"DECIMAL({t.precision}, {t.scale})"

    if isinstance(t, ListType):
        return f"{ducklake_type_to_sql(t.element_type)}[]"

    if isinstance(t, MapType):
        return f"MAP({ducklake_type_to_sql(t.key_type)}, {ducklake_type_to_sql(t.value_type)})"

    if isinstance(t, StructType):
        field_strs = ", ".join(
            f'"{f.name.replace(chr(34), chr(34) + chr(34))}" {ducklake_type_to_sql(f.field_type)}' for f in t.fields
        )
        return f"STRUCT({field_strs})"

    raise TypeError(f"Cannot convert {t} to SQL string")
