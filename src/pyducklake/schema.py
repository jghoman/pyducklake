"""Ducklake schema representation.

A Schema wraps a collection of top-level NestedFields and provides utility
methods for field lookup, projection, and Arrow conversion.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

from pyducklake.types import DucklakeType, NestedField, StructType, ducklake_type_to_arrow

__all__ = ["Schema", "optional", "required"]


_SENTINEL_FIELD_ID = -1


def required(name: str, field_type: DucklakeType, doc: str | None = None) -> NestedField:
    """Create a required field for use with :meth:`Schema.of`.

    The ``field_id`` is set to a sentinel value and will be auto-assigned
    by :meth:`Schema.of`.

    Args:
        name: Column name.
        field_type: Column type.
        doc: Optional documentation string.

    Example:
        >>> from pyducklake import Schema, required, optional, IntegerType, StringType
        >>> schema = Schema.of(
        ...     required("id", IntegerType()),
        ...     optional("name", StringType()),
        ... )
    """
    return NestedField(field_id=_SENTINEL_FIELD_ID, name=name, field_type=field_type, required=True, doc=doc)


def optional(name: str, field_type: DucklakeType, doc: str | None = None) -> NestedField:
    """Create an optional (nullable) field for use with :meth:`Schema.of`.

    The ``field_id`` is set to a sentinel value and will be auto-assigned
    by :meth:`Schema.of`.

    Args:
        name: Column name.
        field_type: Column type.
        doc: Optional documentation string.

    Example:
        >>> schema = Schema.of(
        ...     required("id", IntegerType()),
        ...     optional("name", StringType()),
        ... )
    """
    return NestedField(field_id=_SENTINEL_FIELD_ID, name=name, field_type=field_type, required=False, doc=doc)


class Schema:
    """Represents a Ducklake table schema."""

    __slots__ = ("_fields", "_schema_id", "_name_to_field", "_id_to_field")

    def __init__(self, *fields: NestedField, schema_id: int = 0) -> None:
        # Validate uniqueness
        seen_names: set[str] = set()
        seen_ids: set[int] = set()
        name_index: dict[str, NestedField] = {}
        id_index: dict[int, NestedField] = {}

        for f in fields:
            if f.name in seen_names:
                raise ValueError(f"Duplicate field name: {f.name!r}")
            if f.field_id in seen_ids:
                raise ValueError(f"Duplicate field_id: {f.field_id}")
            seen_names.add(f.name)
            seen_ids.add(f.field_id)
            name_index[f.name] = f
            id_index[f.field_id] = f

        self._fields = fields
        self._schema_id = schema_id
        self._name_to_field = name_index
        self._id_to_field = id_index

    # -- Alternate constructors ----------------------------------------------

    @classmethod
    def of(cls, *args: NestedField | dict[str, DucklakeType]) -> Schema:
        """Create a Schema with auto-assigned field IDs.

        Accepts either :class:`NestedField` objects (from :func:`required` /
        :func:`optional`) or a single *dict* mapping column names to types.

        Examples:
            Using ``required()`` and ``optional()`` helpers::

                schema = Schema.of(
                    required("id", IntegerType()),
                    optional("name", StringType()),
                    optional("value", DoubleType()),
                )

            Using a dict (all fields optional by default)::

                schema = Schema.of({"id": IntegerType(), "name": StringType()})

            Mix is not allowed — either all NestedField or a single dict.

        Args:
            *args: Either NestedField objects or a single dict[str, DucklakeType].

        Returns:
            A new Schema with field IDs assigned sequentially starting from 1.

        Raises:
            TypeError: If args are mixed types or invalid.
            ValueError: If duplicate field names are provided.
        """
        if not args:
            raise TypeError("Schema.of() requires at least one argument")

        # Single-dict form
        if len(args) == 1 and isinstance(args[0], dict):
            mapping = args[0]
            fields: list[NestedField] = []
            for i, (name, ftype) in enumerate(mapping.items(), start=1):
                if not isinstance(ftype, DucklakeType):  # pyright: ignore[reportUnnecessaryIsInstance]
                    raise TypeError(
                        f"Dict values must be DucklakeType instances, got {type(ftype).__name__} for key {name!r}"
                    )
                fields.append(NestedField(field_id=i, name=name, field_type=ftype, required=False))
            return cls(*fields)

        # NestedField form — validate types first
        raw_fields: list[NestedField] = []
        for arg in args:
            if not isinstance(arg, NestedField):
                raise TypeError(
                    f"Schema.of() arguments must be all NestedField or a single dict, got {type(arg).__name__}"
                )
            raw_fields.append(arg)

        # 1. Collect all explicit (non-sentinel) field_ids
        explicit_ids = {f.field_id for f in raw_fields if f.field_id != _SENTINEL_FIELD_ID}
        if len(explicit_ids) != sum(1 for f in raw_fields if f.field_id != _SENTINEL_FIELD_ID):
            raise ValueError("Duplicate explicit field_ids")

        # 2. Auto-assign sentinel fields, skipping explicit IDs
        next_id = 1
        resolved: list[NestedField] = []
        for f in raw_fields:
            if f.field_id == _SENTINEL_FIELD_ID:
                while next_id in explicit_ids:
                    next_id += 1
                resolved.append(
                    NestedField(
                        field_id=next_id,
                        name=f.name,
                        field_type=f.field_type,
                        required=f.required,
                        doc=f.doc,
                    )
                )
                next_id += 1
            else:
                resolved.append(f)

        return cls(*resolved)

    # -- Properties ----------------------------------------------------------

    @property
    def fields(self) -> tuple[NestedField, ...]:
        return self._fields

    @property
    def schema_id(self) -> int:
        return self._schema_id

    # -- Field lookup --------------------------------------------------------

    def find_field(self, name_or_id: str | int, case_sensitive: bool = True) -> NestedField:
        """Find a field by name or field_id. Raises ValueError if not found."""
        if isinstance(name_or_id, int):
            field = self._id_to_field.get(name_or_id)
            if field is None:
                raise ValueError(f"Field with field_id {name_or_id} not found")
            return field

        if case_sensitive:
            field = self._name_to_field.get(name_or_id)
            if field is None:
                raise ValueError(f"Field {name_or_id!r} not found")
            return field

        # Case-insensitive
        lower = name_or_id.lower()
        for f in self._fields:
            if f.name.lower() == lower:
                return f
        raise ValueError(f"Field {name_or_id!r} not found")

    def find_type(self, name_or_id: str | int, case_sensitive: bool = True) -> DucklakeType:
        """Find a field's type by name or field_id."""
        return self.find_field(name_or_id, case_sensitive=case_sensitive).field_type

    def find_column_name(self, field_id: int) -> str | None:
        """Find a column name by field_id. Returns None if not found."""
        field = self._id_to_field.get(field_id)
        return field.name if field is not None else None

    # -- Accessors -----------------------------------------------------------

    def column_names(self) -> list[str]:
        return [f.name for f in self._fields]

    def field_ids(self) -> set[int]:
        return {f.field_id for f in self._fields}

    @property
    def highest_field_id(self) -> int:
        """Highest field_id in the schema (useful for generating new IDs)."""
        if not self._fields:
            return 0
        return max(f.field_id for f in self._fields)

    # -- Conversion ----------------------------------------------------------

    def as_struct(self) -> StructType:
        """Convert to a StructType."""
        return StructType(fields=self._fields)

    def as_arrow(self) -> pa.Schema:
        """Convert to a PyArrow Schema.

        Each field carries ``PARQUET:field_id`` metadata matching Ducklake/Iceberg
        conventions for Parquet files.
        """
        arrow_fields: list[pa.Field[pa.DataType]] = []
        for f in self._fields:
            arrow_type = ducklake_type_to_arrow(f.field_type)
            arrow_field = pa.field(
                f.name,
                arrow_type,
                nullable=not f.required,
                metadata={b"PARQUET:field_id": str(f.field_id).encode()},
            )
            arrow_fields.append(arrow_field)
        return pa.schema(arrow_fields)

    # -- Projection ----------------------------------------------------------

    def select(self, *names: str, case_sensitive: bool = True) -> Schema:
        """Return a new Schema with only the selected columns.

        Field order is preserved from the original schema.
        Raises ValueError for unknown names.
        """
        # Validate all names exist first
        if case_sensitive:
            selected: set[str] = set()
            for n in names:
                if n not in self._name_to_field:
                    raise ValueError(f"Field {n!r} not found")
                selected.add(n)
            kept = tuple(f for f in self._fields if f.name in selected)
        else:
            lower_names = {n.lower() for n in names}
            # Validate
            known_lower = {f.name.lower() for f in self._fields}
            for n in names:
                if n.lower() not in known_lower:
                    raise ValueError(f"Field {n!r} not found")
            kept = tuple(f for f in self._fields if f.name.lower() in lower_names)

        return Schema(*kept, schema_id=self._schema_id)

    # -- Standard methods ----------------------------------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return NotImplemented
        return self._fields == other._fields and self._schema_id == other._schema_id

    def __repr__(self) -> str:
        fields_repr = ", ".join(repr(f) for f in self._fields)
        return f"Schema(fields=({fields_repr}), schema_id={self._schema_id})"

    def __len__(self) -> int:
        return len(self._fields)

    def __iter__(self) -> Iterator[NestedField]:
        return iter(self._fields)
