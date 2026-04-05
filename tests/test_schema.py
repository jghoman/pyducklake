"""Tests for pyducklake.schema."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest

from pyducklake.schema import Schema, optional, required
from pyducklake.types import (
    BigIntType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    HugeIntType,
    IntegerType,
    IntervalType,
    JSONType,
    ListType,
    MapType,
    NestedField,
    SmallIntType,
    StringType,
    StructType,
    TimestampType,
    TimestampTZType,
    TimeType,
    TinyIntType,
    UBigIntType,
    UIntegerType,
    USmallIntType,
    UTinyIntType,
    UUIDType,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _id_field() -> NestedField:
    return NestedField(field_id=1, name="id", field_type=IntegerType(), required=True)


def _name_field() -> NestedField:
    return NestedField(field_id=2, name="name", field_type=StringType())


def _age_field() -> NestedField:
    return NestedField(field_id=3, name="age", field_type=BigIntType())


def _score_field() -> NestedField:
    return NestedField(field_id=4, name="score", field_type=DoubleType())


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_empty_schema(self) -> None:
        s = Schema()
        assert len(s) == 0
        assert s.fields == ()
        assert s.schema_id == 0

    def test_single_field(self) -> None:
        s = Schema(_id_field())
        assert len(s) == 1
        assert s.fields == (_id_field(),)

    def test_multiple_fields(self) -> None:
        s = Schema(_id_field(), _name_field(), _age_field())
        assert len(s) == 3

    def test_custom_schema_id(self) -> None:
        s = Schema(_id_field(), schema_id=42)
        assert s.schema_id == 42


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------


class TestDuplicateDetection:
    def test_duplicate_field_names_raises(self) -> None:
        f1 = NestedField(field_id=1, name="x", field_type=IntegerType())
        f2 = NestedField(field_id=2, name="x", field_type=StringType())
        with pytest.raises(ValueError, match="Duplicate field name"):
            Schema(f1, f2)

    def test_duplicate_field_ids_raises(self) -> None:
        f1 = NestedField(field_id=1, name="a", field_type=IntegerType())
        f2 = NestedField(field_id=1, name="b", field_type=StringType())
        with pytest.raises(ValueError, match="Duplicate field_id"):
            Schema(f1, f2)


# ---------------------------------------------------------------------------
# find_field
# ---------------------------------------------------------------------------


class TestFindField:
    def test_by_name(self) -> None:
        s = Schema(_id_field(), _name_field())
        assert s.find_field("name") == _name_field()

    def test_by_field_id(self) -> None:
        s = Schema(_id_field(), _name_field())
        assert s.find_field(1) == _id_field()

    def test_case_sensitive_not_found(self) -> None:
        s = Schema(_id_field())
        with pytest.raises(ValueError, match="not found"):
            s.find_field("ID", case_sensitive=True)

    def test_case_insensitive(self) -> None:
        s = Schema(_id_field())
        assert s.find_field("ID", case_sensitive=False) == _id_field()

    def test_not_found_by_name(self) -> None:
        s = Schema(_id_field())
        with pytest.raises(ValueError, match="not found"):
            s.find_field("nope")

    def test_not_found_by_id(self) -> None:
        s = Schema(_id_field())
        with pytest.raises(ValueError, match="not found"):
            s.find_field(999)


# ---------------------------------------------------------------------------
# find_type
# ---------------------------------------------------------------------------


class TestFindType:
    def test_returns_type_by_name(self) -> None:
        s = Schema(_id_field(), _name_field())
        assert s.find_type("id") == IntegerType()

    def test_returns_type_by_id(self) -> None:
        s = Schema(_id_field(), _name_field())
        assert s.find_type(2) == StringType()


# ---------------------------------------------------------------------------
# find_column_name
# ---------------------------------------------------------------------------


class TestFindColumnName:
    def test_returns_name(self) -> None:
        s = Schema(_id_field(), _name_field())
        assert s.find_column_name(2) == "name"

    def test_returns_none_for_unknown(self) -> None:
        s = Schema(_id_field())
        assert s.find_column_name(999) is None


# ---------------------------------------------------------------------------
# column_names / field_ids
# ---------------------------------------------------------------------------


class TestAccessors:
    def test_column_names(self) -> None:
        s = Schema(_id_field(), _name_field(), _age_field())
        assert s.column_names() == ["id", "name", "age"]

    def test_field_ids(self) -> None:
        s = Schema(_id_field(), _name_field(), _age_field())
        assert s.field_ids() == {1, 2, 3}


# ---------------------------------------------------------------------------
# highest_field_id
# ---------------------------------------------------------------------------


class TestHighestFieldId:
    def test_correct_value(self) -> None:
        s = Schema(_id_field(), _name_field(), _age_field())
        assert s.highest_field_id == 3

    def test_empty_schema_returns_zero(self) -> None:
        s = Schema()
        assert s.highest_field_id == 0

    def test_non_sequential_ids(self) -> None:
        f1 = NestedField(field_id=5, name="a", field_type=IntegerType())
        f2 = NestedField(field_id=100, name="b", field_type=IntegerType())
        s = Schema(f1, f2)
        assert s.highest_field_id == 100


# ---------------------------------------------------------------------------
# as_struct
# ---------------------------------------------------------------------------


class TestAsStruct:
    def test_converts_to_struct(self) -> None:
        fields = (_id_field(), _name_field())
        s = Schema(*fields)
        st = s.as_struct()
        assert isinstance(st, StructType)
        assert st.fields == fields


# ---------------------------------------------------------------------------
# as_arrow
# ---------------------------------------------------------------------------


class TestAsArrow:
    def test_produces_valid_arrow_schema(self) -> None:
        s = Schema(_id_field(), _name_field())
        arrow = s.as_arrow()
        assert isinstance(arrow, pa.Schema)
        assert len(arrow) == 2
        assert arrow.field("id").type == pa.int32()
        assert arrow.field("name").type == pa.string()

    def test_nullable_from_required(self) -> None:
        s = Schema(_id_field(), _name_field())
        arrow = s.as_arrow()
        assert arrow.field("id").nullable is False  # required=True
        assert arrow.field("name").nullable is True  # required=False

    def test_field_id_metadata(self) -> None:
        s = Schema(_id_field(), _name_field())
        arrow = s.as_arrow()
        meta = arrow.field("id").metadata
        assert meta is not None
        assert meta[b"PARQUET:field_id"] == b"1"
        meta2 = arrow.field("name").metadata
        assert meta2 is not None
        assert meta2[b"PARQUET:field_id"] == b"2"


# ---------------------------------------------------------------------------
# select
# ---------------------------------------------------------------------------


class TestSelect:
    def test_projection(self) -> None:
        s = Schema(_id_field(), _name_field(), _age_field())
        projected = s.select("name", "id")
        # Order should match the original schema, not the select order
        assert projected.column_names() == ["id", "name"]
        assert len(projected) == 2

    def test_unknown_column_raises(self) -> None:
        s = Schema(_id_field(), _name_field())
        with pytest.raises(ValueError, match="not found"):
            s.select("nope")

    def test_case_insensitive_select(self) -> None:
        s = Schema(_id_field(), _name_field())
        projected = s.select("ID", "NAME", case_sensitive=False)
        assert len(projected) == 2

    def test_preserves_schema_id(self) -> None:
        s = Schema(_id_field(), _name_field(), schema_id=7)
        projected = s.select("id")
        assert projected.schema_id == 7


# ---------------------------------------------------------------------------
# Equality
# ---------------------------------------------------------------------------


class TestEquality:
    def test_same_fields_equal(self) -> None:
        s1 = Schema(_id_field(), _name_field())
        s2 = Schema(_id_field(), _name_field())
        assert s1 == s2

    def test_different_fields_not_equal(self) -> None:
        s1 = Schema(_id_field())
        s2 = Schema(_name_field())
        assert s1 != s2

    def test_different_schema_id_not_equal(self) -> None:
        s1 = Schema(_id_field(), schema_id=0)
        s2 = Schema(_id_field(), schema_id=1)
        assert s1 != s2

    def test_not_equal_to_other_type(self) -> None:
        s = Schema(_id_field())
        assert s != "not a schema"


# ---------------------------------------------------------------------------
# len and iter
# ---------------------------------------------------------------------------


class TestLenAndIter:
    def test_len(self) -> None:
        s = Schema(_id_field(), _name_field(), _age_field())
        assert len(s) == 3

    def test_iter(self) -> None:
        fields = (_id_field(), _name_field())
        s = Schema(*fields)
        assert tuple(s) == fields


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_readable(self) -> None:
        s = Schema(_id_field(), _name_field(), schema_id=5)
        r = repr(s)
        assert "Schema" in r
        assert "schema_id=5" in r
        assert "id" in r
        assert "name" in r


# ---------------------------------------------------------------------------
# P1: case insensitive edge cases
# ---------------------------------------------------------------------------


class TestCaseInsensitiveEdgeCases:
    def test_find_field_case_insensitive_not_found_raises(self) -> None:
        s = Schema(_id_field(), _name_field())
        with pytest.raises(ValueError, match="not found"):
            s.find_field("NONEXISTENT", case_sensitive=False)

    def test_select_case_insensitive_unknown_raises(self) -> None:
        s = Schema(_id_field(), _name_field())
        with pytest.raises(ValueError, match="not found"):
            s.select("NONEXISTENT", case_sensitive=False)


# ---------------------------------------------------------------------------
# Schema.of / required / optional
# ---------------------------------------------------------------------------


class TestSchemaOf:
    """Tests for Schema.of(), required(), and optional() helpers."""

    # -- Basic construction --------------------------------------------------

    def test_of_with_required_and_optional(self) -> None:
        s = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
        )
        assert len(s) == 2
        assert s.find_field("id").required is True
        assert s.find_field("name").required is False

    def test_of_auto_assigns_field_ids(self) -> None:
        s = Schema.of(
            required("a", IntegerType()),
            optional("b", StringType()),
            optional("c", DoubleType()),
        )
        assert [f.field_id for f in s.fields] == [1, 2, 3]

    def test_of_required_fields_are_required(self) -> None:
        s = Schema.of(required("x", IntegerType()))
        assert s.find_field("x").required is True

    def test_of_optional_fields_are_optional(self) -> None:
        s = Schema.of(optional("x", IntegerType()))
        assert s.find_field("x").required is False

    def test_of_preserves_field_order(self) -> None:
        s = Schema.of(
            required("c", IntegerType()),
            optional("a", StringType()),
            optional("b", DoubleType()),
        )
        assert s.column_names() == ["c", "a", "b"]

    def test_of_with_doc(self) -> None:
        s = Schema.of(
            required("id", IntegerType(), doc="Primary key"),
            optional("name", StringType(), doc="User name"),
        )
        assert s.find_field("id").doc == "Primary key"
        assert s.find_field("name").doc == "User name"

    # -- Dict form -----------------------------------------------------------

    def test_of_dict_basic(self) -> None:
        s = Schema.of({"a": IntegerType(), "b": StringType()})
        assert len(s) == 2
        assert s.find_type("a") == IntegerType()
        assert s.find_type("b") == StringType()

    def test_of_dict_auto_assigns_ids(self) -> None:
        s = Schema.of({"x": IntegerType(), "y": StringType(), "z": DoubleType()})
        assert [f.field_id for f in s.fields] == [1, 2, 3]

    def test_of_dict_all_optional(self) -> None:
        s = Schema.of({"a": IntegerType(), "b": StringType()})
        for f in s:
            assert f.required is False

    def test_of_dict_preserves_order(self) -> None:
        s = Schema.of({"z": IntegerType(), "a": StringType(), "m": DoubleType()})
        assert s.column_names() == ["z", "a", "m"]

    def test_of_dict_single_field(self) -> None:
        s = Schema.of({"only": IntegerType()})
        assert len(s) == 1
        assert s.find_field("only").field_id == 1

    # -- All types -----------------------------------------------------------

    def test_of_with_all_primitive_types(self) -> None:
        primitives: list[tuple[str, object]] = [
            ("bool", BooleanType()),
            ("i8", TinyIntType()),
            ("i16", SmallIntType()),
            ("i32", IntegerType()),
            ("i64", BigIntType()),
            ("huge", HugeIntType()),
            ("u8", UTinyIntType()),
            ("u16", USmallIntType()),
            ("u32", UIntegerType()),
            ("u64", UBigIntType()),
            ("f32", FloatType()),
            ("f64", DoubleType()),
            ("str", StringType()),
            ("bin", BinaryType()),
            ("date", DateType()),
            ("time", TimeType()),
            ("ts", TimestampType()),
            ("tstz", TimestampTZType()),
            ("uuid", UUIDType()),
            ("json", JSONType()),
            ("interval", IntervalType()),
        ]
        fields = [optional(name, ftype) for name, ftype in primitives]  # type: ignore[arg-type]
        s = Schema.of(*fields)
        assert len(s) == len(primitives)
        for i, (name, ftype) in enumerate(primitives, start=1):
            assert s.find_field(name).field_id == i
            assert s.find_type(name) == ftype

    def test_of_with_decimal_type(self) -> None:
        s = Schema.of(optional("amount", DecimalType(10, 2)))
        assert s.find_type("amount") == DecimalType(10, 2)

    def test_of_with_nested_struct(self) -> None:
        inner = StructType(
            fields=(
                NestedField(field_id=10, name="street", field_type=StringType()),
                NestedField(field_id=11, name="city", field_type=StringType()),
            )
        )
        s = Schema.of(
            required("id", IntegerType()),
            optional("address", inner),
        )
        assert len(s) == 2
        assert isinstance(s.find_type("address"), StructType)

    def test_of_with_list_type(self) -> None:
        lt = ListType(element_id=10, element_type=StringType())
        s = Schema.of(optional("tags", lt))
        assert isinstance(s.find_type("tags"), ListType)

    def test_of_with_map_type(self) -> None:
        mt = MapType(key_id=10, key_type=StringType(), value_id=11, value_type=IntegerType())
        s = Schema.of(optional("props", mt))
        assert isinstance(s.find_type("props"), MapType)

    # -- Equivalence ---------------------------------------------------------

    def test_of_equivalent_to_manual(self) -> None:
        manual = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )
        fluent = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
        )
        assert manual == fluent

    def test_of_schema_works_with_catalog(self, tmp_path: Path) -> None:
        from pyducklake import Catalog

        meta_db = str(tmp_path / "meta.duckdb")
        data_dir = str(tmp_path / "data")
        os.makedirs(data_dir, exist_ok=True)
        catalog = Catalog("test_cat", meta_db, data_path=data_dir)

        schema = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
        )
        table = catalog.create_table("roundtrip", schema)

        df = pa.table({"id": [1, 2], "name": ["a", "b"]})
        table.append(df)

        result = table.scan().to_arrow()
        assert result.num_rows == 2
        assert result.column("id").to_pylist() == [1, 2]
        assert result.column("name").to_pylist() == ["a", "b"]

    def test_of_as_arrow(self) -> None:
        s = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
        )
        arrow = s.as_arrow()
        assert isinstance(arrow, pa.Schema)
        assert arrow.field("id").type == pa.int32()
        assert arrow.field("id").nullable is False
        assert arrow.field("name").type == pa.string()
        assert arrow.field("name").nullable is True

    # -- Error cases ---------------------------------------------------------

    def test_of_empty_raises(self) -> None:
        with pytest.raises(TypeError, match="requires at least one argument"):
            Schema.of()

    def test_of_mixed_dict_and_field_raises(self) -> None:
        with pytest.raises(TypeError, match="must be all NestedField or a single dict"):
            Schema.of({"a": IntegerType()}, required("b", StringType()))  # type: ignore[arg-type]

    def test_of_duplicate_names_raises(self) -> None:
        with pytest.raises(ValueError, match="Duplicate field name"):
            Schema.of(
                required("x", IntegerType()),
                optional("x", StringType()),
            )

    def test_of_invalid_arg_type_raises(self) -> None:
        with pytest.raises(TypeError):
            Schema.of(42)  # type: ignore[arg-type]

    def test_of_dict_with_invalid_value_raises(self) -> None:
        with pytest.raises(TypeError, match="DucklakeType"):
            Schema.of({"a": "not_a_type"})  # type: ignore[dict-item]

    # -- Edge cases ----------------------------------------------------------

    def test_of_single_required_field(self) -> None:
        s = Schema.of(required("only", IntegerType()))
        assert len(s) == 1
        assert s.find_field("only").field_id == 1
        assert s.find_field("only").required is True

    def test_of_many_fields(self) -> None:
        fields = [optional(f"col_{i}", IntegerType()) for i in range(25)]
        s = Schema.of(*fields)
        assert len(s) == 25
        assert [f.field_id for f in s.fields] == list(range(1, 26))

    def test_of_field_with_explicit_id_kept(self) -> None:
        explicit = NestedField(field_id=99, name="explicit", field_type=IntegerType())
        s = Schema.of(
            required("a", StringType()),
            explicit,
            optional("b", DoubleType()),
        )
        assert s.find_field("a").field_id == 1
        assert s.find_field("explicit").field_id == 99
        assert s.find_field("b").field_id == 2

    def test_required_helper_returns_nested_field(self) -> None:
        f = required("x", IntegerType())
        assert isinstance(f, NestedField)
        assert f.required is True

    def test_optional_helper_returns_nested_field(self) -> None:
        f = optional("x", IntegerType())
        assert isinstance(f, NestedField)
        assert f.required is False

    def test_of_schema_select_works(self) -> None:
        s = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
            optional("age", BigIntType()),
        )
        projected = s.select("name")
        assert len(projected) == 1
        assert projected.find_field("name").field_type == StringType()

    # -- P0: field_id collision / auto-assignment tests ----------------------

    def test_of_explicit_id_before_sentinel_adjusts(self) -> None:
        """Explicit field_id=5 first, then sentinels get 1, 2 (skipping 5)."""
        s = Schema.of(
            NestedField(field_id=5, name="x", field_type=IntegerType()),
            optional("a", StringType()),
            optional("b", DoubleType()),
        )
        assert s.find_field("x").field_id == 5
        assert s.find_field("a").field_id == 1
        assert s.find_field("b").field_id == 2

    def test_of_explicit_id_lower_than_auto(self) -> None:
        """Explicit field_id=2 after a sentinel; sentinel gets 1, explicit stays 2, next sentinel gets 3."""
        s = Schema.of(
            optional("a", IntegerType()),
            NestedField(field_id=2, name="b", field_type=StringType()),
            optional("c", DoubleType()),
        )
        assert s.find_field("a").field_id == 1
        assert s.find_field("b").field_id == 2
        assert s.find_field("c").field_id == 3

    def test_of_sentinel_not_leaked(self) -> None:
        """No field in result has field_id == -1."""
        s = Schema.of(
            required("a", IntegerType()),
            optional("b", StringType()),
            optional("c", DoubleType()),
        )
        for f in s:
            assert f.field_id != -1

    def test_of_multiple_explicit_with_gaps(self) -> None:
        """Explicit IDs 10 and 50 with sentinels filling 1, 2, 3..."""
        s = Schema.of(
            optional("a", IntegerType()),
            NestedField(field_id=10, name="b", field_type=StringType()),
            optional("c", DoubleType()),
            NestedField(field_id=50, name="d", field_type=BooleanType()),
            optional("e", FloatType()),
        )
        assert s.find_field("a").field_id == 1
        assert s.find_field("b").field_id == 10
        assert s.find_field("c").field_id == 2
        assert s.find_field("d").field_id == 50
        assert s.find_field("e").field_id == 3

    def test_of_duplicate_explicit_and_auto_collision(self) -> None:
        """Explicit field_id=1 after a sentinel; sentinel skips 1 and gets 2."""
        s = Schema.of(
            optional("a", IntegerType()),
            NestedField(field_id=1, name="b", field_type=StringType()),
        )
        assert s.find_field("a").field_id == 2
        assert s.find_field("b").field_id == 1

    def test_of_duplicate_explicit_ids_raises(self) -> None:
        """Two fields both with explicit field_id=5 raises ValueError."""
        with pytest.raises(ValueError, match="Duplicate explicit field_ids"):
            Schema.of(
                NestedField(field_id=5, name="a", field_type=IntegerType()),
                NestedField(field_id=5, name="b", field_type=StringType()),
            )

    def test_of_as_arrow_field_id_metadata(self) -> None:
        """Arrow schema has correct PARQUET:field_id (not -1)."""
        s = Schema.of(
            optional("a", IntegerType()),
            NestedField(field_id=5, name="b", field_type=StringType()),
            optional("c", DoubleType()),
        )
        arrow = s.as_arrow()
        meta_a = arrow.field("a").metadata
        assert meta_a is not None
        assert meta_a[b"PARQUET:field_id"] == b"1"
        meta_b = arrow.field("b").metadata
        assert meta_b is not None
        assert meta_b[b"PARQUET:field_id"] == b"5"
        meta_c = arrow.field("c").metadata
        assert meta_c is not None
        assert meta_c[b"PARQUET:field_id"] == b"2"

    def test_of_find_field_by_auto_id(self) -> None:
        """find_field(1), find_field(2) work with auto-assigned IDs."""
        s = Schema.of(
            required("a", IntegerType()),
            optional("b", StringType()),
        )
        assert s.find_field(1).name == "a"
        assert s.find_field(2).name == "b"

    def test_of_equivalence_with_doc(self) -> None:
        """Schema.of with doc= equals manual schema with same doc."""
        manual = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True, doc="Primary key"),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False, doc="User name"),
        )
        fluent = Schema.of(
            required("id", IntegerType(), doc="Primary key"),
            optional("name", StringType(), doc="User name"),
        )
        assert manual == fluent

    # -- P1: additional coverage ---------------------------------------------

    def test_of_explicit_id_negative_not_sentinel(self) -> None:
        """field_id=-2 is kept as-is (only -1 is sentinel)."""
        s = Schema.of(
            NestedField(field_id=-2, name="neg", field_type=IntegerType()),
            optional("pos", StringType()),
        )
        assert s.find_field("neg").field_id == -2
        assert s.find_field("pos").field_id == 1

    def test_of_explicit_id_zero(self) -> None:
        """field_id=0 preserved."""
        s = Schema.of(
            NestedField(field_id=0, name="zero", field_type=IntegerType()),
            optional("one", StringType()),
        )
        assert s.find_field("zero").field_id == 0
        assert s.find_field("one").field_id == 1

    def test_of_dict_with_struct(self) -> None:
        """Dict form with StructType value, nested field IDs untouched."""
        inner = StructType(
            fields=(
                NestedField(field_id=100, name="x", field_type=IntegerType()),
                NestedField(field_id=101, name="y", field_type=StringType()),
            )
        )
        s = Schema.of({"data": inner})
        st = s.find_type("data")
        assert isinstance(st, StructType)
        assert st.fields[0].field_id == 100
        assert st.fields[1].field_id == 101

    def test_of_dict_with_list_and_map(self) -> None:
        """Dict form with ListType/MapType."""
        lt = ListType(element_id=10, element_type=StringType())
        mt = MapType(key_id=20, key_type=StringType(), value_id=21, value_type=IntegerType())
        s = Schema.of({"tags": lt, "props": mt})
        assert isinstance(s.find_type("tags"), ListType)
        assert isinstance(s.find_type("props"), MapType)

    def test_of_highest_field_id(self) -> None:
        """Correct max with mixed explicit/auto."""
        s = Schema.of(
            optional("a", IntegerType()),
            NestedField(field_id=50, name="b", field_type=StringType()),
            optional("c", DoubleType()),
        )
        assert s.highest_field_id == 50

    def test_of_field_ids_set(self) -> None:
        """field_ids() returns expected set."""
        s = Schema.of(
            optional("a", IntegerType()),
            NestedField(field_id=10, name="b", field_type=StringType()),
            optional("c", DoubleType()),
        )
        assert s.field_ids() == {1, 10, 2}

    def test_of_as_struct(self) -> None:
        """as_struct() correct."""
        s = Schema.of(
            required("a", IntegerType()),
            optional("b", StringType()),
        )
        st = s.as_struct()
        assert isinstance(st, StructType)
        assert len(st.fields) == 2
        assert st.fields[0].name == "a"
        assert st.fields[0].field_id == 1

    def test_of_select_preserves_ids(self) -> None:
        """select() preserves auto-assigned IDs."""
        s = Schema.of(
            required("a", IntegerType()),
            optional("b", StringType()),
            optional("c", DoubleType()),
        )
        projected = s.select("b", "c")
        assert projected.find_field("b").field_id == 2
        assert projected.find_field("c").field_id == 3

    def test_of_equality_symmetry(self) -> None:
        """Two identical Schema.of() calls are equal."""
        s1 = Schema.of(required("a", IntegerType()), optional("b", StringType()))
        s2 = Schema.of(required("a", IntegerType()), optional("b", StringType()))
        assert s1 == s2

    def test_of_dict_empty(self) -> None:
        """Schema.of({}) produces an empty schema."""
        s = Schema.of({})  # type: ignore[arg-type]
        assert len(s) == 0
        assert s.fields == ()
