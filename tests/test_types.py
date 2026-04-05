"""Tests for pyducklake type system."""

from __future__ import annotations

import pyarrow as pa
import pytest

from pyducklake.types import (
    BigIntType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    DucklakeType,
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
    arrow_type_to_ducklake,
    ducklake_type_to_arrow,
    ducklake_type_to_sql,
)

# ---------------------------------------------------------------------------
# 1. Singleton behavior
# ---------------------------------------------------------------------------


class TestSingleton:
    @pytest.mark.parametrize(
        "type_cls",
        [
            BooleanType,
            TinyIntType,
            SmallIntType,
            IntegerType,
            BigIntType,
            HugeIntType,
            UTinyIntType,
            USmallIntType,
            UIntegerType,
            UBigIntType,
            FloatType,
            DoubleType,
            StringType,
            BinaryType,
            DateType,
            TimeType,
            TimestampType,
            TimestampTZType,
            UUIDType,
            JSONType,
            IntervalType,
        ],
    )
    def test_singleton_identity(self, type_cls: type[DucklakeType]) -> None:
        assert type_cls() is type_cls()

    def test_decimal_not_singleton(self) -> None:
        a = DecimalType(10, 2)
        b = DecimalType(10, 3)
        assert a is not b


# ---------------------------------------------------------------------------
# 2. Equality
# ---------------------------------------------------------------------------


class TestEquality:
    def test_same_primitive(self) -> None:
        assert IntegerType() == IntegerType()

    def test_same_decimal(self) -> None:
        assert DecimalType(10, 2) == DecimalType(10, 2)

    def test_nested_field_equality(self) -> None:
        f1 = NestedField(field_id=1, name="x", field_type=IntegerType(), required=True)
        f2 = NestedField(field_id=1, name="x", field_type=IntegerType(), required=True)
        assert f1 == f2

    def test_struct_equality(self) -> None:
        fields = (NestedField(1, "a", IntegerType()),)
        assert StructType(fields) == StructType(fields)


# ---------------------------------------------------------------------------
# 3. Inequality
# ---------------------------------------------------------------------------


class TestInequality:
    def test_different_primitives(self) -> None:
        assert IntegerType() != BigIntType()

    def test_different_decimal(self) -> None:
        assert DecimalType(10, 2) != DecimalType(10, 3)

    def test_different_nested_field(self) -> None:
        f1 = NestedField(1, "x", IntegerType())
        f2 = NestedField(2, "x", IntegerType())
        assert f1 != f2


# ---------------------------------------------------------------------------
# 4. Hashing
# ---------------------------------------------------------------------------


class TestHashing:
    def test_primitive_as_dict_key(self) -> None:
        d: dict[DucklakeType, str] = {IntegerType(): "int", StringType(): "str"}
        assert d[IntegerType()] == "int"

    def test_primitive_in_set(self) -> None:
        s = {IntegerType(), IntegerType(), BigIntType()}
        assert len(s) == 2

    def test_decimal_hashable(self) -> None:
        s = {DecimalType(10, 2), DecimalType(10, 2), DecimalType(10, 3)}
        assert len(s) == 2

    def test_nested_field_hashable(self) -> None:
        f = NestedField(1, "x", IntegerType())
        assert hash(f) == hash(NestedField(1, "x", IntegerType()))


# ---------------------------------------------------------------------------
# 5. String representation
# ---------------------------------------------------------------------------


class TestStringRepr:
    def test_integer_str(self) -> None:
        assert "Integer" in str(IntegerType()) or "INTEGER" in str(IntegerType()).upper()

    def test_decimal_str(self) -> None:
        s = str(DecimalType(10, 2))
        assert "10" in s and "2" in s

    def test_nested_field_str(self) -> None:
        f = NestedField(1, "col", IntegerType(), required=True)
        s = str(f)
        assert "col" in s

    def test_list_type_str(self) -> None:
        s = str(ListType(element_id=1, element_type=IntegerType()))
        assert s  # non-empty

    def test_map_type_str(self) -> None:
        s = str(MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType()))
        assert s


# ---------------------------------------------------------------------------
# 6. NestedField
# ---------------------------------------------------------------------------


class TestNestedField:
    def test_construction(self) -> None:
        f = NestedField(field_id=1, name="x", field_type=IntegerType(), required=True, doc="test")
        assert f.field_id == 1
        assert f.name == "x"
        assert f.field_type is IntegerType()
        assert f.required is True
        assert f.doc == "test"

    def test_defaults(self) -> None:
        f = NestedField(field_id=1, name="x", field_type=IntegerType())
        assert f.required is False
        assert f.doc is None

    def test_immutable(self) -> None:
        f = NestedField(field_id=1, name="x", field_type=IntegerType())
        with pytest.raises(AttributeError):
            f.name = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 7. StructType
# ---------------------------------------------------------------------------


class TestStructType:
    def test_field_access(self) -> None:
        f1 = NestedField(1, "a", IntegerType())
        f2 = NestedField(2, "b", StringType())
        s = StructType((f1, f2))
        assert s.fields == (f1, f2)

    def test_iteration(self) -> None:
        f1 = NestedField(1, "a", IntegerType())
        f2 = NestedField(2, "b", StringType())
        s = StructType((f1, f2))
        assert list(s) == [f1, f2]

    def test_immutable(self) -> None:
        s = StructType((NestedField(1, "a", IntegerType()),))
        with pytest.raises(AttributeError):
            s.fields = ()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 8. ListType and MapType
# ---------------------------------------------------------------------------


class TestListType:
    def test_construction(self) -> None:
        lt = ListType(element_id=1, element_type=IntegerType(), element_required=False)
        assert lt.element_id == 1
        assert lt.element_type is IntegerType()
        assert lt.element_required is False

    def test_defaults(self) -> None:
        lt = ListType(element_id=1, element_type=IntegerType())
        assert lt.element_required is True


class TestMapType:
    def test_construction(self) -> None:
        mt = MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=False)
        assert mt.key_id == 1
        assert mt.key_type is StringType()
        assert mt.value_id == 2
        assert mt.value_type is IntegerType()
        assert mt.value_required is False

    def test_defaults(self) -> None:
        mt = MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType())
        assert mt.value_required is True


# ---------------------------------------------------------------------------
# 9. Arrow conversion — round-trip
# ---------------------------------------------------------------------------


class TestArrowConversion:
    @pytest.mark.parametrize(
        "dl_type, expected_arrow",
        [
            (BooleanType(), pa.bool_()),
            (TinyIntType(), pa.int8()),
            (SmallIntType(), pa.int16()),
            (IntegerType(), pa.int32()),
            (BigIntType(), pa.int64()),
            (HugeIntType(), pa.decimal128(38, 0)),
            (UTinyIntType(), pa.uint8()),
            (USmallIntType(), pa.uint16()),
            (UIntegerType(), pa.uint32()),
            (UBigIntType(), pa.uint64()),
            (FloatType(), pa.float32()),
            (DoubleType(), pa.float64()),
            (StringType(), pa.string()),
            (BinaryType(), pa.binary()),
            (DateType(), pa.date32()),
            (TimeType(), pa.time64("us")),
            (TimestampType(), pa.timestamp("us")),
            (TimestampTZType(), pa.timestamp("us", tz="UTC")),
            (IntervalType(), pa.month_day_nano_interval()),
        ],
    )
    def test_primitive_to_arrow(self, dl_type: DucklakeType, expected_arrow: pa.DataType) -> None:
        assert ducklake_type_to_arrow(dl_type) == expected_arrow

    def test_uuid_to_arrow(self) -> None:
        assert ducklake_type_to_arrow(UUIDType()) == pa.string()

    def test_json_to_arrow(self) -> None:
        assert ducklake_type_to_arrow(JSONType()) == pa.string()

    def test_decimal_to_arrow(self) -> None:
        assert ducklake_type_to_arrow(DecimalType(10, 2)) == pa.decimal128(10, 2)

    def test_list_to_arrow(self) -> None:
        lt = ListType(element_id=1, element_type=IntegerType())
        arrow = ducklake_type_to_arrow(lt)
        assert arrow == pa.list_(pa.field("element", pa.int32(), nullable=False))

    def test_list_nullable_to_arrow(self) -> None:
        lt = ListType(element_id=1, element_type=IntegerType(), element_required=False)
        arrow = ducklake_type_to_arrow(lt)
        assert arrow == pa.list_(pa.field("element", pa.int32(), nullable=True))

    def test_map_to_arrow(self) -> None:
        mt = MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType())
        arrow = ducklake_type_to_arrow(mt)
        assert arrow == pa.map_(pa.string(), pa.field("value", pa.int32(), nullable=False))

    def test_struct_to_arrow(self) -> None:
        st = StructType(
            (
                NestedField(1, "a", IntegerType(), required=True),
                NestedField(2, "b", StringType(), required=False),
            )
        )
        arrow = ducklake_type_to_arrow(st)
        expected = pa.struct(
            [
                pa.field("a", pa.int32(), nullable=False),
                pa.field("b", pa.string(), nullable=True),
            ]
        )
        assert arrow == expected

    # Round-trip tests for arrow_type_to_ducklake
    @pytest.mark.parametrize(
        "dl_type, arrow_type",
        [
            (BooleanType(), pa.bool_()),
            (TinyIntType(), pa.int8()),
            (SmallIntType(), pa.int16()),
            (IntegerType(), pa.int32()),
            (BigIntType(), pa.int64()),
            (UTinyIntType(), pa.uint8()),
            (USmallIntType(), pa.uint16()),
            (UIntegerType(), pa.uint32()),
            (UBigIntType(), pa.uint64()),
            (FloatType(), pa.float32()),
            (DoubleType(), pa.float64()),
            (StringType(), pa.string()),
            (BinaryType(), pa.binary()),
            (DateType(), pa.date32()),
            (TimestampType(), pa.timestamp("us")),
            (TimestampTZType(), pa.timestamp("us", tz="UTC")),
        ],
    )
    def test_arrow_round_trip(self, dl_type: DucklakeType, arrow_type: pa.DataType) -> None:
        assert arrow_type_to_ducklake(arrow_type) == dl_type

    def test_arrow_decimal_round_trip(self) -> None:
        assert arrow_type_to_ducklake(pa.decimal128(10, 2)) == DecimalType(10, 2)

    def test_arrow_hugeint_round_trip(self) -> None:
        # decimal128(38, 0) maps back to HugeIntType
        assert arrow_type_to_ducklake(pa.decimal128(38, 0)) == HugeIntType()

    def test_arrow_time_round_trip(self) -> None:
        assert arrow_type_to_ducklake(pa.time64("us")) == TimeType()

    def test_arrow_interval_round_trip(self) -> None:
        assert arrow_type_to_ducklake(pa.month_day_nano_interval()) == IntervalType()


# ---------------------------------------------------------------------------
# 10. SQL conversion
# ---------------------------------------------------------------------------


class TestSQLConversion:
    @pytest.mark.parametrize(
        "dl_type, expected_sql",
        [
            (BooleanType(), "BOOLEAN"),
            (TinyIntType(), "TINYINT"),
            (SmallIntType(), "SMALLINT"),
            (IntegerType(), "INTEGER"),
            (BigIntType(), "BIGINT"),
            (HugeIntType(), "HUGEINT"),
            (UTinyIntType(), "UTINYINT"),
            (USmallIntType(), "USMALLINT"),
            (UIntegerType(), "UINTEGER"),
            (UBigIntType(), "UBIGINT"),
            (FloatType(), "FLOAT"),
            (DoubleType(), "DOUBLE"),
            (StringType(), "VARCHAR"),
            (BinaryType(), "BLOB"),
            (DateType(), "DATE"),
            (TimeType(), "TIME"),
            (TimestampType(), "TIMESTAMP"),
            (TimestampTZType(), "TIMESTAMPTZ"),
            (UUIDType(), "UUID"),
            (JSONType(), "JSON"),
            (IntervalType(), "INTERVAL"),
        ],
    )
    def test_primitive_sql(self, dl_type: DucklakeType, expected_sql: str) -> None:
        assert ducklake_type_to_sql(dl_type) == expected_sql

    def test_decimal_sql(self) -> None:
        assert ducklake_type_to_sql(DecimalType(10, 2)) == "DECIMAL(10, 2)"

    def test_list_sql(self) -> None:
        lt = ListType(element_id=1, element_type=IntegerType())
        assert ducklake_type_to_sql(lt) == "INTEGER[]"

    def test_map_sql(self) -> None:
        mt = MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType())
        assert ducklake_type_to_sql(mt) == "MAP(VARCHAR, INTEGER)"

    def test_struct_sql(self) -> None:
        st = StructType(
            (
                NestedField(1, "a", IntegerType()),
                NestedField(2, "b", StringType()),
            )
        )
        assert ducklake_type_to_sql(st) == 'STRUCT("a" INTEGER, "b" VARCHAR)'

    def test_struct_sql_quoted_field_name(self) -> None:
        st = StructType((NestedField(1, 'col"name', IntegerType()),))
        assert ducklake_type_to_sql(st) == 'STRUCT("col""name" INTEGER)'

    def test_nested_list_sql(self) -> None:
        lt = ListType(element_id=1, element_type=ListType(element_id=2, element_type=IntegerType()))
        assert ducklake_type_to_sql(lt) == "INTEGER[][]"

    def test_nested_map_sql(self) -> None:
        mt = MapType(
            key_id=1,
            key_type=StringType(),
            value_id=2,
            value_type=MapType(
                key_id=3,
                key_type=StringType(),
                value_id=4,
                value_type=IntegerType(),
            ),
        )
        assert ducklake_type_to_sql(mt) == "MAP(VARCHAR, MAP(VARCHAR, INTEGER))"


# ---------------------------------------------------------------------------
# 11. DecimalType validation
# ---------------------------------------------------------------------------


class TestDecimalValidation:
    def test_valid_decimal(self) -> None:
        d = DecimalType(38, 10)
        assert d.precision == 38
        assert d.scale == 10

    def test_precision_too_large(self) -> None:
        with pytest.raises(ValueError):
            DecimalType(39, 0)

    def test_precision_zero(self) -> None:
        with pytest.raises(ValueError):
            DecimalType(0, 0)

    def test_negative_scale(self) -> None:
        with pytest.raises(ValueError):
            DecimalType(10, -1)

    def test_scale_exceeds_precision(self) -> None:
        with pytest.raises(ValueError):
            DecimalType(5, 6)


# ---------------------------------------------------------------------------
# 12. Arrow → Ducklake: unsupported types
# ---------------------------------------------------------------------------


class TestArrowUnsupportedTypes:
    def test_arrow_timestamp_non_us_unit_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported timestamp unit"):
            arrow_type_to_ducklake(pa.timestamp("ns"))

    def test_arrow_timestamp_us_non_utc_tz_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported timezone"):
            arrow_type_to_ducklake(pa.timestamp("us", tz="US/Eastern"))

    def test_arrow_timestamp_ns_no_tz_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported timestamp unit"):
            arrow_type_to_ducklake(pa.timestamp("ns"))

    def test_arrow_unsupported_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot convert Arrow type"):
            arrow_type_to_ducklake(pa.large_string())

        with pytest.raises(TypeError, match="Cannot convert Arrow type"):
            arrow_type_to_ducklake(pa.time32("ms"))


# ---------------------------------------------------------------------------
# 13. P1: unknown type raises
# ---------------------------------------------------------------------------


class TestUnknownTypeRaises:
    def test_ducklake_type_to_arrow_unknown_type_raises(self) -> None:
        class FakeType(DucklakeType):
            def __repr__(self) -> str:
                return "FakeType()"

        with pytest.raises(TypeError, match="Cannot convert"):
            ducklake_type_to_arrow(FakeType())

    def test_ducklake_type_to_sql_unknown_type_raises(self) -> None:
        class FakeType(DucklakeType):
            def __repr__(self) -> str:
                return "FakeType()"

        with pytest.raises(TypeError, match="Cannot convert"):
            ducklake_type_to_sql(FakeType())

    def test_arrow_type_to_ducklake_unsupported_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot convert Arrow type"):
            arrow_type_to_ducklake(pa.large_string())


# ---------------------------------------------------------------------------
# 14. P1: nested type round-trips
# ---------------------------------------------------------------------------


class TestNestedRoundTrips:
    def test_arrow_to_ducklake_list_round_trip(self) -> None:
        original = ListType(element_id=0, element_type=IntegerType(), element_required=True)
        arrow = ducklake_type_to_arrow(original)
        back = arrow_type_to_ducklake(arrow)
        assert isinstance(back, ListType)
        assert back.element_type == original.element_type
        assert back.element_required == original.element_required

    def test_arrow_to_ducklake_map_round_trip(self) -> None:
        original = MapType(
            key_id=0,
            key_type=StringType(),
            value_id=0,
            value_type=IntegerType(),
            value_required=True,
        )
        arrow = ducklake_type_to_arrow(original)
        back = arrow_type_to_ducklake(arrow)
        assert isinstance(back, MapType)
        assert back.key_type == original.key_type
        assert back.value_type == original.value_type
        assert back.value_required == original.value_required

    def test_arrow_to_ducklake_struct_round_trip(self) -> None:
        original = StructType(
            (
                NestedField(field_id=0, name="x", field_type=IntegerType(), required=True),
                NestedField(field_id=1, name="y", field_type=StringType(), required=False),
            )
        )
        arrow = ducklake_type_to_arrow(original)
        back = arrow_type_to_ducklake(arrow)
        assert isinstance(back, StructType)
        assert len(back.fields) == 2
        assert back.fields[0].name == "x"
        assert back.fields[0].field_type == IntegerType()
        assert back.fields[0].required is True
        assert back.fields[1].name == "y"
        assert back.fields[1].field_type == StringType()
        assert back.fields[1].required is False
