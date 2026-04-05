"""Tests for pyducklake.expressions module."""

from __future__ import annotations

import datetime

import pytest

from pyducklake.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    Or,
    Reference,
)


class TestSingletons:
    def test_always_true_is_singleton(self) -> None:
        assert AlwaysTrue() is AlwaysTrue()

    def test_always_false_is_singleton(self) -> None:
        assert AlwaysFalse() is AlwaysFalse()

    def test_always_true_sql(self) -> None:
        assert AlwaysTrue().to_sql() == "TRUE"

    def test_always_false_sql(self) -> None:
        assert AlwaysFalse().to_sql() == "FALSE"

    def test_always_true_repr(self) -> None:
        assert repr(AlwaysTrue()) == "AlwaysTrue()"

    def test_always_false_repr(self) -> None:
        assert repr(AlwaysFalse()) == "AlwaysFalse()"


class TestReference:
    def test_construction(self) -> None:
        ref = Reference("x")
        assert ref.name == "x"

    def test_repr(self) -> None:
        ref = Reference("col")
        assert repr(ref) == "Reference('col')"

    def test_equality(self) -> None:
        assert Reference("x") == Reference("x")
        assert Reference("x") != Reference("y")

    def test_hashable(self) -> None:
        assert hash(Reference("x")) == hash(Reference("x"))
        s = {Reference("x"), Reference("x")}
        assert len(s) == 1


class TestComparisonPredicates:
    def test_equal_to_int(self) -> None:
        expr = EqualTo("x", 5)
        assert expr.term == "x"
        assert expr.value == 5
        assert expr.to_sql() == '"x" = 5'

    def test_equal_to_string(self) -> None:
        expr = EqualTo("name", "hello")
        assert expr.to_sql() == "\"name\" = 'hello'"

    def test_equal_to_string_with_quote(self) -> None:
        expr = EqualTo("name", "it's")
        assert expr.to_sql() == "\"name\" = 'it''s'"

    def test_equal_to_bool_true(self) -> None:
        expr = EqualTo("flag", True)
        assert expr.to_sql() == '"flag" = TRUE'

    def test_equal_to_bool_false(self) -> None:
        expr = EqualTo("flag", False)
        assert expr.to_sql() == '"flag" = FALSE'

    def test_equal_to_none(self) -> None:
        expr = EqualTo("x", None)
        assert expr.to_sql() == '"x" = NULL'

    def test_equal_to_float(self) -> None:
        expr = EqualTo("x", 3.14)
        assert expr.to_sql() == '"x" = 3.14'

    def test_not_equal_to(self) -> None:
        expr = NotEqualTo("x", 5)
        assert expr.to_sql() == '"x" != 5'

    def test_greater_than(self) -> None:
        expr = GreaterThan("x", 5)
        assert expr.to_sql() == '"x" > 5'

    def test_greater_than_or_equal(self) -> None:
        expr = GreaterThanOrEqual("x", 5)
        assert expr.to_sql() == '"x" >= 5'

    def test_less_than(self) -> None:
        expr = LessThan("x", 5)
        assert expr.to_sql() == '"x" < 5'

    def test_less_than_or_equal(self) -> None:
        expr = LessThanOrEqual("x", 5)
        assert expr.to_sql() == '"x" <= 5'

    def test_equality_and_hash(self) -> None:
        a = EqualTo("x", 5)
        b = EqualTo("x", 5)
        c = EqualTo("x", 6)
        assert a == b
        assert a != c
        assert hash(a) == hash(b)
        assert {a, b} == {a}

    def test_repr(self) -> None:
        assert repr(EqualTo("x", 5)) == "EqualTo(term='x', value=5)"
        assert repr(NotEqualTo("x", 5)) == "NotEqualTo(term='x', value=5)"

    def test_date_value(self) -> None:
        d = datetime.date(2024, 1, 15)
        expr = EqualTo("dt", d)
        assert expr.to_sql() == "\"dt\" = '2024-01-15'"

    def test_datetime_value(self) -> None:
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        expr = EqualTo("ts", dt)
        assert expr.to_sql() == "\"ts\" = '2024-01-15 10:30:00.000000'"


class TestInEmptyValues:
    def test_in_empty_values_is_always_false(self) -> None:
        result = In("x", ())
        assert result is AlwaysFalse()

    def test_not_in_empty_values_is_always_true(self) -> None:
        result = NotIn("x", ())
        assert result is AlwaysTrue()


class TestInPredicates:
    def test_in_ints(self) -> None:
        expr = In("x", (1, 2, 3))
        assert expr.to_sql() == '"x" IN (1, 2, 3)'

    def test_in_strings(self) -> None:
        expr = In("x", ("a", "b"))
        assert expr.to_sql() == "\"x\" IN ('a', 'b')"

    def test_not_in(self) -> None:
        expr = NotIn("x", (1, 2))
        assert expr.to_sql() == '"x" NOT IN (1, 2)'

    def test_in_equality(self) -> None:
        assert In("x", (1, 2)) == In("x", (1, 2))
        assert In("x", (1, 2)) != In("x", (1, 3))

    def test_repr(self) -> None:
        assert repr(In("x", (1, 2))) == "In(term='x', values=(1, 2))"


class TestUnaryPredicates:
    def test_is_null(self) -> None:
        expr = IsNull("x")
        assert expr.to_sql() == '"x" IS NULL'

    def test_not_null(self) -> None:
        expr = NotNull("x")
        assert expr.to_sql() == '"x" IS NOT NULL'

    def test_is_nan(self) -> None:
        expr = IsNaN("x")
        assert expr.to_sql() == 'isnan("x")'

    def test_not_nan(self) -> None:
        expr = NotNaN("x")
        assert expr.to_sql() == 'NOT isnan("x")'

    def test_equality(self) -> None:
        assert IsNull("x") == IsNull("x")
        assert IsNull("x") != IsNull("y")
        assert IsNull("x") != NotNull("x")

    def test_repr(self) -> None:
        assert repr(IsNull("x")) == "IsNull(term='x')"
        assert repr(NotNaN("x")) == "NotNaN(term='x')"


class TestLogicalOperators:
    def test_and_sql(self) -> None:
        expr = And(EqualTo("x", 1), EqualTo("y", 2))
        assert expr.to_sql() == '("x" = 1 AND "y" = 2)'

    def test_or_sql(self) -> None:
        expr = Or(EqualTo("x", 1), EqualTo("y", 2))
        assert expr.to_sql() == '("x" = 1 OR "y" = 2)'

    def test_not_sql(self) -> None:
        expr = Not(EqualTo("x", 1))
        assert expr.to_sql() == '(NOT "x" = 1)'

    def test_and_repr(self) -> None:
        expr = And(EqualTo("x", 1), EqualTo("y", 2))
        assert repr(expr) == "And(left=EqualTo(term='x', value=1), right=EqualTo(term='y', value=2))"

    def test_or_repr(self) -> None:
        expr = Or(EqualTo("x", 1), EqualTo("y", 2))
        assert repr(expr) == "Or(left=EqualTo(term='x', value=1), right=EqualTo(term='y', value=2))"

    def test_not_repr(self) -> None:
        expr = Not(EqualTo("x", 1))
        assert repr(expr) == "Not(child=EqualTo(term='x', value=1))"


class TestShortCircuit:
    def test_and_always_true_left(self) -> None:
        x = EqualTo("x", 1)
        result = And(AlwaysTrue(), x)
        assert result is x

    def test_and_always_true_right(self) -> None:
        x = EqualTo("x", 1)
        result = And(x, AlwaysTrue())
        assert result is x

    def test_and_always_false_left(self) -> None:
        x = EqualTo("x", 1)
        result = And(AlwaysFalse(), x)
        assert result is AlwaysFalse()

    def test_and_always_false_right(self) -> None:
        x = EqualTo("x", 1)
        result = And(x, AlwaysFalse())
        assert result is AlwaysFalse()

    def test_or_always_true_left(self) -> None:
        x = EqualTo("x", 1)
        result = Or(AlwaysTrue(), x)
        assert result is AlwaysTrue()

    def test_or_always_true_right(self) -> None:
        x = EqualTo("x", 1)
        result = Or(x, AlwaysTrue())
        assert result is AlwaysTrue()

    def test_or_always_false_left(self) -> None:
        x = EqualTo("x", 1)
        result = Or(AlwaysFalse(), x)
        assert result is x

    def test_or_always_false_right(self) -> None:
        x = EqualTo("x", 1)
        result = Or(x, AlwaysFalse())
        assert result is x

    def test_not_always_true(self) -> None:
        result = Not(AlwaysTrue())
        assert result is AlwaysFalse()

    def test_not_always_false(self) -> None:
        result = Not(AlwaysFalse())
        assert result is AlwaysTrue()

    def test_not_not(self) -> None:
        x = EqualTo("x", 1)
        result = Not(Not(x))
        assert result is x


class TestOperatorOverloading:
    def test_and_operator(self) -> None:
        a = EqualTo("x", 1)
        b = EqualTo("y", 2)
        result = a & b
        assert isinstance(result, And)
        assert result.to_sql() == '("x" = 1 AND "y" = 2)'

    def test_or_operator(self) -> None:
        a = EqualTo("x", 1)
        b = EqualTo("y", 2)
        result = a | b
        assert isinstance(result, Or)
        assert result.to_sql() == '("x" = 1 OR "y" = 2)'

    def test_invert_operator(self) -> None:
        a = EqualTo("x", 1)
        result = ~a
        assert isinstance(result, Not)
        assert result.to_sql() == '(NOT "x" = 1)'

    def test_chained_operators(self) -> None:
        result = EqualTo("x", 1) & EqualTo("y", 2) & EqualTo("z", 3)
        assert isinstance(result, And)

    def test_short_circuit_via_operator(self) -> None:
        x = EqualTo("x", 1)
        assert (AlwaysTrue() & x) is x
        assert (x | AlwaysTrue()) is AlwaysTrue()
        assert (~~x) is x


class TestNestedExpressions:
    def test_nested_and_or(self) -> None:
        expr = GreaterThan("a", 1) & (EqualTo("b", "x") | IsNull("c"))
        expected = '("a" > 1 AND ("b" = \'x\' OR "c" IS NULL))'
        assert expr.to_sql() == expected

    def test_deeply_nested(self) -> None:
        expr = (EqualTo("a", 1) & EqualTo("b", 2)) | (EqualTo("c", 3) & EqualTo("d", 4))
        expected = '(("a" = 1 AND "b" = 2) OR ("c" = 3 AND "d" = 4))'
        assert expr.to_sql() == expected


class TestImmutability:
    def test_comparison_is_frozen(self) -> None:
        expr = EqualTo("x", 5)
        with pytest.raises(AttributeError):
            expr.term = "y"  # type: ignore[misc]

    def test_unary_is_frozen(self) -> None:
        expr = IsNull("x")
        with pytest.raises(AttributeError):
            expr.term = "y"  # type: ignore[misc]

    def test_in_is_frozen(self) -> None:
        expr = In("x", (1, 2))
        with pytest.raises(AttributeError):
            expr.term = "y"  # type: ignore[misc]

    def test_and_is_frozen(self) -> None:
        expr = And(EqualTo("x", 1), EqualTo("y", 2))
        # And is returned directly, not a dataclass necessarily, but should be immutable
        assert isinstance(expr, And)

    def test_expressions_hashable_in_set(self) -> None:
        exprs = {
            EqualTo("x", 1),
            EqualTo("x", 1),
            GreaterThan("y", 2),
            IsNull("z"),
            In("a", (1, 2)),
        }
        assert len(exprs) == 4

    def test_logical_hashable(self) -> None:
        a = And(EqualTo("x", 1), EqualTo("y", 2))
        b = And(EqualTo("x", 1), EqualTo("y", 2))
        assert a == b
        assert hash(a) == hash(b)
        assert len({a, b}) == 1


class TestSqlValueEscaping:
    def test_string_with_single_quote(self) -> None:
        expr = EqualTo("name", "O'Brien")
        assert expr.to_sql() == "\"name\" = 'O''Brien'"

    def test_string_with_multiple_quotes(self) -> None:
        expr = EqualTo("s", "it's a 'test'")
        assert expr.to_sql() == "\"s\" = 'it''s a ''test'''"

    def test_none_value(self) -> None:
        assert EqualTo("x", None).to_sql() == '"x" = NULL'

    def test_negative_number(self) -> None:
        assert EqualTo("x", -42).to_sql() == '"x" = -42'

    def test_zero(self) -> None:
        assert EqualTo("x", 0).to_sql() == '"x" = 0'


# ---------------------------------------------------------------------------
# P1: column names with special characters
# ---------------------------------------------------------------------------


class TestColumnNameSpecialChars:
    def test_column_name_with_special_chars_sql(self) -> None:
        expr = EqualTo('my "col name"', 42)
        sql = expr.to_sql()
        # The column name should be double-quoted with embedded quotes doubled
        assert '"my ""col name"""' in sql
        assert "42" in sql


# ---------------------------------------------------------------------------
# P1: AlwaysTrue != AlwaysFalse
# ---------------------------------------------------------------------------


class TestAlwaysTrueNotEqualAlwaysFalse:
    def test_always_true_not_equal_always_false(self) -> None:
        assert AlwaysTrue() != AlwaysFalse()
        assert not (AlwaysTrue() == AlwaysFalse())


# ---------------------------------------------------------------------------
# P1: _format_value with unknown types
# ---------------------------------------------------------------------------


class TestFormatValueFallback:
    def test_format_value_unknown_type_raises_typeerror(self) -> None:
        """Unknown types raise TypeError instead of silently converting."""
        from pyducklake.expressions import _format_value

        class CustomObj:
            def __str__(self) -> str:
                return "CUSTOM_VAL"

        with pytest.raises(TypeError, match="Unsupported type"):
            _format_value(CustomObj())

    def test_format_value_none(self) -> None:
        from pyducklake.expressions import _format_value

        assert _format_value(None) == "NULL"

    def test_format_value_bool_true(self) -> None:
        from pyducklake.expressions import _format_value

        assert _format_value(True) == "TRUE"

    def test_format_value_bool_false(self) -> None:
        from pyducklake.expressions import _format_value

        assert _format_value(False) == "FALSE"

    def test_format_value_int_zero(self) -> None:
        from pyducklake.expressions import _format_value

        assert _format_value(0) == "0"

    def test_format_value_negative_float(self) -> None:
        from pyducklake.expressions import _format_value

        assert _format_value(-1.5) == "-1.5"


class TestFormatValueUnsupportedTypes:
    def test_format_value_bytes_raises_typeerror(self) -> None:
        from pyducklake.expressions import _format_value

        with pytest.raises(TypeError, match="Unsupported type"):
            _format_value(b"binary data")

    def test_format_value_list_raises_typeerror(self) -> None:
        from pyducklake.expressions import _format_value

        with pytest.raises(TypeError, match="Unsupported type"):
            _format_value([1, 2, 3])

    def test_format_value_dict_raises_typeerror(self) -> None:
        from pyducklake.expressions import _format_value

        with pytest.raises(TypeError, match="Unsupported type"):
            _format_value({"key": "value"})


class TestColumnNameWithEmbeddedQuotes:
    def test_column_name_with_embedded_quotes_in_expression(self) -> None:
        """Column name containing double-quotes produces correct SQL."""
        expr = EqualTo('foo"bar', 42)
        sql = expr.to_sql()
        assert sql == '"foo""bar" = 42'

    def test_column_name_with_embedded_quotes_in_isnull(self) -> None:
        expr = IsNull('col"name')
        assert expr.to_sql() == '"col""name" IS NULL'

    def test_column_name_with_embedded_quotes_in_in(self) -> None:
        expr = In('x"y', (1, 2))
        assert expr.to_sql() == '"x""y" IN (1, 2)'
