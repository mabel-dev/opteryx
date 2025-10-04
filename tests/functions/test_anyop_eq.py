import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.list_ops import list_anyop_eq

def _test_eq_comparison(literal, test_value, expected_result, _type=pa.string()):
    array = pa.array([[test_value]], type=pa.list_(_type))
    result = list(list_anyop_eq(literal, array))
    assert result == [expected_result], f"Expected {literal} == {test_value} to be {expected_result} got {result[0]}"

def test_eq_strings():
    _test_eq_comparison("a", "a", 1)
    _test_eq_comparison("a", "b", 0)
    _test_eq_comparison("abc", "abc", 1)
    _test_eq_comparison("abc", "ABC", 0)
    _test_eq_comparison("", "", 1)
    _test_eq_comparison(" ", " ", 1)
    _test_eq_comparison("a\0", "a\0", 1)
    _test_eq_comparison("a\0", "a", 0)

def test_eq_bytes():
    _test_eq_comparison(b"a", b"a", 1, pa.binary())
    _test_eq_comparison(b"a", b"b", 0, pa.binary())
    _test_eq_comparison(b"abc", b"abc", 1, pa.binary())
    _test_eq_comparison(b"abc", b"ABC", 0, pa.binary())
    _test_eq_comparison(b"", b"", 1, pa.binary())
    _test_eq_comparison(b" ", b" ", 1, pa.binary())
    _test_eq_comparison(b"a\0", b"a\0", 1, pa.binary())
    _test_eq_comparison(b"a\0", b"a", 0, pa.binary())

def test_eq_integers():
    _test_eq_comparison(1, 1, 1, pa.int64())
    _test_eq_comparison(1, 0, 0, pa.int64())
    _test_eq_comparison(-1, -1, 1, pa.int64())
    _test_eq_comparison(0, 0, 1, pa.int64())
    _test_eq_comparison(123456789, 123456789, 1, pa.int64())

def test_eq_floats():
    _test_eq_comparison(1.0, 1.0, 1, pa.float64())
    _test_eq_comparison(1.0, 1.0000001, 0, pa.float64())
    _test_eq_comparison(float("inf"), float("inf"), 1, pa.float64())
    _test_eq_comparison(float("-inf"), float("-inf"), 1, pa.float64())
#    _test_eq_comparison(float("nan"), float("nan"), 0, pa.float64())  # NaN != NaN

def test_eq_booleans():
    _test_eq_comparison(True, True, 1, pa.bool_())
    _test_eq_comparison(False, False, 1, pa.bool_())
    _test_eq_comparison(True, False, 0, pa.bool_())
    _test_eq_comparison(False, True, 0, pa.bool_())

def test_eq_dates():
    import datetime
    _test_eq_comparison(datetime.date(2023, 1, 1), datetime.date(2023, 1, 1), 1, pa.date32())
    _test_eq_comparison(datetime.date(2023, 1, 1), datetime.date(2023, 1, 2), 0, pa.date32())

def test_eq_timestamps():
    import datetime
    _test_eq_comparison(datetime.datetime(2023, 1, 1, 12, 0, 0), datetime.datetime(2023, 1, 1, 12, 0, 0), 1, pa.timestamp("s"))
    _test_eq_comparison(datetime.datetime(2023, 1, 1, 12, 0, 0), datetime.datetime(2023, 1, 1, 12, 0, 1), 0, pa.timestamp("s"))

def test_eq_decimals():
    import decimal
    _test_eq_comparison(decimal.Decimal("1.23"), decimal.Decimal("1.23"), 1, pa.decimal128(5, 2))
    _test_eq_comparison(decimal.Decimal("1.23"), decimal.Decimal("1.230"), 1, pa.decimal128(5, 3))
    _test_eq_comparison(decimal.Decimal("1.23"), decimal.Decimal("1.24"), 0, pa.decimal128(5, 2))

def test_eq_list_comparison():
    array = pa.array([["a", "b", "c"], ["x", "y", "z"], []], type=pa.list_(pa.string()))
    result = list(list_anyop_eq("b", array))
    assert result == [1, 0, 0], f"Expected [1, 0, 0], got {result}"

def test_eq_nulls():
    array = pa.array([[None, "a"], [None], []], type=pa.list_(pa.string()))
    result = list(list_anyop_eq("a", array))
    assert result == [1, 0, 0], f"Expected [1, 0, 0], got {result}"

def test_eq_float_precision():
    _test_eq_comparison(1.0000000000000001, 1.0, 1, pa.float64())  # within double precision
    _test_eq_comparison(1.0000000000001, 1.0, 0, pa.float64())     # outside of equality tolerance

def test_eq_string_unicode_and_bytes():
    _test_eq_comparison("café", "café", 1, pa.string())
    _test_eq_comparison("café", "café", 0, pa.string())  # visually same, different Unicode normalization
    _test_eq_comparison("💡", "💡", 1, pa.string())
    _test_eq_comparison("💡", "💡 ", 0, pa.string())

def test_eq_decimal_precision():
    import decimal
    _test_eq_comparison(decimal.Decimal("1.000"), decimal.Decimal("1.00"), 1, pa.decimal128(10, 3))
    _test_eq_comparison(decimal.Decimal("1.000"), decimal.Decimal("1.0000"), 1, pa.decimal128(10, 4))
    _test_eq_comparison(decimal.Decimal("1.000"), decimal.Decimal("1.0001"), 0, pa.decimal128(10, 4))

def test_eq_lists_with_nulls_and_values():
    array = pa.array([[None, "a", "b"], [None], ["a"]], type=pa.list_(pa.string()))
    result = list(list_anyop_eq("a", array))
    assert result == [1, 0, 1], f"Expected [1, 0, 1], got {result}"

def test_eq_large_numbers():
    _test_eq_comparison(2**60, 2**60, 1, pa.int64())
    _test_eq_comparison(2**60, 2**60 - 1, 0, pa.int64())

def test_eq_mixed_case_strings():
    _test_eq_comparison("Test", "test", 0)
    _test_eq_comparison("TEST", "TEST", 1)
    _test_eq_comparison("Test", "Test", 1)

def test_eq_edge_strings():
    _test_eq_comparison("\0", "\0", 1)
    _test_eq_comparison("\n", "\n", 1)
    _test_eq_comparison("\t", "\t", 1)
    _test_eq_comparison("a\tb", "a\tb", 1)
    _test_eq_comparison("a\tb", "a b", 0)

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests
    run_tests()
