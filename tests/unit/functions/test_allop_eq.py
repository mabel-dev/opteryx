import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.list_ops import list_allop_eq

def _test_all_eq_comparison(literal, test_value, expected_result, _type=pa.string()):
    array = pa.array([test_value], type=pa.list_(_type))
    result = list(list_allop_eq(literal, array))
    assert result == [expected_result], f"Expected all({test_value}) == {literal} to be {expected_result}, got {result[0]}"

def test_all_eq_basic():
    _test_all_eq_comparison("a", ["a", "a"], 1)
    _test_all_eq_comparison("a", ["a", "b"], 0)
    _test_all_eq_comparison("a", ["b", "b"], 0)
    _test_all_eq_comparison("a", [], 0)

def test_all_eq_nulls():
    _test_all_eq_comparison("a", [None, "a"], 0)
    _test_all_eq_comparison("a", [None], 0)
    _test_all_eq_comparison("a", ["a", "a", "a"], 1)

def test_all_eq_types():
    _test_all_eq_comparison(1, [1, 1, 1], 1, pa.int64())
    _test_all_eq_comparison(1, [1, 2, 1], 0, pa.int64())
    _test_all_eq_comparison(True, [True, True], 1, pa.bool_())
    _test_all_eq_comparison(True, [True, False], 0, pa.bool_())

def test_all_eq_unicode_and_edge():
    _test_all_eq_comparison("💡", ["💡", "💡"], 1)
    _test_all_eq_comparison("💡", ["💡", "💡 "], 0)
    _test_all_eq_comparison("a\0", ["a\0", "a\0"], 1)

def test_all_eq_floats():
    _test_all_eq_comparison(1.0, [1.0, 1.0], 1, pa.float64())
    _test_all_eq_comparison(float("nan"), [float("nan")], 0, pa.float64())
    _test_all_eq_comparison(float("inf"), [float("inf"), float("inf")], 1, pa.float64())

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    run_tests()