import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.list_ops import list_anyop_gte

def _test_comparison(literal, test_value, expected_result, _type=pa.string()):
    array = pa.array([[test_value]], type=pa.list_(_type))
    result = list(list_anyop_gte(literal, array))
    assert result == [expected_result], f"Expected {literal} >= {test_value} to be {expected_result}, got {result[0]}"

def test_basic_comparison_strings():
    _test_comparison("b", "a", 1)  # b >= a -> True
    _test_comparison("a", "b", 0)  # a >= b -> False
    _test_comparison("a", "a", 1)  # a >= a -> 1

def test_basic_comparison_ints():
    # Integer comparisons
    _test_comparison(2, 1, 1, pa.int64())  # 2 >= 1 -> True
    _test_comparison(1, 2, 0, pa.int64())  # 1 >= 2 -> False
    _test_comparison(1, 1, 1, pa.int64())  # 1 >= 1 -> True
    _test_comparison(-1, -2, 1, pa.int64())  # -1 >= -2 -> True
    _test_comparison(0, -1, 1, pa.int64())  # 0 >= -1 -> True

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    run_tests()
