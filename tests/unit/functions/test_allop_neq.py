import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.list_ops import list_allop_neq

def _test_all_neq_comparison(literal, test_value, expected_result, _type=pa.string()):
    array = pa.array([test_value], type=pa.list_(_type))
    result = list(list_allop_neq(literal, array))
    assert result == [expected_result], f"Expected all({test_value}) != {literal} to be {expected_result}, got {result[0]}"

def test_all_neq_basic():
    _test_all_neq_comparison("a", ["b", "c"], 1)
    _test_all_neq_comparison("a", ["a", "b"], 0)
    _test_all_neq_comparison("a", ["a", "a"], 0)
    _test_all_neq_comparison("a", [], 0)

def test_all_neq_nulls():
    _test_all_neq_comparison("a", [None, "b"], 0)
    _test_all_neq_comparison("a", [None], 0)
    _test_all_neq_comparison("a", ["b", "c"], 1)

def test_all_neq_types():
    _test_all_neq_comparison(1, [2, 3, 4], 1, pa.int64())
    _test_all_neq_comparison(1, [1, 2, 3], 0, pa.int64())
    _test_all_neq_comparison(False, [True, True], 1, pa.bool_())
    _test_all_neq_comparison(False, [False, True], 0, pa.bool_())

def test_all_neq_unicode_and_edge():
    _test_all_neq_comparison("💡", ["x", "y"], 1)
    _test_all_neq_comparison("💡", ["💡", "x"], 0)
    _test_all_neq_comparison("💡", ["💡", "💡"], 0)
    _test_all_neq_comparison("a\0", ["b\0", "c\0"], 1)

def test_all_neq_floats():
    _test_all_neq_comparison(1.0, [1.0, 2.0], 0, pa.float64())
    _test_all_neq_comparison(float("nan"), [float("nan")], 0, pa.float64())
    _test_all_neq_comparison(2.0, [1.0, 3.0], 1, pa.float64())

def test_all_neq_nulls_strings():
    # Existing cases
    _test_all_neq_comparison("a", [None, "b"], 0)
    _test_all_neq_comparison("a", [None], 0)
    _test_all_neq_comparison("a", ["b", "c"], 1)

    # NEW CASES: all nulls
    _test_all_neq_comparison("a", [None, None], 0)  # can't prove they're all not equal
    _test_all_neq_comparison(None, [None], 0)       # None != None is unknown → false
    _test_all_neq_comparison(None, ["a"], 1)        # "a" != None → True
    _test_all_neq_comparison(None, ["a", None], 0)  # One unknown = whole result unknown = false

    # Mixing nulls and matches
    _test_all_neq_comparison("a", ["a", None], 0)   # "a" == "a" fails → false
    _test_all_neq_comparison("a", ["b", None], 0)   # None makes total unknown
    _test_all_neq_comparison("a", ["b", "c", None], 0)  # Some good, one unknown → false
    _test_all_neq_comparison("a", [None, None, None], 0)  # still unknown
    _test_all_neq_comparison("a", ["a", "a", None], 0)    # matched = false
    _test_all_neq_comparison("a", ["b", "c", "d"], 1)     # still good base case

def test_all_neq_nulls_integers():
    _test_all_neq_comparison(1, [2, None, 3], 0, pa.int64())
    _test_all_neq_comparison(1, [1, None], 0, pa.int64())
    _test_all_neq_comparison(1, [None], 0, pa.int64())
    _test_all_neq_comparison(None, [1], 0, pa.int64())
    _test_all_neq_comparison(None, [None], 0, pa.int64())

def test_all_neq_nulls_floats():
    _test_all_neq_comparison(1.0, [2.0, None], 0, pa.float64())
    _test_all_neq_comparison(1.0, [1.0, None], 0, pa.float64())
    _test_all_neq_comparison(1.0, [None], 0, pa.float64())
    _test_all_neq_comparison(None, [1.0], 0, pa.float64())
    _test_all_neq_comparison(None, [None], 0, pa.float64())
    _test_all_neq_comparison(float("nan"), [None], 0, pa.float64())  # NaN != NULL → UNKNOWN

def test_all_neq_nulls_booleans():
    _test_all_neq_comparison(True, [False, None], 0, pa.bool_())
    _test_all_neq_comparison(False, [False, None], 0, pa.bool_())
    _test_all_neq_comparison(True, [None], 0, pa.bool_())
    _test_all_neq_comparison(None, [True], 0, pa.bool_())
    _test_all_neq_comparison(None, [None], 0, pa.bool_())

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    test_all_neq_floats()
    run_tests()