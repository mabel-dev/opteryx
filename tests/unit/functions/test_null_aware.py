import os
import sys
import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

def test_if_not_null():
    from opteryx.functions.other_functions import if_not_null

    # All values are not null → use replacements
    act = if_not_null(numpy.array([1, 2]), numpy.array([9, 9])).tolist()
    assert act == [9, 9], act

    # Some nulls in values → preserve them
    act = if_not_null(numpy.array([None, 2]), numpy.array([9, 9])).tolist()
    assert act == [None, 9], act

    # All values null → nothing replaced
    act = if_not_null(numpy.array([None, None]), numpy.array([1, 2])).tolist()
    assert act == [None, None], act

    # Different types: string fallback for int value
    act = if_not_null(numpy.array([1, None], dtype=object), numpy.array(["x", "y"])).tolist()
    assert act == ["x", None], act

    # Fallbacks also include nulls (but are only used where values are not null)
    act = if_not_null(numpy.array([1, 2]), numpy.array([None, 99])).tolist()
    assert act == [None, 99], act

    # Mixed fallback types with strings and ints
    act = if_not_null(numpy.array([None, 2, 3], dtype=object), numpy.array(["a", "b", "c"])).tolist()
    assert act == [None, "b", "c"], act

    # Broadcasting fallback scalar
    act = if_not_null(numpy.array([None, 1, 2]), numpy.array([999])).tolist()
    assert act == [None, 999, 999], act

    # Test with booleans
    act = if_not_null(numpy.array([None, True, False], dtype=object), numpy.array([True, False, True])).tolist()
    assert act == [None, False, True], act

def test_if_null():
    from opteryx.functions.other_functions import if_null

    # Both values not null → keep original
    act = if_null(numpy.array([1, 2]), numpy.array([9, 9])).tolist()
    assert act == [1, 2], act

    # Left has one null → fallback used
    act = if_null(numpy.array([None, 2]), numpy.array([9, 9])).tolist()
    assert act == [9, 2], act

    # Both null → fallback is also null
    act = if_null(numpy.array([None, None]), numpy.array([None, 5])).tolist()
    assert act == [None, 5], act

    # Different types, coercion check
    act = if_null(numpy.array([None, "foo"]), numpy.array(["bar", "baz"])).tolist()
    assert act == ["bar", "foo"], act


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
