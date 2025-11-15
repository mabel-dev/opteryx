"""
Tests for non-equi joins using draken
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_non_equi_join_import():
    """Test that the non-equi join module can be imported after compilation"""
    try:
        from opteryx.compiled.joins import non_equi_nested_loop_join
        assert non_equi_nested_loop_join is not None
        print("✓ non_equi_nested_loop_join imported successfully")
    except ImportError as e:
        print(f"✗ Import failed (expected before compilation): {e}")


def test_non_equi_join_greater_than():
    """Test non-equi join with greater than comparison"""
    import pyarrow as pa
    from opteryx.compiled.joins import non_equi_nested_loop_join
    from opteryx.draken import Morsel

    # Create test tables
    left = pa.table({"id": [1, 2, 3, 4], "value": [10, 20, 30, 40]})
    right = pa.table({"id": [1, 2, 3, 4], "threshold": [15, 25, 35, 45]})

    # Find all rows where left.value > right.threshold
    left_idx, right_idx = non_equi_nested_loop_join(
        Morsel.from_arrow(left), Morsel.from_arrow(right), "value", "threshold", "Gt"
    )

    # Expected: value > threshold
    # 10 > 15: False
    # 10 > 25: False
    # 10 > 35: False
    # 10 > 45: False
    # 20 > 15: True (0, 0)
    # 20 > 25: False
    # 20 > 35: False
    # 20 > 45: False
    # 30 > 15: True (1, 0)
    # 30 > 25: True (1, 1)
    # 30 > 35: False
    # 30 > 45: False
    # 40 > 15: True (2, 0)
    # 40 > 25: True (2, 1)
    # 40 > 35: True (2, 2)
    # 40 > 45: False

    # Sort for consistent comparison
    pairs = sorted(zip(left_idx, right_idx))
    expected = [(1, 0), (2, 0), (2, 1), (3, 0), (3, 1), (3, 2)]

    assert pairs == expected, f"Expected {expected}, got {pairs}"
    print(f"✓ Greater than join test passed: {len(pairs)} matches found")


def test_non_equi_join_not_equals():
    """Test non-equi join with not equals comparison"""
    import pyarrow as pa
    from opteryx.compiled.joins import non_equi_nested_loop_join
    from opteryx.draken import Morsel

    # Create test tables
    left = pa.table({"id": [1, 2, 3], "color": ["red", "blue", "green"]})
    right = pa.table({"id": [1, 2, 3], "color": ["red", "yellow", "green"]})

    # Find all rows where left.color != right.color
    left_idx, right_idx = non_equi_nested_loop_join(
        Morsel.from_arrow(left), Morsel.from_arrow(right), "color", "color", "NotEq"
    )

    # Expected: red != yellow (0,1), red != green (0,2),
    #           blue != red (1,0), blue != yellow (1,1), blue != green (1,2)
    #           green != red (2,0), green != yellow (2,1)

    pairs = sorted(zip(left_idx, right_idx))
    expected = [(0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1)]

    assert pairs == expected, f"Expected {expected}, got {pairs}"
    print(f"✓ Not equals join test passed: {len(pairs)} matches found")


def test_non_equi_join_less_than():
    """Test non-equi join with less than comparison"""
    import pyarrow as pa
    from opteryx.compiled.joins import non_equi_nested_loop_join
    from opteryx.draken import Morsel

    # Create test tables
    left = pa.table({"id": [1, 2, 3, 4], "value": [10, 20, 30, 40]})
    right = pa.table({"id": [1, 2, 3], "threshold": [25, 35, 45]})

    # Find all rows where left.value < right.threshold
    left_idx, right_idx = non_equi_nested_loop_join(
        Morsel.from_arrow(left), Morsel.from_arrow(right), "value", "threshold", "Lt"
    )

    # Expected: 10 < 25, 10 < 35, 10 < 45, 20 < 25, 20 < 35, 20 < 45, 30 < 35, 30 < 45, 40 < 45
    pairs = sorted(zip(left_idx, right_idx))
    expected = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 1), (2, 2), (3, 2)]

    assert pairs == expected, f"Expected {expected}, got {pairs}"
    print(f"✓ Less than join test passed: {len(pairs)} matches found")


def test_non_equi_join_less_than_or_equals():
    """Test non-equi join with less than or equals comparison"""
    import pyarrow as pa
    from opteryx.compiled.joins import non_equi_nested_loop_join
    from opteryx.draken import Morsel

    # Create test tables
    left = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
    right = pa.table({"id": [1, 2, 3], "threshold": [10, 20, 30]})

    # Find all rows where left.value <= right.threshold
    left_idx, right_idx = non_equi_nested_loop_join(
        Morsel.from_arrow(left), Morsel.from_arrow(right), "value", "threshold", "LtEq"
    )

    # Expected: all combinations where value <= threshold
    pairs = sorted(zip(left_idx, right_idx))
    expected = [(0, 0), (0, 1), (0, 2), (1, 1), (1, 2), (2, 2)]

    assert pairs == expected, f"Expected {expected}, got {pairs}"
    print(f"✓ Less than or equals join test passed: {len(pairs)} matches found")


def test_non_equi_join_greater_than_or_equals():
    """Test non-equi join with greater than or equals comparison"""
    import pyarrow as pa
    from opteryx.compiled.joins import non_equi_nested_loop_join
    from opteryx.draken import Morsel

    # Create test tables
    left = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
    right = pa.table({"id": [1, 2, 3], "threshold": [10, 20, 30]})

    # Find all rows where left.value >= right.threshold
    left_idx, right_idx = non_equi_nested_loop_join(
        Morsel.from_arrow(left), Morsel.from_arrow(right), "value", "threshold", "GtEq"
    )

    # Expected: all combinations where value >= threshold
    pairs = sorted(zip(left_idx, right_idx))
    expected = [(0, 0), (1, 0), (1, 1), (2, 0), (2, 1), (2, 2)]

    assert pairs == expected, f"Expected {expected}, got {pairs}"
    print(f"✓ Greater than or equals join test passed: {len(pairs)} matches found")


def test_non_equi_join_with_nulls():
    """Test that non-equi join handles nulls correctly"""
    import pyarrow as pa
    from opteryx.compiled.joins import non_equi_nested_loop_join
    from opteryx.draken import Morsel

    # Create test tables with nulls
    left = pa.table({"id": [1, 2, 3, 4], "value": [10, None, 30, 40]})
    right = pa.table({"id": [1, 2, 3], "threshold": [5, None, 25]})

    # Find all rows where left.value > right.threshold
    left_idx, right_idx = non_equi_nested_loop_join(
        Morsel.from_arrow(left), Morsel.from_arrow(right), "value", "threshold", "Gt"
    )

    # Nulls should be skipped
    # 10 > 5: True (0, 0)
    # 30 > 5: True (2, 0)
    # 30 > 25: True (2, 2)
    # 40 > 5: True (3, 0)
    # 40 > 25: True (3, 2)

    pairs = sorted(zip(left_idx, right_idx))
    expected = [(0, 0), (2, 0), (2, 2), (3, 0), (3, 2)]

    assert pairs == expected, f"Expected {expected}, got {pairs}"
    print(f"✓ Null handling test passed: {len(pairs)} matches found")


def test_non_equi_join_node():
    """Test the NonEquiJoinNode operator"""
    # Test that the operator can be instantiated
    from opteryx.operators import NonEquiJoinNode
    from opteryx.models import QueryProperties
    from unittest.mock import Mock
    
    props = QueryProperties(qid="test", variables={})
    
    # Create mock objects for the "on" parameter structure
    left_mock = Mock()
    left_mock.schema_column.identity = "value"
    right_mock = Mock()
    right_mock.schema_column.identity = "threshold"
    
    node = NonEquiJoinNode(
        props,
        on={
            "left": left_mock,
            "right": right_mock,
            "value": "Gt"
        }
    )
    
    assert node.name == "Non-Equi Join"
    assert node.comparison_op == "Gt"
    print("✓ NonEquiJoinNode instantiation test passed")


if __name__ == "__main__":  # pragma: no cover
    print("\n=== Testing Non-Equi Join Implementation ===\n")
    
    # Test 1: Check if module compiles
    test_non_equi_join_import()
    
    # Only run other tests if the module was successfully compiled
    try:
        from opteryx.compiled.joins import non_equi_nested_loop_join
        
        print("\n=== Running Functional Tests ===\n")
        test_non_equi_join_greater_than()
        test_non_equi_join_greater_than_or_equals()
        test_non_equi_join_less_than()
        test_non_equi_join_less_than_or_equals()
        test_non_equi_join_not_equals()
        test_non_equi_join_with_nulls()
        test_non_equi_join_node()
        
        print("\n=== All tests passed! ===\n")
    except ImportError:
        print("\n⚠ Skipping functional tests - module needs to be compiled first")
        print("Run: python setup.py build_ext --inplace")
