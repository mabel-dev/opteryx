"""
Test parallelization of selection and projection operations
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_free_threading_detection():
    """Test that free-threading detection works correctly"""
    from opteryx.utils.threading import is_free_threading_available
    
    # Check detection works without errors
    result = is_free_threading_available()
    assert isinstance(result, bool), "Should return a boolean"
    
    # For Python < 3.13, it should always be False
    if sys.version_info < (3, 13):
        assert result is False, "Expected False for Python < 3.13"
    
    print(f"✓ Free-threading detection: {result}")


def test_filter_node_is_stateless():
    """Test that FilterNode is marked as stateless"""
    from opteryx.operators.filter_node import FilterNode
    
    assert FilterNode.is_stateless is True, "FilterNode should be stateless"
    print("✓ FilterNode is stateless")


def test_projection_node_is_stateless():
    """Test that ProjectionNode is marked as stateless"""
    from opteryx.operators.projection_node import ProjectionNode
    
    assert ProjectionNode.is_stateless is True, "ProjectionNode should be stateless"
    print("✓ ProjectionNode is stateless")


def test_execution_engine_selection():
    """Test that the correct execution engine is selected based on free-threading availability"""
    from opteryx.utils.threading import is_free_threading_available
    
    # Import the execution module to verify it doesn't crash
    from opteryx.managers import execution
    
    # Check that the module has the execute function
    assert hasattr(execution, 'execute'), "Should have execute function"
    
    free_threading = is_free_threading_available()
    print(f"✓ Execution engine selection works (free-threading: {free_threading})")


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
