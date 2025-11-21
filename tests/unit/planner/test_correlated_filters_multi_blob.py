"""
Test that correlated filters are created when joining datasets.

This tests the optimization where statistics are used to create range filters on joins.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest

import opteryx


def test_correlated_filters_on_join():
    """
    Test that correlated filters are created when joining datasets.
    
    When joining satellites and planets on planetId = id, the correlated filter
    optimization should use the statistics from one table to create filters on the other.
    """
    
    # Use existing test data - satellites and planets datasets
    query = """
        SELECT s.name, p.name as planet_name
        FROM $satellites s
        INNER JOIN $planets p
        ON s.planetId = p.id
    """
    
    result = opteryx.query(query)
    stats = result.stats
    
    # Check that the correlated filter optimization was invoked
    assert stats.get('optimization_inner_join_correlated_filter') is not None, \
        f"Correlated filter optimization was not invoked. Stats: {stats}"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    print("Testing correlated filters on joins...")
    test_correlated_filters_on_join()
    print("✓ Correlated filter test passed")
    
    print("\n✅ All correlated filter tests passed")
