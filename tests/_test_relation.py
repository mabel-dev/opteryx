"""
Test Relation Functionality

We're going to use the Satellite Dataset for these tests.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from opteryx.sample_data import SatelliteData


def test_satellite_data_is_a_relation():
    """
    The other tests aren't really valid if we don't confirm this.
    """
    from opteryx import Relation

    assert issubclass(SatelliteData, Relation)


def test_relation_dimensions():
    """
    Test the dimensions are reported correctly
    """
    sd = SatelliteData()
    assert sd.count() == 177, sd.count()
    assert sd.shape == (8, 177), sd.shape


def test_distinct():

    sd = SatelliteData()

    assert sd.distinct().count() == 177

    assert sd.apply_projection("planetId").distinct().count() == 7
    assert sd["planetId"].distinct().count() == 7


if __name__ == "__main__":
    test_satellite_data_is_a_relation()
    test_relation_dimensions()
    test_distinct()
