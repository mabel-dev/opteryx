"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Limiting is selecting a fixed number of records.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.sample_data import SatelliteData
from opteryx.engine.planner.operations import DistinctNode


def test_dictinct_node():

    dn = DistinctNode()

    # ensure we don't filter out when everything is unique
    satellite_data = SatelliteData().get()
    assert dn.execute(relation=satellite_data).num_rows == 177

    # reduce to high duplicate attribute
    planets = satellite_data.select(["planetId"])
    assert dn.execute(relation=planets).num_rows == 7

    # another test
    moons = satellite_data.select(["planetId", "density"])
    assert dn.execute(relation=moons).num_rows == 43


if __name__ == "__main__":

    test_dictinct_node()
