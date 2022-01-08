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

    print(SatelliteData())

    # ensure we don't filter out when everything is unique
    dn = DistinctNode(True)
    assert dn.execute(relation=SatelliteData()).count() == 177

    # reduce to high duplicate attribute
    dn = DistinctNode(True)
    assert dn.execute(relation=SatelliteData()["planetId"]).count() == 7

    # another test
    dn = DistinctNode(True)
    assert dn.execute(relation=SatelliteData()["planetId","density"]).count() == 43


if __name__ == "__main__":

    test_dictinct_node()