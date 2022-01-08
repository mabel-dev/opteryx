"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Limiting is selecting a fixed number of records.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.sample_data import SatelliteData
from opteryx.engine.planner.operations import LimitNode


def test_limit_node():

    ln = LimitNode(limit=1)
    assert ln.execute(relation=SatelliteData()).count() == 1

    ln = LimitNode(limit=1000000)
    assert ln.execute(relation=SatelliteData()).count() == 177 # this is the number in the full dataset

    ln = LimitNode(limit=0)
    assert ln.execute(relation=SatelliteData()).count() == 0

    ln = LimitNode(limit=177)
    assert ln.execute(relation=SatelliteData()).count() == 177


if __name__ == "__main__":

    test_limit_node()