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

    satellite_data = SatelliteData.get()

    ln = LimitNode(limit=1)
    assert ln.execute(relation=satellite_data).num_rows == 1

    ln = LimitNode(limit=1000000)
    assert (
        ln.execute(relation=satellite_data).num_rows == 177
    )  # this is the number in the full dataset

    ln = LimitNode(limit=None)
    assert (
        ln.execute(relation=satellite_data).num_rows == 177
    )  # this is the number in the full dataset

    ln = LimitNode(limit=0)
    assert ln.execute(relation=satellite_data).num_rows == 0

    ln = LimitNode(limit=177)
    assert ln.execute(relation=satellite_data).num_rows == 177


if __name__ == "__main__":

    test_limit_node()
