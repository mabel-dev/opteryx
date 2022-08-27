"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Limiting is selecting a fixed number of records.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.planner.operations import LimitNode
from opteryx.models.query_statistics import QueryStatistics
from opteryx.sample_data import SatelliteData


def test_limit_node():

    satellite_data = SatelliteData.get()

    ln = LimitNode(QueryStatistics(), limit=1)
    assert ln.execute(relation=satellite_data).num_rows == 1

    ln = LimitNode(QueryStatistics(), limit=1000000)
    assert (
        ln.execute(relation=satellite_data).num_rows == 177
    )  # this is the number in the full dataset

    ln = LimitNode(QueryStatistics(), limit=None)
    assert (
        ln.execute(relation=satellite_data).num_rows == 177
    )  # this is the number in the full dataset

    ln = LimitNode(QueryStatistics(), limit=0)
    assert ln.execute(relation=satellite_data).num_rows == 0

    ln = LimitNode(QueryStatistics(), limit=177)
    assert ln.execute(relation=satellite_data).num_rows == 177


if __name__ == "__main__":

    test_limit_node()
