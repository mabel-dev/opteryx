"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Limiting is selecting a fixed number of records.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.engine.query_statistics import QueryStatistics
from opteryx.sample_data import SatelliteData
from opteryx.engine.planner.operations import DistinctNode


def size(d):
    from opteryx.utils.pyarrow import fetchall

    return len(list(fetchall(d)))


def test_dictinct_node():

    dn = DistinctNode(QueryStatistics())

    # ensure we don't filter out when everything is unique
    satellite_data = SatelliteData().get()
    assert size(dn.execute(data_pages=[satellite_data])) == 177, size(
        dn.execute(data_pages=[satellite_data])
    )

    # reduce to high duplicate attribute
    planets = satellite_data.select(["planetId"])
    assert size(dn.execute(data_pages=[planets])) == 7

    # another test
    moons = satellite_data.select(["planetId", "density"])
    assert size(dn.execute(data_pages=[moons])) == 43


if __name__ == "__main__":

    test_dictinct_node()
