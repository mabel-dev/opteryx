"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Limiting is selecting a fixed number of records.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.engine.planner.operations import DistinctNode
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.samples import satellites
import pyarrow


def test_dictinct_node():

    dn = DistinctNode(QueryStatistics())

    # ensure we don't filter out when everything is unique
    satellite_list = satellites()

    satellite_data = dn.execute(data_pages=[satellite_list])
    satellite_data = pyarrow.concat_tables(satellite_data)
    assert satellite_data.num_rows == 177, satellite_data.num_rows

    # test with a column with duplicates
    planets = satellite_list.select(["planetId"])
    planets = dn.execute(data_pages=[planets])
    planets = pyarrow.concat_tables(planets)
    assert planets.num_rows == 7

    # test with a compound column
    moons = satellite_list.select(["planetId", "density"])
    moons = dn.execute(data_pages=[moons])
    moons = pyarrow.concat_tables(moons)
    assert moons.num_rows == 43


if __name__ == "__main__":

    test_dictinct_node()
