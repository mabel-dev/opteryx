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


def test_dictinct_node_unique():

    dn = DistinctNode(QueryStatistics())

    # ensure we don't filter out when everything is unique
    satellite_list = satellites()

    satellite_data = dn.execute(data_pages=[satellite_list])
    satellite_data = pyarrow.concat_tables(satellite_data, promote=True)
    assert satellite_data.num_rows == 177, satellite_data.num_rows


def test_dictinct_node_nonunique():

    dn = DistinctNode(QueryStatistics())

    satellite_list = satellites()

    # test with a column with duplicates
    planets = satellite_list.select(["planetId"])
    planets = dn.execute(data_pages=[planets])
    planets = pyarrow.concat_tables(planets, promote=True)
    assert planets.num_rows == 7


def test_dictinct_node_multicolumn():

    dn = DistinctNode(QueryStatistics())

    satellite_list = satellites()

    # test with a compound column
    moons = satellite_list.select(["planetId", "density"])
    moons = dn.execute(data_pages=[moons])
    moons = pyarrow.concat_tables(moons, promote=True)
    assert moons.num_rows == 43


if __name__ == "__main__":  # pragma: no cover

    test_dictinct_node_multicolumn()
    test_dictinct_node_nonunique()
    test_dictinct_node_unique()
