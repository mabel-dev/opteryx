"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Limiting is selecting a fixed number of records.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.sample_data import PlanetData
from opteryx.engine.planner.operations import SelectionNode


def test_selection_node():

    planet_data = PlanetData.get()
    print(planet_data)

    # name = Earth
    sn = SelectionNode(filter=("name", "=", "Earth"))
    result = sn.execute(relation=planet_data)
    assert result.num_rows == 1, result.num_rows

    # diameter > 10000
    sn = SelectionNode(filter=("diameter", ">", 10000))
    result = sn.execute(relation=planet_data)
    assert result.num_rows == 6, result.num_rows

    # gravity < 1 AND name = Pluto
    # both are the same result
    sn = SelectionNode(filter=[("gravity", "<", 1), ("name", "=", "Pluto")])
    result = sn.execute(relation=planet_data)
    assert result.num_rows == 1, result.num_rows

    # gravity > 1 OR name = Pluto
    # should be all
    sn = SelectionNode(filter=[[("gravity", ">", 1)], [("name", "=", "Pluto")]])
    result = sn.execute(relation=planet_data)
    assert result.num_rows == 9, result.num_rows

    # lengthOfDay >= 24 AND lengthOfDay <= 25
    sn = SelectionNode(filter=[("lengthOfDay", ">=", 24), ("lengthOfDay", "<=", 25)])
    result = sn.execute(relation=planet_data)
    assert result.num_rows == 2, result.num_rows

    # density > 5000 OR escapeVelocity < 2
    sn = SelectionNode(filter=[[("density", ">", 5000)], [("escapeVelocity", "<", 2)]])
    result = sn.execute(relation=planet_data)
    assert result.num_rows == 4, result.num_rows


if __name__ == "__main__":

    test_selection_node()
