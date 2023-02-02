"""
Test Execution Nodes

We're going to use the Satellite Dataset for these tests.

Projection is eliminating columns which are not wanted, this node also performs columns
which aren't needed. We call these columns or attributes interchangably.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.planner.operations import ProjectionNode
from opteryx.sample_data import SatelliteData


def test_projection_node():
    satellite_data = SatelliteData().get()

    # test None does nothing to the attributes
    pn = ProjectionNode(projection={"*": "*"})
    assert (
        pn.execute(relation=satellite_data).column_names == satellite_data.column_names
    )

    # test renames, reorder and elimination
    pn = ProjectionNode(
        projection={"name": "name", "id": "identifier", "gm": "gravity"}
    )
    assert pn.execute(relation=satellite_data).column_names == [
        "name",
        "identifier",
        "gravity",
    ]


if __name__ == "__main__":
    test_projection_node()
