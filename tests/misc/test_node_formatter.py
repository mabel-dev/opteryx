import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.types import OrsoTypes

from opteryx.managers.expression import format_expression
from opteryx.managers.expression import NodeType
from opteryx.models.node import Node


def test_format_nodes():
    left_node = Node(NodeType.LITERAL, type=OrsoTypes.INTEGER, value=1)
    right_node = Node(NodeType.LITERAL, type=OrsoTypes.DOUBLE, value=1.1)

    print(format_expression(left_node))
    print(format_expression(right_node))


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
