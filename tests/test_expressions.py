import decimal
import os
import sys

import numpy

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from rich import traceback

import opteryx
from opteryx.engine.planner.expression import ExpressionTreeNode
from opteryx.engine.planner.expression import NodeType
from opteryx.engine.planner.expression import evaluate
from opteryx.engine.planner.expression import NUMPY_TYPES


traceback.install()


# fmt:off
LITERALS = [
        (NodeType.LITERAL_BOOLEAN, True),
        (NodeType.LITERAL_BOOLEAN, False),
        (NodeType.LITERAL_BOOLEAN, None),
        (NodeType.LITERAL_LIST, ['a', 'b', 'c']),
        (NodeType.LITERAL_LIST, []),
        (NodeType.LITERAL_LIST, [True]),
        (NodeType.LITERAL_NUMERIC, 0),
        (NodeType.LITERAL_NUMERIC, None),
        (NodeType.LITERAL_NUMERIC, 0.1),
        (NodeType.LITERAL_NUMERIC, 1e10),
        (NodeType.LITERAL_NUMERIC, decimal.Decimal(4)), 
        (NodeType.LITERAL_NUMERIC, int(4)), 
        (NodeType.LITERAL_NUMERIC, float(4)),
        (NodeType.LITERAL_NUMERIC, numpy.float64(4)),  
        (NodeType.LITERAL_STRUCT, {"a":"b"}),
        (NodeType.LITERAL_TIMESTAMP, opteryx.utils.dates.parse_iso('2022-01-01')),
        (NodeType.LITERAL_TIMESTAMP, opteryx.utils.dates.parse_iso('2022-01-01 13:31'))
    ]
# fmt:on


@pytest.mark.parametrize("node_type, value", LITERALS)
def test_literals(node_type, value):

    planets = opteryx.samples.planets()

    node = ExpressionTreeNode(node_type, value=value)
    values = evaluate(node, table=planets)
    if node_type != NodeType.LITERAL_LIST:
        assert values.dtype == NUMPY_TYPES[node_type], values
    else:
        assert type(values[0]) == numpy.ndarray, values[0]
    assert len(values) == planets.num_rows

    print(values[0])


if __name__ == "__main__":

    print(f"RUNNING BATTERY OF {len(LITERALS)} LITERAL TYPE TESTS")
    for node_type, value in LITERALS:
        print(node_type)
        test_literals(node_type, value)
    print("okay")

    planets = opteryx.samples.planets()

    true = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=True)
    false = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=False)
    none = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=None)
    
    T_AND_T = ExpressionTreeNode(NodeType.AND, left_node=true, right_node=true)
    T_AND_F = ExpressionTreeNode(NodeType.AND, left_node=true, right_node=false)
    F_AND_T = ExpressionTreeNode(NodeType.AND, left_node=false, right_node=true)
    F_AND_F = ExpressionTreeNode(NodeType.AND, left_node=false, right_node=false)

    print(evaluate(T_AND_T, table=planets))
    print(evaluate(T_AND_F, table=planets))
    print(evaluate(F_AND_T, table=planets))
    print(evaluate(F_AND_F, table=planets))

    T_OR_T = ExpressionTreeNode(NodeType.OR, left_node=true, right_node=true)
    T_OR_F = ExpressionTreeNode(NodeType.OR, left_node=true, right_node=false)
    F_OR_T = ExpressionTreeNode(NodeType.OR, left_node=false, right_node=true)
    F_OR_F = ExpressionTreeNode(NodeType.OR, left_node=false, right_node=false)

    print(evaluate(T_OR_T, table=planets))
    print(evaluate(T_OR_F, table=planets))
    print(evaluate(F_OR_T, table=planets))
    print(evaluate(F_OR_F, table=planets))

    NOT_T = ExpressionTreeNode(NodeType.NOT, centre_node=true)
    NOT_F = ExpressionTreeNode(NodeType.NOT, centre_node=false)

    print(evaluate(NOT_T, table=planets))
    print(evaluate(NOT_F, table=planets))

    T_XOR_T = ExpressionTreeNode(NodeType.XOR, left_node=true, right_node=true)
    T_XOR_F = ExpressionTreeNode(NodeType.XOR, left_node=true, right_node=false)
    F_XOR_T = ExpressionTreeNode(NodeType.XOR, left_node=false, right_node=true)
    F_XOR_F = ExpressionTreeNode(NodeType.XOR, left_node=false, right_node=false)

    print(evaluate(T_XOR_T, table=planets))
    print(evaluate(T_XOR_F, table=planets))
    print(evaluate(F_XOR_T, table=planets))
    print(evaluate(F_XOR_F, table=planets))