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


def test_logical_expressions():
    planets = opteryx.samples.planets()

    true = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=True)
    false = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=False)

    T_AND_T = ExpressionTreeNode(NodeType.AND, left_node=true, right_node=true)
    T_AND_F = ExpressionTreeNode(NodeType.AND, left_node=true, right_node=false)
    F_AND_T = ExpressionTreeNode(NodeType.AND, left_node=false, right_node=true)
    F_AND_F = ExpressionTreeNode(NodeType.AND, left_node=false, right_node=false)

    result = evaluate(T_AND_T, table=planets)
    assert len(result) == 9
    assert all(result)
    result = evaluate(T_AND_F, table=planets)
    assert len(result) == 9
    assert not any(result), result
    result = evaluate(F_AND_T, table=planets)
    assert len(result) == 9
    assert not any(result)
    result = evaluate(F_AND_F, table=planets)
    assert len(result) == 9
    assert not any(result)

    T_OR_T = ExpressionTreeNode(NodeType.OR, left_node=true, right_node=true)
    T_OR_F = ExpressionTreeNode(NodeType.OR, left_node=true, right_node=false)
    F_OR_T = ExpressionTreeNode(NodeType.OR, left_node=false, right_node=true)
    F_OR_F = ExpressionTreeNode(NodeType.OR, left_node=false, right_node=false)

    result = evaluate(T_OR_T, table=planets)
    assert len(result) == 9
    assert all(result)
    result = evaluate(T_OR_F, table=planets)
    assert len(result) == 9
    assert all(result)
    result = evaluate(F_OR_T, table=planets)
    assert len(result) == 9
    assert all(result)
    result = evaluate(F_OR_F, table=planets)
    assert len(result) == 9
    assert not any(result)

    NOT_T = ExpressionTreeNode(NodeType.NOT, centre_node=true)
    NOT_F = ExpressionTreeNode(NodeType.NOT, centre_node=false)

    result = evaluate(NOT_T, table=planets)
    assert len(result) == 9
    assert not any(result)
    result = evaluate(NOT_F, table=planets)
    assert len(result) == 9
    assert all(result)

    T_XOR_T = ExpressionTreeNode(NodeType.XOR, left_node=true, right_node=true)
    T_XOR_F = ExpressionTreeNode(NodeType.XOR, left_node=true, right_node=false)
    F_XOR_T = ExpressionTreeNode(NodeType.XOR, left_node=false, right_node=true)
    F_XOR_F = ExpressionTreeNode(NodeType.XOR, left_node=false, right_node=false)

    result = evaluate(T_XOR_T, table=planets)
    assert len(result) == 9
    assert not any(result)
    result = evaluate(T_XOR_F, table=planets)
    assert len(result) == 9
    assert all(result)
    result = evaluate(F_XOR_T, table=planets)
    assert len(result) == 9
    assert all(result)
    result = evaluate(F_XOR_F, table=planets)
    assert len(result) == 9
    assert not any(result)


def test_reading_identifiers():
    planets = opteryx.samples.planets()

    names_node = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")
    names = evaluate(names_node, planets)
    assert len(names) == 9
    assert sorted(names) == [
        "Earth",
        "Jupiter",
        "Mars",
        "Mercury",
        "Neptune",
        "Pluto",
        "Saturn",
        "Uranus",
        "Venus",
    ], sorted(names)

    gravity_node = ExpressionTreeNode(NodeType.IDENTIFIER, value="gravity")
    gravities = evaluate(gravity_node, planets)
    assert len(gravities) == 9
    assert sorted(gravities) == [0.7, 3.7, 3.7, 8.7, 8.9, 9.0, 9.8, 11.0, 23.1], sorted(
        gravities
    )


def test_function_operations():

    planets = opteryx.samples.planets()

    name = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")
    concat = ExpressionTreeNode(
        NodeType.FUNCTION_OPERATOR,
        value="stringconcat",
        left_node=name,
        right_node=name,
    )

    gravity = ExpressionTreeNode(NodeType.IDENTIFIER, value="gravity")
    seven = ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=7)
    plus = ExpressionTreeNode(
        NodeType.FUNCTION_OPERATOR, value="plus", left_node=gravity, right_node=seven
    )
    multiply = ExpressionTreeNode(
        NodeType.FUNCTION_OPERATOR,
        value="multiply",
        left_node=gravity,
        right_node=seven,
    )

    names = evaluate(concat, planets)
    assert len(names) == 9
    assert True, list(names) == [
        "MercuryMercury",
        "VenusVenus",
        "EarthEarth",
        "MarsMars",
        "JupiterJupiter",
        "SaturnSaturn",
        "UranusUranus",
        "NeptuneNeptune",
        "PlutoPluto",
    ]  # , list(names)

    plussed = evaluate(plus, planets)
    assert len(plussed) == 9
    assert set(plussed).issubset(
        [10.7, 15.9, 16.8, 10.7, 30.1, 16, 15.7, 18, 7.7]
    ), plussed

    timesed = evaluate(multiply, planets)
    assert len(timesed) == 9
    assert set(timesed) == {
        161.70000000000002,
        68.60000000000001,
        4.8999999999999995,
        77.0,
        25.900000000000002,
        60.89999999999999,
        62.300000000000004,
        63.0,
    }, set(timesed)


if __name__ == "__main__":

    print(f"RUNNING BATTERY OF {len(LITERALS)} LITERAL TYPE TESTS")
    for node_type, value in LITERALS:
        print(node_type)
        test_literals(node_type, value)
    print("okay")

    test_logical_expressions()
    test_reading_identifiers()
    test_function_operations()
