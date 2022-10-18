import decimal
import os
import sys

import numpy

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from rich import traceback

import opteryx
import opteryx.samples
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import NUMPY_TYPES
from opteryx.shared import QueryStatistics


traceback.install()
stats = QueryStatistics()


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
    """
    In this test we return the indexes of the matching rows, the source table is
    meaningless as we're using literals everywhere - but the source table is 9
    records long.

    If the result is TRUE, we have 9 indicies (rows) returned, if the result is
    FALSE, we we have 0 returned. So we test our truth tables by looking for
    9 (TRUE) or 0 (FALSE)
    """

    planets = opteryx.samples.planets()

    true = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=True)
    false = ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=False)

    T_AND_T = ExpressionTreeNode(NodeType.AND, left=true, right=true)
    T_AND_F = ExpressionTreeNode(NodeType.AND, left=true, right=false)
    F_AND_T = ExpressionTreeNode(NodeType.AND, left=false, right=true)
    F_AND_F = ExpressionTreeNode(NodeType.AND, left=false, right=false)

    result = evaluate(T_AND_T, table=planets)
    assert len(result) == 9
    result = evaluate(T_AND_F, table=planets)
    assert len(result) == 0
    result = evaluate(F_AND_T, table=planets)
    assert len(result) == 0
    result = evaluate(F_AND_F, table=planets)
    assert len(result) == 0

    T_OR_T = ExpressionTreeNode(NodeType.OR, left=true, right=true)
    T_OR_F = ExpressionTreeNode(NodeType.OR, left=true, right=false)
    F_OR_T = ExpressionTreeNode(NodeType.OR, left=false, right=true)
    F_OR_F = ExpressionTreeNode(NodeType.OR, left=false, right=false)

    result = evaluate(T_OR_T, table=planets)
    assert len(result) == 9
    result = evaluate(T_OR_F, table=planets)
    assert len(result) == 9
    result = evaluate(F_OR_T, table=planets)
    assert len(result) == 9
    result = evaluate(F_OR_F, table=planets)
    assert len(result) == 0

    NOT_T = ExpressionTreeNode(NodeType.NOT, centre=true)
    NOT_F = ExpressionTreeNode(NodeType.NOT, centre=false)

    result = evaluate(NOT_T, table=planets)
    assert len(result) == 0
    result = evaluate(NOT_F, table=planets)
    assert len(result) == 9

    T_XOR_T = ExpressionTreeNode(NodeType.XOR, left=true, right=true)
    T_XOR_F = ExpressionTreeNode(NodeType.XOR, left=true, right=false)
    F_XOR_T = ExpressionTreeNode(NodeType.XOR, left=false, right=true)
    F_XOR_F = ExpressionTreeNode(NodeType.XOR, left=false, right=false)

    result = evaluate(T_XOR_T, table=planets)
    assert len(result) == 0
    result = evaluate(T_XOR_F, table=planets)
    assert len(result) == 9
    result = evaluate(F_XOR_T, table=planets)
    assert len(result) == 9
    result = evaluate(F_XOR_F, table=planets)
    assert len(result) == 0


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
        NodeType.BINARY_OPERATOR,
        value="StringConcat",
        left=name,
        right=name,
    )

    gravity = ExpressionTreeNode(NodeType.IDENTIFIER, value="gravity")
    seven = ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=7)
    plus = ExpressionTreeNode(
        NodeType.BINARY_OPERATOR, value="Plus", left=gravity, right=seven
    )
    multiply = ExpressionTreeNode(
        NodeType.BINARY_OPERATOR,
        value="Multiply",
        left=gravity,
        right=seven,
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


def test_compound_expressions():

    planets = opteryx.samples.planets()

    # this builds and tests the following `3.7 * gravity > mass`

    gravity = ExpressionTreeNode(NodeType.IDENTIFIER, value="gravity")
    three_point_seven = ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=3.7)
    mass = ExpressionTreeNode(NodeType.IDENTIFIER, value="mass")

    multiply = ExpressionTreeNode(
        NodeType.BINARY_OPERATOR,
        value="Multiply",
        right=three_point_seven,
        left=gravity,
    )
    gt = ExpressionTreeNode(
        NodeType.COMPARISON_OPERATOR, value="Gt", left=multiply, right=mass
    )

    result = evaluate(gt, planets)
    assert len(result) == 5, result
    assert set(result) == {0, 1, 2, 3, 8}


def test_functions():

    planets = opteryx.samples.planets()

    gravity = ExpressionTreeNode(NodeType.IDENTIFIER, value="gravity")
    _round = ExpressionTreeNode(NodeType.FUNCTION, value="ROUND", parameters=[gravity])

    rounded = evaluate(_round, planets)
    assert len(rounded) == 9
    assert set(r.as_py() for r in rounded) == {4, 23, 9, 1, 11, 10}


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(LITERALS)} LITERAL TYPE TESTS")
    for node_type, value in LITERALS:
        print(node_type)
        test_literals(node_type, value)
    print("okay")

    test_logical_expressions()
    test_reading_identifiers()
    test_function_operations()
    test_compound_expressions()
    test_functions()
