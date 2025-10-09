import os
import sys
from decimal import Decimal

import numpy
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.schema import ConstantColumn, FunctionColumn
from orso.types import OrsoTypes

import opteryx
import opteryx.virtual_datasets
from opteryx.managers.expression import NodeType, evaluate
from opteryx.models import Node, QueryStatistics

stats = QueryStatistics()


# fmt:off
LITERALS = [
        (NodeType.LITERAL, OrsoTypes.BOOLEAN, True),
        (NodeType.LITERAL, OrsoTypes.BOOLEAN, False),
        (NodeType.LITERAL, OrsoTypes.BOOLEAN,  None),
        (NodeType.LITERAL, OrsoTypes.ARRAY, ['a', 'b', 'c']),
        (NodeType.LITERAL, OrsoTypes.ARRAY, []),
        (NodeType.LITERAL, OrsoTypes.ARRAY, [True]),
        (NodeType.LITERAL, OrsoTypes.INTEGER, 0),
        (NodeType.LITERAL, OrsoTypes.DOUBLE, None),
        (NodeType.LITERAL, OrsoTypes.DOUBLE, 0.1),
        (NodeType.LITERAL, OrsoTypes.DOUBLE, 1e10),
        (NodeType.LITERAL, OrsoTypes.DECIMAL, Decimal(4)),
        (NodeType.LITERAL, OrsoTypes.INTEGER, int(4)),
        (NodeType.LITERAL, OrsoTypes.DOUBLE, float(4)),
        (NodeType.LITERAL, OrsoTypes.DOUBLE, numpy.float64(4)),
        (NodeType.LITERAL, OrsoTypes.STRUCT, {"a":"b"}),
        (NodeType.LITERAL, OrsoTypes.DATE, opteryx.utils.dates.parse_iso('2022-01-01').date()),
        (NodeType.LITERAL, OrsoTypes.TIMESTAMP, opteryx.utils.dates.parse_iso('2022-01-01 13:31'))
    ]
# fmt:on


@pytest.mark.parametrize("node_type, value_type, value", LITERALS)
def test_literals(node_type, value_type, value):
    planets = opteryx.virtual_datasets.planets.read()

    schema_column = ConstantColumn(name="test", value=value, type=value_type)

    node = Node(node_type, type=value_type, value=value, schema_column=schema_column)
    values = evaluate(node, table=planets)
    if value_type != OrsoTypes.ARRAY:
        assert values.dtype == value_type.numpy_dtype, values
    else:
        assert type(values[0]) == numpy.ndarray, values[0]
#    assert len(values) == planets.num_rows, f"{len(values)} != {planets.num_rows}"


def test_logical_expressions():
    """
    In this test we return the indexes of the matching rows, the source table is
    meaningless as we're using literals everywhere - but the source table is 9
    records long.

    One thing to be aware of is that a pyarrow boolean scalar is a truthy, which is
    illogical from a user perspective but technically correct.
    """

    planets = opteryx.virtual_datasets.planets.read()

    true = Node(
        NodeType.LITERAL,
        type=OrsoTypes.BOOLEAN,
        value=True,
        schema_column=ConstantColumn(name="true", value=True, type=OrsoTypes.BOOLEAN),
    )
    false = Node(
        NodeType.LITERAL,
        type=OrsoTypes.BOOLEAN,
        value=False,
        schema_column=ConstantColumn(name="false", value=False, type=OrsoTypes.BOOLEAN),
    )

    T_AND_T = Node(
        NodeType.AND, left=true, right=true, schema_column=FunctionColumn(name="func", type=0)
    )
    T_AND_F = Node(
        NodeType.AND, left=true, right=false, schema_column=FunctionColumn(name="func", type=0)
    )
    F_AND_T = Node(
        NodeType.AND, left=false, right=true, schema_column=FunctionColumn(name="func", type=0)
    )
    F_AND_F = Node(
        NodeType.AND, left=false, right=false, schema_column=FunctionColumn(name="func", type=0)
    )

    result = evaluate(T_AND_T, table=planets)
    assert all(result)
    result = evaluate(T_AND_F, table=planets)
    assert not any(c for c in result)
    result = evaluate(F_AND_T, table=planets)
    assert not any(c for c in result)
    result = evaluate(F_AND_F, table=planets)
    assert not any(c for c in result)

    T_OR_T = Node(
        NodeType.OR, left=true, right=true, schema_column=FunctionColumn(name="func", type=0)
    )
    T_OR_F = Node(
        NodeType.OR, left=true, right=false, schema_column=FunctionColumn(name="func", type=0)
    )
    F_OR_T = Node(
        NodeType.OR, left=false, right=true, schema_column=FunctionColumn(name="func", type=0)
    )
    F_OR_F = Node(
        NodeType.OR, left=false, right=false, schema_column=FunctionColumn(name="func", type=0)
    )

    result = evaluate(T_OR_T, table=planets)
    assert all(result)
    result = evaluate(T_OR_F, table=planets)
    assert all(result)
    result = evaluate(F_OR_T, table=planets)
    assert all(result)
    result = evaluate(F_OR_F, table=planets)
    assert not any(c for c in result)

    NOT_T = Node(NodeType.NOT, centre=true, schema_column=FunctionColumn(name="func", type=0))
    NOT_F = Node(NodeType.NOT, centre=false, schema_column=FunctionColumn(name="func", type=0))

    result = evaluate(NOT_T, table=planets)
    assert not any([r.as_py() for r in result]), [r.as_py() for r in result]
    result = evaluate(NOT_F, table=planets)
    assert all([r.as_py() for r in result]), [r.as_py() for r in result]

    T_XOR_T = Node(
        NodeType.XOR, left=true, right=true, schema_column=FunctionColumn(name="func", type=0)
    )
    T_XOR_F = Node(
        NodeType.XOR, left=true, right=false, schema_column=FunctionColumn(name="func", type=0)
    )
    F_XOR_T = Node(
        NodeType.XOR, left=false, right=true, schema_column=FunctionColumn(name="func", type=0)
    )
    F_XOR_F = Node(
        NodeType.XOR, left=false, right=false, schema_column=FunctionColumn(name="func", type=0)
    )

    result = evaluate(T_XOR_T, table=planets)
    assert not any(c.as_py() for c in result)
    result = evaluate(T_XOR_F, table=planets)
    assert all(result), result
    result = evaluate(F_XOR_T, table=planets)
    assert all(result)
    result = evaluate(F_XOR_F, table=planets)
    assert not any(c.as_py() for c in result)


def test_reading_identifiers():
    planets = opteryx.virtual_datasets.planets.read()

    name_column = opteryx.virtual_datasets.planets.schema().find_column("name")
    name_column.identity = name_column.name
    assert name_column is not None
    names_node = Node(
        NodeType.IDENTIFIER,
        type=OrsoTypes.VARCHAR,
        value=name_column.identity,
        schema_column=name_column,
    )
    names = evaluate(names_node, planets)
    assert len([n for n in names]) == 9
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

    gravity_column = opteryx.virtual_datasets.planets.schema().find_column("gravity")
    gravity_column.identity = gravity_column.name
    assert gravity_column is not None
    gravity_node = Node(
        NodeType.IDENTIFIER,
        type=OrsoTypes.DOUBLE,
        value=gravity_column.identity,
        schema_column=gravity_column,
    )
    gravities = evaluate(gravity_node, planets)
    assert sorted(gravities) == [
        Decimal("0.7"),
        Decimal("3.7"),
        Decimal("3.7"),
        Decimal("8.7"),
        Decimal("8.9"),
        Decimal("9.0"),
        Decimal("9.8"),
        Decimal("11.0"),
        Decimal("23.1"),
    ], sorted(gravities)


def test_function_operations():
    planets = opteryx.virtual_datasets.planets.read()

    name_column = opteryx.virtual_datasets.planets.schema().find_column("name")
    name_column.identity = name_column.name
    assert name_column is not None
    names_node = Node(
        NodeType.IDENTIFIER,
        type=OrsoTypes.VARCHAR,
        value=name_column.identity,
        schema_column=name_column,
    )
    concat = Node(
        NodeType.BINARY_OPERATOR,
        value="StringConcat",
        left=names_node,
        right=names_node,
        schema_column=FunctionColumn(name="add", type=OrsoTypes.VARCHAR),
    )

    gravity_column = opteryx.virtual_datasets.planets.schema().find_column("gravity")
    gravity_column.identity = gravity_column.name
    assert gravity_column is not None
    gravity_node = Node(
        NodeType.IDENTIFIER,
        type=OrsoTypes.DOUBLE,
        value=gravity_column.identity,
        schema_column=gravity_column,
    )
    seven = Node(
        NodeType.LITERAL,
        type=OrsoTypes.INTEGER,
        value=7,
        schema_column=ConstantColumn(name="seven", type=OrsoTypes.INTEGER, value=7),
    )
    plus = Node(
        NodeType.BINARY_OPERATOR,
        value="Plus",
        left=gravity_node,
        right=seven,
        schema_column=FunctionColumn(name="add", type=0),
    )
    multiply = Node(
        NodeType.BINARY_OPERATOR,
        value="Multiply",
        left=gravity_node,
        right=seven,
        schema_column=FunctionColumn(name="times", type=OrsoTypes.DOUBLE),
    )

    names = evaluate(concat, planets)
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
    assert set(plussed).issubset(
        [
            Decimal("10.7"),
            Decimal("15.9"),
            Decimal("16.8"),
            Decimal("10.7"),
            Decimal("30.1"),
            Decimal("16.0"),
            Decimal("15.7"),
            Decimal("18.0"),
            Decimal("7.7"),
        ]
    ), plussed

    timesed = evaluate(multiply, planets)
    assert set(timesed) == {
        Decimal("62.3"),
        Decimal("25.9"),
        Decimal("77.0"),
        Decimal("60.9"),
        Decimal("68.6"),
        Decimal("4.9"),
        Decimal("161.7"),
        Decimal("63.0"),
    }, set(timesed)


def test_compound_expressions():
    planets = opteryx.virtual_datasets.planets.read()

    eight = Node(
        NodeType.LITERAL,
        type=OrsoTypes.INTEGER,
        value=8,
        schema_column=ConstantColumn(name="8", value=8, type=OrsoTypes.INTEGER),
    )
    three_point_seven = Node(
        NodeType.LITERAL,
        type=OrsoTypes.DOUBLE,
        value=3.7,
        schema_column=ConstantColumn(name="3.7", value=3.7, type=OrsoTypes.DOUBLE),
    )
    four_point_two = Node(
        NodeType.LITERAL,
        type=OrsoTypes.DOUBLE,
        value=4.2,
        schema_column=ConstantColumn(name="4.2", value=4.2, type=OrsoTypes.DOUBLE),
    )

    multiply = Node(
        NodeType.BINARY_OPERATOR,
        value="Multiply",
        right=three_point_seven,
        left=eight,
        schema_column=FunctionColumn(name="multiply"),
    )
    gt = Node(
        NodeType.COMPARISON_OPERATOR,
        value="Gt",
        left=multiply,
        right=four_point_two,
        schema_column=FunctionColumn(name="compound"),
    )

    result = evaluate(gt, planets)
    assert all(result), result


def test_functions():
    planets = opteryx.virtual_datasets.planets.read()

    gravity_column = opteryx.virtual_datasets.planets.schema().find_column("gravity")
    gravity_column.identity = gravity_column.name
    assert gravity_column is not None
    gravity_node = Node(
        NodeType.IDENTIFIER,
        type=OrsoTypes.DOUBLE,
        value=gravity_column.identity,
        schema_column=gravity_column,
    )
    _round = Node(
        NodeType.FUNCTION,
        value="ROUND",
        function=lambda x: [round(i) for i in x],
        parameters=[gravity_node],
        schema_column=FunctionColumn(name="func", type=0),
    )

    rounded = evaluate(_round, planets)
    assert len(rounded) == 9
    assert set(r.as_py() for r in rounded) == {4, 23, 9, 1, 11, 10}, list(rounded)


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(LITERALS)} LITERAL TYPE TESTS")
    for node_type, value_type, value in LITERALS:
        print(node_type)
        test_literals(node_type, value_type, value)
    print("okay")

    test_logical_expressions()
    test_reading_identifiers()
    test_function_operations()
    test_compound_expressions()
    test_functions()
