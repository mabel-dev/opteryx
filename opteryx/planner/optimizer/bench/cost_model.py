# pragma: no cover
import decimal

import pyarrow
from orso.types import OrsoTypes

import opteryx
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression.ops import filter_operations
from opteryx.models import Node

OPERATORS = [
    "Eq",
    "NotEq",
    "Gt",
    "GtEq",
    "Lt",
    "LtEq",
    "Like",
    "ILike",
    "NotLike",
    "NotILike",
    "InList",
    "RLike",
    "NotRLike",
    "InStr",
    "IInStr",
    "NotInStr",
    "NotIInStr",
]

LITERALS = [
    (OrsoTypes.BOOLEAN, True),
    (OrsoTypes.ARRAY, ["a", "b", "c"]),
    (OrsoTypes.INTEGER, 0),
    (OrsoTypes.DOUBLE, 1e10),
    (OrsoTypes.DECIMAL, decimal.Decimal(4)),
    (OrsoTypes.INTEGER, int(4)),
    (OrsoTypes.DOUBLE, float(4)),
    (OrsoTypes.STRUCT, {"a": "b"}),
    (OrsoTypes.DATE, opteryx.utils.dates.parse_iso("2022-01-01").date()),
    (OrsoTypes.TIMESTAMP, opteryx.utils.dates.parse_iso("2022-01-01 13:31")),
    (OrsoTypes.VARCHAR, "1" * 50),
    (OrsoTypes.BLOB, b"1" * 50),
]


def measure_performance(data_type, op):
    import time

    table = pyarrow.Table.from_arrays([[1] * int(1e6)], ["one"])

    operand = None
    for literal, value in LITERALS:
        if literal == data_type:
            operand = Node(node_type=NodeType.LITERAL, type=data_type, value=value)
            operand = evaluate(operand, table)
            break

    t = time.monotonic_ns()
    try:
        filter_operations(operand, data_type, op, operand, data_type)
    except Exception as E:
        print(f"{op}, {E}")
        return None
    return ((time.monotonic_ns() - t) / 1e9, data_type, op)


for t in OrsoTypes:
    for o in OPERATORS:
        perf = measure_performance(t, o)
        if perf:
            print(measure_performance(t, o))
