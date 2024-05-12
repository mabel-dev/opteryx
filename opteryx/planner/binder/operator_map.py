from typing import Callable
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Tuple

from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType


class OperatorMapType(NamedTuple):
    result_type: OrsoTypes
    operation_function: Optional[Callable] = None
    cost_estimate: float = 100.0


# fmt: off
OPERATOR_MAP: Dict[Tuple[OrsoTypes, OrsoTypes, str], OperatorMapType] = {
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "PGRegexIMatch"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "PGRegexNotIMatch"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.BOOLEAN, "Or"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Plus"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Minus"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Divide"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Multiply"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Plus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Minus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Plus"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Minus"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Multiply"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Modulo"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "MyIntegerDivide"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "BitwiseOr"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "BitwiseAnd"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "BitwiseXor"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "ShiftLeft"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "ShiftRight"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Plus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "PGRegexIMatch"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "PGRegexNotIMatch"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "PGRegexIMatch"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "PGRegexNotIMatch"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.VARCHAR, None, 100.0),
}
# fmt:on


def determine_type(node) -> OrsoTypes:
    # initial version, needs to be improved
    if node.node_type in (
        NodeType.UNARY_OPERATOR,
        NodeType.AND,
        NodeType.NOT,
        NodeType.NOT,
        NodeType.XOR,
    ):
        return OrsoTypes.BOOLEAN
    if node.node_type == NodeType.NESTED:
        return determine_type(node.centre)
    if node.node_type == NodeType.WILDCARD:
        return OrsoTypes._MISSING_TYPE
    if node.node_type == NodeType.EXPRESSION_LIST:
        if node.parameters[-1].type is not None:
            return node.parameters[-1].type
        return OrsoTypes._MISSING_TYPE  # we can work this out
    if node.node_type == NodeType.LITERAL:
        return node.type

    if node.value in ("NotInSubQuery", "InSubQuery"):
        return OrsoTypes.BOOLEAN

    if node.left.node_type == NodeType.LITERAL:
        left_type = node.left.type
    elif node.left.schema_column:
        left_type = node.left.schema_column.type
    else:
        return OrsoTypes._MISSING_TYPE

    if node.right.node_type == NodeType.LITERAL:
        right_type = node.right.type
    elif node.right.schema_column:
        right_type = node.right.schema_column.type
    else:
        return OrsoTypes._MISSING_TYPE

    operator = node.value

    result = OPERATOR_MAP.get((left_type, right_type, operator))

    if result is None:
        # print(left_type, right_type, operator)
        return OrsoTypes._MISSING_TYPE

    return result.result_type
