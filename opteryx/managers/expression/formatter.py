from dataclasses import dataclass
from typing import Any

from orso.schema import FlatColumn
from orso.types import OrsoTypes


@dataclass
class ExpressionColumn(FlatColumn):
    expression: Any = None


def format_expression(root, qualify: bool = False):
    # circular imports
    from . import INTERNAL_TYPE
    from . import NodeType

    if root is None:
        return "null"

    if not qualify and root.left and root.right:
        # if left and right would look the same without qualifying, force qualification
        qualify = (root.left.current_name == root.right.current_name) and (
            root.right.current_name is not None
        )

    if isinstance(root, list):
        return [format_expression(item, qualify) for item in root]

    node_type = root.node_type
    _map: dict = {}

    # LITERAL TYPES
    if node_type == NodeType.LITERAL:
        literal_type = root.type
        if literal_type == OrsoTypes.VARCHAR:
            return "'" + root.value.replace("'", "'") + "'"
        if literal_type == OrsoTypes.TIMESTAMP:
            return "'" + str(root.value) + "'"
        if literal_type == OrsoTypes.INTERVAL:
            return "<INTERVAL>"
        return str(root.value)
    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:
        if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            if root.value == "CASE":
                con = [format_expression(a, qualify) for a in root.parameters[0].value]
                vals = [format_expression(a, qualify) for a in root.parameters[1].value]
                return "CASE " + "".join([f"WHEN {c} THEN {v} " for c, v in zip(con, vals)]) + "END"
            distinct = "DISTINCT " if root.distinct else ""
            order = ""
            if root.order:
                order = f" ORDER BY {', '.join(item[0].value + (' DESC' if not item[1] else '') for item in (root.order or []))}"
            if root.value == "ARRAY_AGG":
                limit = f" LIMIT {root.limit}" if root.limit else ""
                return f"{root.value.upper()}({distinct}{format_expression(root.expression, qualify)}{order}{limit})"
            return f"{root.value.upper()}({distinct}{','.join([format_expression(e, qualify) for e in root.parameters])}{order})"
        if node_type == NodeType.WILDCARD:
            if root.value:
                return f"{root.value[0]}.*"
            return "*"
        if node_type == NodeType.BINARY_OPERATOR:
            _map = {
                "StringConcat": "||",
                "Plus": "+",
                "Minus": "-",
                "Multiply": "*",
                "Divide": "/",
                "MyIntegerDivide": "div",
                "Modulo": "%",
                "BitwiseOr": "|",
                "BitwiseAnd": "&",
                "BitwiseXor": "^",
                "ShiftLeft": "<<",
                "ShiftRight": ">>",
            }
            return f"{format_expression(root.left, qualify)} {_map.get(root.value, root.value).upper()} {format_expression(root.right, qualify)}"
    if node_type == NodeType.COMPARISON_OPERATOR:
        _map = {
            "Eq": "=",
            "Lt": "<",
            "Gt": ">",
            "NotEq": "!=",
            "BitwiseOr": "|",
            "LtEq": "<=",
            "GtEq": ">=",
        }
        return f"{format_expression(root.left, qualify)} {_map.get(root.value, root.value).upper()} {format_expression(root.right, qualify)}"
    if node_type == NodeType.UNARY_OPERATOR:
        _map = {"IsNull": "%s IS NULL", "IsNotNull": "%s IS NOT NULL"}
        return _map.get(root.value, root.value + "(%s)").replace(
            "%s", format_expression(root.centre, qualify)
        )
    if node_type == NodeType.NOT:
        return f"NOT {format_expression(root.centre, qualify)}"
    if node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        _map = {
            NodeType.AND: "AND",
            NodeType.OR: "OR",
            NodeType.XOR: "XOR",
        }  # type:ignore
        return f"{format_expression(root.left, qualify)} {_map[node_type]} {format_expression(root.right, qualify)}"
    if node_type == NodeType.NESTED:
        return f"({format_expression(root.centre, qualify)})"
    if node_type == NodeType.IDENTIFIER:
        if qualify and root.source:
            return root.qualified_name
        return root.current_name
    return str(root.value)
