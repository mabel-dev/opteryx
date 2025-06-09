# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This module contains various converters for parts of the AST, this
helps to ensure new AST-based functionality can be added by adding
a function and a reference to it in the dictionary.
"""

import decimal
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import numpy
from orso.types import OrsoTypes

from opteryx import functions
from opteryx import operators
from opteryx.exceptions import ArrayWithMixedTypesError
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.managers.expression.binary_operators import BINARY_OPERATORS
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.utils import dates
from opteryx.utils import suggest_alternative


def any_op(branch, alias: Optional[List[str]] = None, key=None):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value="AnyOp" + branch.get("compare_op", "Unsupported"),
        left=build(branch["left"]),
        right=build(branch["right"]),
    )


def all_op(branch, alias: Optional[List[str]] = None, key=None):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value="AllOp" + branch.get("compare_op", "Unsupported"),
        left=build(branch["left"]),
        right=build(branch["right"]),
    )


def array(branch, alias: Optional[List[str]] = None, key=None):
    value_nodes = [build(elem) for elem in branch["elem"]]
    value_list = {v.value for v in value_nodes}
    element_type = {v.type for v in value_nodes}
    if len(element_type) > 1:
        raise ArrayWithMixedTypesError("Literal ARRAY has values with mixed types.")
    element_type = element_type.pop() if len(element_type) == 1 else OrsoTypes.VARCHAR

    return Node(
        node_type=NodeType.LITERAL,
        type=OrsoTypes.ARRAY,
        element_type=element_type,
        value=value_list,
    )


def between(branch, alias: Optional[List[str]] = None, key=None):
    expr = build(branch["expr"])
    low = build(branch["low"])
    high = build(branch["high"])
    inverted = branch["negated"]

    if inverted:
        # LEFT <= LOW AND LEFT >= HIGH (not between)
        left_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="Lt",
            left=expr,
            right=low,
        )
        right_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="Gt",
            left=expr,
            right=high,
        )

        return Node(NodeType.OR, left=left_node, right=right_node)
    else:
        # LEFT > LOW and LEFT < HIGH (between)
        left_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="GtEq",
            left=expr,
            right=low,
        )
        right_node = Node(
            NodeType.COMPARISON_OPERATOR,
            value="LtEq",
            left=expr,
            right=high,
        )

        return Node(NodeType.AND, left=left_node, right=right_node)


def binary_op(branch, alias: Optional[List[str]] = None, key=None):
    left = build(branch["left"])
    operator = branch["op"]
    right = build(branch["right"])

    if operator in ("PGRegexMatch", "SimilarTo"):
        operator = "RLike"
    if operator in ("PGRegexNotMatch", "NotSimilarTo"):
        operator = "NotRLike"

    operator_type = NodeType.COMPARISON_OPERATOR
    if operator in BINARY_OPERATORS:
        operator_type = NodeType.BINARY_OPERATOR
    if operator == "And":
        operator_type = NodeType.AND
    if operator == "Or":
        operator_type = NodeType.OR
    if operator == "Xor":
        operator_type = NodeType.XOR

    return Node(
        operator_type,
        value=operator,
        left=left,
        right=right,
        alias=alias,
    )


def case_when(value, alias: Optional[List[str]] = None, key=None):
    fixed_operand = build(value["operand"])
    else_result = build(value["else_result"])

    conditions = []
    results = []
    for condition in value["conditions"]:
        operand = build(condition["condition"])
        if fixed_operand is None:
            conditions.append(operand)
        else:
            conditions.append(
                Node(
                    NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    left=fixed_operand,
                    right=operand,
                )
            )
        result = build(condition["result"])
        results.append(result)

    if else_result is not None:
        conditions.append(Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=True))
        results.append(else_result)
    conditions_node = Node(NodeType.EXPRESSION_LIST, parameters=conditions)
    results_node = Node(NodeType.EXPRESSION_LIST, parameters=results)

    return Node(
        NodeType.FUNCTION,
        value="CASE",
        parameters=[conditions_node, results_node],
        alias=alias,
    )


def cast(branch, alias: Optional[List[str]] = None, key=None):
    # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)

    from opteryx.planner import build_literal_node

    args = [build(branch["expr"])]
    kind = branch["kind"]
    data_type = branch["data_type"]
    if isinstance(data_type, dict):
        # timestamps have the timezone as a value
        type_key = next(iter(data_type))
        if type_key == "Timestamp" and data_type[type_key] not in (
            (None, "None"),
            (None, "WithoutTimeZone"),
        ):
            raise UnsupportedSyntaxError("TIMESTAMPS do not support `TIME ZONE`")
        data_type = type_key
    if "Custom" in data_type:
        data_type = branch["data_type"]["Custom"][0][0]["Identifier"]["value"].upper()
    if data_type == "Timestamp":
        data_type = "TIMESTAMP"
    elif data_type == "Date":
        data_type = "DATE"
    elif "Varchar" in data_type:
        data_type = "VARCHAR"
    elif "Decimal" in data_type:
        data_type = "DECIMAL"
        if "PrecisionAndScale" in branch["data_type"]["Decimal"]:
            precision = branch["data_type"]["Decimal"]["PrecisionAndScale"][0]
            scale = branch["data_type"]["Decimal"]["PrecisionAndScale"][1]
            args.append(build_literal_node(precision))
            args.append(build_literal_node(scale))
    elif "Integer" in data_type:
        data_type = "INTEGER"
    elif "Double" in data_type:
        data_type = "DOUBLE"
    elif "Boolean" in data_type:
        data_type = "BOOLEAN"
    elif "STRUCT" in data_type:
        data_type = "STRUCT"
    elif "Blob" in data_type:
        data_type = "BLOB"
    elif "Array" in data_type:
        element_key = branch["data_type"]["Array"].get("AngleBracket", {"Varchar": None})
        if isinstance(element_key, dict):
            element_key = next(iter(element_key))
        if isinstance(element_key, str):
            element_key = build_literal_node(element_key.upper())
            args.append(element_key)
        data_type = "ARRAY"
    else:
        raise SqlError(f"Unsupported type for CAST  - '{data_type}'")

    if kind in {"TryCast", "SafeCast"}:
        data_type = "TRY_" + data_type

    return Node(
        NodeType.FUNCTION,
        value=data_type.upper(),
        parameters=args,
        alias=alias,
    )


def ceiling(value, alias: Optional[List[str]] = None, key=None):
    data_value = build(value["expr"])
    scale = build(value["field"]["Scale"]) if "Scale" in value["field"] else literal_number([0])
    return Node(NodeType.FUNCTION, value="CEIL", parameters=[data_value, scale], alias=alias)


def compound_identifier(branch, alias: Optional[List[str]] = None, key=None):
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER,  # column type
        alias=alias,  # type: ignore
        source_column=branch[-1]["value"],  # the source column
        source=".".join(p["value"] for p in branch[:-1]),  # the source relation
    )


def expression_with_alias(branch, alias: Optional[List[str]] = None, key=None):
    """an alias"""
    return build(branch["expr"], alias=branch["alias"]["value"])


def exists(branch, alias: Optional[List[str]] = None, key=None):
    from opteryx.planner.logical_planner.logical_planner import plan_query

    subplan = plan_query(branch["subquery"])
    not_exists = Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=branch["negated"])

    raise UnsupportedSyntaxError("EXISTS is not supported in Opteryx")

    return Node(
        NodeType.UNARY_OPERATOR,
        value="EXISTS",
        parameters=[Node(NodeType.SUBQUERY, plan=subplan), not_exists],
        alias=alias,
    )


def expressions(branch, alias: Optional[List[str]] = None, key=None):
    return [build(part) for part in branch]


def extract(branch, alias: Optional[List[str]] = None, key=None):
    # EXTRACT(part FROM timestamp)
    datepart_value = branch["field"]
    if isinstance(datepart_value, dict):
        datepart_value = list(datepart_value)[0]
    datepart = Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=datepart_value)
    identifier = build(branch["expr"])

    return Node(
        NodeType.FUNCTION,
        value="DATEPART",
        parameters=[datepart, identifier],
        alias=alias,
    )


def floor(value, alias: Optional[List[str]] = None, key=None):
    data_value = build(value["expr"])
    scale = build(value["field"]["Scale"]) if "Scale" in value["field"] else literal_number([0])
    return Node(NodeType.FUNCTION, value="FLOOR", parameters=[data_value, scale], alias=alias)


def function(branch, alias: Optional[List[str]] = None, key=None):
    func = ".".join(build(p).value for p in branch["name"]).upper()

    order_by = None
    limit = None
    duplicate_treatment = None
    null_treatment = None
    filters = None
    args = []

    if branch["args"] != "None":
        args = [build(a) for a in branch["args"]["List"]["args"]]

        for clause in branch["args"]["List"]["clauses"]:
            if "OrderBy" in clause:
                order_by = [
                    (
                        build(item["expr"]),
                        True if item["options"]["asc"] is None else item["options"]["asc"],
                    )
                    for item in clause["OrderBy"]
                ]
            elif "Limit" in clause:
                limit = build(clause["Limit"]).value

        duplicate_treatment = branch["args"]["List"].get("duplicate_treatment")
        null_treatment = branch["args"].get("null_treatment")
        filters = branch["args"].get("filters")

    if functions.is_function(func):
        node_type = NodeType.FUNCTION
    elif operators.is_aggregator(func):
        node_type = NodeType.AGGREGATOR
    else:  # pragma: no cover
        from opteryx.exceptions import FunctionNotFoundError
        from opteryx.functions import DEPRECATED_FUNCTIONS

        if func in DEPRECATED_FUNCTIONS:
            alt = DEPRECATED_FUNCTIONS.get(func)
            if alt:
                raise UnsupportedSyntaxError(
                    f"Function '{func}' has been deprecated, '{alt}' offers similar functionality."
                )
            raise UnsupportedSyntaxError(f"Function '{func}' has been deprecated.")

        likely_match = suggest_alternative(func, operators.aggregators() + functions.functions())
        if likely_match is None:
            raise UnsupportedSyntaxError(f"Unknown function or aggregate '{func}'")
        raise FunctionNotFoundError(
            f"Unknown function or aggregate '{func}'. Did you mean '{likely_match}'?"
        )

    # rewrite COUNT_DISTINCT() to COUNT(DISTINCT)
    if func == "COUNT_DISTINCT":
        func = "COUNT"
        duplicate_treatment = "Distinct"

    node = Node(
        node_type=node_type,
        value=func,
        parameters=args,
        alias=alias,
        duplicate_treatment=duplicate_treatment,
        null_treatment=null_treatment,
        filters=filters,
        order=order_by,
        limit=limit,
    )
    node.qualified_name = format_expression(node)
    return node


def hex_literal(branch, alias: Optional[List[str]] = None, key=None):
    value = int(branch, 16)
    return Node(
        NodeType.LITERAL,
        type=OrsoTypes.INTEGER,
        value=value,
        #    alias=alias or f"0x{branch}"
    )


def identifier(branch, alias: Optional[List[str]] = None, key=None):
    """idenitifier doesn't have a qualifier (recorded in source)"""
    if "Identifier" in branch:
        return build(branch["Identifier"], alias=alias)
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER,  # column type
        alias=alias,  # type: ignore
        source_column=branch["value"],  # the source column
    )


def in_list(branch, alias: Optional[List[str]] = None, key=None):
    left_node = build(branch["expr"])
    value_nodes = [build(v) for v in branch["list"]]
    value_list = {v.value for v in value_nodes}
    element_type = {v.type for v in value_nodes}
    if len(element_type) > 1:
        raise ArrayWithMixedTypesError("Array in IN condition has values with mixed types.")
    element_type = element_type.pop()
    operator = "NotInList" if branch["negated"] else "InList"
    right_node = Node(
        node_type=NodeType.LITERAL,
        type=OrsoTypes.ARRAY,
        value=value_list,
        element_type=element_type,
    )
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
    )


def in_subquery(branch, alias: Optional[List[str]] = None, key=None):
    # if it's a sub-query we create a plan for it
    from opteryx.planner.logical_planner.logical_planner import plan_query

    left = build(branch["expr"])
    ast = {}
    ast["Query"] = branch["subquery"]
    subquery_plan = plan_query(ast)
    exit_node = subquery_plan.get_exit_points()[0]
    subquery_plan.remove_node(exit_node, heal=True)
    operator = "NotInSubQuery" if branch["negated"] else "InSubQuery"

    sub_query = Node(NodeType.SUBQUERY, value=subquery_plan)
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left,
        right=sub_query,
    )


def in_unnest(branch, alias: Optional[List[str]] = None, key=None):
    left_node = build(branch["expr"])
    operator = "AllOpNotEq" if branch["negated"] else "AnyOpEq"
    right_node = build(branch["array_expr"])
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
    )


def is_compare(branch, alias: Optional[List[str]] = None, key=None):
    centre = build(branch)
    return Node(NodeType.UNARY_OPERATOR, value=key, centre=centre)


def json_access(branch, alias: Optional[List[str]] = None, key=None):
    identifier_node = build(branch["value"])
    key_node = build(branch["path"]["path"][0]["Bracket"]["key"])

    if key_node.node_type == NodeType.IDENTIFIER:
        raise UnsupportedSyntaxError("Subscript values must be literals.")

    key_value = key_node.value
    if isinstance(key_value, str):
        key_value = f"'{key_value}'"
        return Node(
            NodeType.BINARY_OPERATOR,
            value="Arrow",
            left=identifier_node,
            right=key_node,
            alias=alias or f"{identifier_node.current_name} -> {key_value}",
        )

    return Node(
        NodeType.FUNCTION,
        value="GET",
        parameters=[identifier_node, key_node],
        alias=alias or f"{identifier_node.current_name}[{key_value}]",
    )


def literal_boolean(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a literal boolean branch"""
    return Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=branch, alias=alias)


def literal_interval(branch, alias: Optional[List[str]] = None, key=None):
    """
    Create node for a time literal.

    This should look like this in the SQL:
        INTERVAL '1' YEAR
        INTERVAL '1 3' YEAR TO MONTH
    """
    parts = ("Year", "Month", "Day", "Hour", "Minute", "Second")

    if "Value" not in branch["value"]:
        raise SqlError("Invalid INTERVAL, expected format `INTERVAL '1' MONTH`")
    values = build(branch["value"]["Value"]).value
    if not isinstance(values, str):
        raise SqlError("Invalid INTERVAL, values must be provided as a VARCHAR.")

    values = values.split(" ")
    leading_unit = branch["leading_field"]

    if leading_unit is None:
        raise SqlError(f"Invalid INTERVAL, valid units are {', '.join(p.upper() for p in parts)}")

    unit_index = parts.index(leading_unit)

    month, seconds = (0, 0)

    for index, value in enumerate(values):
        value = int(value)
        unit = parts[unit_index + index]
        if unit == "Year":
            month += 12 * value
        if unit == "Month":
            month += value
        if unit == "Day":
            seconds = value * 24 * 60 * 60
        if unit == "Hour":
            seconds += value * 60 * 60
        if unit == "Minute":
            seconds += value * 60
        if unit == "Second":
            seconds += value

    interval = (month, seconds)

    return Node(NodeType.LITERAL, type=OrsoTypes.INTERVAL, value=interval, alias=alias)


def literal_null(branch=None, alias: Optional[List[str]] = None, key=None):
    """create node for a literal null branch"""
    return Node(NodeType.LITERAL, type=OrsoTypes.NULL, alias=alias)


def literal_number(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a literal number branch"""
    # we have one internal numeric type

    value = branch[0]
    try:
        # Try converting to int first
        value = int(value)
        return Node(
            NodeType.LITERAL,
            type=OrsoTypes.INTEGER,
            value=numpy.int64(branch[0]),  # value
            alias=alias,
        )
    except ValueError:
        # If int conversion fails, try converting to float
        value = float(value)
        return Node(
            NodeType.LITERAL,
            type=OrsoTypes.DOUBLE,
            value=numpy.float64(branch[0]),  # value
            alias=alias,
        )


def literal_string(branch, alias: Optional[List[str]] = None, key=None):
    """create node for a string branch, this is either a date or a string"""
    if not str(branch).isdigit():
        dte_value = dates.parse_iso(branch)
        if dte_value:
            if len(branch) <= 10:
                return Node(
                    NodeType.LITERAL,
                    type=OrsoTypes.DATE,
                    value=numpy.datetime64(dte_value, "D"),
                    alias=alias,
                )
            return Node(
                NodeType.LITERAL,
                type=OrsoTypes.TIMESTAMP,
                value=numpy.datetime64(dte_value, "us"),
                alias=alias,
            )
    return Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=branch, alias=alias)


def map_access(branch, alias: Optional[List[str]] = None, key=None):
    # Identifier[key] -> GET(Identifier, key)

    identifier_node = build(branch["column"])
    key_node = build(branch["keys"][0]["key"])
    key_value = key_node.value
    if isinstance(key_value, str):
        key_value = f"'{key_value}'"

    if key_node.node_type != NodeType.LITERAL:
        raise UnsupportedSyntaxError("Subscript values must be literals")

    return Node(
        NodeType.FUNCTION,
        value="GET",
        parameters=[identifier_node, key_node],
        alias=alias or f"{identifier_node.current_name}[{key_value}]",
    )


def match_against(branch, alias: Optional[List[str]] = None, key=None):
    columns = [identifier(col) for col in branch["columns"]]
    match_to = build(branch["match_value"])
    mode = branch["opt_search_modifier"]

    return Node(
        NodeType.FUNCTION,
        value="MATCH_AGAINST",
        parameters=[columns[0], match_to],
        alias=alias or f"MATCH ({columns[0].value}) AGAINST ({match_to.value})",
    )


def nested(branch, alias: Optional[List[str]] = None, key=None):
    return Node(
        node_type=NodeType.NESTED,
        centre=build(branch),
    )


def pattern_match(branch, alias: Optional[List[str]] = None, key=None):
    negated = branch["negated"]
    left = build(branch["expr"])
    right = build(branch["pattern"])
    is_any = branch.get("any", False)
    if key in ("PGRegexMatch", "SimilarTo"):
        key = "RLike"
    if negated:
        key = f"Not{key}"
    if is_any:
        key = f"AnyOp{key}"
        if right.node_type == NodeType.IDENTIFIER:
            raise UnsupportedSyntaxError(
                "LIKE ANY syntax incorrect, `column LIKE ANY (patterns)` expected."
            )
        if right.node_type == NodeType.NESTED:
            right = right.centre
        if right.type != OrsoTypes.ARRAY:
            right.value = (right.value,)
            right.type = OrsoTypes.ARRAY
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=key,
        left=left,
        right=right,
        alias=alias,
    )


def placeholder(value, alias: Optional[List[str]] = None, key=None):
    from opteryx.exceptions import ParameterError

    raise ParameterError("Unresolved parameter in query.")


def position(value, alias: Optional[List[str]] = None, key=None):
    sub = build(value["expr"])
    string = build(value["in"])
    return Node(NodeType.FUNCTION, value="POSITION", parameters=[sub, string], alias=alias)


def qualified_wildcard(branch, alias: Optional[List[str]] = None, key=None):
    parts = [build(part).value for part in branch[0]["ObjectName"]]
    qualifier = (".".join(parts),)
    return Node(NodeType.WILDCARD, value=qualifier, alias=alias)


def substring(branch, alias: Optional[List[str]] = None, key=None):
    node_node = Node(NodeType.LITERAL, type=OrsoTypes.NULL, value=None)
    string = build(branch["expr"])
    substring_from = build(branch["substring_from"]) or node_node
    substring_for = build(branch["substring_for"]) or node_node
    return Node(
        NodeType.FUNCTION,
        value="SUBSTRING",
        parameters=[string, substring_from, substring_for],
        alias=alias,
    )


def trim_string(branch, alias: Optional[List[str]] = None, key=None):
    who = build(branch["trim_what"])
    what = build(branch["expr"])
    where = branch["trim_where"] or "Both"

    function = "TRIM"
    if where == "Leading":
        function = "LTRIM"
    if where == "Trailing":
        function = "RTRIM"

    parameters = [what]
    if who is not None:
        parameters.append(who)

    return Node(
        NodeType.FUNCTION,
        value=function,
        parameters=parameters,
        alias=alias,
    )


def tuple_literal(branch, alias: Optional[List[str]] = None, key=None):
    # Tuples can have values of different types
    # if they all are the same type, be explicit about it
    node_values = [build(t) for t in branch]
    values = [t.value for t in node_values]

    # see if we can specify the element type for the arrat
    node_types = {t.type for t in node_values}
    element_type = None
    if len(node_types) == 1:
        element_type = node_types.pop()

    if values and isinstance(values[0], dict):
        values = [build(val["Identifier"]).value for val in values]
    return Node(
        NodeType.LITERAL,
        type=OrsoTypes.ARRAY,
        element_type=element_type,
        value=tuple(values),
        alias=alias,
    )


def typed_string(branch, alias: Optional[List[str]] = None, key=None):
    data_type = branch["data_type"]

    if isinstance(data_type, dict):
        # timestamps have the timezone as a value
        type_key = next(iter(data_type))
        if type_key == "Timestamp" and data_type[type_key] not in (
            (None, "None"),
            (None, "WithoutTimeZone"),
        ):
            raise UnsupportedSyntaxError("TIMESTAMPS do not support `TIME ZONE`")
        data_type = type_key
    data_type = data_type.upper()

    data_value = build(branch["value"]).value

    Datatype_Map: Dict[str, Tuple[str, Callable]] = {
        "TIMESTAMP": (OrsoTypes.TIMESTAMP, lambda x: numpy.datetime64(x, "us")),
        "DATE": (OrsoTypes.DATE, lambda x: numpy.datetime64(x, "D")),
        "INTEGER": (OrsoTypes.INTEGER, numpy.int64),
        "DOUBLE": (OrsoTypes.DOUBLE, numpy.float64),
        "DECIMAL": (OrsoTypes.DECIMAL, decimal.Decimal),
        "BOOLEAN": (OrsoTypes.BOOLEAN, bool),
    }

    mapper = Datatype_Map.get(data_type)
    if mapper is None:
        raise UnsupportedSyntaxError(f"Cannot Type String type {data_type}")

    return Node(NodeType.LITERAL, type=mapper[0], value=mapper[1](data_value), alias=alias)


def unary_op(branch, alias: Optional[List[str]] = None, key=None):
    if branch["op"] == "Not":
        centre = build(branch["expr"])
        return Node(node_type=NodeType.NOT, centre=centre)
    if branch["op"] == "Minus":
        node = literal_number(branch["expr"]["Value"]["value"]["Number"], alias=alias)
        node.value = 0 - node.value
        return node
    if branch["op"] == "Plus":
        return literal_number(branch["expr"]["Value"]["value"]["Number"], alias=alias)


def wildcard_filter(branch, alias: Optional[List[str]] = None, key=None):
    """a wildcard"""
    except_columns = None
    if isinstance(branch, dict) and branch.get("opt_except") is not None:
        except_columns = [build({"Identifier": branch["opt_except"]["first_element"]})]
        except_columns.extend(
            [build({"Identifier": e}) for e in branch["opt_except"]["additional_elements"]]
        )
    return Node(NodeType.WILDCARD, except_columns=except_columns)


# ----------


def unsupported(branch, alias: Optional[List[str]] = None, key=None):
    """raise an error"""
    print("[INTERNAL]", branch)
    raise SqlError(f"Unhandled token in Syntax Tree `{key}`")


def build(value, alias: Optional[List[str]] = None, key=None):
    """
    Extract values from a value node in the AST and create a ExpressionNode for it

    More of the builders will be migrated to this approach to keep the code
    more succinct and easier to read.
    """
    ignored = ("filter",)

    if value in ("Null", "Wildcard"):
        return BUILDERS[value](value)
    if isinstance(value, dict):
        key = next(iter(value))
        if key in ignored:
            return None
        return BUILDERS.get(key, unsupported)(value[key], alias, key)
    if isinstance(value, list):
        return [build(item, alias) for item in value]
    return None


# parts to build the literal parts of a query
BUILDERS = {
    "AnyOp": any_op,
    "All": lambda x, y, z: [NodeType.WILDCARD],
    "AllOp": all_op,
    "Array": array,  # not actually implemented
    "Between": between,
    "BinaryOp": binary_op,
    "Boolean": literal_boolean,
    "Case": case_when,
    "Cast": cast,
    "Ceil": ceiling,
    "CompoundIdentifier": compound_identifier,
    "DoubleQuotedString": literal_string,
    "Exists": exists,
    "Expr": build,
    "Expressions": expressions,
    "ExprWithAlias": expression_with_alias,
    "Extract": extract,
    "Floor": floor,
    "Function": function,
    "HexStringLiteral": hex_literal,
    "Identifier": identifier,
    "ILike": pattern_match,
    "InList": in_list,
    "InSubquery": in_subquery,
    "Interval": literal_interval,
    "InUnnest": in_unnest,
    "IsFalse": is_compare,
    "IsNotFalse": is_compare,
    "IsNotNull": is_compare,
    "IsNotTrue": is_compare,
    "IsNull": is_compare,
    "IsTrue": is_compare,
    "JsonAccess": json_access,
    "Like": pattern_match,
    "MapAccess": map_access,
    "MatchAgainst": match_against,
    "Nested": nested,
    "Null": literal_null,
    "Number": literal_number,
    "Placeholder": placeholder,
    "Position": position,
    "QualifiedWildcard": qualified_wildcard,
    "RLike": pattern_match,
    "SingleQuotedString": literal_string,
    "SimilarTo": pattern_match,
    "Substring": substring,
    "Tuple": tuple_literal,
    "Trim": trim_string,
    "TypedString": typed_string,
    "UnaryOp": unary_op,
    "Unnamed": build,
    "Value": build,
    "value": build,
    "Wildcard": wildcard_filter,
    "UnnamedExpr": build,
}
