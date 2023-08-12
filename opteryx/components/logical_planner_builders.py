# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains various converters for parts of the AST, this 
helps to ensure new AST-based functionality can be added by adding
a function and a reference to it in the dictionary.
"""

import numpy
import pyarrow
from orso.types import OrsoTypes

from opteryx import functions
from opteryx import operators
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.functions.binary_operators import BINARY_OPERATORS
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.utils import dates
from opteryx.utils import fuzzy_search


def literal_boolean(branch, alias: list = None, key=None):
    """create node for a literal boolean branch"""
    return Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=branch, alias=alias)


def literal_null(branch, alias: list = None, key=None):
    """create node for a literal null branch"""
    return Node(NodeType.LITERAL, type=OrsoTypes.NULL, alias=alias)


def literal_number(branch, alias: list = None, key=None):
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


def literal_string(branch, alias: list = None, key=None):
    """create node for a string branch, this is either a date or a string"""
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


def literal_interval(branch, alias: list = None, key=None):
    """
    Create node for a time literal.

    This should look like this in the SQL:
        INTERVAL '1' YEAR
        INTERVAL '1 3' YEAR TO MONTH
    """
    parts = ("Year", "Month", "Day", "Hour", "Minute", "Second")

    values = build(branch["value"]["Value"]).value
    if not isinstance(values, str):
        raise SqlError(f"Invalid INTERVAL, values must be provided as a VARCHAR.")

    values = values.split(" ")
    leading_unit = branch["leading_field"]

    if leading_unit is None:
        raise SqlError(f"Invalid INTERVAL, valid units are {', '.join(p.upper() for p in parts)}")

    unit_index = parts.index(leading_unit)

    month, day, nano = (0, 0, 0)

    for index, value in enumerate(values):
        value = int(value)
        unit = parts[unit_index + index]
        if unit == "Year":
            month += 12 * value
        if unit == "Month":
            month += value
        if unit == "Day":
            day = value
        if unit == "Hour":
            nano += value * 60 * 60 * 1000000000
        if unit == "Minute":
            nano += value * 60 * 1000000000
        if unit == "Second":
            nano += value * 1000000000

    interval = pyarrow.MonthDayNano(
        (
            month,
            day,
            nano,
        )
    )

    return Node(NodeType.LITERAL, type=OrsoTypes.INTERVAL, value=interval, alias=alias)


def wildcard_filter(branch, alias=None, key=None):
    """a wildcard"""
    return Node(NodeType.WILDCARD)


def expression_with_alias(branch, alias=None, key=None):
    """an alias"""
    return build(branch["expr"], alias=branch["alias"]["value"])


def qualified_wildcard(branch, alias=None, key=None):
    parts = [part["value"] for part in [node for node in branch if isinstance(node, list)][0]]
    qualifier = (".".join(parts),)
    return Node(NodeType.WILDCARD, value=qualifier, alias=alias)


def identifier(branch, alias=None, key=None):
    return Node(
        node_type=NodeType.IDENTIFIER,
        value=branch["value"],
        alias=alias,
        query_column=branch["value"],
    )


def compound_identifier(branch, alias=None, key=None):
    if alias is None:
        alias = ".".join(p["value"] for p in branch)
    return Node(
        node_type=NodeType.IDENTIFIER,
        value=".".join(p["value"] for p in branch),
        alias=alias,
        query_column=".".join(p["value"] for p in branch),
        source_column=branch[-1]["value"],
        source=".".join(p["value"] for p in branch[:-1]),
    )


def function(branch, alias=None, key=None):
    func = branch["name"][0]["value"].upper()
    args = [build(a) for a in branch["args"]]
    if functions.is_function(func):
        node_type = NodeType.FUNCTION
    elif operators.is_aggregator(func):
        node_type = NodeType.AGGREGATOR
    else:  # pragma: no cover
        likely_match = fuzzy_search(func, operators.aggregators() + functions.functions())
        if likely_match is None:
            raise UnsupportedSyntaxError(f"Unknown function or aggregate '{func}'")
        raise UnsupportedSyntaxError(
            f"Unknown function or aggregate '{func}'. Did you mean '{likely_match}'?"
        )

    order_by = [(build(item["expr"]), not bool(item["asc"])) for item in branch.get("order_by", [])]

    return Node(
        node_type=node_type,
        value=func,
        parameters=args,
        alias=alias,
        distinct=branch.get("distinct"),
        order=order_by,
    )


def binary_op(branch, alias=None, key=None):
    left = build(branch["left"])
    operator = branch["op"]
    right = build(branch["right"])

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


def cast(branch, alias=None, key=None):
    # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)

    args = [build(branch["expr"])]
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
        data_type = branch["data_type"]["Custom"][0][0]["value"].upper()
    if data_type == "Timestamp":
        data_type = "TIMESTAMP"
    elif "Varchar" in data_type:
        data_type = "VARCHAR"
    elif "Decimal" in data_type:
        data_type = "NUMERIC"
    elif "Numeric" in data_type:
        data_type = "NUMERIC"
    elif "Boolean" in data_type:
        data_type = "BOOLEAN"
    elif "STRUCT" in data_type:
        data_type = "STRUCT"
    else:
        raise SqlError(f"Unsupported type for CAST  - '{data_type}'")

    return Node(
        NodeType.FUNCTION,
        value=data_type.upper(),
        parameters=args,
        alias=alias,
    )


def try_cast(branch, alias=None, key="TryCast"):
    # TRY_CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
    # also: SAFE_CAST
    if not isinstance(alias, list):
        alias = [] if alias is None else [alias]

    function_name = key.replace("Cast", "_Cast").upper()
    args = [build(branch["expr"])]
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
        data_type = branch["data_type"]["Custom"][0][0]["value"].upper()
    if data_type == "Timestamp":
        data_type = "TIMESTAMP"
    elif "Varchar" in data_type:
        data_type = "VARCHAR"
    elif "Decimal" in data_type:
        data_type = "NUMERIC"
    elif "Numeric" in data_type:
        data_type = "NUMERIC"
    elif "Boolean" in data_type:
        data_type = "BOOLEAN"
    elif "STRUCT" in data_type:
        data_type = "STRUCT"
    else:
        raise SqlError(f"Unsupported type for `{function_name}`  - '{data_type}'")

    #    alias.append(f"{function_name}({args[0].value} AS {data_type})")
    #    alias.append(f"{data_type.upper} {args[0].value}")

    return Node(
        NodeType.FUNCTION,
        value=f"TRY_{data_type.upper()}",
        parameters=args,
        alias=alias,
    )


def extract(branch, alias=None, key=None):
    # EXTRACT(part FROM timestamp)
    if not isinstance(alias, list):
        alias = [] if alias is None else [alias]
    datepart = Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=branch["field"])
    value = build(branch["expr"])

    return Node(
        NodeType.FUNCTION,
        value="DATEPART",
        parameters=[datepart, value],
        alias=alias,
    )


def map_access(branch, alias=None, key=None):
    # Identifier[key] -> GET(Identifier, key)

    field = branch["column"]["Identifier"]["value"]
    key_dict = branch["keys"][0]["Value"]
    if "SingleQuotedString" in key_dict:
        key = key_dict["SingleQuotedString"]
        key_node = Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=key)
    if "Number" in key_dict:
        key = int(key_dict["Number"][0])
        key_node = Node(NodeType.LITERAL, type=OrsoTypes.INTEGER, value=key)

    identifier_node = Node(NodeType.IDENTIFIER, value=field)
    return Node(
        NodeType.FUNCTION,
        value="GET",
        parameters=[identifier_node, key_node],
        alias=alias or f"{field}[{repr(key) if isinstance(key, str) else key}]",
    )


def unary_op(branch, alias=None, key=None):
    if branch["op"] == "Not":
        centre = build(branch["expr"])
        return Node(node_type=NodeType.NOT, centre=centre)
    if branch["op"] == "Minus":
        node = literal_number(branch["expr"]["Value"]["Number"], alias=alias)
        node.value = 0 - node.value
        return node
    if branch["op"] == "Plus":
        return literal_number(branch["expr"]["Value"]["Number"], alias=alias)


def between(branch, alias=None, key=None):
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


def in_subquery(branch, alias=None, key=None):
    # if it's a sub-query we create a plan for it
    from opteryx.components.logical_planner import plan_query

    left = build(branch["expr"])
    ast = {}
    ast["Query"] = branch["subquery"]
    subquery_plan = plan_query(ast)
    exit_node = subquery_plan.get_exit_points()[0]
    subquery_plan.remove_node(exit_node, heal=True)
    operator = "NotInList" if branch["negated"] else "InList"

    sub_query = Node(NodeType.SUBQUERY, value=subquery_plan)
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left,
        right=sub_query,
    )


def is_compare(branch, alias=None, key=None):
    centre = build(branch)
    return Node(NodeType.UNARY_OPERATOR, value=key, centre=centre)


def pattern_match(branch, alias=None, key=None):
    negated = branch["negated"]
    left = build(branch["expr"])
    right = build(branch["pattern"])
    if negated:
        key = f"Not{key}"
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=key,
        left=left,
        right=right,
    )


def in_list(branch, alias=None, key=None):
    left_node = build(branch["expr"])
    list_values = {build(v).value for v in branch["list"]}
    operator = "NotInList" if branch["negated"] else "InList"
    right_node = Node(node_type=NodeType.LITERAL, type=OrsoTypes.ARRAY, value=list_values)
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
    )


def in_unnest(branch, alias=None, key=None):
    left_node = build(branch["expr"])
    operator = "NotContains" if branch["negated"] else "Contains"
    right_node = build(branch["array_expr"])
    return Node(
        node_type=NodeType.COMPARISON_OPERATOR,
        value=operator,
        left=left_node,
        right=right_node,
    )


def nested(branch, alias=None, key=None):
    return Node(
        node_type=NodeType.NESTED,
        centre=build(branch),
    )


def tuple_literal(branch, alias=None, key=None):
    return Node(
        NodeType.LITERAL,
        type=OrsoTypes.ARRAY,
        value=[build(t["Value"]).value for t in branch],
        alias=alias,
    )


def substring(branch, alias=None, key=None):
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


def typed_string(branch, alias=None, key=None):
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

    data_value = branch["value"]

    Datatype_Map = {
        "TIMESTAMP": ("TIMESTAMP", lambda x: numpy.datetime64(x, "us")),
        "DATE": ("DATE", lambda x: numpy.datetime64(x, "D")),
    }

    mapper = Datatype_Map.get(data_type)
    if mapper is None:
        raise UnsupportedSyntaxError(f"Cannot Type String type {data_type}")

    return Node(NodeType.LITERAL, type=mapper[0], value=mapper[1](data_value), alias=alias)


def ceiling(value, alias: list = None, key=None):
    data_value = build(value["expr"])
    return Node(NodeType.FUNCTION, value="CEIL", parameters=[data_value], alias=alias)


def floor(value, alias: list = None, key=None):
    data_value = build(value["expr"])
    return Node(NodeType.FUNCTION, value="FLOOR", parameters=[data_value], alias=alias)


def position(value, alias: list = None, key=None):
    sub = build(value["expr"])
    string = build(value["in"])
    return Node(NodeType.FUNCTION, value="POSITION", parameters=[sub, string], alias=alias)


def case_when(value, alias: list = None, key=None):
    fixed_operand = build(value["operand"])
    else_result = build(value["else_result"])

    conditions = []
    for condition in value["conditions"]:
        operand = build(condition)
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
    if else_result is not None:
        conditions.append(Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=True))
    conditions_node = Node(NodeType.EXPRESSION_LIST, value=conditions)

    results = []
    for result in value["results"]:
        results.append(build(result))
    if else_result is not None:
        results.append(else_result)
    results_node = Node(NodeType.EXPRESSION_LIST, value=results)

    return Node(
        NodeType.FUNCTION,
        value="CASE",
        parameters=[conditions_node, results_node],
        alias=alias,
    )


def array_agg(branch, alias=None, key=None):
    from opteryx.components.logical_planner import custom_builders

    distinct = branch["distinct"]
    expression = build(branch["expr"])
    order = None
    if branch["order_by"]:
        order = custom_builders.extract_order({"Query": {"order_by": branch["order_by"]}})
    limit = None
    if branch["limit"]:
        limit = int(build(branch["limit"]).value)

    return Node(
        node_type=NodeType.AGGREGATOR,
        value="ARRAY_AGG",
        expression=expression,
        distinct=distinct,
        order=order,
        limit=limit,
        alias=alias,
    )


def trim_string(branch, alias=None, key=None):
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


def unsupported(branch, alias=None, key=None):
    """raise an error"""
    raise SqlError(f"Unhandled token in Syntax Tree `{key}`")


def build(value, alias: list = None, key=None):
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
    "ArrayAgg": array_agg,
    "Between": between,
    "BinaryOp": binary_op,
    "Boolean": literal_boolean,
    "Case": case_when,
    "Cast": cast,
    "Ceil": ceiling,
    "CompoundIdentifier": compound_identifier,
    "DoubleQuotedString": literal_string,
    "Expr": build,
    "ExprWithAlias": expression_with_alias,
    "Extract": extract,
    "Floor": floor,
    "Function": function,
    "Identifier": identifier,
    "ILike": pattern_match,
    "InList": in_list,
    "InSubquery": in_subquery,
    "Interval": literal_interval,
    "InUnnest": in_unnest,
    "IsFalse": is_compare,
    "IsNotNull": is_compare,
    "IsNull": is_compare,
    "IsTrue": is_compare,
    "Like": pattern_match,
    "MapAccess": map_access,
    "Nested": nested,
    "Null": literal_null,
    "Number": literal_number,
    "Position": position,
    "QualifiedWildcard": qualified_wildcard,
    "SafeCast": try_cast,
    "SingleQuotedString": literal_string,
    "SimilarTo": pattern_match,
    "Substring": substring,
    "Tuple": tuple_literal,
    "Trim": trim_string,
    "TryCast": try_cast,
    "TypedString": typed_string,
    "UnaryOp": unary_op,
    "Unnamed": build,
    "Value": build,
    "Wildcard": wildcard_filter,
    "UnnamedExpr": build,
}
