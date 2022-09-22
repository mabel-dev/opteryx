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
import numpy
import pyarrow

from opteryx import operators, functions
from opteryx.exceptions import SqlError
from opteryx.functions.binary_operators import BINARY_OPERATORS
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.utils import dates, fuzzy_search


def literal_boolean(branch, alias: list = None, key=None):
    """create node for a literal boolean branch"""
    return ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=branch, alias=alias)


def literal_null(branch, alias: list = None, key=None):
    """create node for a literal null branch"""
    return ExpressionTreeNode(NodeType.LITERAL_NONE, alias=alias)


def literal_number(branch, alias: list = None, key=None):
    """create node for a literal number branch"""
    # we have one internal numeric type
    return ExpressionTreeNode(
        NodeType.LITERAL_NUMERIC,
        value=numpy.float64(branch[0]),
        alias=alias,
    )


def literal_string(branch, alias: str = None, key=None):
    """create node for a string branch, this is either a data or a string"""
    dte_value = dates.parse_iso(branch)
    if dte_value:
        return ExpressionTreeNode(
            NodeType.LITERAL_TIMESTAMP, value=dte_value, alias=alias
        )
    return ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=branch, alias=alias)


def literal_interval(branch, alias: list = None, key=None):
    """create node for a time literal"""
    values = literal_string(branch["value"]["Value"]).value.split(" ")
    leading_unit = branch["leading_field"]

    parts = ["Year", "Month", "Day", "Hour", "Minute", "Second"]
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
    #    interval = numpy.timedelta64(month, 'M')
    #    interval = numpy.timedelta64(day, 'D')
    #    interval = numpy.timedelta64(nano, 'us')

    return ExpressionTreeNode(NodeType.LITERAL_INTERVAL, value=interval, alias=alias)


def wildcard_filter(branch, alias=None, key=None):
    """a wildcard"""
    return ExpressionTreeNode(NodeType.WILDCARD)


def expression_with_alias(branch, alias=None, key=None):
    """an alias"""
    if not isinstance(alias, list):
        alias = [alias or []]
    alias.append(branch["ast"]["value"])
    return build(branch["expr"], alias=alias)


def qualified_wildcard(branch, alias=None, key=None):
    return ExpressionTreeNode(NodeType.WILDCARD, value=branch[0]["value"], alias=alias)


def identifier(branch, alias=None, key=None):
    # TODO: restore variable exchanges
    #        token_name = function["value"]
    #        if token_name[0] == "@":
    #            if token_name not in self.properties.variables:  # pragma: no cover
    #                raise SqlError(f"Undefined variable found in query `{token_name}`.")
    #            return self.properties.variables.get(token_name)
    #        else:
    return ExpressionTreeNode(
        token_type=NodeType.IDENTIFIER, value=branch["value"], alias=alias
    )


def compound_identifier(branch, alias=None, key=None):
    if not isinstance(alias, list):
        alias = [alias or []]
    alias.append(".".join(p["value"] for p in branch))
    return ExpressionTreeNode(
        token_type=NodeType.IDENTIFIER,
        value=".".join(p["value"] for p in branch),
        alias=alias,
    )


def function(branch, alias=None, key=None):

    func = branch["name"][0]["value"].upper()
    args = [build(a) for a in branch["args"]]
    if functions.is_function(func):
        node_type = NodeType.FUNCTION
    elif operators.is_aggregator(func):
        node_type = NodeType.AGGREGATOR
    else:
        likely_match = fuzzy_search(
            func, operators.aggregators() + functions.functions()
        )
        if likely_match is None:
            raise SqlError(f"Unknown function or aggregate '{func}'")
        raise SqlError(
            f"Unknown function or aggregate '{func}'. Did you mean '{likely_match}'?"
        )
    return ExpressionTreeNode(
        token_type=node_type, value=func, parameters=args, alias=alias
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

    return ExpressionTreeNode(
        operator_type,
        value=operator,
        left_node=left,
        right_node=right,
        alias=alias,
    )


def cast(branch, alias=None, key=None):
    # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
    args = [build(branch["expr"])]
    data_type = branch["data_type"]
    if data_type == "Timestamp":
        data_type = "TIMESTAMP"
    elif "Varchar" in data_type:
        data_type = "VARCHAR"
    elif "Decimal" in data_type:
        data_type = "NUMERIC"
    elif "Boolean" in data_type:
        data_type = "BOOLEAN"
    else:
        raise SqlError(f"Unsupported type for CAST  - '{data_type}'")

    alias.append(f"CAST({args[0].value} AS {data_type})")

    return ExpressionTreeNode(
        NodeType.FUNCTION,
        value=data_type.upper(),
        parameters=args,
        alias=alias,
    )


def try_cast(branch, alias=None, key="TryCast"):
    # TRY_CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
    # also: SAFE_CAST
    function_name = key.replace("Cast", "_Cast").upper()
    args = [build(branch["expr"])]
    data_type = branch["data_type"]
    if data_type == "Timestamp":
        data_type = "TIMESTAMP"
    elif "Varchar" in data_type:
        data_type = "VARCHAR"
    elif "Decimal" in data_type:
        data_type = "NUMERIC"
    elif "Boolean" in data_type:
        data_type = "BOOLEAN"
    else:
        raise SqlError(f"Unsupported type for `{function_name}`  - '{data_type}'")

    alias.append(f"{function_name}({args[0].value} AS {data_type})")

    return ExpressionTreeNode(
        NodeType.FUNCTION,
        value=f"TRY_{data_type.upper()}",
        parameters=args,
        alias=alias,
    )


def extract(branch, alias=None, key=None):
    # EXTRACT(part FROM timestamp)
    if not isinstance(alias, list):
        alias = [alias or []]
    datepart = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=branch["field"])
    value = build(branch["expr"])

    alias.append(f"EXTRACT({datepart.value} FROM {value.value})")
    alias.append(f"DATEPART({datepart.value}, {value.value}")

    return ExpressionTreeNode(
        NodeType.FUNCTION,
        value="DATEPART",
        parameters=[datepart, value],
        alias=alias,
    )


def map_access(branch, alias=None, key=None):
    # Identifier[key] -> GET(Identifier, key) -> alias of I[k] or alias
    if not isinstance(alias, list):
        alias = [alias or []]
    field = branch["column"]["Identifier"]["value"]
    key_dict = branch["keys"][0]["Value"]
    if "SingleQuotedString" in key_dict:
        key = key_dict["SingleQuotedString"]
        key_node = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=key)
    if "Number" in key_dict:
        key = key_dict["Number"][0]
        key_node = ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=key)
    alias.append(f"{identifier}[{key}]")

    identifier_node = ExpressionTreeNode(NodeType.IDENTIFIER, value=field)
    return ExpressionTreeNode(
        NodeType.FUNCTION,
        value="GET",
        parameters=[identifier_node, key_node],
        alias=alias,
    )


def unary_op(branch, alias=None, key=None):
    if not isinstance(alias, list):
        alias = [alias or []]
    if branch["op"] == "Not":
        right = build(branch["expr"])
        return ExpressionTreeNode(token_type=NodeType.NOT, centre_node=right)
    if branch["op"] == "Minus":
        number = 0 - numpy.float64(branch["expr"]["Value"]["Number"][0])
        return ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=number, alias=alias)


def unsupported(branch, alias=None):
    """raise an error"""
    raise SqlError(branch)


def build(value, alias: list = None):
    """
    Extract values from a value node in the AST and create a ExpressionNode for it

    More of the builders will be migrated to this approach to keep the code
    more succinct and easier to read.
    """
    if value == "Null":
        return BUILDERS["Null"](value)
    if isinstance(value, dict):
        return [
            BUILDERS.get(key, unsupported)(value[key], alias, key)
            for key in value.keys()
        ]
    if isinstance(value, list):
        return [build(item, alias) for item in value]


# parts to build the literal parts of a query
BUILDERS = {
    "BinaryOp": binary_op,
    "Boolean": literal_boolean,
    "Cast": cast,
    "CompoundIdentifier": compound_identifier,
    "DoubleQuotedString": literal_string,
    "Expr": build,
    "ExprWithAlias": expression_with_alias,
    "Extract": extract,
    "Function": function,
    "Identifier": identifier,
    "Interval": literal_interval,
    "MapAccess": map_access,
    "Null": literal_null,
    "Number": literal_number,
    "QualifiedWildcard": qualified_wildcard,
    "SafeCast": try_cast,
    "SingleQuotedString": literal_string,
    "TryCast": try_cast,
    "UnaryOp": unary_op,
    "Unnamed": build,
    "Value": build,
    "Wildcard": wildcard_filter,
    "UnnamedExpr": build,
}
