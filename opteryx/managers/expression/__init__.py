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
Expressions describe a calculation or evaluation of some sort.

It is defined as an expression tree of binary and unary operators, and functions.

Expressions are evaluated against an entire page at a time.
"""
from enum import Enum

import numpy
import pyarrow

from cityhash import CityHash64
from pyarrow import Table

from opteryx.functions import FUNCTIONS
from opteryx.functions.binary_operators import binary_operations
from opteryx.functions.unary_operations import UNARY_OPERATIONS
from opteryx.models import Columns
from opteryx.third_party.pyarrow_ops.ops import filter_operations


# These are bit-masks
LOGICAL_TYPE: int = int("0001", 2)
INTERNAL_TYPE: int = int("0010", 2)
LITERAL_TYPE: int = int("0100", 2)


def format_expression(root):
    node_type = root.token_type

    # LITERAL TYPES
    if node_type & LITERAL_TYPE == LITERAL_TYPE:
        if node_type == NodeType.LITERAL_VARCHAR:
            return "'" + root.value.replace("'", "'") + "'"
        if node_type == NodeType.LITERAL_TIMESTAMP:
            return "'" + root.value.isoformat() + "'"
        if node_type == NodeType.LITERAL_INTERVAL:
            return "<INTERVAL>"
        return str(root.value)
    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:
        if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            return f"{root.value.upper()}({','.join([format_expression(e) for e in root.parameters])})"
        if node_type == NodeType.WILDCARD:
            return "*"
        if node_type == NodeType.BINARY_OPERATOR:
            _map = {
                "StringConcat": "||",
                "Plus": "+",
                "Minus": "-",
                "Multiply": "*",
                "Divide": "/",
            }
            return f"{format_expression(root.left)}{_map.get(root.value, root.value)}{format_expression(root.right)}"
    if node_type == NodeType.COMPARISON_OPERATOR:
        _map = {"Eq": "=", "Lt": "<", "Gt": ">"}
        return f"{format_expression(root.left)}{_map.get(root.value, root.value)}{format_expression(root.right)}"
    return str(root.value)


class NodeType(int, Enum):
    """
    The types of Nodes we will see.

    The second nibble (4 bits) is a category marker, the first nibble is just an
    enumeration of the values in that category.

    We could just code these as ints, but by building them here, either we're
    going to confuse the hell out of the reader, or they'll see what we've done
    and it'll make perfect sense.
    """

    # fmt:off

    # 00000000
    UNKNOWN: int = 0

    # LOGICAL OPERATORS
    # nnnn0001
    AND: int = 17
    OR: int = 33
    XOR: int = 49
    NOT: int = 65

    # INTERAL IDENTIFIERS
    # nnnn0010
    WILDCARD: int = 18
    COMPARISON_OPERATOR: int = 34
    BINARY_OPERATOR: int = 50
    UNARY_OPERATOR: int = 66
    FUNCTION: int = 82
    IDENTIFIER: int = 98
    SUBQUERY: int = 114
    NESTED: int = 130
    AGGREGATOR = 146

    # LITERAL TYPES
    # nnnn0100
    LITERAL_NUMERIC: int = 20
    LITERAL_VARCHAR: int = 36
    LITERAL_BOOLEAN: int = 52
    LITERAL_INTERVAL: int = 68
    LITERAL_LIST: int = 84
    LITERAL_STRUCT: int = 100
    LITERAL_TIMESTAMP: int = 116
    LITERAL_NONE: int = 132
    LITERAL_TABLE: int = 148

    # fmt:on


NUMPY_TYPES = {
    NodeType.LITERAL_NUMERIC: numpy.dtype("float64"),
    NodeType.LITERAL_VARCHAR: numpy.unicode_(),
    NodeType.LITERAL_BOOLEAN: numpy.dtype("?"),
    NodeType.LITERAL_INTERVAL: numpy.dtype("m"),
    NodeType.LITERAL_LIST: numpy.dtype("O"),
    NodeType.LITERAL_STRUCT: numpy.dtype("O"),
    NodeType.LITERAL_TIMESTAMP: "datetime64[us]",
}


class ExpressionTreeNode:
    __slots__ = (
        "token_type",
        "value",
        "left",
        "right",
        "centre",
        "parameters",
        "alias",
    )

    def __init__(
        self,
        token_type,
        *,
        value=None,
        left_node=None,
        right_node=None,
        centre_node=None,
        parameters=None,
        alias=None,
    ):
        self.token_type: NodeType = token_type
        self.value = value
        self.left = left_node
        self.right = right_node
        self.centre = centre_node
        self.parameters = parameters
        self.alias = alias

        if self.token_type == NodeType.UNKNOWN:
            raise ValueError(f"ExpressionNode of unknown type in plan. {self.value}")

    def __repr__(self):
        return (
            f"<ExpressionTreeNode {str(self.token_type).upper()}"
            f"{' `' + str(self.value) + '`' if self.value else ''} "
            f"{'L' if self.left is not None else '.'}"
            f"{'C' if self.centre is not None else '.'}"
            f"{'R' if self.right is not None else '.'}"
            f"{('[' + str(len(self.parameters)) + ']') if self.parameters is not None else '.'} "
            f"({id(self)})>"
        )

    def _inner_print(self, node, prefix):
        ret = prefix + str(node.value) + "\n"
        prefix += " |"
        if node.left:
            ret += self._inner_print(node.left, prefix=prefix + "- ")
        if node.right:
            ret += self._inner_print(node.right, prefix=prefix + "- ")
        return ret

    def __str__(self):
        return self._inner_print(self, "")


def _inner_evaluate(root: ExpressionTreeNode, table: Table, columns):

    node_type = root.token_type

    # BOOLEAN OPERATORS
    if node_type & LOGICAL_TYPE == LOGICAL_TYPE:

        left, right, centre = None, None, None

        if root.left is not None:
            left = _inner_evaluate(root.left, table, columns)
        if root.right is not None:
            right = _inner_evaluate(root.right, table, columns)
        if root.centre is not None:
            centre = _inner_evaluate(root.centre, table, columns)

        if node_type == NodeType.AND:
            if left.dtype == bool:
                left = numpy.where(left)[0]  # [#325]
            if right.dtype == bool:
                right = numpy.where(right)[0]  # [#325]
            return numpy.intersect1d(left, right)
        if node_type == NodeType.OR:
            if left.dtype == bool:
                left = numpy.where(left)[0]  # [#325]
            if right.dtype == bool:
                right = numpy.where(right)[0]  # [#325]
            return numpy.union1d(left, right)
        if node_type == NodeType.NOT:
            if centre.dtype == bool:
                centre = numpy.where(centre)[0]  # [#325]
            mask = numpy.arange(table.num_rows, dtype=numpy.int32)
            return numpy.setdiff1d(mask, centre, assume_unique=True)
        if node_type == NodeType.XOR:
            filter_indices = numpy.logical_xor(left, right)
            indices = numpy.arange(table.num_rows)
            return indices[filter_indices]

    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:
        if node_type == NodeType.FUNCTION:
            parameters = [
                _inner_evaluate(param, table, columns) for param in root.parameters
            ]
            # zero parameter functions get the number of rows as the parameter
            if len(parameters) == 0:
                parameters = [table.num_rows]
            return FUNCTIONS[root.value](*parameters)
        if node_type == NodeType.AGGREGATOR:
            # detected as an aggregator, but here it's an identifier because it
            # should have been evaluated
            node_type = NodeType.IDENTIFIER
            root.value = format_expression(root)
            root.token_type = NodeType.IDENTIFIER
        if node_type == NodeType.IDENTIFIER:
            if root.value in table.column_names:
                mapped_column = root.value
            else:
                mapped_column = columns.get_column_from_alias(root.value, only_one=True)
            return table[mapped_column].to_numpy()
        if node_type == NodeType.COMPARISON_OPERATOR:
            left = _inner_evaluate(root.left, table, columns)
            right = _inner_evaluate(root.right, table, columns)
            indices = numpy.arange(table.num_rows)
            f_idxs = filter_operations(left, root.value, right)
            indices = indices[f_idxs]

            return indices
        if node_type == NodeType.BINARY_OPERATOR:
            left = _inner_evaluate(root.left, table, columns)
            right = _inner_evaluate(root.right, table, columns)
            return binary_operations(left, root.value, right)
        if node_type == NodeType.WILDCARD:
            numpy.full(table.num_rows, "*", dtype=numpy.str_)
        if node_type == NodeType.SUBQUERY:
            # we should have a query plan here
            sub = root.value.execute()
            return pyarrow.concat_tables(sub, promote=True)
        if node_type == NodeType.NESTED:
            return _inner_evaluate(root.centre, table, columns)
        if node_type == NodeType.UNARY_OPERATOR:
            centre = _inner_evaluate(root.centre, table, columns)
            return UNARY_OPERATIONS[root.value](centre)

    # LITERAL TYPES
    if node_type & LITERAL_TYPE == LITERAL_TYPE:
        # if it's a literal value, return it once for every value in the table
        if node_type == NodeType.LITERAL_LIST:
            # this isn't as fast as .full - but lists and strings are problematic
            return numpy.array([root.value] * table.num_rows)
        if node_type == NodeType.LITERAL_VARCHAR:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.unicode_)
        if node_type == NodeType.LITERAL_INTERVAL:
            return pyarrow.array([root.value] * table.num_rows)
        return numpy.full(
            shape=table.num_rows, fill_value=root.value, dtype=NUMPY_TYPES[node_type]
        )  # type:ignore


def evaluate(expression: ExpressionTreeNode, table: Table):

    columns = Columns(table)
    result = _inner_evaluate(root=expression, table=table, columns=columns)

    if not isinstance(result, (pyarrow.Array, numpy.ndarray)):
        result = numpy.array(result)
    return result


def get_all_nodes_of_type(root, select_nodes):
    """
    Walk a expression tree collecting all the nodes of a specified type.
    """
    if not isinstance(root, list):
        root = [root]

    identifiers = []
    for node in root:
        if node.token_type in select_nodes:
            identifiers.append(node)
        if node.left:
            identifiers.extend(get_all_nodes_of_type(node.left, select_nodes))
        if node.centre:
            identifiers.extend(get_all_nodes_of_type(node.centre, select_nodes))
        if node.right:
            identifiers.extend(get_all_nodes_of_type(node.right, select_nodes))
        if node.parameters:
            for parameter in node.parameters:
                if isinstance(parameter, ExpressionTreeNode):
                    identifiers.extend(get_all_nodes_of_type(parameter, select_nodes))

    return identifiers


def evaluate_and_append(expressions, table: Table, seed: str = None):
    """
    Evaluate an expression and add it to the table.

    This needs to be able to deal with and avoid cascading problems where field names
    are duplicated, this is most common when performing many joins on the same table.
    """

    columns = Columns(table)
    return_expressions = []

    for statement in expressions:

        if statement.token_type in (
            NodeType.FUNCTION,
            NodeType.BINARY_OPERATOR,
            NodeType.COMPARISON_OPERATOR,
        ) or (statement.token_type & LITERAL_TYPE == LITERAL_TYPE):
            new_column_name = format_expression(statement)
            raw_column_name = new_column_name

            # avoid clashes in column names
            alias = statement.alias
            if not alias:
                alias = [new_column_name]
            if seed is not None:
                alias.append(new_column_name)
                new_column_name = hex(CityHash64(seed + new_column_name))

            # if we've already been evaluated - don't do it again
            if len(columns.get_column_from_alias(raw_column_name)) > 0:
                statement = ExpressionTreeNode(
                    NodeType.IDENTIFIER, value=raw_column_name, alias=alias
                )
                return_expressions.append(statement)
                continue

            # do the evaluation
            new_column = evaluate(statement, table)

            # some activities give us masks rather than the values, if we don't have
            # enough values, assume it's a mask
            if (
                len(new_column) < table.num_rows
                or statement.token_type == NodeType.COMPARISON_OPERATOR
            ):
                bool_list = numpy.full(table.num_rows, False)
                bool_list[new_column] = True
                new_column = bool_list

            # large arrays appear to have a bug in PyArrow where they're automatically
            # converted to a chunked array, but the internal function can't handle
            # chunked arrays - 50Mb columns are rare when we have 64Mb pages.
            if new_column.nbytes > 50000000:
                new_column = [[i] for i in new_column]
            else:
                new_column = [new_column]

            table = table.append_column(new_column_name, new_column)

            # add the column to the schema and because it's been evaluated and added to
            # table, it's an INDENTIFIER rather than a FUNCTION
            columns.add_column(new_column_name)
            columns.add_alias(new_column_name, alias)
            columns.set_preferred_name(new_column_name, alias[0])

            statement = ExpressionTreeNode(
                NodeType.IDENTIFIER, value=new_column_name, alias=alias
            )

        return_expressions.append(statement)

    table = columns.apply(table)

    return columns, return_expressions, table
