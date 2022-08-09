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

from pyarrow import Table

from opteryx.utils.columns import Columns
from opteryx.third_party.pyarrow_ops.ops import filter_operations, FILTER_OPERATORS
from opteryx.engine.functions import FUNCTIONS
from opteryx.engine.functions.binary_operators import binary_operations
from opteryx.engine.functions.binary_operators import BINARY_OPERATORS


LOGICAL_TYPE: int = int("0001", 2)
INTERNAL_TYPE: int = int("0010", 2)
LITERAL_TYPE: int = int("0100", 2)


def operator_type_factory(function):
    if function in FILTER_OPERATORS:
        return NodeType.COMPARISON_OPERATOR
    if function in BINARY_OPERATORS:
        return NodeType.BINARY_OPERATOR
    if function == "And":
        return NodeType.AND
    if function == "Or":
        return NodeType.OR
    if function == "Xor":
        return NodeType.XOR
    return NodeType.UNKNOWN


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
    # UNARY_OPERATOR: int = 66
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
    # LITERAL_TABLE: int = 148 (may be needed for CTEs/Subqueries and manual literals)

    # fmt:on


NUMPY_TYPES = {
    NodeType.LITERAL_NUMERIC: numpy.dtype("float64"),
    NodeType.LITERAL_VARCHAR: numpy.str_,
    NodeType.LITERAL_BOOLEAN: numpy.dtype("?"),
    NodeType.LITERAL_INTERVAL: numpy.dtype("m"),
    NodeType.LITERAL_LIST: numpy.dtype("O"),
    NodeType.LITERAL_STRUCT: numpy.dtype("O"),
    NodeType.LITERAL_TIMESTAMP: "datetime64[s]",
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

    def _inner_print(self, node, prefix):
        ret = prefix + node.value + "\n"
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
                left = numpy.where(left)[0]
            if right.dtype == bool:
                right = numpy.where(right)[0]
            return numpy.intersect1d(left, right)
        if node_type == NodeType.OR:
            if left.dtype == bool:
                left = numpy.where(left)[0]
            if right.dtype == bool:
                right = numpy.where(right)[0]
            return numpy.union1d(left, right)
        if node_type == NodeType.NOT:
            if centre.dtype == bool:
                centre = numpy.where(centre)[0]
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
            return FUNCTIONS[root.value][1](*parameters)
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
            raise NotImplementedError("subquery")
        if node_type == NodeType.NESTED:
            return _inner_evaluate(root.centre, table, columns)

    # LITERAL TYPES
    if node_type & LITERAL_TYPE == LITERAL_TYPE:
        # if it's a literal value, return it once for every value in the table
        if node_type == NodeType.LITERAL_LIST:
            # this isn't as fast as .full - but lists and strings are problematic
            return numpy.array([root.value] * table.num_rows)
        if node_type == NodeType.LITERAL_VARCHAR:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.str_)
        return numpy.full(table.num_rows, root.value, dtype=NUMPY_TYPES[node_type])


def evaluate(expression: ExpressionTreeNode, table: Table):

    columns = Columns(table)
    return _inner_evaluate(root=expression, table=table, columns=columns)
