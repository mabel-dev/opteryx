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

Expressions are evaluated against an entire morsel at a time.
"""
from enum import Enum

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import Table

from opteryx.functions import FUNCTIONS
from opteryx.functions.binary_operators import binary_operations
from opteryx.functions.unary_operations import UNARY_OPERATIONS
from opteryx.models.node import Node
from opteryx.third_party.pyarrow_ops.ops import filter_operations

from .formatter import format_expression

# These are bit-masks
LOGICAL_TYPE: int = int("00010000", 2)
INTERNAL_TYPE: int = int("00100000", 2)


class NodeType(int, Enum):
    """
    The types of Nodes we will see.

    The second nibble (4 bits) is a category marker, the first nibble is just an
    enumeration of the values in that category.

    This allows us to use bitmasks to add a category to the enumerations.
    """

    # fmt:off

    # 00000000
    UNKNOWN: int = 0

    # LOGICAL OPERATORS
    # 0001 nnnn
    AND: int = 17  # 0001 0001
    OR: int = 18  # 0001 0010 
    XOR: int = 19  # 0001 0011
    NOT: int = 20  # 0001 0100

    # INTERAL IDENTIFIERS
    # 0010 nnnn
    WILDCARD: int = 33  # 0010 0001
    COMPARISON_OPERATOR: int = 34  # 0010 0010
    BINARY_OPERATOR: int = 35  # 0010 0011
    UNARY_OPERATOR: int = 36  # 0010 0100
    FUNCTION: int = 37  # 0010 0101
    IDENTIFIER: int = 38  # 0010 0110
    SUBQUERY: int = 39  # 0010 0111
    NESTED: int = 40  # 0010 1000
    AGGREGATOR:int = 41  # 0010 1001
    LITERAL:int = 42  # 0010 1010
    EXPRESSION_LIST: int = 43  # 0010 1011 (CASE WHEN)


ORSO_TO_NUMPY_MAP = {
    OrsoTypes.ARRAY: numpy.dtype("O"),
    OrsoTypes.BLOB: numpy.dtype("S"),
    OrsoTypes.BOOLEAN: numpy.dtype("?"),
    OrsoTypes.DATE: numpy.dtype("datetime64[D]"),  # [2.5e16 BC, 2.5e16 AD]
    OrsoTypes.DECIMAL: numpy.dtype("O"),
    OrsoTypes.DOUBLE: numpy.dtype("float64"),
    OrsoTypes.INTEGER: numpy.dtype("int64"),
    OrsoTypes.INTERVAL: numpy.dtype("m"),
    OrsoTypes.STRUCT: numpy.dtype("O"),
    OrsoTypes.TIMESTAMP: numpy.dtype("datetime64[us]"),  # [290301 BC, 294241 AD]
    OrsoTypes.TIME: numpy.dtype("O"),
    OrsoTypes.VARCHAR: numpy.unicode_(),
}


def _inner_evaluate(root: Node, table: Table):
    node_type = root.node_type

    # LITERAL TYPES
    if node_type == NodeType.LITERAL:
        # if it's a literal value, return it once for every value in the table
        literal_type = root.type
        if literal_type == OrsoTypes.ARRAY:
            # this isn't as fast as .full - but lists and strings are problematic
            return numpy.array([root.value] * table.num_rows)
        if literal_type == OrsoTypes.VARCHAR:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.unicode_)
        if literal_type == OrsoTypes.INTERVAL:
            return pyarrow.array([root.value] * table.num_rows)
        return numpy.full(
            shape=table.num_rows, fill_value=root.value, dtype=ORSO_TO_NUMPY_MAP[literal_type]
        )  # type:ignore

    # BOOLEAN OPERATORS
    if node_type & LOGICAL_TYPE == LOGICAL_TYPE:
        left, right, centre = None, None, None

        if root.left is not None:
            left = _inner_evaluate(root.left, table)
        if root.right is not None:
            right = _inner_evaluate(root.right, table)
        if root.centre is not None:
            centre = _inner_evaluate(root.centre, table)

        if node_type == NodeType.AND:
            return pyarrow.compute.and_(left, right)
        if node_type == NodeType.OR:
            return pyarrow.compute.or_(left, right)
        if node_type == NodeType.NOT:
            null_val = pyarrow.compute.is_null(centre, nan_is_null=True)
            if any(null_val) and isinstance(centre, numpy.ndarray):
                null_val = numpy.invert(null_val)
                results_mask = centre.compress(null_val)
                results_mask = numpy.invert(results_mask)
                results = numpy.full(len(centre), -1)
                results[numpy.nonzero(null_val)] = results_mask
                return [bool(r) if r != -1 else None for r in results]
            return numpy.invert(centre)

        if node_type == NodeType.XOR:
            return pyarrow.compute.xor(left, right)

    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:
        if node_type == NodeType.FUNCTION:
            parameters = [_inner_evaluate(param, table) for param in root.parameters]
            # zero parameter functions get the number of rows as the parameter
            if len(parameters) == 0:
                parameters = [table.num_rows]
            result = root.function(*parameters)
            if isinstance(result, list):
                result = numpy.array(result)
            return result
        if node_type in (NodeType.AGGREGATOR,):
            # detected as an aggregator, but here it's an identifier because it
            # will have already been evaluated
            node_type = NodeType.IDENTIFIER
            root.value = format_expression(root)
            root.node_type = NodeType.IDENTIFIER
        if node_type == NodeType.IDENTIFIER:
            return table[root.schema_column.identity].to_numpy()
        if node_type == NodeType.COMPARISON_OPERATOR:
            left = _inner_evaluate(root.left, table)
            right = _inner_evaluate(root.right, table)
            return filter_operations(left, root.value, right)
        if node_type == NodeType.BINARY_OPERATOR:
            left = _inner_evaluate(root.left, table)
            right = _inner_evaluate(root.right, table)
            return binary_operations(left, root.value, right)
        if node_type == NodeType.WILDCARD:
            numpy.full(table.num_rows, "*", dtype=numpy.unicode_)
        if node_type == NodeType.SUBQUERY:
            # we should have a query plan here
            sub = root.value.execute()
            return pyarrow.concat_tables(sub, promote=True)
        if node_type == NodeType.NESTED:
            return _inner_evaluate(root.centre, table)
        if node_type == NodeType.UNARY_OPERATOR:
            centre = _inner_evaluate(root.centre, table)
            return UNARY_OPERATIONS[root.value](centre)
        if node_type == NodeType.EXPRESSION_LIST:
            values = [_inner_evaluate(val, table) for val in root.value]
            return values


def evaluate(expression: Node, table: Table):
    result = _inner_evaluate(root=expression, table=table)

    if not isinstance(result, (pyarrow.Array, numpy.ndarray)):
        result = numpy.array(result)
    return result


def get_all_nodes_of_type(root, select_nodes):
    """
    Walk a expression tree collecting all the nodes of a specified type.
    """
    if root is None:
        return []
    if not isinstance(root, list):
        root = [root]

    identifiers = []
    for node in root:
        if node.node_type in select_nodes:
            identifiers.append(node)
        if node.left:
            identifiers.extend(get_all_nodes_of_type(node.left, select_nodes))
        if node.centre:
            identifiers.extend(get_all_nodes_of_type(node.centre, select_nodes))
        if node.right:
            identifiers.extend(get_all_nodes_of_type(node.right, select_nodes))
        if node.parameters:
            for parameter in node.parameters:
                if isinstance(parameter, Node):
                    identifiers.extend(get_all_nodes_of_type(parameter, select_nodes))

    return identifiers


def evaluate_and_append(expressions, table: Table):
    """
    Evaluate an expression and add it to the table.

    This needs to be able to deal with and avoid cascading problems where field names
    are duplicated, this is most common when performing many joins on the same table.
    """

    for statement in expressions:
        if statement.node_type in (
            NodeType.FUNCTION,
            NodeType.BINARY_OPERATOR,
            NodeType.COMPARISON_OPERATOR,
            NodeType.UNARY_OPERATOR,
            NodeType.NOT,
            NodeType.AND,
            NodeType.OR,
            NodeType.XOR,
            NodeType.LITERAL,
        ):
            # do the evaluation
            new_column = evaluate(statement, table)

            # some activities give us masks rather than the values, if we don't have
            # enough values, assume it's a mask
            if len(new_column) < table.num_rows or statement.node_type in (
                NodeType.UNARY_OPERATOR,
            ):
                bool_list = numpy.full(table.num_rows, False)
                bool_list[new_column] = True
                new_column = bool_list

            # Large arrays appear to have a bug in PyArrow where they're automatically
            # converted to a chunked array, but the internal functions can't handle
            # chunked arrays - 50Mb columns are rare when we have 64Mb morsels.
            if new_column.nbytes > 50000000:
                new_column = [[i] for i in new_column]
            else:
                new_column = [new_column]

            table = table.append_column(statement.schema_column.identity, new_column)

    return table


def deduplicate_list_of_nodes(nodes):
    seen = set()
    deduped = []
    for column in nodes:
        if column.value not in seen:
            deduped.append(column)
            seen.add(column.value)
            if column.alias:
                seen.update(column.alias)
    return deduped
