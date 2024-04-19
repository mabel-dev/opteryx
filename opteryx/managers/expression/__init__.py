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
from typing import Callable
from typing import Dict
from typing import Optional

import numpy
import pyarrow
from orso.tools import random_string
from orso.types import OrsoTypes
from pyarrow import Table

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression.binary_operators import binary_operations
from opteryx.managers.expression.ops import filter_operations
from opteryx.managers.expression.unary_operations import UNARY_OPERATIONS
from opteryx.models import LogicalColumn
from opteryx.models import Node

from .formatter import ExpressionColumn  # this is used
from .formatter import format_expression

# These are bit-masks
LOGICAL_TYPE: int = int("00010000", 2)
INTERNAL_TYPE: int = int("00100000", 2)
MAX_COLUMN_BYTE_SIZE: int = 50000000


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
    EVALUATED: int = 44  # 0010 1100 - memoize results


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
    OrsoTypes.NULL: numpy.dtype("O"),
}

LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: pyarrow.compute.and_,
    NodeType.OR: pyarrow.compute.or_,
    NodeType.XOR: pyarrow.compute.xor,
}


class ExecutionContext:
    def __init__(self):
        self.results = {}

    def store(self, identity, result):
        self.results[identity] = result

    def retrieve(self, identity):
        return self.results.get(identity)

    def has_result(self, identity):
        return identity in self.results

    @property
    def identities(self):
        return list(self.results.keys())


def short_cut_and(root, table, context):
    # Convert to NumPy arrays
    true_indices = numpy.arange(table.num_rows)

    # Evaluate left expression
    left_result = numpy.array(evaluate(root.left, table, context))
    left_result = numpy.asarray(left_result, dtype=bool)

    # If all values in left_result are False, no need to evaluate the right expression
    if not left_result.any():
        return left_result

    # Filter out indices where left_result is FALSE
    subset_indices = true_indices[left_result]

    # Create a subset table for evaluating the right expression
    subset_table = table.take(subset_indices)

    # Evaluate right expression on the subset table
    right_result = numpy.array(evaluate(root.right, subset_table, None))

    # Combine results
    # Iterate over subset_indices and update left_result at those positions
    left_result[subset_indices] = right_result

    return left_result


def short_cut_or(root, table, context):
    # Assuming table.num_rows returns the number of rows in the table
    false_indices = numpy.arange(table.num_rows)

    # Evaluate left expression
    left_result = numpy.array(evaluate(root.left, table, context), dtype=numpy.bool_)

    # Filter out indices where left_result is TRUE
    subset_indices = false_indices[~left_result]

    if subset_indices.size == 0:
        return left_result

    # Create a subset table for evaluating the right expression
    subset_table = table.take(subset_indices)

    # Evaluate right expression on the subset table
    right_result = numpy.array(evaluate(root.right, subset_table, None), dtype=numpy.bool_)

    # Combine results
    # Update left_result with the right_result where left_result was False
    left_result[subset_indices] = left_result[subset_indices] | right_result

    return left_result


def prioritize_evaluation(expressions):
    non_dependent_expressions = []
    dependent_expressions = []

    for expression in expressions:
        if not get_all_nodes_of_type(expression, (NodeType.EVALUATED,)):
            non_dependent_expressions.append(expression)
        else:
            dependent_expressions.append(expression)

    # Now that we have split the expressions into non-dependent and dependent,
    # we can return them in the desired order of evaluation.
    return non_dependent_expressions + dependent_expressions


def _inner_evaluate(root: Node, table: Table, context: ExecutionContext):
    node_type = root.node_type  # type:ignore

    if node_type == NodeType.SUBQUERY:
        raise UnsupportedSyntaxError("IN (<subquery>) temporarily not supported.")

    if root.schema_column:
        identity = root.schema_column.identity
    else:
        identity = random_string()

    # If already evaluated, return memoized result.
    if context.has_result(identity):
        return context.retrieve(identity)

    # if we have this column already, just return it
    if identity in table.column_names:
        return table[identity].to_numpy()

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
    if node_type & LOGICAL_TYPE == LOGICAL_TYPE:  # type:ignore
        if node_type == NodeType.OR:
            return short_cut_or(root, table, context)
        if node_type == NodeType.AND:
            return short_cut_and(root, table, context)

        if node_type in LOGICAL_OPERATIONS:
            left = _inner_evaluate(root.left, table, context) if root.left else [None]
            right = _inner_evaluate(root.right, table, context) if root.right else [None]
            return LOGICAL_OPERATIONS[node_type](left, right)  # type:ignore

        if node_type == NodeType.NOT:
            centre = _inner_evaluate(root.centre, table, context) if root.centre else [None]
            centre = pyarrow.array(centre, type=pyarrow.bool_())
            return pyarrow.compute.invert(centre)

    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:  # type:ignore
        if node_type == NodeType.FUNCTION:
            parameters = [_inner_evaluate(param, table, context) for param in root.parameters]
            # zero parameter functions get the number of rows as the parameter
            if len(parameters) == 0:
                parameters = [table.num_rows]
            result = root.function(*parameters)
            if isinstance(result, list):
                result = numpy.array(result)
            context.store(identity, result)
            return result
        if node_type == NodeType.AGGREGATOR:
            # detected as an aggregator, but here it's an identifier because it
            # will have already been evaluated
            node_type = NodeType.EVALUATED
            root.value = format_expression(root)
            root.node_type = NodeType.EVALUATED
        if node_type == NodeType.EVALUATED:
            if not root.schema_column.identity in table.column_names:
                raise ColumnReferencedBeforeEvaluationError(column=root.schema_column.name)
            return table[root.schema_column.identity].to_numpy()
        if node_type == NodeType.COMPARISON_OPERATOR:
            left = _inner_evaluate(root.left, table, context)
            right = _inner_evaluate(root.right, table, context)
            result = filter_operations(
                left, root.left.schema_column.type, root.value, right, root.right.schema_column.type
            )
            context.store(identity, result)
            return result
        if node_type == NodeType.BINARY_OPERATOR:
            left = _inner_evaluate(root.left, table, context)
            right = _inner_evaluate(root.right, table, context)
            result = binary_operations(
                left, root.left.schema_column.type, root.value, right, root.right.schema_column.type
            )
            context.store(identity, result)
            return result
        if node_type == NodeType.WILDCARD:
            numpy.full(table.num_rows, "*", dtype=numpy.unicode_)
        if node_type == NodeType.SUBQUERY:
            # we should have a query plan here
            sub = root.value.execute()
            return pyarrow.concat_tables(sub, promote_options="none")
        if node_type == NodeType.NESTED:
            return _inner_evaluate(root.centre, table, context)
        if node_type == NodeType.UNARY_OPERATOR:
            centre = _inner_evaluate(root.centre, table, context)
            result = UNARY_OPERATIONS[root.value](centre)
            context.store(identity, result)
            return result
        if node_type == NodeType.EXPRESSION_LIST:
            values = [_inner_evaluate(val, table, context) for val in root.parameters]
            return values
        from opteryx.exceptions import ColumnNotFoundError

        raise ColumnNotFoundError(
            message=f"Unable to locate column '{root.source_column}' this is likely due to differences in SELECT and GROUP BY clauses."
        )


def evaluate(expression: Node, table: Table, context: Optional[ExecutionContext] = None):
    if context is None:
        context = ExecutionContext()

    result = _inner_evaluate(root=expression, table=table, context=context)

    if not isinstance(result, (pyarrow.Array, numpy.ndarray)):
        result = numpy.array(result)
    return result


def get_all_nodes_of_type(root, select_nodes):
    """
    Walk a expression tree collecting all the nodes of a specified type.
    """
    if root is None:
        return []
    if not isinstance(root, (set, tuple, list)):
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
                if isinstance(parameter, (Node, LogicalColumn)):
                    identifiers.extend(get_all_nodes_of_type(parameter, select_nodes))

    return identifiers


def evaluate_and_append(expressions, table: Table):
    """
    Evaluate an expression and add it to the table.

    This needs to be able to deal with and avoid cascading problems where field names
    are duplicated, this is most common when performing many joins on the same table.
    """
    prioritized_expressions = prioritize_evaluation(expressions)

    context = ExecutionContext()

    for statement in prioritized_expressions:
        if statement.schema_column.identity in table.column_names:
            continue

        if should_evaluate(statement):
            new_column = evaluate_statement(statement, table, context)
            table = table.append_column(statement.schema_column.identity, new_column)
            for identity in context.identities:
                if identity not in table.column_names:
                    table = table.append_column(identity, [context.retrieve(identity)])

    return table


def should_evaluate(statement):
    """Determine if the given statement should be evaluated."""
    valid_node_types = {
        NodeType.FUNCTION,
        NodeType.BINARY_OPERATOR,
        NodeType.COMPARISON_OPERATOR,
        NodeType.UNARY_OPERATOR,
        NodeType.NESTED,
        NodeType.NOT,
        NodeType.AND,
        NodeType.OR,
        NodeType.XOR,
        NodeType.LITERAL,
    }
    return statement.node_type in valid_node_types


def evaluate_statement(statement, table, context):
    """Evaluate a statement and return the corresponding column."""
    new_column = evaluate(statement, table, context)
    if is_mask(new_column, statement, table):
        new_column = create_mask(new_column, table.num_rows)
    return format_column(new_column)


def is_mask(new_column, statement, table):
    """Determine if the given column represents a mask."""
    return len(new_column) < table.num_rows or statement.node_type == NodeType.UNARY_OPERATOR


def create_mask(column, num_rows):
    """Create a boolean mask based on the given column."""
    bool_list = numpy.full(num_rows, False)
    bool_list[column] = True
    return bool_list


def format_column(column):
    """Format the column based on its size."""
    if column.nbytes > MAX_COLUMN_BYTE_SIZE:
        return [[i] for i in column]
    return [column]
