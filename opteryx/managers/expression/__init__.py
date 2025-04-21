# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Expressions describe a calculation or evaluation of some sort.

It is defined as an expression tree of binary and unary operators, and functions.

Expressions are evaluated against an entire morsel at a time.
"""

from enum import Enum
from typing import Callable
from typing import Dict
from typing import List

import numpy
import pyarrow
from orso.tools import random_string
from orso.types import OrsoTypes
from pyarrow import Table
from pyarrow import compute

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.exceptions import IncorrectTypeError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.functions import apply_function
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

__all__ = ("NodeType", "evaluate", "evaluate_and_append", "get_all_nodes_of_type")


class NodeType(int, Enum):
    """
    The types of Nodes we will see.

    The second nibble (4 bits) is a category marker, the first nibble is just an
    enumeration of the values in that category.

    This allows us to use bitmasks to add a category to the enumerations.
    """

    # fmt:off

    # 00000000
    UNKNOWN = 0

    # LOGICAL OPERATORS
    # 0001 nnnn
    AND = 17  # 0001 0001
    OR = 18  # 0001 0010 
    XOR = 19  # 0001 0011
    NOT = 20  # 0001 0100
    DNF = 21  # 0001 0101

    # INTERAL IDENTIFIERS
    # 0010 nnnn
    WILDCARD = 33  # 0010 0001
    COMPARISON_OPERATOR = 34  # 0010 0010
    BINARY_OPERATOR = 35  # 0010 0011
    UNARY_OPERATOR = 36  # 0010 0100
    FUNCTION = 37  # 0010 0101
    IDENTIFIER = 38  # 0010 0110
    SUBQUERY = 39  # 0010 0111
    NESTED = 40  # 0010 1000
    AGGREGATOR = 41  # 0010 1001
    LITERAL = 42  # 0010 1010
    EXPRESSION_LIST = 43  # 0010 1011 (CASE WHEN)
    EVALUATED = 44  # 0010 1100 - memoize results


LOGICAL_OPERATIONS: Dict[NodeType, Callable] = {
    NodeType.AND: pyarrow.compute.and_,
    NodeType.OR: pyarrow.compute.or_,
    NodeType.XOR: pyarrow.compute.xor,
}


def evaluate_dnf(expressions: List[Node], table: Table):
    num_rows = table.num_rows
    true_indices = numpy.arange(num_rows)
    working_table = table

    for predicate in expressions:
        result = evaluate(predicate, working_table)
        result_bool = numpy.asarray(result, dtype=numpy.bool_)

        if not result_bool.any():
            return numpy.zeros(num_rows, dtype=bool)

        # Filter the current true_indices based on the predicate result
        true_indices = true_indices[result_bool]
        working_table = table.take(true_indices)

    # Create the final boolean array with original size
    final_result = numpy.zeros(num_rows, dtype=bool)
    final_result[true_indices] = True
    return final_result


def short_cut_and(root, table):
    # Convert to NumPy arrays
    true_indices = numpy.arange(table.num_rows)

    # Evaluate left expression
    left_result = numpy.array(evaluate(root.left, table))
    null_indices = compute.is_null(left_result, nan_is_null=True).to_numpy(False)
    left_result = numpy.asarray(left_result, dtype=numpy.bool_)

    # If all values in left_result are False, no need to evaluate the right expression
    if not left_result.any():
        return left_result

    # Filter out indices where left_result is FALSE
    subset_indices = true_indices[left_result]

    # Create a subset table for evaluating the right expression
    subset_table = table.take(subset_indices)

    # Evaluate right expression on the subset table
    right_result = numpy.array(evaluate(root.right, subset_table))

    # Combine results
    left_result[subset_indices] = right_result

    # handle nulls
    if null_indices.any():
        left_result = left_result.astype(object)
        numpy.place(left_result, null_indices, [None])
        return left_result

    return left_result


def short_cut_or(root, table):
    # Assuming table.num_rows returns the number of rows in the table
    false_indices = numpy.arange(table.num_rows)

    # Evaluate left expression
    left_result = numpy.array(evaluate(root.left, table))
    null_indices = compute.is_null(left_result, nan_is_null=True).to_numpy(False)
    left_result = numpy.asarray(left_result, dtype=numpy.bool_)

    # Filter out indices where left_result is TRUE
    subset_indices = false_indices[~left_result]

    if subset_indices.size == 0:
        return left_result

    # Create a subset table for evaluating the right expression
    subset_table = table.take(subset_indices)

    # Evaluate right expression on the subset table
    right_result = numpy.array(evaluate(root.right, subset_table), dtype=numpy.bool_)

    # Combine results
    # Update left_result with the right_result where left_result was False
    left_result[subset_indices] = left_result[subset_indices] | right_result

    # handle nulls
    if null_indices.any():
        left_result = left_result.astype(object)
        numpy.place(left_result, null_indices, [None])
        return left_result

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


def _inner_evaluate(root: Node, table: Table):
    node_type = root.node_type  # type:ignore

    if node_type == NodeType.DNF:
        return evaluate_dnf(root.parameters, table)

    if node_type == NodeType.SUBQUERY:
        raise UnsupportedSyntaxError("IN (<subquery>) temporarily not supported.")

    identity = root.schema_column.identity if root.schema_column else random_string()

    # if we have this column already, just return it
    if identity in table.column_names:
        return table[identity].to_numpy(False)

    # LITERAL TYPES
    if node_type == NodeType.LITERAL:
        # if it's a literal value, return it once for every value in the table
        literal_type = root.type
        if literal_type == OrsoTypes.ARRAY:
            # creating ARRAY columns is expensive, so we don't create one full length
            return numpy.array([root.value], dtype=numpy.ndarray)
        if literal_type == OrsoTypes.VARCHAR:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.str_)
        if literal_type == OrsoTypes.BLOB:
            return numpy.array([root.value] * table.num_rows, dtype=numpy.bytes_)
        if literal_type == OrsoTypes.INTERVAL:
            return pyarrow.array([root.value] * table.num_rows)
        if isinstance(literal_type, OrsoTypes):
            literal_type = literal_type.numpy_dtype
        return numpy.full(
            shape=table.num_rows,
            fill_value=root.value,
            dtype=literal_type,
        )  # type:ignore

    # BOOLEAN OPERATORS
    if node_type & LOGICAL_TYPE == LOGICAL_TYPE:  # type:ignore
        if node_type == NodeType.OR:
            return short_cut_or(root, table)
        if node_type == NodeType.AND:
            return short_cut_and(root, table)

        if node_type in LOGICAL_OPERATIONS:
            left = _inner_evaluate(root.left, table) if root.left else [None]
            right = _inner_evaluate(root.right, table) if root.right else [None]
            return LOGICAL_OPERATIONS[node_type](left, right)  # type:ignore

        if node_type == NodeType.NOT:
            centre = _inner_evaluate(root.centre, table) if root.centre else [None]
            centre = pyarrow.array(centre, type=pyarrow.bool_())
            return pyarrow.compute.invert(centre)

    # INTERAL IDENTIFIERS
    if node_type & INTERNAL_TYPE == INTERNAL_TYPE:  # type:ignore
        if node_type == NodeType.FUNCTION:
            parameters = [_inner_evaluate(param, table) for param in root.parameters]
            # zero parameter functions get the number of rows as the parameter
            if len(parameters) == 0:
                parameters = [table.num_rows]
            result = apply_function(root.value, *parameters)
            if isinstance(result, list):
                result = numpy.array(result)
            return result
        if node_type == NodeType.AGGREGATOR:
            # detected as an aggregator, but here it's an identifier because it
            # will have already been evaluated
            node_type = NodeType.EVALUATED
            root.value = format_expression(root)
            root.node_type = NodeType.EVALUATED
        if node_type == NodeType.EVALUATED:
            if root.schema_column.identity not in table.column_names:
                raise ColumnReferencedBeforeEvaluationError(column=root.schema_column.name)
            return table[root.schema_column.identity].to_numpy()
        if node_type == NodeType.COMPARISON_OPERATOR:
            right = None
            left = None

            if root.right.node_type == NodeType.LITERAL:
                right = [root.right.value]

            if right is None:
                if root.right.node_type == NodeType.IDENTIFIER:
                    right = table[root.right.schema_column.identity]
                else:
                    right = _inner_evaluate(root.right, table)
            if left is None:
                if root.left.node_type == NodeType.IDENTIFIER:
                    left = table[root.left.schema_column.identity]
                else:
                    left = _inner_evaluate(root.left, table)

            result = filter_operations(
                left,
                root.left.schema_column.type,
                root.value,
                right,
                root.right.schema_column.type,
            )
            return result
        if node_type == NodeType.BINARY_OPERATOR:
            left = _inner_evaluate(root.left, table)
            right = _inner_evaluate(root.right, table)
            result = binary_operations(
                left,
                root.left.schema_column.type,
                root.value,
                right,
                root.right.schema_column.type,
            )
            return result
        if node_type == NodeType.WILDCARD:
            numpy.full(table.num_rows, "*", dtype=numpy.str_)
        if node_type == NodeType.SUBQUERY:
            # we should have a query plan here
            sub = root.value.execute()
            return pyarrow.concat_tables(sub, promote_options="none")
        if node_type == NodeType.NESTED:
            return _inner_evaluate(root.centre, table)
        if node_type == NodeType.UNARY_OPERATOR:
            centre = _inner_evaluate(root.centre, table)
            result = UNARY_OPERATIONS[root.value](centre)
            return result
        if node_type == NodeType.EXPRESSION_LIST:
            values = [_inner_evaluate(val, table) for val in root.parameters]
            return values
        from opteryx.exceptions import ColumnNotFoundError

        raise ColumnNotFoundError(
            message=f"Unable to locate column '{root.source_column}' this is likely due to differences in SELECT and GROUP BY clauses."
        )


def evaluate(expression: Node, table: Table):
    result = _inner_evaluate(root=expression, table=table)
    if not isinstance(result, (pyarrow.Array, numpy.ndarray)):
        result = numpy.array(result)
    return result


def get_all_nodes_of_type(root, select_nodes: tuple) -> list:
    """
    Walk an expression tree collecting all nodes of a specified type.
    """
    if root is None:
        return []
    if not isinstance(root, (set, tuple, list)):
        root = [root]

    # Prepare to collect all nodes if select_nodes is ('*',), else convert to a set
    collect_all = "*" in select_nodes
    select_nodes_set = set(select_nodes) if not collect_all else set()

    identifiers = []
    stack = list(root)
    appender = stack.append

    while stack:
        node = stack.pop()

        # Check whether to collect the node
        if collect_all or node.node_type in select_nodes_set:
            identifiers.append(node)

        # Append parameters if they are valid nodes
        if node.parameters:
            stack.extend(
                [param for param in node.parameters if isinstance(param, (Node, LogicalColumn))]
            )

        # Append child nodes
        child = node.right
        if child:
            appender(child)
        child = node.centre
        if child:
            appender(child)
        child = node.left
        if child:
            appender(child)

    return identifiers


def evaluate_and_append(expressions, table: Table):
    """
    Evaluate an expression and add it to the table.

    This needs to be able to deal with and avoid cascading problems where field names
    are duplicated, this is most common when performing many joins on the same table.
    """
    prioritized_expressions = prioritize_evaluation(expressions)

    for statement in prioritized_expressions:
        if statement.schema_column.identity in table.column_names:
            continue

        if should_evaluate(statement):
            if table.num_rows > 0:
                new_column = evaluate_statement(statement, table)
            else:
                # we make all unknown fields to object type
                new_column = numpy.array([], dtype=statement.schema_column.type.numpy_dtype)
                new_column = pyarrow.array(new_column)

            if isinstance(new_column, pyarrow.ChunkedArray):
                new_column = new_column.combine_chunks()

            # if we know the intended type of the result column, cast it
            field = statement.schema_column.identity
            if statement.schema_column.type not in (
                0,
                OrsoTypes._MISSING_TYPE,
                OrsoTypes.INTERVAL,
            ):
                field = pyarrow.field(
                    name=statement.schema_column.identity,
                    type=statement.schema_column.arrow_field.type,
                )
                try:
                    if isinstance(new_column, pyarrow.Array):
                        new_column = new_column.cast(field.type)
                    else:
                        new_column = pyarrow.array(new_column[0], type=field.type)
                except pyarrow.lib.ArrowInvalid as e:
                    raise IncorrectTypeError(
                        f"Unable to cast '{statement.schema_column.name}' to {field.type}"
                    ) from e

            table = table.append_column(field, new_column)

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


def evaluate_statement(statement, table):
    """Evaluate a statement and return the corresponding column."""
    new_column = evaluate(statement, table)
    if is_mask(new_column, statement, table):
        new_column = create_mask(new_column, table.num_rows)
    return [new_column]


def is_mask(new_column, statement, table):
    """Determine if the given column represents a mask."""
    return len(new_column) < table.num_rows or statement.node_type == NodeType.UNARY_OPERATOR


def create_mask(column, num_rows):
    """Create a boolean mask based on the given column."""
    bool_list = numpy.full(num_rows, False)
    bool_list[column] = True
    return bool_list
