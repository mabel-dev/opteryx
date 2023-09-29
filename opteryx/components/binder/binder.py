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


import copy
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.schema import RelationSchema

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.functions import FUNCTIONS
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.operators.aggregate_node import AGGREGATORS

COMBINED_FUNCTIONS = {**FUNCTIONS, **AGGREGATORS}


def merge_schemas(*dicts: Dict[str, RelationSchema]) -> Dict[str, RelationSchema]:
    """
    Handles the merging of relations, requiring a custom merge function.

    Parameters:
        dicts: Tuple[Dict[str, RelationSchema]]
            Dictionaries to be merged.

    Returns:
        A merged dictionary containing RelationSchemas.
    """
    merged_dict: Dict[str, RelationSchema] = {}
    for dic in dicts:
        if not isinstance(dic, dict):
            raise InvalidInternalStateError("Internal Error - merge_schemas expected dicts")
        for key, value in dic.items():
            if key in merged_dict:
                if isinstance(value, RelationSchema):
                    merged_dict[key] += value
                else:
                    raise InvalidInternalStateError(
                        "Internal Error - merge_schemas expects schemas"
                    )
            else:
                merged_dict[key] = copy.deepcopy(value)
    return merged_dict


def locate_identifier_in_loaded_schemas(
    value: str, schemas: Dict[str, RelationSchema]
) -> Tuple[Optional[str], Optional[RelationSchema]]:
    """
    Locate a given identifier in a set of loaded schemas.

    Parameters:
        value: str
            The identifier to locate.
        schemas: Dict[str, Schema]
            The loaded schemas to search within.

    Returns:
        A tuple containing the column and its source schema, if found.
    """
    found_source_relation = None
    column = None

    for schema in schemas.values():
        found = schema.find_column(value)
        if found:
            if column and found_source_relation:
                raise AmbiguousIdentifierError(identifier=value)
            found_source_relation = schema
            column = found

    return column, found_source_relation


def locate_identifier(node: Node, context: Dict[str, Any]) -> Tuple[Node, Dict]:
    """
    Locate which schema the identifier is defined in. We return a populated node
    and the context.

    Parameters:
        node: Node
            The node representing the identifier
        context: Dict[str, Any]
            The current query context.

    Returns:
        Tuple[Node, Dict]: The updated node and the current context.

    Raises:
        UnexpectedDatasetReferenceError: If the source dataset is not found.
        ColumnNotFoundError: If the column is not found in the schema.
    """

    def create_variable_node(node: Node, context: Dict[str, Any]) -> Node:
        """Populates a Node object for a variable."""
        node.schema_column = context.connection.variables.as_column(node.value)
        node.node_type = NodeType.LITERAL
        node.type = node.schema_column.type
        node.value = node.schema_column.value
        return node

    # Check if the identifier is a variable
    if node.current_name[0] == "@":
        node = create_variable_node(node, context)
        return node, context

    schemas = context.schemas
    found_source_relation = schemas.get(node.source)

    # Handle fully qualified fields
    if node.source:
        # If the source relation is not found, raise an error
        if not found_source_relation:
            raise UnexpectedDatasetReferenceError(dataset=node.source)

        # Try to find the column in the source relation
        column = found_source_relation.find_column(node.source_column)
        if not column:
            from opteryx.utils import suggest_alternative

            suggestion = suggest_alternative(node.value, found_source_relation.all_column_names())
            raise ColumnNotFoundError(column=node.value, dataset=node.source, suggestion=suggestion)

    # Handle non-qualified fields
    else:
        column, found_source_relation = locate_identifier_in_loaded_schemas(
            node.source_column, schemas
        )
        if not found_source_relation:
            from opteryx.utils import suggest_alternative

            suggestion = suggest_alternative(
                node.source_column,
                [
                    column_name
                    for _, schema in schemas.items()
                    for column_name in schema.all_column_names()
                    if column_name is not None
                ],
            )
            raise ColumnNotFoundError(column=node.value, suggestion=suggestion)

        # Update node.source to the found relation name
        node.source = found_source_relation.name

    # Update node.schema_column with the found column
    node.schema_column = column
    return node, context


def inner_binder(node: Node, context: Dict[str, Any], step: str) -> Tuple[Node, Dict[str, Any]]:
    """
    Note, this is a tree within a tree. This function represents a single step in the execution
    plan (associated with the relational algebra) which may itself be an evaluation plan
    (executing comparisons).
    """
    # Import relevant classes and functions
    from opteryx.managers.expression import ExpressionColumn
    from opteryx.managers.expression import format_expression

    # Retrieve the node type for further processing.
    node_type = node.node_type

    # Early exit for columns that are already bound.
    # If the node has a 'schema_column' already set, it doesn't need to be processed again.
    # This is an optimization to avoid unnecessary work.
    if node.schema_column is not None:
        return node, context

    # Early exit for nodes representing IDENTIFIER types.
    # If the node is of type IDENTIFIER, it's just a simple look up to bind the node.
    if node_type in (NodeType.IDENTIFIER, NodeType.EVALUATED):
        return locate_identifier(node, context)

    # Early exit for nodes representing calculated columns.
    # If the node represents a calculated column, if we're seeing it again it's because it
    # has appeared earlier in the plan and in that case we don't need to recalcuate, we just
    # need to treat the result like an IDENTIFIER
    column_name = node.query_column or format_expression(node)
    for schema in context.schemas.values():
        found_column = schema.find_column(column_name)

        # If the column exists in the schema, update node and context accordingly.
        if found_column:
            node.schema_column = found_column
            node.query_column = node.alias or column_name

            return node, context

    schemas = context.schemas

    # First recurse and do this for all the sub parts of the evaluation plan
    if node.left:
        node.left, context = inner_binder(node.left, context, step)
    if node.right:
        node.right, context = inner_binder(node.right, context, step)
    if node.centre:
        node.centre, context = inner_binder(node.centre, context, step)
    if node.parameters:
        node.parameters, new_contexts = zip(
            *(inner_binder(parm, context, step) for parm in node.parameters)
        )
        merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
        context.schemas = merged_schemas

    # Now do the node we're at

    if node_type == NodeType.LITERAL:
        column_name = format_expression(node)
        schema_column = ConstantColumn(
            name=column_name,
            aliases=[node.alias],
            type=node.type,
            value=node.value,
            nullable=False,
        )
        schemas["$derived"].columns.append(schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif not node_type == NodeType.SUBQUERY and not node.do_not_create_column:
        column_name = format_expression(node)
        schema_column = schemas["$derived"].find_column(column_name)

        if schema_column:
            schema_column = FlatColumn(
                name=column_name,
                aliases=[schema_column.aliases],
                type=0,
                identity=schema_column.identity,
            )
            schemas["$derived"].columns = [
                col for col in schemas["$derived"].columns if col.identity != schema_column.identity
            ]
            schemas["$derived"].columns.append(schema_column)
            node.schema_column = schema_column
            node.query_column = node.alias or column_name
            node.node_type = NodeType.EVALUATED

            return node, context

        elif node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            # we're just going to bind the function into the node
            func = COMBINED_FUNCTIONS.get(node.value)
            if not func:
                # v1:
                from opteryx.utils import suggest_alternative

                suggest = suggest_alternative(node.value, COMBINED_FUNCTIONS.keys())
                # v2: suggest = FUNCTIONS.suggest(node.value)
                raise FunctionNotFoundError(function=node.value, suggestion=suggest)

            # we need to add this new column to the schema
            column_name = format_expression(node)
            aliases = [node.alias] if node.alias else []
            schema_column = FunctionColumn(name=column_name, type=0, binding=func, aliases=aliases)
            schemas["$derived"].columns.append(schema_column)
            node.function = func
            node.derived_from = []
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

        else:
            schema_column = ExpressionColumn(
                name=column_name, aliases=[node.alias], type=0, expression=node.value
            )
            schemas["$derived"].columns.append(schema_column)
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

    context.schemas = schemas
    return node, context
