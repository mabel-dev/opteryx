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
from orso.types import OrsoTypes

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.functions import FUNCTIONS
from opteryx.functions import fixed_value_function
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.operators.aggregate_node import AGGREGATORS
from opteryx.planner.binder.operator_map import determine_type

COMBINED_FUNCTIONS = {**FUNCTIONS, **AGGREGATORS}


def merge_schemas(*schemas: Dict[str, RelationSchema]) -> Dict[str, RelationSchema]:
    """
    Handles the merging of relations, requiring a custom merge function.

    Parameters:
        dicts: Tuple[Dict[str, RelationSchema]]
            Dictionaries to be merged.

    Returns:
        A merged dictionary containing RelationSchemas.
    """
    merged_dict: Dict[str, RelationSchema] = {}
    for dic in schemas:
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
) -> Tuple[Optional[FlatColumn], Optional[RelationSchema]]:
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
                # test for duplicates
                raise AmbiguousIdentifierError(identifier=value)
            found_source_relation = schema
            column = found  # don't exit here, so we can test for duplicates

    return column, found_source_relation


def locate_identifier(node: Node, context: Any) -> Tuple[Node, Dict]:
    """
    Locate which schema the identifier is defined in. We return a populated node
    and the context.

    Parameters:
        node: Node
            The node representing the identifier
        context: BindingContext
            The current query context.

    Returns:
        Tuple[Node, Dict]: The updated node and the current context.

    Raises:
        UnexpectedDatasetReferenceError: If the source dataset is not found.
        ColumnNotFoundError: If the column is not found in the schema.
    """
    from opteryx.planner.binder import BindingContext

    def create_variable_node(node: Node, context: BindingContext) -> Node:
        """Populates a Node object for a variable."""
        schema_column = context.connection.variables.as_column(node.value)
        new_node = Node(
            node_type=NodeType.LITERAL,
            schema_column=schema_column,
            type=schema_column.type,
            value=schema_column.value,
            relations={},
        )
        return new_node

    # get the list of candidate schemas
    if node.source:
        candidate_schemas = {
            name: schema
            for name, schema in context.schemas.items()
            if name.startswith("$shared") or name == node.source
        }
    else:
        candidate_schemas = context.schemas

    # if there are no candidates, we probably don't know the relation
    if not candidate_schemas:
        raise UnexpectedDatasetReferenceError(dataset=node.source)

    # look up the column in the candidate schemas
    column, found_source_relation = locate_identifier_in_loaded_schemas(
        node.source_column, candidate_schemas
    )

    # if we didn't find the column, suggest alternatives
    if not column:
        # Check if the identifier is a variable
        if node.current_name[0] == "@":
            node = create_variable_node(node, context)
            context.schemas["$derived"].columns.append(node.schema_column)
            return node, context

        from opteryx.utils import suggest_alternative

        suggestion = suggest_alternative(
            node.source_column,
            [
                column_name
                for _, schema in candidate_schemas.items()
                for column_name in schema.all_column_names()
                if column_name is not None
            ],
        )
        raise ColumnNotFoundError(column=node.value, suggestion=suggestion)
    elif node.current_name[0] == "@":
        new_node = Node(
            node_type=NodeType.LITERAL,
            schema_column=column,
            type=column.type,
            value=column.value,
        )
        return new_node, context

    # Update node.source to the found relation name
    if not node.source:
        node.source = found_source_relation.name

    # if we have an alias for a column not known about in the schema, add it
    if node.alias and node.alias not in column.all_names:
        column.aliases.append(node.alias)

    # Update node.schema_column with the found column
    node.schema_column = column
    # if may need to map source aliases to the columns if they weren't able to be
    # mapped before now
    if column.origin and len(column.origin) == 1:
        node.source = column.origin[0]
    return node, context


def traversive_recursive_bind(node: Node, context: Any) -> Tuple[Node, Any]:
    # First recurse and do this for all the sub parts of the evaluation plan
    for attr in ("left", "right", "centre"):
        if hasattr(node, attr) and getattr(node, attr) is not None:
            value, context = inner_binder(getattr(node, attr), context)
            setattr(node, attr, value)
    if node.parameters:
        node.parameters, new_contexts = zip(
            *(inner_binder(parm, context) for parm in node.parameters)
        )
        merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
        context.schemas = merged_schemas
    return node, context


def inner_binder(node: Node, context: Any) -> Tuple[Node, Any]:
    """
    Note, this is a tree within a tree. This function represents a single step in the execution
    plan (associated with the relational algebra) which may itself be an evaluation plan
    (executing comparisons).
    """
    # Import relevant classes and functions
    from opteryx.managers.expression import ExpressionColumn
    from opteryx.managers.expression import format_expression
    from opteryx.managers.expression import get_all_nodes_of_type

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
    # We discard columns not referenced, so this sometimes holds the only reference to
    # child columns, e.g. MAX(id), we may not have 'id' next time we see it, only MAX(id)
    column_name = node.query_column or format_expression(node, True)
    for schema in context.schemas.values():
        found_column = schema.find_column(column_name)
        # If the column exists in the schema, update node and context accordingly.
        if found_column:
            # found_identity = found_column.identity
            try:
                node, _ = traversive_recursive_bind(node, context)
            except:  # nosec
                pass

            node.schema_column = found_column
            node.query_column = node.alias or column_name
            node.fully_bound = False

            if isinstance(found_column, ConstantColumn):
                node.node_type = NodeType.LITERAL
                node.value = found_column.value
                node.type = found_column.type

            return node, context

    schemas = context.schemas

    # do the sub trees off this node
    node, context = traversive_recursive_bind(node, context)

    # Now do the node we're at
    if node_type == NodeType.LITERAL:
        schema_column = ConstantColumn(
            name=column_name,
            aliases=[node.alias] if node.alias else [],
            type=node.type,
            value=node.value,
            nullable=False,
        )
        schemas["$derived"].columns.append(schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif not node_type == NodeType.SUBQUERY and not node.do_not_create_column:
        if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            # we're just going to bind the function into the node
            func = COMBINED_FUNCTIONS.get(node.value)

            # we need to add this new column to the schema
            aliases = [node.alias] if node.alias else []
            result_type, fixed_function_result = fixed_value_function(node.value, context)
            if result_type:
                # Some functions return constants, so return the constant
                schema_column = ConstantColumn(
                    name=column_name,
                    aliases=aliases,
                    type=result_type,
                    value=fixed_function_result,
                    nullable=False,
                )
                node.node_type = NodeType.LITERAL
                node.type = result_type
                node.value = fixed_function_result
            else:
                schema_column = FunctionColumn(
                    name=column_name, type=0, binding=func, aliases=aliases
                )
            schemas["$derived"].columns.append(schema_column)
            node.function = func
            node.derived_from = []
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

        elif node.value and node.value.startswith("AnyOp"):
            # IMPROVE: check types here
            schema_column = ExpressionColumn(name=column_name, type=OrsoTypes.BOOLEAN)
            node.schema_column = schema_column
        elif node.value and node.value.startswith("AllOp"):
            # IMPROVE: check types here
            schema_column = ExpressionColumn(name=column_name, type=OrsoTypes.BOOLEAN)
            node.schema_column = schema_column
        else:
            # fmt:off
            from opteryx.planner.binder.binder_visitor import get_mismatched_condition_column_types

            # fmt:on
            mismatches = get_mismatched_condition_column_types(node, relaxed=True)
            if mismatches:
                from opteryx.exceptions import IncompatibleTypesError

                raise IncompatibleTypesError(**mismatches)

            schema_column = ExpressionColumn(
                name=column_name,
                aliases=[node.alias] if node.alias else [],
                type=determine_type(node),
                expression=node.value,
            )
            schemas["$derived"].columns.append(schema_column)
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

    identifiers = get_all_nodes_of_type(node, (NodeType.IDENTIFIER,))
    sources = []
    for col in identifiers:
        if col.source is not None:
            sources.append(col.source)
        if col.schema_column is not None:
            sources.extend(col.schema_column.origin)
    node.relations = set(sources)

    context.schemas = schemas
    return node, context
