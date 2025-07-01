# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import copy
from contextlib import suppress
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
from opteryx.exceptions import IncompatibleTypesError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.functions import DEPRECATED_FUNCTIONS
from opteryx.functions import FUNCTIONS
from opteryx.functions import fixed_value_function
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.operator_map import determine_type


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
        if node.source in context.relations:
            raise UnexpectedDatasetReferenceError(
                dataset=node.source,
                message=f"Dataset `{node.source}` is not available after being used on the right side of a ANTI or SEMI JOIN",
            )
        else:
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
    node.source_connector = {context.relations.get(a) for a in found_source_relation.aliases} - {
        None
    }
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


def inner_binder(node: Node, context: BindingContext) -> Tuple[Node, Any]:
    """
    Note, this is a tree within a tree. This function represents a single step in the execution
    plan (associated with the relational algebra) which may itself be an evaluation plan
    (executing comparisons).
    """
    # Import relevant classes and functions
    from orso.types import find_compatible_type

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
            with suppress(Exception):
                node, _ = traversive_recursive_bind(node, context)

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

    elif node_type != NodeType.SUBQUERY and not node.do_not_create_column:
        if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            if node.value in DEPRECATED_FUNCTIONS:
                import warnings

                replacement = DEPRECATED_FUNCTIONS[node.value]
                if replacement is not None:
                    message = f"Function '{node.value}' is deprecated and will be removed in a future version. Use '{DEPRECATED_FUNCTIONS[node.value]}' instead."
                else:
                    message = f"Function '{node.value}' is deprecated and will be removed in a future version."
                context.statistics.add_message(message)
                warnings.warn(message, category=DeprecationWarning, stacklevel=2)

            # we need to add this new column to the schema
            aliases = [node.alias] if node.alias else []
            result_type = None
            fixed_function_result = None
            if len(node.parameters) == 0:
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
                _, result_type, _ = FUNCTIONS.get(node.value, (None, "VARIANT", None))
                element_type = None  # for types with elements (ARRAYs)
                precision = 38  # Maximum precision for Decimal128
                scale = 21  # A reasonable scale that's less than precision

                if node.value == "DECIMAL":
                    result_type = OrsoTypes.DECIMAL
                    precision = node.parameters[1].value if len(node.parameters) > 1 else precision
                    scale = node.parameters[2].value if len(node.parameters) > 2 else scale

                # If we don't know the return type from the function name, we can usually
                # work it out from the parameters - all of the aggs are worked out this way
                # even COUNT which is always an integer.
                if result_type == "VARIANT":
                    # Some functions return a fixed type, so return that type
                    if node.value == "COUNT":
                        result_type = OrsoTypes.INTEGER
                    elif node.value == "AVG":
                        result_type = OrsoTypes.DOUBLE
                    elif node.value in ("ARRAY", "TRY_ARRAY"):
                        result_type, _, _, _, element_type = OrsoTypes.from_name(
                            f"ARRAY<{node.parameters[1].value}>"
                        )
                    elif node.value == "ARRAY_AGG":
                        result_type = OrsoTypes.ARRAY
                        element_type = node.parameters[0].schema_column.type
                    # Some functions return different types based on the input
                    # we need to find the first non-null parameter
                    elif node.value == "CASE":
                        for param in node.parameters[1].parameters:
                            if param.node_type in (
                                NodeType.LITERAL,
                                NodeType.IDENTIFIER,
                                NodeType.FUNCTION,
                            ) and param.schema_column.type not in (
                                OrsoTypes.NULL,
                                0,
                                OrsoTypes._MISSING_TYPE,
                            ):
                                result_type = param.schema_column.type
                                break
                        # if we have a type, we should ensure all the parameters are the same type
                        if result_type not in (OrsoTypes._MISSING_TYPE, 0):
                            parameters = []
                            for param in node.parameters[1].parameters:
                                if param.node_type == NodeType.LITERAL and param.value is not None:
                                    param.value = result_type.parse(param.value)
                                    param.type = result_type
                                    param.schema_column.type = result_type
                                parameters.append(param)
                            node.parameters[1].parameters = parameters
                    elif node.value == "IIF":
                        result_type = node.parameters[1].schema_column.type
                    elif node.value in ("ABS", "MAX", "MIN", "NULLIF", "PASSTHRU", "SUM"):
                        result_type = node.parameters[0].schema_column.type
                    elif node.value in ("GREATEST", "LEAST", "SORT"):
                        result_type = node.parameters[0].schema_column.element_type
                    # Some functions support nulls different positions
                    elif node.value in ("COALESCE", "IFNULL", "IFNOTNULL"):
                        discovered_types = []
                        for param in node.parameters:
                            if param.node_type in (
                                NodeType.LITERAL,
                                NodeType.IDENTIFIER,
                                NodeType.FUNCTION,
                                NodeType.AGGREGATOR,
                            ) and param.schema_column.type not in (
                                OrsoTypes.NULL,
                                0,
                                OrsoTypes._MISSING_TYPE,
                            ):
                                discovered_types.append(param.schema_column.type)
                        result_type = find_compatible_type(discovered_types)
                        # if we have a type, we should ensure all the parameters are the same type
                        if result_type not in (OrsoTypes._MISSING_TYPE, 0):
                            parameters = []
                            for param in node.parameters:
                                if (
                                    param.node_type == NodeType.LITERAL
                                    and param.value is not None
                                    and param.value != set()
                                ):
                                    param.value = result_type.parse(param.value)
                                    param.type = result_type
                                    param.schema_column.type = result_type
                                parameters.append(param)
                            node.parameters = parameters
                    # some functions return different types based on fixed input
                    elif node.value == "DATEPART":
                        datepart = node.parameters[0].value.lower()
                        if datepart in ("epoch", "juian"):
                            result_type = OrsoTypes.DOUBLE
                        elif datepart == "day":
                            result_type = OrsoTypes.VARCHAR
                        elif datepart == "date":
                            result_type = OrsoTypes.DATE
                        else:
                            result_type = OrsoTypes.INTEGER
                    # Some function we don't know the return type until run time
                    elif node.value == "GET":
                        result_type = 0
                        if node.parameters[1].type == OrsoTypes.INTEGER:
                            schema_column = node.parameters[0].schema_column
                            if schema_column.type == OrsoTypes.ARRAY:
                                result_type = schema_column.element_type
                            elif schema_column.type in (OrsoTypes.VARCHAR, OrsoTypes.BLOB):
                                result_type = schema_column.type

                schema_column = FunctionColumn(
                    name=column_name,
                    type=result_type,
                    element_type=element_type,
                    aliases=aliases,
                    precision=precision,
                    scale=scale,
                )
            schemas["$derived"].columns.append(schema_column)
            node.derived_from = []
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

        elif node.value and node.value.startswith(
            (
                "AnyOp",
                "AllOp",
            )
        ):
            # IMPROVE: check types here
            if node.right.node_type == NodeType.LITERAL:
                import pyarrow

                try:
                    node.right.value = pyarrow.array(node.right.value)

                except pyarrow.ArrowTypeError as e:
                    raise IncompatibleTypesError(
                        message=f"Cannot construct ARRAY from incompatible types."
                    ) from e
            schema_column = ExpressionColumn(name=column_name, type=OrsoTypes.BOOLEAN)
            node.schema_column = schema_column
            schemas["$derived"].columns.append(schema_column)
        else:
            # fmt:off
            from opteryx.planner.binder.binder_visitor import get_mismatched_condition_column_types

            # fmt:on
            mismatches = get_mismatched_condition_column_types(node, relaxed=True)
            if mismatches:
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
