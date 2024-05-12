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

import re
from typing import List
from typing import Set
from typing import Tuple

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import random_string
from orso.types import OrsoTypes

from opteryx.exceptions import AmbiguousDatasetError
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.binder.binder import inner_binder
from opteryx.planner.binder.binder import locate_identifier_in_loaded_schemas
from opteryx.planner.binder.binder import merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.virtual_datasets import derived

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def get_mismatched_condition_column_types(node: Node, relaxed: bool = False) -> dict:
    """
    Checks that the types of the fields involved a comparison are the same on both sides.

    Parameters:
        node: Node
            The condition node representing the condition.

    Returns:
        a dictionary describing the columns
    """
    if node.node_type in (NodeType.AND, NodeType.OR):
        left_mismatches = get_mismatched_condition_column_types(node.left, relaxed)
        right_mismatches = get_mismatched_condition_column_types(node.right, relaxed)
        return left_mismatches or right_mismatches

    elif node.node_type == NodeType.COMPARISON_OPERATOR:
        if (
            node.value in ("InList", "NotInList", "Arrow", "LongArrow")
            or node.value.startswith("AllOp")
            or node.value.startswith("AnyOp")
        ):
            return None  # Some ops are meant to have different types
        left_type = node.left.schema_column.type if node.left.schema_column else None
        right_type = node.right.schema_column.type if node.right.schema_column else None

        if left_type and right_type and left_type != right_type:
            if (
                relaxed
                and (left_type.is_numeric() and right_type.is_numeric())
                or (left_type.is_temporal() and right_type.is_temporal())
                or (left_type.is_large_object() and right_type.is_large_object())
            ):
                return None
            if left_type == OrsoTypes.NULL or right_type == OrsoTypes.NULL:
                return None  # None comparisons are allowed
            return {
                "left_column": f"{node.left.source}.{node.left.value}",
                "left_type": left_type.name,
                "left_node": node.left,
                "right_column": f"{node.right.source}.{node.right.value}",
                "right_type": right_type.name,
                "right_node": node.right,
            }

    return None  # if we reach here, it means we didn't find any inconsistencies


def extract_join_fields(
    condition_node: Node, left_relation_names: List[str], right_relation_names: List[str]
) -> Tuple[List[str], List[str]]:
    """
    Extracts join fields from a condition node that may have multiple ANDed conditions.

    Parameters:
        condition_node: Node
            The condition node in the join clause.
        left_relation_name: str
            Name of the left relation.
        right_relation_name: str
            Name of the right relation.

    Returns:
        Tuple[List[str], List[str]]
            Lists of columns participating in the join from the left and right tables.
    """
    left_fields = []
    right_fields = []

    if condition_node.node_type == NodeType.AND:
        left_fields_1, right_fields_1 = extract_join_fields(
            condition_node.left, left_relation_names, right_relation_names
        )
        left_fields_2, right_fields_2 = extract_join_fields(
            condition_node.right, left_relation_names, right_relation_names
        )

        left_fields.extend(left_fields_1)
        left_fields.extend(left_fields_2)

        right_fields.extend(right_fields_1)
        right_fields.extend(right_fields_2)

    elif condition_node.node_type == NodeType.COMPARISON_OPERATOR and condition_node.value == "Eq":
        if any(
            [
                condition_node.left.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL),
                condition_node.right.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL),
            ]
        ):
            raise UnsupportedSyntaxError("JOIN conditions only support column comparisons.")
        if (
            condition_node.left.source in left_relation_names
            and condition_node.right.source in right_relation_names
        ):
            left_fields.append(condition_node.left.schema_column.identity)
            right_fields.append(condition_node.right.schema_column.identity)
        elif (
            condition_node.left.source in right_relation_names
            and condition_node.right.source in left_relation_names
        ):
            right_fields.append(condition_node.left.schema_column.identity)
            left_fields.append(condition_node.right.schema_column.identity)

    return left_fields, right_fields


def convert_using_to_on(
    using_fields: Set[str], left_relation_names: List[str], right_relation_names: List[str]
) -> Node:
    """
    Converts a USING field to an ON field for JOIN operations.

    Parameters:
        using_fields: Set[str]
            Set of common fields to use for joining.
        left_relation_names: List[str]
            Names of the left relations.
        right_relation_names: List[str]
            Names of the right relations.

    Returns:
        Node
            The condition node representing the ON clause.
    """
    all_conditions = []

    # Loop through all combinations of left and right relation names
    for left_relation_name in left_relation_names:
        for right_relation_name in right_relation_names:
            conditions = []
            for field in using_fields:
                condition = Node(
                    node_type=NodeType.COMPARISON_OPERATOR, value="Eq", do_not_create_column=True
                )
                condition.left = LogicalColumn(
                    node_type=NodeType.IDENTIFIER, source=left_relation_name, source_column=field
                )
                condition.right = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=right_relation_name,
                    source_column=field,
                )
                conditions.append(condition)

            if len(conditions) == 1:
                all_conditions.append(conditions[0])
            else:
                # Create a tree of ANDed conditions
                while len(conditions) > 1:
                    new_conditions = []
                    for i in range(0, len(conditions), 2):
                        if i + 1 < len(conditions):
                            and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
                            and_node.left = conditions[i]
                            and_node.right = conditions[i + 1]
                            new_conditions.append(and_node)
                        else:
                            new_conditions.append(conditions[i])
                    conditions = new_conditions
                all_conditions.append(conditions[0])

    return conditions[0]


class BinderVisitor:
    """
    The BinderVisitor visits each node in the query plan and adds catalogue information
    to each node. This includes:

    - identifiers, bound from the schemas
    - functions and aggregatros, bound from the function catalogue
    - variables, bound from the variables collection

    """

    def visit_node(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        """
        Visits a given node and returns a new node and context after binding catalog information.

        Parameters:
            node: Node
                The query plan node to visit.
            context: Dict[str, Any]
                The current binding context.

        Returns:
            Tuple[Node, Dict]
            The node and context after binding.
        """
        node_type = node.node_type.name  # type:ignore
        visit_method_name = f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"
        visit_method = getattr(self, visit_method_name, None)
        if visit_method is None:
            return node, context

        return_node, return_context = visit_method(node.copy(), context.copy())

        # DEBUG: from opteryx.exceptions import InvalidInternalStateError
        # DEBUG:
        # DEBUG: if not isinstance(return_context, BindingContext):
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function '{visit_method_name}' didn't return a BindingContext"
        # DEBUG:     )
        # DEBUG:
        # DEBUG: if not all(isinstance(schema, RelationSchema) for schema in context.schemas.values()):
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function '{visit_method_name}' returned invalid Schemas"
        # DEBUG:     )
        # DEBUG:
        # DEBUG: if not all(isinstance(col, (Node, LogicalColumn)) for col in return_node.columns or []):
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function '{visit_method_name}' put unexpected items in 'columns' attribute"
        # DEBUG:     )
        # DEBUG:
        # DEBUG: if return_node.node_type.name != "Scan" and return_node.columns is None:
        # DEBUG:     raise InvalidInternalStateError(
        # DEBUG:         f"Internal Error - function {visit_method_name} did not populate 'columns'"
        # DEBUG:     )

        return return_node, return_context

    def visit_aggregate_and_group(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        """
        Handles the binding logic for aggregate and group nodes.

        This function maps the field to the existing schema fields, disposes of the existing
        schemas, and replaces it with a new '$group-by' schema.

        Parameters:
            node: Node
                The node containing the aggregate and group data.
            context: Optional[Dict[str, Any]]
                The current binding context, defaults to None.

        Returns:
            Tuple[Node, Dict[str, Any]]
            The modified node and the updated context.
        """
        tmp_aggregates: tuple = tuple()
        if node.aggregates:
            tmp_aggregates, _ = zip(
                *(inner_binder(aggregate, context) for aggregate in node.aggregates)
            )
            node.aggregates = list(tmp_aggregates)

        # We're going to trim down the schemas to just the columns used in the GROUP BY.
        # 1) the easy one - the columns explictly in the GROUP BY
        columns_to_keep = set()
        if node.groups:
            tmp_groups, _ = zip(*(inner_binder(group, context) for group in node.groups))
            columns_to_keep = {col.schema_column.identity for col in tmp_groups}
        # 2) the columns referenced in the SELECT
        node.columns = get_all_nodes_of_type(
            node.aggregates + node.groups, select_nodes=(NodeType.IDENTIFIER,)
        )
        all_identifiers = [node.schema_column.identity for node in node.columns]
        columns_to_keep = columns_to_keep.union(all_identifiers)

        for name, schema in list(context.schemas.items()):
            schema_columns = [
                column for column in schema.columns if column.identity in columns_to_keep
            ]
            if schema_columns:
                context.schemas[name].columns = schema_columns
            else:
                context.schemas.pop(name)

        for array_agg in [agg for agg in tmp_aggregates if agg.value == "ARRAY_AGG"]:
            if array_agg.order:
                if len(array_agg.order) > 1:
                    raise UnsupportedSyntaxError(
                        "ARRAY_AGG can only ORDER BY the aggregated column."
                    )
                if array_agg.order[0][0].current_name != array_agg.parameters[0].current_name:
                    raise UnsupportedSyntaxError(
                        "ARRAY_AGG can only ORDER BY the aggregated column."
                    )

        # we should always have a derived schema
        if not "$derived" in context.schemas:
            context.schemas["$derived"] = derived.schema()

        # the aggregates and any calculated expressions in the SELECT should be in $derived
        context.schemas["$derived"].columns.extend(col.schema_column for col in node.aggregates)
        node.schema = context.schemas["$derived"]
        return node, context

    visit_aggregate = visit_aggregate_and_group

    def visit_distinct(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        node.columns = []
        if node.on:
            # Bind the local columns to physical columns
            node.on, group_contexts = zip(*(inner_binder(col, context) for col in node.on))
            context.schemas = merge_schemas(*[ctx.schemas for ctx in group_contexts])
            node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))

        return node, context

    def visit_exit(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        # clear the derived schema
        context.schemas.pop("$derived", None)
        context.schemas["$derived"] = derived.schema()

        seen = set()
        needs_qualifier = any(
            column.name in seen or seen.add(column.name) is not None  # type: ignore
            for schema in context.schemas.values()
            for column in schema.columns
        )

        def name_column(qualifier, column):
            for projection_column in node.columns:
                if (
                    projection_column.schema_column
                    and projection_column.schema_column.identity == column.identity
                ):
                    if projection_column.alias:
                        return projection_column.alias

                    if len(context.relations) > 1 or needs_qualifier:
                        if isinstance(projection_column, LogicalColumn):
                            if qualifier:
                                projection_column.source = qualifier
                            return projection_column.qualified_name
                        return f"{qualifier}.{column.name}"

                    if projection_column.query_column:
                        return str(projection_column.query_column)
                    if projection_column.current_name:
                        return projection_column.current_name

            return column.name

        def keep_column(column, identities):
            if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
                if node.columns[0].value:
                    if isinstance(column.origin, str):
                        column.origin = [column.origin]
                    if node.columns[0].value[0] in column.origin:
                        identities.append(column.identity)
                        return True
                    else:
                        return False
                identities.append(column.identity)
                return True
            return column.identity in identities

        identities = []
        for column in (col for col in node.columns if col.node_type != NodeType.WILDCARD):
            new_col, _ = inner_binder(column, context)
            identities.append(new_col.schema_column.identity)

        columns = []
        for qualifier, schema in context.schemas.items():
            for column in schema.columns:
                if keep_column(column, identities):
                    column_name = name_column(qualifier=qualifier, column=column)
                    column_reference = LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=column_name,
                        source=None,
                        alias=None,
                        schema_column=column,
                    )
                    columns.append(column_reference)

        # we bound as we came across items in schemas, not the order the user wants them
        desired_order = {id: index for index, id in enumerate(identities)}
        node.columns = sorted(columns, key=lambda item: desired_order[item.schema_column.identity])

        context.schemas["$derived"] = derived.schema()

        return node, context

    def visit_filter(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        # We don't update the context, otherwise we'd be adding the predicates as columns
        original_context = context.copy()
        node.condition, context = inner_binder(node.condition, context)
        node.columns = get_all_nodes_of_type(node.condition, (NodeType.IDENTIFIER,))
        node.relations = node.condition.relations or {}

        return node, original_context

    def visit_function_dataset(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        # We need to build the schema and add it to the schema collection.
        if node.function == "VALUES":
            relation_name = node.alias or f"$values-{random_string()}"
            columns = [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=column,
                    source=relation_name,
                    schema_column=FlatColumn(name=column, type=0),
                )
                for column in node.columns
            ]
            schema = RelationSchema(
                name=relation_name,
                columns=[c.schema_column for c in columns],
            )
            context.schemas[relation_name] = schema
            node.columns = columns
            node.schema = schema
        elif node.function == "UNNEST":
            relation_name = node.alias

            columns = [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=node.unnest_target,
                    source=relation_name,
                    schema_column=FlatColumn(name=node.unnest_target, type=0),
                )
            ]
            schema = RelationSchema(name=relation_name, columns=[c.schema_column for c in columns])
            context.schemas[relation_name] = schema
            node.columns = columns
            node.schema = schema
        elif node.function == "GENERATE_SERIES":
            node.relation_name = node.alias
            columns = [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=node.alias,
                    source=node.relation_name,
                    schema_column=FlatColumn(name=node.alias, type=0),
                )
            ]
            schema = RelationSchema(
                name=node.relation_name,
                columns=[c.schema_column for c in columns],
            )
            context.schemas[node.relation_name] = schema
            node.columns = columns
            node.schema = schema
        elif node.function == "FAKE":
            from orso.schema import ColumnDisposition

            node.relation_name = node.alias
            node.rows = int(node.args[0].value)

            if len(node.args) < 2:
                raise InvalidFunctionParameterError(
                    f"FAKE function expects at least two parameters, the number of rows, and then either the number of columns, or an array of the column types."
                )

            if node.args[1].node_type == NodeType.NESTED:
                column_definition = [node.args[1].centre]
            else:
                column_definition = node.args[1].value

            special_handling = {
                "NAME": (OrsoTypes.VARCHAR, ColumnDisposition.NAME),
                "AGE": (OrsoTypes.INTEGER, ColumnDisposition.AGE),
            }

            columns = []
            if isinstance(column_definition, tuple):
                for i, column_type in enumerate(column_definition):
                    name = node.columns[i] if i < len(node.columns) else f"column_{i}"
                    column_type = str(column_type).upper()
                    if column_type in special_handling:
                        actual_type, disposition = special_handling[column_type]
                        schema_column = FlatColumn(
                            name=name, type=actual_type, disposition=disposition
                        )
                    else:
                        schema_column = FlatColumn(name=name, type=column_type)
                    columns.append(
                        LogicalColumn(
                            node_type=NodeType.IDENTIFIER,
                            source_column=schema_column.name,
                            source=node.alias,
                            schema_column=schema_column,
                        )
                    )
                schema = RelationSchema(
                    name=node.alias,
                    columns=[c.schema_column for c in columns],
                )
                node.columns = columns
                node.schema = schema
            else:
                try:
                    column_definition = int(column_definition)  # type: ignore
                except TypeError:
                    raise InvalidFunctionParameterError(
                        f"Expected number of rows for 'FAKE' function or list of column types. Are you missing parenthesis?"
                    )
                names = node.columns + tuple(
                    f"column_{i}" for i in range(len(node.columns), column_definition)  # type: ignore
                )
                node.columns = [
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=names[i],
                        source=node.alias,
                        schema_column=FlatColumn(name=names[i], type=OrsoTypes.INTEGER),
                    )
                    for i in range(column_definition)  # type: ignore
                ]

            schema = RelationSchema(
                name=node.relation_name,
                columns=[c.schema_column for c in node.columns],
            )
            context.schemas[node.relation_name] = schema
            node.schema = schema
        else:
            raise NotImplementedError(f"{node.function} does not exist")
        return node, context

    def visit_join(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        """
        Visits a JOIN node and handles different types of joins.

        Parameters:
            node: Node
                The node representing the join operation.
            context: Dict
                The context containing relevant information like schemas.

        Returns:
            Tuple[Node, Dict]
                Updated node and context.
        """
        node.columns = []
        # Handle 'natural join' by converting to a 'using'
        if node.type == "natural join":
            left_columns = [
                col
                for relation_name in node.left_relation_names
                for col in context.schemas[relation_name].column_names
            ]
            right_columns = [
                col
                for relation_name in node.right_relation_names
                for col in context.schemas[relation_name].column_names
            ]
            node.using = [
                Node("temp", value=n) for n in set(left_columns).intersection(right_columns)
            ]
            node.type = "inner"
        # Handle 'using' by converting to a an 'on'
        if node.using:
            node.on = convert_using_to_on(
                {n.value for n in node.using}, node.left_relation_names, node.right_relation_names
            )
        if node.on:
            # All except CROSS JOINs have been mapped to have an ON condition
            # The JOIN operator only support ON conditions.
            comparisons = get_all_nodes_of_type(node.on, (NodeType.COMPARISON_OPERATOR,))
            if not all(com.value == "Eq" for com in comparisons):
                from opteryx.exceptions import UnsupportedSyntaxError

                raise UnsupportedSyntaxError("Only JOINs with equals comparisons supported")

            node.on, context = inner_binder(node.on, context)
            node.left_columns, node.right_columns = extract_join_fields(
                node.on, node.left_relation_names, node.right_relation_names
            )
            mismatches = get_mismatched_condition_column_types(node.on)
            if mismatches:
                from opteryx.exceptions import IncompatibleTypesError

                raise IncompatibleTypesError(**mismatches)

            # we need to put the referenced columns into the columns attribute for the
            # optimizers
            node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))

        if node.using:
            # Remove the columns used in the join condition from both relations, they're in
            # the result set but not belonging to either table, whilst still belonging to both.
            # We create a new schema to put them in, $shared-nnn.
            columns = []

            # Loop through all using fields in the node
            left_relation_name = ""
            right_relation_name = ""
            for column_name in (n.value for n in node.using):
                # Pop the column from the left relation
                for left_relation_name in node.left_relation_names:
                    left_column = context.schemas[left_relation_name].pop_column(column_name)

                # Pop the column from the right relation
                for right_relation_name in node.right_relation_names:
                    right_column = context.schemas[right_relation_name].pop_column(column_name)

                # we need to decide which column we're going to keep
                if node.type in ("right anti", "right semi"):
                    left_column.origin = [left_relation_name, right_relation_name]
                    columns.append(left_column)
                else:
                    right_column.origin = [left_relation_name, right_relation_name]
                    columns.append(right_column)

            # shared columns exist in both schemas in some uses and in neither in others
            context.schemas[f"$shared-{random_string()}"] = RelationSchema(
                name=f"^{left_relation_name}#^{right_relation_name}#", columns=columns
            )

        # SEMI and ANTI joins only return columns from one table
        if node.type in ("left anti", "left semi"):
            for schema in node.left_relation_names:
                context.schemas.pop(schema)
        if node.type in ("right anti", "right semi"):
            for schema in node.right_relation_names:
                context.schemas.pop(schema)

        # If we have an unnest_column, how how it is bound is different to other columns
        if node.unnest_column:
            # this is the column which is being unnested
            node.unnest_column, context = inner_binder(node.unnest_column, context)
            node.columns += [node.unnest_column]
            # this is the column that is being created - find it from its name
            node.unnest_target, found_source_relation = locate_identifier_in_loaded_schemas(
                node.unnest_alias, context.schemas
            )
            if node.unnest_column.schema_column.type not in (
                OrsoTypes._MISSING_TYPE,
                OrsoTypes.ARRAY,
            ):
                from opteryx.exceptions import IncorrectTypeError

                raise IncorrectTypeError("CROSS JOIN UNNEST requires an ARRAY type column.")

        if node.type == "inner" and node.on is None:
            from opteryx.exceptions import SqlError

            raise SqlError("INNER and NATURAL joins must have a either an ON or USING condition.")

        return node, context

    def visit_order(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        order_by = []
        columns = []
        for column, direction in node.order_by:
            bound_column, context = inner_binder(column, context)

            order_by.append(
                (
                    bound_column,
                    "ascending" if direction else "descending",
                )
            )
            columns.append(bound_column)

        node.order_by = order_by
        node.columns = columns
        return node, context

    def visit_project(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        columns = []

        # Handle wildcards, including qualified wildcards.
        for column in node.columns + node.order_by_columns:
            if not column.node_type == NodeType.WILDCARD:
                columns.append(column)
            else:
                # Handle qualified wildcards
                for name, schema in list(context.schemas.items()):
                    if (
                        name == column.value[0]
                        or name.startswith("$shared")
                        and f"^{column.value[0]}#" in schema.name
                    ):
                        for schema_column in schema.columns:
                            column_reference = LogicalColumn(
                                node_type=NodeType.IDENTIFIER,  # column type
                                source_column=schema_column.name,  # the source column
                                source=column.value[0],  # the source relation
                                schema_column=schema_column,
                            )
                            columns.append(column_reference)
                    if name.startswith("$shared") and f"^{column.value[0]}#" in schema.name:
                        context.schemas.pop(name)

                    context.schemas[column.value[0]] = RelationSchema(
                        name=name, columns=[col.schema_column for col in columns]
                    )

        node.columns = columns

        # Bind the local columns to physical columns
        node.columns, group_contexts = zip(*(inner_binder(col, context) for col in node.columns))
        context.schemas = merge_schemas(*[ctx.schemas for ctx in group_contexts])

        # Check for duplicates
        all_identities = [c.schema_column.identity for c in node.columns]
        if len(set(all_identities)) != len(all_identities):
            from collections import Counter

            from opteryx.exceptions import AmbiguousIdentifierError

            duplicates = [column for column, count in Counter(all_identities).items() if count > 1]
            matches = {c.value for c in node.columns if c.schema_column.identity in duplicates}
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
            )

        # Remove columns not being projected from the schemas, and remove empty schemas
        columns = []
        for relation, schema in list(context.schemas.items()):
            schema_columns = [
                column for column in schema.columns if column.identity in all_identities
            ]
            if len(schema_columns) == 0:
                context.schemas.pop(relation)
            else:
                schema.columns = schema_columns
                for column in node.columns:
                    if column.schema_column.identity in [i.identity for i in schema_columns]:
                        columns.append(column)

        # We always have a $derived schema, even if it's empty
        if "$derived" in context.schemas:
            context.schemas["$project"] = context.schemas.pop("$derived")
            context.schemas["$project"].name = "$project"
        if not "$derived" in context.schemas:
            context.schemas["$derived"] = derived.schema()

        node.columns = columns

        return node, context

    def visit_scan(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        from opteryx.connectors import connector_factory
        from opteryx.connectors.capabilities import Asynchronous
        from opteryx.connectors.capabilities import Cacheable
        from opteryx.connectors.capabilities import Partitionable
        from opteryx.connectors.capabilities.cacheable import async_read_thru_cache
        from opteryx.connectors.capabilities.cacheable import read_thru_cache

        if node.alias in context.relations:
            raise AmbiguousDatasetError(dataset=node.alias)
        # work out which connector will be serving this request
        node.connector = connector_factory(node.relation, statistics=context.statistics)
        connector_capabilities = node.connector.__class__.mro()

        if hasattr(node.connector, "variables"):
            node.connector.variables = context.connection.variables
        if Partitionable in connector_capabilities:
            node.connector.start_date = node.start_date
            node.connector.end_date = node.end_date
        if Cacheable in connector_capabilities:
            # We add the caching mechanism here if the connector is Cacheable and
            # we've not disable caching
            if "NO_CACHE" in (node.hints or []):
                pass
            if Asynchronous in connector_capabilities:
                original_read_blob = node.connector.async_read_blob
                node.connector.async_read_blob = async_read_thru_cache(original_read_blob)
            else:
                original_read_blob = node.connector.read_blob
                node.connector.read_blob = read_thru_cache(original_read_blob)
        # get them to tell is the schema of the dataset
        # None means we don't know ahead of time - we can usually get something
        node.schema = node.connector.get_dataset_schema()
        node.schema.aliases.append(node.alias)
        context.schemas[node.alias] = node.schema
        for column in node.schema.columns:
            column.origin = [node.alias]
        context.relations.add(node.alias)

        return node, context

    def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        node.variables = context.connection.variables
        node.columns = []
        return node, context

    def visit_show_columns(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        node.schema = context.schemas[node.relation]
        node.columns = []
        for schema_column in node.schema.columns:
            column_reference = LogicalColumn(
                node_type=NodeType.IDENTIFIER,  # column type
                source_column=schema_column.name,  # the source column
                source=node.relation,  # the source relation
                schema_column=schema_column,
            )
            node.columns.append(column_reference)
        return node, context

    def visit_subquery(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        node, context = self.visit_exit(node, context)

        # we sack all the tables we previously knew and create a new set of schemas here
        columns: list = []
        source_relations: list = []
        for name, schema in context.schemas.items():
            for schema_column in schema.columns:
                # Find the column in the projection if it exists
                projection_column = next(
                    (
                        column
                        for column in node.columns
                        if column.schema_column.identity == schema_column.identity
                    ),
                    None,
                )
                if not schema_column.origin:
                    schema_column.origin = []
                source_relations.extend(schema_column.origin or [])
                projection_column.source = node.alias
                schema_column.origin += [node.alias]

                schema_column.name = (
                    projection_column.current_name if projection_column else schema_column.name
                )

                if "." in schema_column.name:
                    # If the column is not in the projection, it should retain its name without any prefix
                    schema_column.name = schema_column.name.split(".")[-1]

                schema_column.aliases = []
                columns.append(schema_column)
            if name[0] != "$" and name in context.relations:
                context.relations.remove(name)
        context.relations.add(node.alias)

        schema = RelationSchema(name=node.alias, columns=columns)

        context.schemas = {"$derived": derived.schema(), node.alias: schema}
        context.relations.add(node.alias)
        node.schema = schema
        node.source_relations = set(source_relations)
        return node, context

    def visit_union(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        for relation in node.right_relation_names:
            context.schemas.pop(relation, None)
        context.relations = set(node.left_relation_names)

        if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
            columns = []
            for schema_name in node.left_relation_names:
                for schema_column in context.schemas[schema_name].columns:
                    columns.append(
                        LogicalColumn(
                            node_type=NodeType.IDENTIFIER,  # column type
                            source_column=schema_column.name,  # the source column
                            schema_column=schema_column,
                        )
                    )
            node.columns = columns

        node, context = self.visit_exit(node, context)
        return node, context

    def post_bind(self, node):
        # The binder skips calculated fields when it performs binding because
        # sometimes it doesn't have access to all of the fields used in the
        # calculation - so we bind these now
        seen: dict = {}

        def _inner(branch):
            if branch.fully_bound == False:
                if branch.schema_column.identity in seen:
                    branch = seen[branch.schema_column.identity]
            elif branch.schema_column:
                seen[branch.schema_column.identity] = branch.copy()
            for attr in ("left", "right", "centre"):
                if hasattr(branch, attr) and getattr(branch, attr) is not None:
                    setattr(branch, attr, _inner(getattr(branch, attr)))
            if branch.parameters:
                branch.parameters = [_inner(p) for p in branch.parameters]
            return branch

        if node.condition:
            node.condition = _inner(node.condition)
        if node.columns:
            # if it doesn't have a schema column here - we can remove it
            node.columns = [_inner(c) for c in node.columns if c.schema_column is not None]
        return node

    def traverse(
        self, graph: LogicalPlan, node: Node, context: BindingContext
    ) -> Tuple[LogicalPlan, BindingContext]:
        """
        Traverses the given graph starting at the given node and calling the
        appropriate visit methods for each node in the graph. This method uses
        a post-order traversal, which visits the children of a node before
        visiting the node itself.

        Args:
            graph: The graph to traverse.
            node: The node to start the traversal from.
            context: An optional context object to pass to each visit method.
        Returns:
            A tuple containing the updated graph and the context.
        """
        # Recursively visit children
        children = graph.ingoing_edges(node)

        if children:
            exit_context = context.copy()
            for child in children:
                # Each peer gets the exact copy of the context so they don't affect each other
                _, child_context = self.traverse(graph, child[0], context.copy())
                # Assuming merge_schemas is a function that merges the schemas from two contexts
                exit_context.schemas = merge_schemas(child_context.schemas, exit_context.schemas)

                # Update relations if necessary
                context.relations = context.relations.union(exit_context.relations).union(
                    child_context.relations
                )

            context.schemas = merge_schemas(context.schemas, exit_context.schemas)

        # Visit node and return updated context
        return_node, context = self.visit_node(graph[node], context=context)
        return_node = self.post_bind(return_node)
        graph[node] = return_node
        return graph, context
