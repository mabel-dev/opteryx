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

from opteryx.components.binder.binder import inner_binder
from opteryx.components.binder.binder import locate_identifier_in_loaded_schemas
from opteryx.components.binder.binder import merge_schemas
from opteryx.components.binder.binding_context import BindingContext
from opteryx.exceptions import AmbiguousDatasetError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.third_party.travers import Graph
from opteryx.virtual_datasets import derived

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


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
                condition.left = Node(
                    node_type=NodeType.IDENTIFIER,
                    value=field,
                    source=left_relation_name,
                    source_column=field,
                    aliases=[],
                )
                condition.right = Node(
                    node_type=NodeType.IDENTIFIER,
                    value=field,
                    source=right_relation_name,
                    source_column=field,
                    aliases=[],
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

    # If there's only one condition, return it directly
    if len(all_conditions) == 1:
        return all_conditions[0]

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
        node_type = node.node_type.name
        visit_method_name = f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"

        visit_method = getattr(self, visit_method_name, None)
        if visit_method is None:
            return node, context

        return_node, return_context = visit_method(node.copy(), context.copy())

        if not isinstance(return_context, BindingContext):
            raise InvalidInternalStateError(
                f"Internal Error - function {visit_method_name} didn't return a BindingContext"
            )

        if not all(isinstance(schema, RelationSchema) for schema in context.schemas.values()):
            raise InvalidInternalStateError(
                f"Internal Error - function {visit_method_name} returned invalid Schemas"
            )

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
        if node.groups:
            tmp_groups, _ = zip(
                *(inner_binder(group, context, node.identity) for group in node.groups)
            )
            node.groups = list(tmp_groups)

        if node.aggregates:
            tmp_aggregates, _ = zip(
                *(inner_binder(aggregate, context, node.identity) for aggregate in node.aggregates)
            )
            node.aggregates = list(tmp_aggregates)

        columns = [
            i.schema_column
            for i in (node.aggregates or []) + (node.groups or [])
            if i.schema_column
        ]

        # although this is now the only dataset, we use $derived as there is logic
        # specific to that dataset with regards to reevaluating expressions
        context.schemas = {"$derived": RelationSchema(name="$derived", columns=columns)}

        return node, context

    visit_aggregate = visit_aggregate_and_group

    def visit_exit(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        # LOG: Exit

        # remove the derived schema
        context.schemas.pop("$derived", None)

        columns = []

        seen = set()
        needs_qualifier = any(
            column.name in seen or seen.add(column.name) is not None
            for schema in context.schemas.values()
            for column in schema.columns
        )

        def name_column(qualifier, column):
            if len(context.schemas) > 1 or needs_qualifier:
                # if len(column.aliases) == 1:
                #    return column.aliases[0]
                return f"{qualifier}.{column.name}"
            return column.name

        for qualifier, schema in context.schemas.items():
            for column in schema.columns:
                column_reference = Node(
                    node_type=NodeType.IDENTIFIER,
                    name=column.name,
                    schema_column=column,
                    type=column.type,
                    query_column=name_column(qualifier, column),
                )
                columns.append(column_reference)

        node.columns = columns

        return node, context

    def visit_function_dataset(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        # We need to build the schema and add it to the schema collection.
        if node.function == "VALUES":
            relation_name = f"$values-{random_string()}"
            schema = RelationSchema(
                name=relation_name,
                columns=[FlatColumn(name=column, type=0) for column in node.columns],
            )
            context.schemas[relation_name] = schema
            node.columns = [column.identity for column in schema.columns]
        elif node.function == "UNNEST":
            if not node.alias:
                if node.args[0].node_type == NodeType.IDENTIFIER:
                    node.alias = f"UNNEST({node.args[0].value})"
            relation_name = f"$unnest-{random_string()}"
            schema = RelationSchema(
                name=relation_name, columns=[FlatColumn(name=node.alias or "unnest", type=0)]
            )
            context.schemas[relation_name] = schema
            node.columns = [schema.columns[0].identity]
        elif node.function == "GENERATE_SERIES":
            schema = RelationSchema(
                name=node.alias,
                columns=[FlatColumn(name=node.alias or "generate_series", type=0)],
            )
            context.schemas[node.alias] = schema
            node.columns = [schema.columns[0].identity]
            node.relation_name = node.alias
        else:
            raise NotImplementedError(f"{node.function} binding isn't written yet")
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
            node.using = [Node(value=n) for n in set(left_columns).intersection(right_columns)]
            node.type = "inner"
        # Handle 'using' by converting to a an 'on'
        if node.using:
            node.on = convert_using_to_on(
                [n.value for n in node.using], node.left_relation_names, node.right_relation_names
            )
        if node.on:
            # cross joins, natural joins and 'using' joins don't have an "on"
            node.on, context = inner_binder(node.on, context, node.identity)
            node.left_columns, node.right_columns = extract_join_fields(
                node.on, node.left_relation_names, node.right_relation_names
            )

        if node.using:
            columns = []

            # Loop through all using fields in the node
            for column_name in (n.value for n in node.using):
                # Try to pop the column from each left relation until found
                for left_relation_name in node.left_relation_names:
                    left_column = context.schemas[left_relation_name].pop_column(column_name)
                    if left_column is not None:
                        left_column.source_relation = None
                        break

                # Try to pop the column from each right relation until found
                for right_relation_name in node.right_relation_names:
                    right_column = context.schemas[right_relation_name].pop_column(column_name)
                    if right_column is not None:
                        columns.append(right_column)
                        break

            context.schemas[f"$joined-{random_string}"] = RelationSchema(
                name="$joined", columns=columns
            )
        if node.column:
            if not node.alias:
                node.alias = f"UNNEST({node.column.query_column})"
            # Find which relation on the left side of the plan the field is in
            for left_relation_name in node.left_relation_names:
                if context.schemas[left_relation_name].find_column(node.column.value):
                    node.source = left_relation_name
                    break
            # this is the column which is being unnested
            node.column, context = inner_binder(node.column, context, node.identity)
            # this is the column that is being created - find it from it's name
            node.target_column, found_source_relation = locate_identifier_in_loaded_schemas(
                node.alias, context.schemas
            )
            if not found_source_relation:
                from opteryx.utils import suggest_alternative

                suggestion = suggest_alternative(
                    node.value,
                    [schema.all_column_names() for schema in context.schemas.values()],
                )
                raise ColumnNotFoundError(column=node.value, suggestion=suggestion)

        return node, context

    def visit_project(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        node.columns, group_contexts = zip(
            *(inner_binder(col, context, node.identity) for col in node.columns)
        )
        context.schemas = merge_schemas(*[ctx.schemas for ctx in group_contexts])

        all_identities = [c.schema_column.identity for c in node.columns]

        if len(set(all_identities)) != len(all_identities):
            from collections import Counter

            from opteryx.exceptions import AmbiguousIdentifierError

            duplicates = [column for column, count in Counter(all_identities).items() if count > 1]
            matches = {
                c.query_column for c in node.columns if c.schema_column.identity in duplicates
            }
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
            )

        columns = []
        for relation, schema in list(context.schemas.items()):
            schema_columns = [
                column for column in schema.columns if column.identity in all_identities
            ]
            if len(schema_columns) == 0:
                context.schemas.pop(relation)
            else:
                schema.columns = schema_columns
                columns += [
                    column
                    for column in node.columns
                    if column.schema_column.identity in [i.identity for i in schema_columns]
                ]

        if "$derived" in context.schemas:
            context.schemas["$project"] = context.schemas.pop("$derived")
        if not "$derived" in context.schemas:
            context.schemas["$derived"] = derived.schema()

        node.columns = columns

        return node, context

    def visit_filter(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        original_context = context.copy()
        node.condition, context = inner_binder(node.condition, context, node.identity)

        return node, original_context

    def visit_order(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        order_by = []
        for column, direction in node.order_by:
            bound_column, context = inner_binder(column, context, node.identity)

            order_by.append(
                (
                    bound_column,
                    "ascending" if direction else "descending",
                )
            )

        node.order_by = order_by
        return node, context

    def visit_scan(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        from opteryx.connectors import connector_factory
        from opteryx.connectors.capabilities import Cacheable
        from opteryx.connectors.capabilities import Partitionable
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
            if not "NO_CACHE" in (node.hints or []):
                original_read_blob = node.connector.read_blob
                node.connector.read_blob = read_thru_cache(original_read_blob)
        # get them to tell is the schema of the dataset
        # None means we don't know ahead of time - we can usually get something
        node.schema = node.connector.get_dataset_schema()
        context.schemas[node.alias] = node.schema
        for column in node.schema.columns:
            column.origin = node.alias
        context.relations.add(node.alias)

        return node, context

    def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        node.variables = context.connection.variables
        return node, context

    def visit_show_columns(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        node.schema = context.schemas[node.relation]
        return node, context

    def visit_subquery(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        # we sack all the tables we previously knew and create a new set of schemas here
        columns = []
        for schema in context.schemas.values():
            columns += schema.columns

        schema = RelationSchema(name=node.alias, columns=columns)

        context.schemas = {"$derived": derived.schema(), node.alias: schema}
        return node, context

    def traverse(
        self, graph: Graph, node: Node, context: BindingContext
    ) -> Tuple[Graph, BindingContext]:
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
        graph[node] = return_node
        return graph, context
