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
This is Binder, it sits between the Logical Planner and the Optimizers.

~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │  Rewriter │                               │
   └─────┬─────┘                               │
         │SQL                                  │Plan
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │Stats │Cost-Based │
   │ Rewriter  │      │ Catalogue ├──────► Optimizer │
   └─────┬─────┘      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ╔═════▼═════╗      ┌─────┴─────┐
   │ Logical   │ Plan ║           ║ Plan │ Heuristic │
   │   Planner ├──────►   Binder  ║──────► Optimizer │
   └───────────┘      ╚═══════════╝      └───────────┘
~~~

The Binder is responsible for adding information about the database and engine into the
Logical Plan.

The binder takes the the logical plan, and adds information from various catalogues
into that planand then performs some validation checks.

These catalogues include:
- The Data Catalogue (e.g. data schemas)
- The Function Catalogue (e.g. function inputs and data types)
- The Variable Catalogue (i.e. the @ variables)

The Binder performs these activities:
- schema lookup and propagation (add columns and types, add aliases)

"""


import copy
import re
import typing

from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.schema import RelationSchema
from orso.tools import random_string

from opteryx.exceptions import AmbiguousDatasetError
from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.exceptions import UnsupportedSyntaxError

# from opteryx.functions.v2 import FUNCTIONS
from opteryx.functions import FUNCTIONS
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.operators.aggregate_node import AGGREGATORS
from opteryx.virtual_datasets import derived

COMBINED_FUNCTIONS = {**FUNCTIONS, **AGGREGATORS}
CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def merge_schemas(*dicts) -> dict:
    """we ned to handle merging lists so have our own merge function"""
    merged_dict: dict = {}
    for dic in dicts:
        if not isinstance(dic, dict):
            raise InvalidInternalStateError("Internal Error - merge_dicts expected dicts")
        for key, value in dic.items():
            if key in merged_dict:
                if isinstance(value, list):
                    merged_dict[key].extend(value)
                elif isinstance(value, dict):
                    merged_dict[key] = merge_schemas(value, merged_dict[key])
                elif isinstance(value, RelationSchema):
                    merged_dict[key] += value
                elif isinstance(value, set):
                    merged_dict[key] = merged_dict[key].union(value)
                else:
                    merged_dict[key] = value
            else:
                merged_dict[key] = value.copy() if isinstance(value, (list, dict)) else value
    return merged_dict


def locate_identifier_in_loaded_schemas(value, schemas):
    found_source_relation = None
    column = None

    for _, schema in schemas.items():
        found = schema.find_column(value)
        if found and column and found_source_relation:
            raise AmbiguousIdentifierError(identifier=value)
        if found:
            found_source_relation = schema
            column = found

    return column, found_source_relation


def get_fuzzy_search_suggestion(value, candidates):
    from opteryx.utils import fuzzy_search

    return fuzzy_search(value, candidates)


def locate_identifier(node, context):
    """
    Locate which schema the identifier is defined in. We return a populated node
    and the context.
    """

    if node.value[0] == "@":
        # We're a variable, so load from the variables collection
        # note, we can only use user (@) variables in queries
        # We include variables by exchanging them for constant/literals
        node.schema_column = context["connection"].variables.as_column(node.value)
        node.node_type = NodeType.LITERAL
        node.type = node.schema_column.type
        node.value = node.schema_column.value
        return node, context

    schemas = context["schemas"]
    found_source_relation = schemas.get(node.source)

    if node.source:
        # fully qualified fields (e.g. table.identifier) attest the schema
        # they're in, so we only look for the field in there

        if not found_source_relation:
            # this is the first we've seen this table
            raise UnexpectedDatasetReferenceError(dataset=node.source)

        column = found_source_relation.find_column(node.source_column)
        if not column:
            suggestion = get_fuzzy_search_suggestion(
                node.value, found_source_relation.all_column_names()
            )
            raise ColumnNotFoundError(column=node.value, dataset=node.source, suggestion=suggestion)

    else:
        column, found_source_relation = locate_identifier_in_loaded_schemas(node.value, schemas)
        if not found_source_relation:
            suggestion = get_fuzzy_search_suggestion(
                node.value,
                [
                    column_name
                    for _, schema in schemas.items()
                    for column_name in schema.all_column_names()
                    if column_name is not None
                ],
            )
            raise ColumnNotFoundError(column=node.value, suggestion=suggestion)
        if not node.source:
            node.source = found_source_relation.name

    node.schema_column = column
    return node, context


def inner_binder(node, context) -> typing.Tuple[Node, dict]:
    """
    Note, this is a tree within a tree, this is a single step in the execution plan (i.e. the plan
    associated with the relational algebra) which in itself may be an evaluation plan (i.e.
    executing comparisons)
    """
    from opteryx.managers.expression import ExpressionColumn
    from opteryx.managers.expression import format_expression

    # we're already binded
    if node.schema_column is not None:
        return node, context

    schemas = context["schemas"]
    node_type = node.node_type
    if node_type == NodeType.IDENTIFIER:
        return locate_identifier(node, context)

    elif node_type == NodeType.LITERAL:
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

    elif not node_type == NodeType.SUBQUERY:
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
                suggest = get_fuzzy_search_suggestion(node.value, COMBINED_FUNCTIONS.keys())
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

    # Now recurse and do this again for all the sub parts of the evaluation plan
    if node.left:
        node.left, context = inner_binder(node.left, context)
    if node.right:
        node.right, context = inner_binder(node.right, context)
    if node.centre:
        node.centre, context = inner_binder(node.centre, context)
    if node.parameters:
        node.parameters, new_contexts = zip(
            *(inner_binder(parm, context) for parm in node.parameters)
        )
        merged_schemas = merge_schemas(*[ctx["schemas"] for ctx in new_contexts])
        context["schemas"] = merged_schemas

    return node, copy.deepcopy(context)


class BinderVisitor:
    """
    The BinderVisitor visits each node in the query plan and adds catalogue information
    to each node. This includes:

    - identifiers, bound from the schemas
    - functions and aggregatros, bound from the function catalogue
    - variables, bound from the variables collection

    """

    def visit_node(self, node, context=None):
        node_type = node.node_type.name
        visit_method_name = f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"
        visit_method = getattr(self, visit_method_name, self.visit_unsupported)
        return_node, return_context = visit_method(node, context)
        if not isinstance(return_context, dict):
            raise InvalidInternalStateError(
                f"Internal Error - function {visit_method_name} didn't return a dict"
            )
        if not all(
            isinstance(schema, RelationSchema) for name, schema in context["schemas"].items()
        ):
            raise InvalidInternalStateError(
                f"Internal Error - function {visit_method_name} returned invalid Schemas"
            )
        return return_node, return_context

    def visit_unsupported(self, node, context):
        opteryx_logger.debug(f"No visit method implemented for node type {node.node_type.name}")
        return node, context

    def visit_aggregate_and_group(self, node, context):
        """
        Group by maps the field to the existing schema fields and then disposes of the
        existing schemas and replaces it with a new $group-by schema.
        """
        if node.groups:
            node.groups, _ = zip(*(inner_binder(group, context) for group in node.groups))
            node.groups = list(node.groups)

        if node.aggregates:
            node.aggregates, _ = zip(
                *(inner_binder(aggregate, context) for aggregate in node.aggregates)
            )
            node.aggregates = list(node.aggregates)

        columns = [
            i.schema_column
            for i in (node.aggregates or []) + (node.groups or [])
            if i.schema_column
        ]

        # although this is now the only dataset, we use $derived as there is logic
        # specific to that dataset with regards to reevaluating expressions
        context["schemas"] = {"$derived": RelationSchema(name="$derived", columns=columns)}

        return node, context

    visit_aggregate = visit_aggregate_and_group

    def visit_exit(self, node, context):
        # LOG: EEEExit
        columns = []
        schemas = context.get("schemas", {})

        for column in node.columns:
            if column.node_type == NodeType.WILDCARD:
                qualified_wildcard = column.value
                for schema in schemas:
                    if qualified_wildcard and schema not in qualified_wildcard:
                        continue
                    if schema == "$derived":
                        continue
                    for column in schemas[schema].columns:
                        column_reference = Node(
                            node_type=NodeType.IDENTIFIER,
                            name=column.name,
                            schema_column=column,
                            type=column.type,
                            query_column=f"{schema}.{column.name}",
                        )
                        columns.append(column_reference)
            else:
                bound_column, bound_context = inner_binder(column, context)
                context["schemas"] = merge_schemas(bound_context["schemas"], context["schemas"])
                columns.append(bound_column)

        node.columns = columns

        return node, context

    def visit_function_dataset(self, node, context):
        # We need to build the schema and add it to the schema collection.
        if node.function == "VALUES":
            relation_name = f"$values-{random_string()}"
            schema = RelationSchema(
                name=relation_name,
                columns=[FlatColumn(name=column, type=0) for column in node.columns],
            )
            context["schemas"][relation_name] = schema
            node.columns = [column.identity for column in schema.columns]
        elif node.function == "UNNEST":
            if not node.alias:
                if node.args[0].node_type == NodeType.IDENTIFIER:
                    node.alias = f"UNNEST({node.args[0].value})"
            relation_name = f"$unnest-{random_string()}"
            schema = RelationSchema(
                name=relation_name, columns=[FlatColumn(name=node.alias or "unnest", type=0)]
            )
            context["schemas"][relation_name] = schema
            node.columns = [schema.columns[0].identity]
        elif node.function == "GENERATE_SERIES":
            relation_name = f"$generate-series-{random_string()}"
            schema = RelationSchema(
                name=relation_name,
                columns=[FlatColumn(name=node.alias or "generate_series", type=0)],
            )
            context["schemas"][relation_name] = schema
            node.columns = [schema.columns[0].identity]
        else:
            raise NotImplementedError(f"{node.function} binding isn't written yet")
        return node, context

    def visit_join(self, node, context):
        if node.type == "natural join":
            left_columns = context["schemas"][node.left_relation_name].column_names
            right_columns = context["schemas"][node.right_relation_name].column_names
            node.using = set(left_columns).intersection(right_columns)
        if node.on:
            # cross joins, natural joins and 'using' joins don't have an "on"
            node.on, context = inner_binder(node.on, context)
        if node.using:
            raise UnsupportedSyntaxError(
                "JOIN ... USING and NATURAL JOIN temporarily not supported"
            )
            """
            TODO: The unresolved issue is working out which column will be the one that is removed
            when the tables are joined. It's the one on the right, but that's not always
            the right table here.
            """
            if len(node.using) != 1:
                raise UnsupportedSyntaxError(
                    "JOIN USING syntax currently only supports a single column"
                )
            condition = Node(node_type=NodeType.COMPARISON_OPERATOR, value="Eq")
            condition.left = node.using[0].copy()
            condition.left.source = node.left_relation_name
            condition.left.source_column = condition.left.value
            condition.right = node.using[0].copy()
            condition.right.source = node.right_relation_name
            condition.right.source_column = condition.right.value
            node.on, context = inner_binder(condition, context)
        if node.column:
            if not node.alias:
                node.alias = f"UNNEST({node.column.query_column})"
            node.source = node.left_relation_name
            # this is the column which is being unnested
            node.column, context = inner_binder(node.column, context)
            # this is the column that is being created - find it from it's name
            node.target_column, found_source_relation = locate_identifier_in_loaded_schemas(
                node.alias, context["schemas"]
            )
            if not found_source_relation:
                suggestion = get_fuzzy_search_suggestion(
                    node.value,
                    [schema.all_column_names() for _, schema in context["schemas".items()]],
                )
                raise ColumnNotFoundError(column=node.value, suggestion=suggestion)

        return node, context

    def visit_project(self, node, context):
        # For each of the columns in the projection, identify the relation it
        # will be taken from
        node.columns, group_contexts = zip(*(inner_binder(col, context) for col in node.columns))
        merged_schemas = merge_schemas(*[ctx["schemas"] for ctx in group_contexts])
        context["schemas"] = merged_schemas

        # we create a projection schema
        schema = RelationSchema(
            name="$projection", columns=[col.schema_column for col in node.columns]
        )
        context["schemas"] = {"$derived": derived.schema(), "$projection": schema}

        return node, context

    def visit_filter(self, node, context):
        node.condition, context = inner_binder(node.condition, context)

        return node, context

    def visit_order(self, node, context):
        order_by = []
        for column, direction in node.order_by:
            bound_column, context = inner_binder(column, context)

            order_by.append(
                (
                    bound_column,
                    "ascending" if direction else "descending",
                )
            )

        node.order_by = order_by
        return node, context

    def visit_scan(self, node, context):
        from opteryx.connectors import connector_factory
        from opteryx.connectors.capabilities import Cacheable
        from opteryx.connectors.capabilities import Partitionable
        from opteryx.connectors.capabilities.cacheable import read_thru_cache
        from opteryx.shared import QueryStatistics

        if "statistics" not in context:
            context["statistics"] = QueryStatistics(context["qid"])

        if node.alias in context["relations"]:
            raise AmbiguousDatasetError(dataset=node.alias)
        # work out which connector will be serving this request
        node.connector = connector_factory(node.relation, statistics=context["statistics"])
        connector_capabilities = node.connector.__class__.mro()

        if hasattr(node.connector, "variables"):
            node.connector.variables = context["connection"].variables
        if Partitionable in connector_capabilities:
            node.connector.start_date = node.start_date
            node.connector.end_date = node.end_date
        if Cacheable in connector_capabilities:
            # We add the caching mechanism here if the connector is Cacheable and
            # we've not disable caching
            if not "NO_CACHE" in node.hints:
                original_read_blob = node.connector.read_blob
                node.connector.read_blob = read_thru_cache(original_read_blob)
        # get them to tell is the schema of the dataset
        # None means we don't know ahead of time - we can usually get something
        node.schema = node.connector.get_dataset_schema()
        context.setdefault("schemas", {})[node.alias] = node.schema
        context["relations"].add(node.alias)

        return node, context

    def visit_set(self, node, context):
        node.variables = context["connection"].variables
        return node, context

    def visit_show_columns(self, node, context):
        node.schema = context["schemas"][node.relation]
        return node, context

    def visit_subquery(self, node, context):
        # we sack all the tables we previously knew and create a new set of schemas here
        columns = []
        for schema in context["schemas"]:
            columns += context["schemas"][schema].columns

        schema = RelationSchema(name=node.alias, columns=columns)

        context["schemas"] = {"$derived": derived.schema(), node.alias: schema}
        return node, context

    def traverse(self, graph, node, context=None):
        """
        Traverses the given graph starting at the given node and calling the
        appropriate visit methods for each node in the graph. This method uses
        a post-order traversal, which visits the children of a node before
        visiting the node itself.

        Args:
            graph: The graph to traverse.
            node: The node to start the traversal from.
            context: An optional context object to pass to each visit method.
        """
        if context is None:
            raise InvalidInternalStateError("Binder hasn't been provided context")

        # Recursively visit children
        children = graph.ingoing_edges(node)

        if children:
            exit_context = copy.deepcopy(context)
            for child in children:
                returned_graph, child_context = self.traverse(
                    graph, child[0], copy.deepcopy(context)
                )
                # Assuming merge_schemas is a function that merges the schemas from two contexts
                exit_context["schemas"] = merge_schemas(
                    child_context["schemas"], exit_context["schemas"]
                )

                # Update relations if necessary
                context["relations"] = (
                    context["relations"]
                    .union(exit_context["relations"])
                    .union(child_context["relations"])
                )

            context["schemas"] = merge_schemas(context["schemas"], exit_context["schemas"])

        # Visit node and return updated context
        return_node, context = self.visit_node(graph[node], context=context)
        graph[node] = return_node
        return graph, context


def do_bind_phase(plan, connection=None, qid: str = None, common_table_expressions=None):
    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    context = {
        "schemas": {"$derived": derived.schema()},
        "qid": qid,
        "connection": connection,
        "relations": set(),
    }
    if len(root_node) > 1:
        raise ValueError(f"{qid} - logical plan has {len(root_node)} heads - this is an error")
    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)
    return plan
