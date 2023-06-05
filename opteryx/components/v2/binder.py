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
- The Variable Catalogue (i.e. the @@ variables)

We also bind infromation about '@' variables.

The Binder then performs these activities:
- schema lookup and propagation (add columns and types, add aliases)
- type checks (are the ops and functions compatible with the columns)
? permission enforcement (does the user have the permission to that table, what additional
  constraints should be added for contextual access)
"""

"""
TODO:
- to bind the columns we need to pass them upwards, morsels have these columns with these types
  with these aliases, this is the form of the data at this point. This includes when functions
  and aggregates are run, this column has this type
- we then need to prune columns, once we're at the head of the execution tree, work our way
  back down pruning columns (no point reading a column if it's not used)
- we need to do function type validation
- we need to do evaluation (e.g. a + b) type validation
"""

import copy
import re

from orso.logging import get_logger

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import DatabaseError
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.functions.v2 import FUNCTIONS
from opteryx.managers.expression import NodeType

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")
logger = get_logger()


def inner_binder(node, relations):
    """
    Note, this is a tree within a tree, this is a single step in the execution plan (i.e. the plan
    associated with the relational algebra) which in itself may be an evaluation plan (i.e.
    executing comparisons)
    """

    if node.node_type & NodeType.IDENTIFIER == NodeType.IDENTIFIER:
        column = None
        found_source_relation = relations.get(node.source)

        if node.source is not None:
            if found_source_relation is None:
                # the dataset hasn't been loaded in a FROM or JOIN statement
                raise UnexpectedDatasetReferenceError(dataset=node.source)

            column = found_source_relation.find_column(node.source_column)
            if column is None:
                candidates = found_source_relation.get_all_columns()
                from opteryx.utils import fuzzy_search

                suggestion = fuzzy_search(node.value, candidates)
                raise ColumnNotFoundError(
                    column=node.value, dataset=node.source, suggestion=suggestion
                )

        else:
            # look for the column in the loaded relations
            for _, schema in relations.items():
                column = schema.find_column(node.value)
                if column is not None:
                    # if we've found it again - we're not sure which one to use
                    if found_source_relation:
                        raise AmbiguousIdentifierError(identifier=node.value)
                    found_source_relation = schema

        if found_source_relation is None:
            # If we didn't find the relation, get all of the columns it could have been and
            # see if we can suggest what the user should have entered in the error message
            candidates = []
            for _, schema in relations.items():
                candidates.extend(schema.get_all_columns())
            from opteryx.utils import fuzzy_search

            suggestion = fuzzy_search(node.value, candidates)
            raise ColumnNotFoundError(column=node.value, suggestion=suggestion)

        # add values to the node to indicate the source of this data
        node.source = found_source_relation.table_name
        node.source_column = column
    if (node.node_type & NodeType.FUNCTION == NodeType.FUNCTION) or (
        node.node_type & NodeType.AGGREGATOR == NodeType.AGGREGATOR
    ):
        # we're just going to bind the function into the node
        func = FUNCTIONS.get(node.value)
        if not func:
            suggest = FUNCTIONS.suggest(node.value)
            raise FunctionNotFoundError(function=node.value, suggestion=suggest)
        node.function = func

    # Now recurse and do this again for all the sub parts of the evaluation plan
    if node.left:
        node.left = inner_binder(node.left, relations)
    if node.right:
        node.right = inner_binder(node.right, relations)
    if node.centre:
        node.centre = inner_binder(node.centre, relations)
    if node.parameters:
        node.parameters = [inner_binder(parameter, relations) for parameter in node.parameters]

    return node


class BinderVisitor:
    def visit_node(self, node, context=None):
        node_type = node.node_type.name
        visit_method_name = f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"
        visit_method = getattr(self, visit_method_name, self.visit_unsupported)
        return_node, return_context = visit_method(node, context)
        if not isinstance(return_context, dict):
            raise DatabaseError(
                f"Internal Error - function {visit_method_name} didn't return a dict"
            )
        return return_node, return_context

    def visit_unsupported(self, node, context):
        logger.warning(f"No visit method implemented for node type {node.node_type.name}")
        return node, context

    def visit_project(self, node, context):
        logger.warning("visit_project not fully implemented")
        columns = []
        for column in node.columns:
            columns.append(inner_binder(column, context.get("schemas", {})))
        node.columns = columns
        return node, context

    def visit_scan(self, node, context):
        from opteryx.connectors import connector_factory

        # work out who will be serving this request
        node.connector = connector_factory(node.relation)
        # get them to tell is the schema of the dataset
        # None means we don't know ahead of time - we can usually get something
        context.setdefault("schemas", {})[node.alias] = node.connector.get_dataset_schema(
            node.relation
        )
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

        def merge_dicts(*dicts):
            """we ned to handle merging lists so have our own merge function"""
            merged_dict: dict = {}
            for dic in dicts:
                if not isinstance(dic, dict):
                    raise DatabaseError("Internal Error - merge_dicts expected dicts")
                for key, value in dic.items():
                    if key in merged_dict:
                        if isinstance(value, list):
                            merged_dict[key].extend(value)
                        elif isinstance(value, dict):
                            merged_dict[key].update(value)
                        else:
                            merged_dict[key] = value
                    else:
                        merged_dict[key] = value.copy() if isinstance(value, list) else value
            return merged_dict

        if context is None:
            context = {}

        # Recursively visit children
        children = graph.ingoing_edges(node)

        if children:
            exit_context = copy.deepcopy(context)
            for child in children:
                returned_graph, child_context = self.traverse(
                    graph, child[0], copy.deepcopy(context)
                )
                exit_context = merge_dicts(child_context, exit_context)
            context = merge_dicts(context, exit_context)
        # Visit node and return updated context
        return_node, context = self.visit_node(graph[node], context=context)
        graph[node] = return_node
        return graph, context


def do_bind_phase(plan, context=None, common_table_expressions=None):
    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    if len(root_node) > 1:
        raise ValueError(f"logical plan has {len(root_node)} heads - this is an error")
    plan = binder_visitor.traverse(plan, root_node[0])
    return plan
