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
The binder is responsible for adding information about the database and engine into the logical
plan. It's not a rewrite step but does to value exchanges (which could be seen as a rewrite type
activity).

The binder takes the output from the logical plan, and adds information from various catalogues
into that plan and then performs some validation checks.

These catalogues include:
- The data catalogue (e.g. data schemas)
- The function catalogue (e.g. function inputs and data types)

The binder then performs these activities:
- schema lookup and propagation (add columns and types, add aliases)
- function lookup (does the function exist, if it's a constant evaluation then replace the value
  in the plan)
- type checks (are the ops and functions compatible with the columns)
? permission enforcement (does the user have the permission to that table, what additional
  constraints should be added for contextual access)
"""

import copy
import re

from orso.logging import get_logger

from opteryx.exceptions import DatabaseError

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")
logger = get_logger()


def source_identifiers(node, relations):
    if node.node_type == "Identifier":
        print("I found and identifier - ", node)

    if node.left:
        node.left = source_identifiers(node.left, relations)
    if node.right:
        node.right = source_identifiers(node.right, relations)

    return node


class BinderVisitor:
    def visit_node(self, node, context=None):
        node_type = node.node_type.name
        visit_method_name = f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"
        visit_method = getattr(self, visit_method_name, self.visit_unsupported)
        result = visit_method(node, context)
        if not isinstance(result, dict):
            raise DatabaseError(
                f"Internal Error - function {visit_method_name} didn't return a dict"
            )
        return result

    def visit_unsupported(self, node, context):
        logger.warning(f"No visit method implemented for node type {node.node_type.name}")
        return context

    def visit_project(self, node, context):
        # identify which relation the identifiers come from
        for column in node.columns:
            print(source_identifiers(column, context["sources"]))

        return context

    def visit_scan(self, node, context):
        from opteryx.connectors import connector_factory

        # work out who will be serving this request
        node.connector = connector_factory(node.relation)
        # get them to tell is the schema of the dataset
        # None means we don't know ahead of time - we can usually get something
        context[node.alias] = node.connector.get_dataset_schema(node.relation)

        logger.warning("visit_scan not implemented")
        logger.warning("visit_scan doesn't resolve CTEs")
        return context

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
                child_context = self.traverse(graph, child[0], copy.deepcopy(context))
                exit_context = merge_dicts(child_context, exit_context)
            context = merge_dicts(context, exit_context)
        # Visit node and return updated context
        context = self.visit_node(graph[node], context=context)
        return context


def do_bind_phase(plan, context=None, common_table_expressions=None):
    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    if len(root_node) > 1:
        raise ValueError(f"logical plan has {len(root_node)} heads - this is an error")
    binder_visitor.traverse(plan, root_node[0])
    return plan
