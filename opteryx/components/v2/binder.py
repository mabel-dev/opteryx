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
bind the following:
- the date ranges for the relations
- the schemas to the relations
    - I need to work out the source for each of the relations
- the source relation to each column
- placeholders with the value from the parameters

to do this I need:
- the plan
- the schemas for all of the relations

as a fallback I need to:
- handle not knowing the schemas
"""

import copy

from orso.logging import get_logger

from opteryx.exceptions import DatabaseError

logger = get_logger()


class BinderVisitor:
    def visit_node(self, node, context=None):
        node_type = node.node_type
        visit_method_name = f"visit_{node_type.split('.')[1].lower()}"
        visit_method = getattr(self, visit_method_name, self.visit_unsupported)
        result = visit_method(node, context)
        if not isinstance(result, dict):
            raise DatabaseError(f"function {visit_method_name} didn't return a dict")

    def visit_unsupported(self, node, context):
        raise NotImplementedError(f"No visit method implemented for node type {node.node_type}")

    def visit_project(self, node, context):
        logger.warning("visit_project not implemented")
        return context

    def visit_filter(self, node, context):
        logger.warning("visit_filter not implemented")
        return context

    def visit_union(self, node, context):
        logger.warning("visit_union not implemented")
        return context

    def visit_explain(self, node, context):
        logger.warning("visit_explain not implemented")
        return context

    def visit_difference(self, node, context):
        logger.warning("visit_difference not implemented")
        return context

    def visit_join(self, node, context):
        logger.warning("visit_join not implemented")
        return context

    def visit_group(self, node, context):
        logger.warning("visit_group not implemented")
        return context

    def visit_aggregate(self, node, context):
        logger.warning("visit_aggregate not implemented")
        return context

    def visit_scan(self, node, context):
        if node.relation[0] == "$":
            from opteryx import samples

            node.connector = "Internal"
            _schema = samples.planets.schema

        raise NotImplementedError("visit scan")
        """
        - determine the source of the relation:
            - sample
            - in-memory
            - on-disk
            - storage
            - collection
            - sql
        - if we can get the schema, do that and add it to the context
        """

    def visit_show(self, node, context):
        logger.warning("visit_show not implemented")
        return context

    def visit_show_columns(self, node, context):
        logger.warning("visit_show_columns not implemented")
        return context

    def visit_set(self, node, context):
        logger.warning("visit_set not implemented")
        return context

    def visit_limit(self, node, context):
        logger.warning("visit_limit not implemented")
        return context

    def visit_order(self, node, context):
        logger.warning("visit_order not implemented")
        return context

    def visit_distinct(self, node, context):
        logger.warning("visit_distinct not implemented")
        return context

    def visit_cte(self, node, context):
        logger.warning("visit_cte not implemented")
        return context

    def visit_subquery(self, node, context):
        logger.warning("visit_subquery not implemented")
        return context

    def visit_values(self, node, context):
        logger.warning("visit_values not implemented")
        return context

    def visit_unnest(self, node, context):
        logger.warning("visit_unnest not implemented")
        return context

    def visit_generate_series(self, node, context):
        logger.warning("visit_generate_series not implemented")
        return context

    def visit_fake(self, node, context):
        logger.warning("visit_fake not implemented")
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


def do_bind_phase(plan):
    binder_visitor = BinderVisitor()
    root_node = plan.get_entry_points()
    if len(root_node) > 1:
        raise ValueError(f"logical plan has {len(root_node)} heads - this is an error")
    binder_visitor.traverse(plan, root_node[0])
    return plan
