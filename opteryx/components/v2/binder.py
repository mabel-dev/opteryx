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


class BinderVisitor:
    def visit_node(self, node, context=None):
        node_type = node.node_type
        visit_method_name = f"visit_{node_type.split('.')[1].lower()}"
        visit_method = getattr(self, visit_method_name, self.visit_unsupported)
        return visit_method(node, context)

    def visit_unsupported(self, node, context):
        raise NotImplementedError(f"No visit method implemented for node type {node.node_type}")

    def visit_project(self, node, context):
        raise NotImplementedError("visit project")

    def visit_filter(self, node, context):
        raise NotImplementedError("visit filter")

    def visit_union(self, node, context):
        raise NotImplementedError("visit union")

    def visit_explain(self, node, context):
        raise NotImplementedError("visit explain")

    def visit_difference(self, node, context):
        raise NotImplementedError("visit difference")

    def visit_join(self, node, context):
        raise NotImplementedError("visit join")

    def visit_group(self, node, context):
        raise NotImplementedError("visit group")

    def visit_aggregate(self, node, context):
        raise NotImplementedError("visit aggregate")

    def visit_scan(self, node, context):
        raise NotImplementedError("visit scan")

    def visit_show(self, node, context):
        raise NotImplementedError("visit show")

    def visit_show_columns(self, node, context):
        raise NotImplementedError("visit show columns")

    def visit_set(self, node, context):
        raise NotImplementedError("visit set")

    def visit_limit(self, node, context):
        raise NotImplementedError("visit limit")

    def visit_order(self, node, context):
        raise NotImplementedError("visit order")

    def visit_distinct(self, node, context):
        raise NotImplementedError("visit distinct")

    def visit_cte(self, node, context):
        raise NotImplementedError("visit cte")

    def visit_subquery(self, node, context):
        raise NotImplementedError("visit subquery")

    def visit_values(self, node, context):
        raise NotImplementedError("visit values")

    def visit_unnest(self, node, context):
        raise NotImplementedError("visit unnest")

    def visit_generate_series(self, node, context):
        raise NotImplementedError("visit generate series")

    def visit_fake(self, node, context):
        raise NotImplementedError("visit fake")

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
