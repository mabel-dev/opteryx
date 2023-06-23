"""
Test we're traversing the DAG in the correct order, that we're passing values between operators
correctly and that we're merging values correctly.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.components.binder import BinderVisitor
from opteryx.components.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType


def test_logical_plan_visitor():
    plan = LogicalPlan()
    scan_node_left = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node_left.relation = "left"
    scan_node_right = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node_right.relation = "right"
    project_node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    union_node = LogicalPlanNode(node_type=LogicalPlanStepType.Union)
    plan.add_node(1, scan_node_left)
    plan.add_node(2, project_node)
    plan.add_node(3, filter_node)
    plan.add_node(4, union_node)
    plan.add_node(5, scan_node_right)
    plan.add_edge(1, 3)
    plan.add_edge(3, 2)
    plan.add_edge(2, 4)
    plan.add_edge(5, 4)

    # does this look right?
    print(plan.draw())

    class TestBinderVisitor(BinderVisitor):
        def visit_scan(self, node, context):
            context.setdefault("sources", []).append(node.relation)
            return node, context

        def visit_filter(self, node, context):
            node.sources = context.get("sources")
            return node, context

        def visit_union(self, node, context):
            node.sources = context.get("sources")
            return node, context

        def visit_project(self, node, context):
            node.sources = context.get("sources")
            return node, context

    visitor = TestBinderVisitor()
    visitor.traverse(plan, 4)

    # this is just on the left branch
    assert filter_node.sources == ["left"], filter_node.sources
    # this is where the left and right branches meet
    assert set(union_node.sources) == {"left", "right"}, union_node.sources


if __name__ == "__main__":  # pragma: no cover
    test_logical_plan_visitor()
    print("okay")
