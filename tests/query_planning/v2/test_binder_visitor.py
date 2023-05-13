"""
Test we're traversing the DAG in the correct order, that we're passing values between operators
correctly and that we're merging values correctly.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.components.v2.binder import BinderVisitor
from opteryx.components.v2.logical_planner import LogicalPlan
from opteryx.components.v2.logical_planner import LogicalPlanNode
from opteryx.components.v2.logical_planner import LogicalPlanStepType


def test_logical_plan_visitor():
    plan = LogicalPlan()
    scan_node_left = LogicalPlanNode(LogicalPlanStepType.Scan)
    scan_node_left.relation = "left"
    scan_node_right = LogicalPlanNode(LogicalPlanStepType.Scan)
    scan_node_right.relation = "right"
    project_node = LogicalPlanNode(LogicalPlanStepType.Project)
    filter_node = LogicalPlanNode(LogicalPlanStepType.Filter)
    union_node = LogicalPlanNode(LogicalPlanStepType.Union)
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
            return context

        def visit_filter(self, node, context):
            node.sources = context.get("sources")
            return context

        def visit_union(self, node, context):
            node.sources = context.get("sources")
            return context

        def visit_project(self, node, context):
            node.sources = context.get("sources")
            return context

    visitor = TestBinderVisitor()
    visitor.traverse(plan, 4)

    # this is just on the left branch
    assert filter_node.sources == ["left"], filter_node.sources
    # this is where the left and right branches meet
    assert set(union_node.sources) == {"left", "right"}, union_node.sources


if __name__ == "__main__":
    test_logical_plan_visitor()
    print("okay")
