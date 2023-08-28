"""
Test we're traversing the DAG in the correct order, that we're passing values between operators
correctly and that we're merging values correctly.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.components.binder import BinderVisitor
from opteryx.components.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from orso.schema import RelationSchema


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

    """
    └─ UNION 
       ├─ PROJECT
       │  └─ FILTER (null)
       │     └─ SCAN (left AS None)
       └─ SCAN (right AS None)
    """

    class TestBinderVisitor(BinderVisitor):
        def visit_scan(self, node, context):
            context.setdefault("schemas", [])[node.relation] = RelationSchema(name="test")
            node.source = node.relation
            return node, context

        def visit_filter(self, node, context):
            # the filter has the left scan before it
            node.sources = context.get("schemas")
            return node, context

        def visit_union(self, node, context):
            node.sources = context.get("schemas")
            return node, context

        def visit_project(self, node, context):
            # the project has the left and right scans before it
            node.sources = context.get("schemas")
            return node, context

    context = {
        "schemas": {},
        "cache": None,
        "connection": None,
        "relations": set(),
    }

    visitor = TestBinderVisitor()
    visitor.traverse(plan, 4, context)

    # this is just on the left branch
    assert set(filter_node.sources) == {"left"}, filter_node.sources
    # this is where the left and right branches meet
    assert set(union_node.sources) == {"left", "right"}, union_node.sources


if __name__ == "__main__":  # pragma: no cover
    test_logical_plan_visitor()
    print("okay")
