"""
Tests for flow identification in physical plans.

Flows are chains of operations that can execute together without
needing to report back interim snapshots.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models import PhysicalPlan


class MockNode:
    """Mock node for testing flow identification."""
    
    def __init__(self, is_stateless=True, is_join=False, is_scan=False):
        self.is_stateless = is_stateless
        self.is_join = is_join
        self.is_scan = is_scan
        self.flow_id = None


def test_linear_chain_of_stateless_nodes():
    """Test that a linear chain of stateless nodes forms a single flow."""
    plan = PhysicalPlan()
    
    # Create a linear chain: scan -> filter1 -> filter2 -> project
    scan = MockNode(is_stateless=True, is_scan=True)
    filter1 = MockNode(is_stateless=True)
    filter2 = MockNode(is_stateless=True)
    project = MockNode(is_stateless=True)
    
    plan.add_node("scan", scan)
    plan.add_node("filter1", filter1)
    plan.add_node("filter2", filter2)
    plan.add_node("project", project)
    
    plan.add_edge("scan", "filter1")
    plan.add_edge("filter1", "filter2")
    plan.add_edge("filter2", "project")
    
    # Identify flows
    num_flows = plan.identify_flows()
    
    # All stateless nodes in a chain should be in the same flow
    assert scan.flow_id == filter1.flow_id == filter2.flow_id == project.flow_id
    assert num_flows >= 1


def test_stateful_node_breaks_flow():
    """Test that stateful nodes break flows."""
    plan = PhysicalPlan()
    
    # Create chain: scan -> filter -> aggregate -> project
    scan = MockNode(is_stateless=True, is_scan=True)
    filter_node = MockNode(is_stateless=True)
    aggregate = MockNode(is_stateless=False)  # Stateful
    project = MockNode(is_stateless=True)
    
    plan.add_node("scan", scan)
    plan.add_node("filter", filter_node)
    plan.add_node("aggregate", aggregate)
    plan.add_node("project", project)
    
    plan.add_edge("scan", "filter")
    plan.add_edge("filter", "aggregate")
    plan.add_edge("aggregate", "project")
    
    # Identify flows
    num_flows = plan.identify_flows()
    
    # scan and filter should be in one flow
    assert scan.flow_id == filter_node.flow_id
    
    # aggregate should NOT be in a flow (it's stateful - acts as a boundary)
    assert aggregate.flow_id is None
    
    # project should be in a different flow from scan/filter
    assert project.flow_id != filter_node.flow_id
    assert num_flows >= 2


def test_join_breaks_flow():
    """Test that join nodes break flows."""
    plan = PhysicalPlan()
    
    # Create a join scenario
    scan_left = MockNode(is_stateless=True, is_scan=True)
    scan_right = MockNode(is_stateless=True, is_scan=True)
    join = MockNode(is_stateless=False, is_join=True)
    project = MockNode(is_stateless=True)
    
    plan.add_node("scan_left", scan_left)
    plan.add_node("scan_right", scan_right)
    plan.add_node("join", join)
    plan.add_node("project", project)
    
    plan.add_edge("scan_left", "join", "left")
    plan.add_edge("scan_right", "join", "right")
    plan.add_edge("join", "project")
    
    # Identify flows
    num_flows = plan.identify_flows()
    
    # Each scan should be in its own flow
    assert scan_left.flow_id != scan_right.flow_id
    
    # Join should NOT be in a flow (it's a join node - acts as a boundary)
    assert join.flow_id is None
    
    # Project should be in a different flow from both scans
    assert project.flow_id != scan_left.flow_id
    assert project.flow_id != scan_right.flow_id
    assert num_flows >= 3


def test_branch_breaks_flow():
    """Test that branching breaks flows."""
    plan = PhysicalPlan()
    
    # Create branching: scan -> filter -> [project1, project2]
    scan = MockNode(is_stateless=True, is_scan=True)
    filter_node = MockNode(is_stateless=True)
    project1 = MockNode(is_stateless=True)
    project2 = MockNode(is_stateless=True)
    
    plan.add_node("scan", scan)
    plan.add_node("filter", filter_node)
    plan.add_node("project1", project1)
    plan.add_node("project2", project2)
    
    plan.add_edge("scan", "filter")
    plan.add_edge("filter", "project1")
    plan.add_edge("filter", "project2")
    
    # Identify flows
    num_flows = plan.identify_flows()
    
    # scan and filter should be in the same flow
    assert scan.flow_id == filter_node.flow_id
    
    # Each branch should start a new flow (filter has multiple children)
    assert project1.flow_id != filter_node.flow_id
    assert project2.flow_id != filter_node.flow_id
    # The two branches should be in different flows
    assert project1.flow_id != project2.flow_id


def test_merge_breaks_flow():
    """Test that merge points break flows."""
    plan = PhysicalPlan()
    
    # Create merge: [scan1, scan2] -> union -> project
    scan1 = MockNode(is_stateless=True, is_scan=True)
    scan2 = MockNode(is_stateless=True, is_scan=True)
    union = MockNode(is_stateless=False)  # Stateful union
    project = MockNode(is_stateless=True)
    
    plan.add_node("scan1", scan1)
    plan.add_node("scan2", scan2)
    plan.add_node("union", union)
    plan.add_node("project", project)
    
    plan.add_edge("scan1", "union")
    plan.add_edge("scan2", "union")
    plan.add_edge("union", "project")
    
    # Identify flows
    num_flows = plan.identify_flows()
    
    # Each scan should be in its own flow
    assert scan1.flow_id != scan2.flow_id
    
    # Union should NOT be in a flow (it's stateful - acts as a boundary)
    assert union.flow_id is None
    
    # Project should be in its own flow
    assert project.flow_id != scan1.flow_id
    assert project.flow_id != scan2.flow_id


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
