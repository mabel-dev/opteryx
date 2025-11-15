# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for Substrait import and export functionality.
"""

import pytest


def test_substrait_import_export_available():
    """Test that substrait import/export modules are available."""
    try:
        from opteryx.planner.substrait import export_to_substrait, import_from_substrait
        assert export_to_substrait is not None
        assert import_from_substrait is not None
    except ImportError as e:
        pytest.skip(f"Substrait not available: {e}")


def test_export_simple_scan():
    """Test exporting a simple scan operation to Substrait."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
    from opteryx.planner.substrait import export_to_substrait
    from substrait.gen.proto import plan_pb2
    
    # Create a simple logical plan with a scan
    logical_plan = LogicalPlan()
    scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node.relation = "test_table"
    logical_plan.add_node(scan_node)
    
    # Export to substrait
    substrait_bytes = export_to_substrait(logical_plan, output_format="proto")
    
    # Verify it can be parsed back
    plan = plan_pb2.Plan()
    plan.ParseFromString(substrait_bytes)
    
    assert plan.version.major_number >= 0
    assert len(plan.relations) > 0


def test_export_project():
    """Test exporting a project operation to Substrait."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
    from opteryx.planner.substrait import export_to_substrait
    from substrait.gen.proto import plan_pb2
    
    # Create a logical plan with scan and project
    logical_plan = LogicalPlan()
    
    scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node.relation = "test_table"
    logical_plan.add_node(scan_node)
    
    project_node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
    project_node.columns = []  # Empty for simplicity
    logical_plan.add_node(project_node)
    logical_plan.add_edge(scan_node, project_node)
    
    # Export to substrait
    substrait_bytes = export_to_substrait(logical_plan, output_format="proto")
    
    # Verify it can be parsed
    plan = plan_pb2.Plan()
    plan.ParseFromString(substrait_bytes)
    
    assert len(plan.relations) > 0


def test_export_filter():
    """Test exporting a filter operation to Substrait."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
    from opteryx.planner.substrait import export_to_substrait
    from substrait.gen.proto import plan_pb2
    
    # Create a logical plan with scan and filter
    logical_plan = LogicalPlan()
    
    scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node.relation = "test_table"
    logical_plan.add_node(scan_node)
    
    filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    filter_node.condition = None  # Simplified for test
    logical_plan.add_node(filter_node)
    logical_plan.add_edge(scan_node, filter_node)
    
    # Export to substrait
    substrait_bytes = export_to_substrait(logical_plan, output_format="proto")
    
    # Verify it can be parsed
    plan = plan_pb2.Plan()
    plan.ParseFromString(substrait_bytes)
    
    assert len(plan.relations) > 0


def test_export_json_format():
    """Test exporting to JSON format."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
    from opteryx.planner.substrait import export_to_substrait
    import json
    
    # Create a simple logical plan
    logical_plan = LogicalPlan()
    scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node.relation = "test_table"
    logical_plan.add_node(scan_node)
    
    # Export to JSON
    substrait_json = export_to_substrait(logical_plan, output_format="json")
    
    # Verify it's valid JSON
    parsed_json = json.loads(substrait_json.decode('utf-8'))
    assert 'version' in parsed_json


def test_import_simple_read():
    """Test importing a simple ReadRel from Substrait."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2, algebra_pb2
    from opteryx.planner.substrait import import_from_substrait
    
    # Create a simple Substrait plan with a ReadRel
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    plan.version.patch_number = 0
    
    # Create a read relation
    plan_rel = plan.relations.add()
    root = plan_rel.root
    read_rel = root.input.read
    read_rel.named_table.names.append("test_table")
    
    # Serialize to bytes
    substrait_bytes = plan.SerializeToString()
    
    # Import to Opteryx logical plan
    logical_plan = import_from_substrait(substrait_bytes, input_format="proto")
    
    # Verify the plan was created
    assert logical_plan is not None
    assert len(logical_plan.nodes()) > 0


def test_import_project():
    """Test importing a ProjectRel from Substrait."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2, algebra_pb2
    from opteryx.planner.substrait import import_from_substrait
    
    # Create a Substrait plan with read and project
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    plan_rel = plan.relations.add()
    root = plan_rel.root
    
    # Project with a read input
    project_rel = root.input.project
    read_rel = project_rel.input.read
    read_rel.named_table.names.append("test_table")
    
    # Serialize and import
    substrait_bytes = plan.SerializeToString()
    logical_plan = import_from_substrait(substrait_bytes, input_format="proto")
    
    assert logical_plan is not None
    assert len(logical_plan.nodes()) >= 2


def test_import_filter():
    """Test importing a FilterRel from Substrait."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2, algebra_pb2
    from opteryx.planner.substrait import import_from_substrait
    
    # Create a Substrait plan with read and filter
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    plan_rel = plan.relations.add()
    root = plan_rel.root
    
    # Filter with a read input
    filter_rel = root.input.filter
    read_rel = filter_rel.input.read
    read_rel.named_table.names.append("test_table")
    
    # Add a simple literal condition (true)
    filter_rel.condition.literal.boolean = True
    
    # Serialize and import
    substrait_bytes = plan.SerializeToString()
    logical_plan = import_from_substrait(substrait_bytes, input_format="proto")
    
    assert logical_plan is not None
    assert len(logical_plan.nodes()) >= 2


def test_round_trip_scan():
    """Test exporting and importing a scan operation."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
    from opteryx.planner.substrait import export_to_substrait, import_from_substrait
    
    # Create original plan
    original_plan = LogicalPlan()
    scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    scan_node.relation = "test_table"
    original_plan.add_node(scan_node)
    
    # Export to substrait
    substrait_bytes = export_to_substrait(original_plan, output_format="proto")
    
    # Import back
    imported_plan = import_from_substrait(substrait_bytes, input_format="proto")
    
    # Verify the plan structure is preserved
    assert imported_plan is not None
    assert len(imported_plan.nodes()) > 0


def test_error_on_missing_substrait():
    """Test that appropriate errors are raised when substrait is not available."""
    # Temporarily hide substrait
    import sys
    substrait_module = sys.modules.get('substrait')
    
    try:
        if 'substrait' in sys.modules:
            del sys.modules['substrait']
        
        # Reimport the module to trigger the check
        import importlib
        from opteryx.planner import substrait as substrait_module_new
        importlib.reload(substrait_module_new)
        
        from opteryx.planner.substrait import export_to_substrait
        from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
        
        # Create a simple plan
        logical_plan = LogicalPlan()
        scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        logical_plan.add_node(scan_node)
        
        # This should raise ImportError since substrait is not available
        with pytest.raises(ImportError, match="substrait package is required"):
            export_to_substrait(logical_plan)
            
    finally:
        # Restore substrait module if it was available
        if substrait_module:
            sys.modules['substrait'] = substrait_module


def test_invalid_format_error():
    """Test that invalid format raises appropriate error."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
    from opteryx.planner.substrait import export_to_substrait
    
    # Create a simple plan
    logical_plan = LogicalPlan()
    scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    logical_plan.add_node(scan_node)
    
    # Try to export with invalid format
    with pytest.raises(ValueError, match="Unsupported output format"):
        export_to_substrait(logical_plan, output_format="invalid")


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
