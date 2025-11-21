# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Integration tests for Substrait import and export functionality.

These tests verify the basic structure and API without requiring
full compilation of the Opteryx codebase.
"""

import pytest


def test_substrait_modules_importable():
    """Test that substrait modules can be imported."""
    try:
        from opteryx.planner.substrait import export_to_substrait, import_from_substrait
        assert export_to_substrait is not None
        assert import_from_substrait is not None
        assert callable(export_to_substrait)
        assert callable(import_from_substrait)
    except ImportError as e:
        # If substrait is not installed, that's fine - the feature is optional
        if "substrait" in str(e).lower():
            pytest.skip("Substrait package not installed")
        else:
            # Other import errors should be reported
            raise


def test_exporter_class_exists():
    """Test that SubstraitExporter class exists and can be instantiated."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.substrait.exporter import SubstraitExporter
    
    exporter = SubstraitExporter()
    assert exporter is not None
    assert hasattr(exporter, 'export')
    assert callable(exporter.export)


def test_importer_class_exists():
    """Test that SubstraitImporter class exists and can be instantiated."""
    pytest.importorskip("substrait")
    
    from opteryx.planner.substrait.importer import SubstraitImporter
    
    importer = SubstraitImporter()
    assert importer is not None
    assert hasattr(importer, 'import_plan')
    assert callable(importer.import_plan)


def test_substrait_proto_messages():
    """Test that we can create basic Substrait protobuf messages."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    
    # Create a simple plan
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    plan.version.patch_number = 0
    
    # Verify we can serialize
    serialized = plan.SerializeToString()
    assert isinstance(serialized, bytes)
    assert len(serialized) > 0
    
    # Verify we can deserialize
    plan2 = plan_pb2.Plan()
    plan2.ParseFromString(serialized)
    assert plan2.version.major_number == 0
    assert plan2.version.minor_number == 30


def test_create_simple_read_rel():
    """Test creating a simple ReadRel in Substrait."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    
    # Create a plan with a read relation
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    plan_rel = plan.relations.add()
    root = plan_rel.root
    read_rel = root.input.read
    read_rel.named_table.names.append("test_table")
    
    # Serialize and verify
    serialized = plan.SerializeToString()
    assert len(serialized) > 0
    
    # Parse back
    plan2 = plan_pb2.Plan()
    plan2.ParseFromString(serialized)
    assert len(plan2.relations) == 1
    assert plan2.relations[0].root.input.read.named_table.names[0] == "test_table"


def test_create_project_rel():
    """Test creating a ProjectRel in Substrait."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    
    # Create a plan with project and read
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    plan_rel = plan.relations.add()
    root = plan_rel.root
    
    # Project over a read
    project_rel = root.input.project
    read_rel = project_rel.input.read
    read_rel.named_table.names.append("test_table")
    
    # Serialize and verify
    serialized = plan.SerializeToString()
    plan2 = plan_pb2.Plan()
    plan2.ParseFromString(serialized)
    
    assert len(plan2.relations) == 1
    assert plan2.relations[0].root.input.HasField('project')


def test_create_filter_rel():
    """Test creating a FilterRel in Substrait."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    
    # Create a plan with filter and read
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    plan_rel = plan.relations.add()
    root = plan_rel.root
    
    # Filter over a read
    filter_rel = root.input.filter
    read_rel = filter_rel.input.read
    read_rel.named_table.names.append("test_table")
    
    # Add a simple literal condition
    filter_rel.condition.literal.boolean = True
    
    # Serialize and verify
    serialized = plan.SerializeToString()
    plan2 = plan_pb2.Plan()
    plan2.ParseFromString(serialized)
    
    assert len(plan2.relations) == 1
    assert plan2.relations[0].root.input.HasField('filter')
    assert plan2.relations[0].root.input.filter.condition.literal.boolean == True


def test_error_handling_missing_substrait():
    """Test that appropriate error is raised when substrait is not available."""
    from opteryx.planner.substrait.exporter import SUBSTRAIT_AVAILABLE
    
    if not SUBSTRAIT_AVAILABLE:
        from opteryx.planner.substrait import export_to_substrait
        
        with pytest.raises(ImportError, match="substrait package is required"):
            export_to_substrait(None)


def test_json_serialization():
    """Test JSON serialization of Substrait plans."""
    pytest.importorskip("substrait")
    
    from substrait.gen.proto import plan_pb2
    from google.protobuf import json_format
    import json
    
    # Create a simple plan
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    plan_rel = plan.relations.add()
    root = plan_rel.root
    read_rel = root.input.read
    read_rel.named_table.names.append("test_table")
    
    # Convert to JSON
    json_str = json_format.MessageToJson(plan)
    parsed = json.loads(json_str)
    
    assert 'version' in parsed
    # Proto3 omits fields with default values in JSON
    # Just verify we can serialize/deserialize
    assert isinstance(parsed, dict)


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
