#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Example demonstrating Substrait import/export functionality in Opteryx.

This example shows how to:
1. Create Substrait plans programmatically
2. Serialize and deserialize plans
3. Use both protobuf and JSON formats

To run this example:
    pip install substrait protobuf
    python examples/substrait_example.py
"""

def example_create_and_serialize():
    """Create a simple Substrait plan and serialize it."""
    from substrait.gen.proto import plan_pb2
    
    print("Creating a simple Substrait plan...")
    
    # Create a plan
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    plan.version.patch_number = 0
    
    # Add a read relation
    plan_rel = plan.relations.add()
    root = plan_rel.root
    read_rel = root.input.read
    read_rel.named_table.names.append("customer_table")
    
    # Serialize to protobuf
    proto_bytes = plan.SerializeToString()
    print(f"Serialized plan size: {len(proto_bytes)} bytes")
    
    # Serialize to JSON
    from google.protobuf import json_format
    json_str = json_format.MessageToJson(plan)
    print(f"JSON representation:\n{json_str}")
    
    return proto_bytes


def example_deserialize():
    """Deserialize a Substrait plan."""
    from substrait.gen.proto import plan_pb2
    
    # Get serialized plan from previous example
    proto_bytes = example_create_and_serialize()
    
    print("\nDeserializing plan...")
    
    # Deserialize
    plan = plan_pb2.Plan()
    plan.ParseFromString(proto_bytes)
    
    print(f"Plan version: {plan.version.major_number}.{plan.version.minor_number}.{plan.version.patch_number}")
    print(f"Number of relations: {len(plan.relations)}")
    
    if plan.relations:
        rel = plan.relations[0]
        if rel.root.input.HasField('read'):
            table_name = rel.root.input.read.named_table.names[0]
            print(f"Table name: {table_name}")


def example_complex_plan():
    """Create a more complex Substrait plan with filter and project."""
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    
    print("\n\nCreating a complex plan with Read -> Filter -> Project...")
    
    # Create plan
    plan = plan_pb2.Plan()
    plan.version.major_number = 0
    plan.version.minor_number = 30
    
    # Build the plan from bottom to top
    plan_rel = plan.relations.add()
    root = plan_rel.root
    
    # Project (top level)
    project_rel = root.input.project
    
    # Filter (middle level)
    filter_rel = project_rel.input.filter
    # Add a simple boolean literal as filter condition
    filter_rel.condition.literal.boolean = True
    
    # Read (bottom level)
    read_rel = filter_rel.input.read
    read_rel.named_table.names.append("orders")
    
    # Serialize
    proto_bytes = plan.SerializeToString()
    print(f"Complex plan serialized: {len(proto_bytes)} bytes")
    
    # Verify structure
    plan2 = plan_pb2.Plan()
    plan2.ParseFromString(proto_bytes)
    
    print("Plan structure verified:")
    print("  - Root has Project: ", plan2.relations[0].root.input.HasField('project'))
    print("  - Project has Filter input: ", plan2.relations[0].root.input.project.input.HasField('filter'))
    print("  - Filter has Read input: ", plan2.relations[0].root.input.project.input.filter.input.HasField('read'))


def example_with_opteryx():
    """
    Example showing how to use Substrait with Opteryx.
    
    Note: This requires Opteryx to be compiled. If you see import errors,
    you need to compile the Cython extensions first.
    """
    try:
        from opteryx.planner.substrait import export_to_substrait, import_from_substrait
        from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
        
        print("\n\nCreating an Opteryx logical plan...")
        
        # Create a simple logical plan
        logical_plan = LogicalPlan()
        scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        scan_node.relation = "products"
        logical_plan.add_node(scan_node)
        
        # Export to Substrait
        print("Exporting to Substrait...")
        substrait_bytes = export_to_substrait(logical_plan, output_format="proto")
        print(f"Exported plan: {len(substrait_bytes)} bytes")
        
        # Import back
        print("Importing from Substrait...")
        imported_plan = import_from_substrait(substrait_bytes, input_format="proto")
        print(f"Imported plan has {len(imported_plan.nodes())} nodes")
        
        # Export to JSON for inspection
        json_bytes = export_to_substrait(logical_plan, output_format="json")
        import json
        plan_dict = json.loads(json_bytes.decode('utf-8'))
        print(f"JSON export:\n{json.dumps(plan_dict, indent=2)}")
        
    except ImportError as e:
        print(f"\n\nSkipping Opteryx integration example: {e}")
        print("To run this example, compile Opteryx with: make compile")


def main():
    """Run all examples."""
    print("=" * 60)
    print("Substrait Examples for Opteryx")
    print("=" * 60)
    
    try:
        example_create_and_serialize()
        example_deserialize()
        example_complex_plan()
        example_with_opteryx()
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        
    except ImportError as e:
        print(f"\nError: {e}")
        print("\nPlease install required packages:")
        print("  pip install substrait protobuf")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
