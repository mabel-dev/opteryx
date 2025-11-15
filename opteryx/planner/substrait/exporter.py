# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Export Opteryx logical plans to Substrait format.

This module converts Opteryx's internal logical plan representation to Substrait's
protobuf-based plan format, enabling interoperability with other query engines.
"""

from typing import Optional

try:
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    from substrait.gen.proto import type_pb2
    SUBSTRAIT_AVAILABLE = True
except ImportError:
    SUBSTRAIT_AVAILABLE = False


def export_to_substrait(logical_plan, output_format: str = "proto") -> bytes:
    """
    Export an Opteryx logical plan to Substrait format.
    
    Args:
        logical_plan: The Opteryx logical plan to export
        output_format: Format of the output ("proto" for binary protobuf, "json" for JSON)
        
    Returns:
        Serialized Substrait plan as bytes
        
    Raises:
        ImportError: If substrait package is not installed
        NotImplementedError: If the logical plan contains unsupported operations
    """
    if not SUBSTRAIT_AVAILABLE:
        raise ImportError(
            "substrait package is required for Substrait export. "
            "Install it with: pip install substrait"
        )
    
    exporter = SubstraitExporter()
    return exporter.export(logical_plan, output_format)


class SubstraitExporter:
    """
    Converts Opteryx logical plans to Substrait format.
    """
    
    def __init__(self):
        """Initialize the exporter with type mappings."""
        from opteryx.planner.logical_planner import LogicalPlanStepType
        from orso.types import OrsoTypes
        
        # Map Opteryx logical plan step types to Substrait relation builders
        self.step_type_handlers = {
            LogicalPlanStepType.Scan: self._build_read_rel,
            LogicalPlanStepType.Project: self._build_project_rel,
            LogicalPlanStepType.Filter: self._build_filter_rel,
            LogicalPlanStepType.Join: self._build_join_rel,
            LogicalPlanStepType.AggregateAndGroup: self._build_aggregate_rel,
            LogicalPlanStepType.Limit: self._build_fetch_rel,
            LogicalPlanStepType.Order: self._build_sort_rel,
        }
        
        # Map Orso types to Substrait types
        self.type_mappings = {
            OrsoTypes.BOOLEAN: type_pb2.Type.Boolean,
            OrsoTypes.INTEGER: type_pb2.Type.I64,
            OrsoTypes.DOUBLE: type_pb2.Type.FP64,
            OrsoTypes.VARCHAR: type_pb2.Type.String,
            OrsoTypes.TIMESTAMP: type_pb2.Type.Timestamp,
            OrsoTypes.DATE: type_pb2.Type.Date,
            OrsoTypes.BLOB: type_pb2.Type.Binary,
        }
    
    def export(self, logical_plan, output_format: str = "proto") -> bytes:
        """
        Export a logical plan to Substrait format.
        
        Args:
            logical_plan: The Opteryx logical plan (Graph object)
            output_format: Output format ("proto" or "json")
            
        Returns:
            Serialized Substrait plan
        """
        # Create a Substrait Plan
        plan = plan_pb2.Plan()
        
        # Set plan version
        plan.version.major_number = 0
        plan.version.minor_number = 30
        plan.version.patch_number = 0
        
        # Build the plan root from the logical plan
        if logical_plan:
            root_rel = self._build_rel_tree(logical_plan)
            if root_rel:
                plan.relations.append(plan_pb2.PlanRel(root=root_rel))
        
        # Serialize to requested format
        if output_format == "proto":
            return plan.SerializeToString()
        elif output_format == "json":
            from google.protobuf import json_format
            return json_format.MessageToJson(plan).encode('utf-8')
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _build_rel_tree(self, logical_plan):
        """
        Build a Substrait relation tree from an Opteryx logical plan.
        
        Args:
            logical_plan: The logical plan graph
            
        Returns:
            A Substrait RelRoot message
        """
        from opteryx.planner.logical_planner import LogicalPlanStepType
        
        # Find the root node (typically the last operation)
        root_nodes = [n for n in logical_plan.nodes() if logical_plan.out_degree(n) == 0]
        
        if not root_nodes:
            return None
        
        root_node = root_nodes[0]
        
        # Build the relation tree starting from the root
        root_rel = algebra_pb2.RelRoot()
        root_rel.input.CopyFrom(self._build_rel(root_node, logical_plan))
        
        return root_rel
    
    def _build_rel(self, node, logical_plan):
        """
        Build a Substrait Rel from a logical plan node.
        
        Args:
            node: The logical plan node to convert
            logical_plan: The complete logical plan graph
            
        Returns:
            A Substrait Rel message
        """
        from opteryx.planner.logical_planner import LogicalPlanStepType
        
        # Get the handler for this step type
        handler = self.step_type_handlers.get(node.node_type)
        
        if handler:
            return handler(node, logical_plan)
        else:
            # For unsupported operations, create an extension relation
            rel = algebra_pb2.Rel()
            ext_rel = rel.extension_leaf
            ext_rel.detail.CopyFrom(self._create_extension_object(node))
            return rel
    
    def _build_read_rel(self, node, logical_plan):
        """Build a ReadRel for Scan operations."""
        rel = algebra_pb2.Rel()
        read_rel = rel.read
        
        # Set the named table
        if hasattr(node, 'relation') and node.relation:
            read_rel.named_table.names.append(str(node.relation))
        
        # Get input relation if exists
        predecessors = list(logical_plan.predecessors(node))
        if predecessors:
            read_rel.common.direct.CopyFrom(self._build_rel(predecessors[0], logical_plan))
        
        return rel
    
    def _build_project_rel(self, node, logical_plan):
        """Build a ProjectRel for Project operations."""
        rel = algebra_pb2.Rel()
        project_rel = rel.project
        
        # Get input relation
        predecessors = list(logical_plan.predecessors(node))
        if predecessors:
            project_rel.input.CopyFrom(self._build_rel(predecessors[0], logical_plan))
        
        # Add projection expressions
        if hasattr(node, 'columns') and node.columns:
            for col in node.columns:
                expr = project_rel.expressions.add()
                self._build_expression(col, expr)
        
        return rel
    
    def _build_filter_rel(self, node, logical_plan):
        """Build a FilterRel for Filter operations."""
        rel = algebra_pb2.Rel()
        filter_rel = rel.filter
        
        # Get input relation
        predecessors = list(logical_plan.predecessors(node))
        if predecessors:
            filter_rel.input.CopyFrom(self._build_rel(predecessors[0], logical_plan))
        
        # Add filter condition
        if hasattr(node, 'condition') and node.condition:
            self._build_expression(node.condition, filter_rel.condition)
        
        return rel
    
    def _build_join_rel(self, node, logical_plan):
        """Build a JoinRel for Join operations."""
        rel = algebra_pb2.Rel()
        join_rel = rel.join
        
        # Get input relations (left and right)
        predecessors = list(logical_plan.predecessors(node))
        if len(predecessors) >= 2:
            join_rel.left.CopyFrom(self._build_rel(predecessors[0], logical_plan))
            join_rel.right.CopyFrom(self._build_rel(predecessors[1], logical_plan))
        
        # Set join type
        if hasattr(node, 'join_type'):
            join_type_map = {
                'INNER': algebra_pb2.JoinRel.JOIN_TYPE_INNER,
                'LEFT': algebra_pb2.JoinRel.JOIN_TYPE_LEFT,
                'RIGHT': algebra_pb2.JoinRel.JOIN_TYPE_RIGHT,
                'OUTER': algebra_pb2.JoinRel.JOIN_TYPE_OUTER,
            }
            join_rel.type = join_type_map.get(str(node.join_type).upper(), 
                                             algebra_pb2.JoinRel.JOIN_TYPE_INNER)
        
        # Add join expression
        if hasattr(node, 'condition') and node.condition:
            self._build_expression(node.condition, join_rel.expression)
        
        return rel
    
    def _build_aggregate_rel(self, node, logical_plan):
        """Build an AggregateRel for AggregateAndGroup operations."""
        rel = algebra_pb2.Rel()
        agg_rel = rel.aggregate
        
        # Get input relation
        predecessors = list(logical_plan.predecessors(node))
        if predecessors:
            agg_rel.input.CopyFrom(self._build_rel(predecessors[0], logical_plan))
        
        # Add grouping keys
        if hasattr(node, 'group_by_columns') and node.group_by_columns:
            grouping = agg_rel.groupings.add()
            for col in node.group_by_columns:
                expr = grouping.grouping_expressions.add()
                self._build_expression(col, expr)
        
        # Add aggregate measures
        if hasattr(node, 'aggregates') and node.aggregates:
            for agg in node.aggregates:
                measure = agg_rel.measures.add()
                self._build_aggregate_function(agg, measure.measure)
        
        return rel
    
    def _build_fetch_rel(self, node, logical_plan):
        """Build a FetchRel for Limit operations."""
        rel = algebra_pb2.Rel()
        fetch_rel = rel.fetch
        
        # Get input relation
        predecessors = list(logical_plan.predecessors(node))
        if predecessors:
            fetch_rel.input.CopyFrom(self._build_rel(predecessors[0], logical_plan))
        
        # Set offset and count
        if hasattr(node, 'offset'):
            fetch_rel.offset = node.offset
        if hasattr(node, 'limit'):
            fetch_rel.count = node.limit
        
        return rel
    
    def _build_sort_rel(self, node, logical_plan):
        """Build a SortRel for Order operations."""
        rel = algebra_pb2.Rel()
        sort_rel = rel.sort
        
        # Get input relation
        predecessors = list(logical_plan.predecessors(node))
        if predecessors:
            sort_rel.input.CopyFrom(self._build_rel(predecessors[0], logical_plan))
        
        # Add sort fields
        if hasattr(node, 'order_by_columns') and node.order_by_columns:
            for col in node.order_by_columns:
                sort_field = sort_rel.sorts.add()
                self._build_expression(col, sort_field.expr)
                # Default to ascending
                sort_field.direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_LAST
        
        return rel
    
    def _build_expression(self, expr_node, substrait_expr):
        """
        Build a Substrait expression from an Opteryx expression node.
        
        Args:
            expr_node: The Opteryx expression node
            substrait_expr: The Substrait Expression message to populate
        """
        from opteryx.managers.expression import NodeType
        
        if not expr_node:
            return
        
        # Handle different expression types
        if hasattr(expr_node, 'node_type'):
            if expr_node.node_type == NodeType.LITERAL:
                self._build_literal(expr_node, substrait_expr.literal)
            elif expr_node.node_type == NodeType.IDENTIFIER:
                # Field reference
                field_ref = substrait_expr.selection.direct_reference.struct_field
                if hasattr(expr_node, 'field_index'):
                    field_ref.field = expr_node.field_index
            elif expr_node.node_type == NodeType.FUNCTION:
                # Scalar function
                self._build_scalar_function(expr_node, substrait_expr.scalar_function)
    
    def _build_literal(self, expr_node, literal):
        """Build a Substrait literal from an Opteryx literal node."""
        from orso.types import OrsoTypes
        
        if not hasattr(expr_node, 'value'):
            return
        
        value = expr_node.value
        expr_type = getattr(expr_node, 'type', None)
        
        if value is None:
            literal.null.CopyFrom(type_pb2.Type())
        elif expr_type == OrsoTypes.BOOLEAN or isinstance(value, bool):
            literal.boolean = bool(value)
        elif expr_type == OrsoTypes.INTEGER or isinstance(value, int):
            literal.i64 = int(value)
        elif expr_type == OrsoTypes.DOUBLE or isinstance(value, float):
            literal.fp64 = float(value)
        elif expr_type == OrsoTypes.VARCHAR or isinstance(value, str):
            literal.string = str(value)
    
    def _build_scalar_function(self, expr_node, scalar_func):
        """Build a Substrait scalar function."""
        if hasattr(expr_node, 'function_name'):
            # Note: This is simplified - real implementation would need function mapping
            scalar_func.function_reference = 0  # Would need extension function registry
            
        # Add arguments
        if hasattr(expr_node, 'args'):
            for arg in expr_node.args:
                func_arg = scalar_func.arguments.add()
                self._build_expression(arg, func_arg.value)
    
    def _build_aggregate_function(self, agg_node, agg_func):
        """Build a Substrait aggregate function."""
        if hasattr(agg_node, 'function_name'):
            agg_func.function_reference = 0  # Would need extension function registry
        
        # Add arguments
        if hasattr(agg_node, 'args'):
            for arg in agg_node.args:
                func_arg = agg_func.arguments.add()
                self._build_expression(arg, func_arg.value)
        
        # Set aggregation phase
        agg_func.phase = algebra_pb2.AGGREGATION_PHASE_INITIAL_TO_RESULT
    
    def _create_extension_object(self, node):
        """Create an extension object for unsupported operations."""
        from google.protobuf import any_pb2
        
        ext_obj = any_pb2.Any()
        ext_obj.type_url = f"opteryx.dev/LogicalPlanNode/{node.node_type}"
        # Store node properties as JSON-encoded data
        import json
        ext_obj.value = json.dumps({
            'node_type': str(node.node_type),
            'properties': getattr(node, 'properties', {})
        }).encode('utf-8')
        
        return ext_obj
