# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Import Substrait plans to Opteryx logical plan format.

This module converts Substrait protobuf-based plans to Opteryx's internal logical
plan representation, enabling interoperability with other query engines.
"""

from typing import Optional

try:
    from substrait.gen.proto import plan_pb2
    from substrait.gen.proto import algebra_pb2
    from substrait.gen.proto import type_pb2
    SUBSTRAIT_AVAILABLE = True
except ImportError:
    SUBSTRAIT_AVAILABLE = False


def import_from_substrait(substrait_plan: bytes, input_format: str = "proto"):
    """
    Import a Substrait plan and convert it to an Opteryx logical plan.
    
    Args:
        substrait_plan: The Substrait plan as bytes
        input_format: Format of the input ("proto" for binary protobuf, "json" for JSON)
        
    Returns:
        An Opteryx logical plan (Graph object)
        
    Raises:
        ImportError: If substrait package is not installed
        NotImplementedError: If the Substrait plan contains unsupported operations
    """
    if not SUBSTRAIT_AVAILABLE:
        raise ImportError(
            "substrait package is required for Substrait import. "
            "Install it with: pip install substrait"
        )
    
    importer = SubstraitImporter()
    return importer.import_plan(substrait_plan, input_format)


class SubstraitImporter:
    """
    Converts Substrait plans to Opteryx logical plans.
    """
    
    def __init__(self):
        """Initialize the importer with type mappings."""
        from opteryx.planner.logical_planner import LogicalPlanStepType
        from orso.types import OrsoTypes
        
        # Map Substrait relation types to Opteryx logical plan builders
        self.rel_handlers = {
            'read': self._handle_read_rel,
            'project': self._handle_project_rel,
            'filter': self._handle_filter_rel,
            'join': self._handle_join_rel,
            'aggregate': self._handle_aggregate_rel,
            'fetch': self._handle_fetch_rel,
            'sort': self._handle_sort_rel,
        }
        
        # Map Substrait types to Orso types
        self.type_mappings = {
            'bool': OrsoTypes.BOOLEAN,
            'i64': OrsoTypes.INTEGER,
            'fp64': OrsoTypes.DOUBLE,
            'string': OrsoTypes.VARCHAR,
            'timestamp': OrsoTypes.TIMESTAMP,
            'date': OrsoTypes.DATE,
            'binary': OrsoTypes.BLOB,
        }
    
    def import_plan(self, substrait_plan: bytes, input_format: str = "proto"):
        """
        Import a Substrait plan and convert to Opteryx logical plan.
        
        Args:
            substrait_plan: Serialized Substrait plan
            input_format: Input format ("proto" or "json")
            
        Returns:
            Opteryx logical plan (Graph object)
        """
        # Deserialize the Substrait plan
        plan = plan_pb2.Plan()
        
        if input_format == "proto":
            plan.ParseFromString(substrait_plan)
        elif input_format == "json":
            from google.protobuf import json_format
            json_format.Parse(substrait_plan.decode('utf-8'), plan)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")
        
        # Convert to Opteryx logical plan
        return self._build_logical_plan(plan)
    
    def _build_logical_plan(self, plan):
        """
        Build an Opteryx logical plan from a Substrait Plan.
        
        Args:
            plan: The Substrait Plan message
            
        Returns:
            An Opteryx logical plan (Graph object)
        """
        from opteryx.planner.logical_planner import LogicalPlan
        
        logical_plan = LogicalPlan()
        
        # Process each relation in the plan
        for plan_rel in plan.relations:
            if plan_rel.HasField('root'):
                root_node = self._build_rel_tree(plan_rel.root, logical_plan)
                # The root node is added to the graph by _build_rel_tree
        
        return logical_plan
    
    def _build_rel_tree(self, rel_root, logical_plan):
        """
        Build an Opteryx logical plan tree from a Substrait RelRoot.
        
        Args:
            rel_root: The Substrait RelRoot message
            logical_plan: The logical plan graph to add nodes to
            
        Returns:
            The root node of the built tree
        """
        if rel_root.HasField('input'):
            return self._build_rel(rel_root.input, logical_plan)
        return None
    
    def _build_rel(self, rel, logical_plan):
        """
        Build logical plan nodes from a Substrait Rel.
        
        Args:
            rel: The Substrait Rel message
            logical_plan: The logical plan graph to add nodes to
            
        Returns:
            The created logical plan node
        """
        # Determine which relation type this is
        rel_type = rel.WhichOneof('rel_type')
        
        if rel_type:
            handler = self.rel_handlers.get(rel_type)
            if handler:
                return handler(getattr(rel, rel_type), logical_plan)
            else:
                # Handle extension relations
                return self._handle_extension_rel(rel, logical_plan)
        
        return None
    
    def _handle_read_rel(self, read_rel, logical_plan):
        """Handle ReadRel and create a Scan node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        
        # Extract table name
        if read_rel.HasField('named_table'):
            if read_rel.named_table.names:
                node.relation = read_rel.named_table.names[0]
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle input if exists
        if read_rel.HasField('common') and read_rel.common.HasField('direct'):
            input_node = self._build_rel(read_rel.common.direct, logical_plan)
            if input_node:
                logical_plan.add_edge(input_node, node)
        
        return node
    
    def _handle_project_rel(self, project_rel, logical_plan):
        """Handle ProjectRel and create a Project node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        
        # Extract projection expressions
        columns = []
        for expr in project_rel.expressions:
            col = self._build_expression(expr)
            if col:
                columns.append(col)
        
        if columns:
            node.columns = columns
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle input
        if project_rel.HasField('input'):
            input_node = self._build_rel(project_rel.input, logical_plan)
            if input_node:
                logical_plan.add_edge(input_node, node)
        
        return node
    
    def _handle_filter_rel(self, filter_rel, logical_plan):
        """Handle FilterRel and create a Filter node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        
        # Extract filter condition
        if filter_rel.HasField('condition'):
            node.condition = self._build_expression(filter_rel.condition)
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle input
        if filter_rel.HasField('input'):
            input_node = self._build_rel(filter_rel.input, logical_plan)
            if input_node:
                logical_plan.add_edge(input_node, node)
        
        return node
    
    def _handle_join_rel(self, join_rel, logical_plan):
        """Handle JoinRel and create a Join node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
        
        # Extract join type
        join_type_map = {
            algebra_pb2.JoinRel.JOIN_TYPE_INNER: 'INNER',
            algebra_pb2.JoinRel.JOIN_TYPE_LEFT: 'LEFT',
            algebra_pb2.JoinRel.JOIN_TYPE_RIGHT: 'RIGHT',
            algebra_pb2.JoinRel.JOIN_TYPE_OUTER: 'OUTER',
        }
        node.join_type = join_type_map.get(join_rel.type, 'INNER')
        
        # Extract join condition
        if join_rel.HasField('expression'):
            node.condition = self._build_expression(join_rel.expression)
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle inputs (left and right)
        if join_rel.HasField('left'):
            left_node = self._build_rel(join_rel.left, logical_plan)
            if left_node:
                logical_plan.add_edge(left_node, node)
        
        if join_rel.HasField('right'):
            right_node = self._build_rel(join_rel.right, logical_plan)
            if right_node:
                logical_plan.add_edge(right_node, node)
        
        return node
    
    def _handle_aggregate_rel(self, agg_rel, logical_plan):
        """Handle AggregateRel and create an AggregateAndGroup node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
        
        # Extract grouping keys
        group_by_columns = []
        for grouping in agg_rel.groupings:
            for expr in grouping.grouping_expressions:
                col = self._build_expression(expr)
                if col:
                    group_by_columns.append(col)
        
        if group_by_columns:
            node.group_by_columns = group_by_columns
        
        # Extract aggregate measures
        aggregates = []
        for measure in agg_rel.measures:
            agg = self._build_aggregate_function(measure.measure)
            if agg:
                aggregates.append(agg)
        
        if aggregates:
            node.aggregates = aggregates
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle input
        if agg_rel.HasField('input'):
            input_node = self._build_rel(agg_rel.input, logical_plan)
            if input_node:
                logical_plan.add_edge(input_node, node)
        
        return node
    
    def _handle_fetch_rel(self, fetch_rel, logical_plan):
        """Handle FetchRel and create a Limit node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
        
        # Extract offset and count
        if fetch_rel.HasField('offset'):
            node.offset = fetch_rel.offset
        if fetch_rel.HasField('count'):
            node.limit = fetch_rel.count
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle input
        if fetch_rel.HasField('input'):
            input_node = self._build_rel(fetch_rel.input, logical_plan)
            if input_node:
                logical_plan.add_edge(input_node, node)
        
        return node
    
    def _handle_sort_rel(self, sort_rel, logical_plan):
        """Handle SortRel and create an Order node."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Order)
        
        # Extract sort fields
        order_by_columns = []
        for sort_field in sort_rel.sorts:
            col = self._build_expression(sort_field.expr)
            if col:
                # Could also extract sort direction from sort_field.direction
                order_by_columns.append(col)
        
        if order_by_columns:
            node.order_by_columns = order_by_columns
        
        # Add to logical plan
        logical_plan.add_node(node)
        
        # Handle input
        if sort_rel.HasField('input'):
            input_node = self._build_rel(sort_rel.input, logical_plan)
            if input_node:
                logical_plan.add_edge(input_node, node)
        
        return node
    
    def _handle_extension_rel(self, rel, logical_plan):
        """Handle extension relations (unsupported operations)."""
        from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
        
        # Create a generic node for the extension
        node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)  # Default to Scan
        
        # Try to extract extension details
        if rel.HasField('extension_leaf'):
            ext_rel = rel.extension_leaf
            if ext_rel.HasField('detail'):
                # Could parse extension details here
                pass
        
        logical_plan.add_node(node)
        return node
    
    def _build_expression(self, substrait_expr):
        """
        Build an Opteryx expression node from a Substrait Expression.
        
        Args:
            substrait_expr: The Substrait Expression message
            
        Returns:
            An Opteryx expression node
        """
        from opteryx.managers.expression import NodeType
        from opteryx.models import Node
        
        # Determine expression type
        expr_type = substrait_expr.WhichOneof('rex_type')
        
        if expr_type == 'literal':
            return self._build_literal_node(substrait_expr.literal)
        elif expr_type == 'selection':
            return self._build_field_reference(substrait_expr.selection)
        elif expr_type == 'scalar_function':
            return self._build_scalar_function_node(substrait_expr.scalar_function)
        
        # Return None for unsupported expression types
        return None
    
    def _build_literal_node(self, literal):
        """Build an Opteryx literal node from a Substrait literal."""
        from opteryx.planner import build_literal_node
        from orso.types import OrsoTypes
        
        # Extract literal value based on type
        literal_type = literal.WhichOneof('literal_type')
        
        if literal_type == 'boolean':
            return build_literal_node(literal.boolean, suggested_type=OrsoTypes.BOOLEAN)
        elif literal_type == 'i64':
            return build_literal_node(literal.i64, suggested_type=OrsoTypes.INTEGER)
        elif literal_type == 'fp64':
            return build_literal_node(literal.fp64, suggested_type=OrsoTypes.DOUBLE)
        elif literal_type == 'string':
            return build_literal_node(literal.string, suggested_type=OrsoTypes.VARCHAR)
        elif literal_type == 'null':
            return build_literal_node(None)
        
        return None
    
    def _build_field_reference(self, selection):
        """Build a field reference node."""
        from opteryx.managers.expression import NodeType
        from opteryx.models import Node
        
        node = Node(node_type=NodeType.IDENTIFIER)
        
        # Extract field index from selection
        if selection.HasField('direct_reference'):
            direct_ref = selection.direct_reference
            if direct_ref.HasField('struct_field'):
                node.field_index = direct_ref.struct_field.field
        
        return node
    
    def _build_scalar_function_node(self, scalar_func):
        """Build a scalar function node."""
        from opteryx.managers.expression import NodeType
        from opteryx.models import Node
        
        node = Node(node_type=NodeType.FUNCTION)
        
        # Extract function reference (would need function registry mapping)
        node.function_reference = scalar_func.function_reference
        
        # Extract arguments
        args = []
        for arg in scalar_func.arguments:
            if arg.HasField('value'):
                arg_expr = self._build_expression(arg.value)
                if arg_expr:
                    args.append(arg_expr)
        
        if args:
            node.args = args
        
        return node
    
    def _build_aggregate_function(self, agg_func):
        """Build an aggregate function node."""
        from opteryx.managers.expression import NodeType
        from opteryx.models import Node
        
        node = Node(node_type=NodeType.FUNCTION)
        
        # Extract function reference
        node.function_reference = agg_func.function_reference
        
        # Extract arguments
        args = []
        for arg in agg_func.arguments:
            if arg.HasField('value'):
                arg_expr = self._build_expression(arg.value)
                if arg_expr:
                    args.append(arg_expr)
        
        if args:
            node.args = args
        
        return node
