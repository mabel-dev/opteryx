# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Correlated Filters

Type: Heuristic
Goal: Reduce Rows

When fields are joined on, we can infer ranges of values based on statistics
or filters. This can be used to reduce the number of rows that need to be read
and processed.
"""

from orso.tools import random_string

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


def _write_filters(left_column, right_column):
    new_filters = []
    if left_column.schema_column.highest_value is not None:
        a_side = right_column
        b_side = build_literal_node(left_column.schema_column.highest_value)
        new_filter = Node(
            LogicalPlanStepType.Filter,
            condition=Node(NodeType.COMPARISON_OPERATOR, value="LtEq", left=a_side, right=b_side),
            columns=[right_column],
            relations={right_column.source},
            all_relations={right_column.source},
        )
        new_filters.append(new_filter)

        a_side = right_column
        b_side = build_literal_node(left_column.schema_column.lowest_value)
        new_filter = Node(
            LogicalPlanStepType.Filter,
            condition=Node(NodeType.COMPARISON_OPERATOR, value="GtEq", left=a_side, right=b_side),
            columns=[right_column],
            relations={right_column.source},
            all_relations={right_column.source},
        )
        new_filters.append(new_filter)
    return new_filters


class CorrelatedFiltersStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if (
            node.node_type == LogicalPlanStepType.Join
            and node.type == "inner"
            and len(node.all_relations) == 2
        ):
            left_column = node.on.left
            right_column = node.on.right
            new_filters = []

            # Empty connectors are FUNCTION datasets, we could push filters down and create
            # statistics for them, but there are other issues this creates
            if (
                left_column.node_type == NodeType.IDENTIFIER
                and right_column.node_type == NodeType.IDENTIFIER
                and left_column.source_connector != set()
            ):
                new_filters = _write_filters(left_column, right_column)
            if (
                left_column.node_type == NodeType.IDENTIFIER
                and right_column.node_type == NodeType.IDENTIFIER
                and right_column.source_connector != set()
            ):
                new_filters.extend(_write_filters(right_column, left_column))
            for new_filter in new_filters:
                context.optimized_plan.insert_node_before(
                    random_string(), new_filter, context.node_id
                )
                self.statistics.optimization_inner_join_correlated_filter += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
