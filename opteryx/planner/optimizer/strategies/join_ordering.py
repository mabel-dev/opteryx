# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Ordering

Type: Cost-Based
Goal: Faster Joins

Build a left-deep join tree, where the left relation of any pair is the smaller relation.

We also decide if we should use a nested loop join or a hash join based on the size of the left relation.
"""

from opteryx.config import features
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

DISABLE_NESTED_LOOP_JOIN: bool = features.disable_nested_loop_join
FORCE_NESTED_LOOP_JOIN: bool = features.force_nested_loop_join


class JoinOrderingStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Join and node.type == "cross join":
            # 1438
            pass

        if node.node_type == LogicalPlanStepType.Join and node.type == "inner":
            # our very basic hueristic is to always put the smaller table on the left
            if node.right_size < node.left_size:
                # fmt:off
                node.left_size, node.right_size = node.right_size, node.left_size
                node.left_columns, node.right_columns = node.right_columns, node.left_columns
                node.left_readers, node.right_readers = node.right_readers, node.left_readers
                node.left_relation_names, node.right_relation_names = node.right_relation_names, node.left_relation_names
                # fmt:on
                self.statistics.optimization_inner_join_smallest_table_left += 1
                context.optimized_plan[context.node_id] = node

            # Small datasets benefit from nested loop joins (avoids building a hash table)
            if (
                not DISABLE_NESTED_LOOP_JOIN
                and min(node.left_size, node.right_size) < 1_000
                and max(node.left_size, node.right_size) < 100_000
            ) or FORCE_NESTED_LOOP_JOIN:
                node.type = "nested_inner"
                context.optimized_plan[context.node_id] = node

        # Optimize LEFT OUTER JOIN to ensure the preserved table (left) is used for hash build
        # This is crucial for performance as we build hash table and bloom filter on left side
        if node.node_type == LogicalPlanStepType.Join and node.type == "left outer":
            # For LEFT OUTER JOIN, we want the smaller table on the left (build side)
            # since we build hash table and bloom filter for the left relation
            # This optimization is especially beneficial when the left table is small
            if node.right_size < node.left_size:
                # Note: We cannot swap tables in LEFT OUTER JOIN like we do with INNER
                # because it would change semantics (LEFT -> RIGHT JOIN)
                # However, we can recommend query rewrite in statistics
                self.statistics.optimization_left_outer_join_consider_rewrite = (
                    self.statistics.optimization_left_outer_join_consider_rewrite + 1
                    if hasattr(self.statistics, "optimization_left_outer_join_consider_rewrite")
                    else 1
                )
            
            # Mark if left table is significantly smaller for optimal performance
            if node.left_size < node.right_size * 0.5:
                # Good case: left table is smaller, optimal for our implementation
                pass

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
