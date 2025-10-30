# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Limit Pushdown

Type: Heuristic
Goal: Reduce Rows

We try to push the limit to the other side of PROJECTS
"""

from typing import Optional
from typing import Set

from opteryx.connectors.capabilities import LimitPushable
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class LimitPushdownStrategy(OptimizationStrategy):
    """Push LIMIT operators towards scans when it is safe to do so."""

    _BARRIER_TYPES = {
        LogicalPlanStepType.Aggregate,
        LogicalPlanStepType.AggregateAndGroup,
        LogicalPlanStepType.Distinct,
        LogicalPlanStepType.Filter,
        LogicalPlanStepType.FunctionDataset,
        LogicalPlanStepType.HeapSort,
        LogicalPlanStepType.Limit,
        LogicalPlanStepType.MetadataWriter,
        LogicalPlanStepType.Order,
        LogicalPlanStepType.Set,
        LogicalPlanStepType.Union,
    }

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        if node.node_type == LogicalPlanStepType.Limit:
            if node.offset is not None or node.limit in (None, 0):
                return context
            node.nid = context.node_id
            if not hasattr(node, "pushdown_targets"):
                node.pushdown_targets = set(node.all_relations or [])
            context.collected_limits.append(node)
            return context

        remaining_limits = []
        for limit_node in context.collected_limits:
            if self._should_skip_branch(limit_node, node):
                remaining_limits.append(limit_node)
                continue

            if node.node_type == LogicalPlanStepType.Scan:
                outcome = self._apply_to_scan(limit_node, node, context)
                if outcome is True:
                    continue
                if outcome is None:
                    remaining_limits.append(limit_node)
                    continue
                self._place_before_node(limit_node, node, context)
                continue

            if node.node_type == LogicalPlanStepType.Join:
                if self._refine_targets_for_join(limit_node, node):
                    remaining_limits.append(limit_node)
                    continue
                self._place_before_node(limit_node, node, context)
                continue

            if node.node_type in self._BARRIER_TYPES:
                self._place_before_node(limit_node, node, context)
                continue

            remaining_limits.append(limit_node)

        context.collected_limits = remaining_limits
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        context.collected_limits.clear()
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Limit,))
        return len(candidates) > 0

    @staticmethod
    def _collect_relations(node: LogicalPlanNode) -> Set[str]:
        relations = getattr(node, "all_relations", None)
        if relations:
            return set(relations)
        return set()

    def _should_skip_branch(self, limit_node: LogicalPlanNode, node: LogicalPlanNode) -> bool:
        targets: Set[str] = getattr(limit_node, "pushdown_targets", set())
        if not targets:
            return False
        node_relations = self._collect_relations(node)
        return bool(node_relations) and targets.isdisjoint(node_relations)

    def _apply_to_scan(
        self,
        limit_node: LogicalPlanNode,
        scan_node: LogicalPlanNode,
        context: OptimizerContext,
    ) -> Optional[bool]:
        targets: Set[str] = getattr(
            limit_node, "pushdown_targets", set(limit_node.all_relations or [])
        )
        relation_names = {scan_node.relation, getattr(scan_node, "alias", None)}
        if targets and targets.isdisjoint({name for name in relation_names if name}):
            return None

        connector = getattr(scan_node, "connector", None)
        if connector and LimitPushable in connector.__class__.mro():
            current_limit = getattr(scan_node, "limit", None)
            scan_node.limit = (
                limit_node.limit if current_limit is None else min(current_limit, limit_node.limit)
            )
            if limit_node.nid in context.optimized_plan:
                context.optimized_plan.remove_node(limit_node.nid, heal=True)
            context.optimized_plan[context.node_id] = scan_node
            self.statistics.optimization_limit_pushdown += 1
            return True

        return False

    def _refine_targets_for_join(
        self, limit_node: LogicalPlanNode, join_node: LogicalPlanNode
    ) -> bool:
        join_type = getattr(join_node, "type", None)
        if not join_type:
            return False

        targets: Set[str] = getattr(
            limit_node, "pushdown_targets", set(limit_node.all_relations or [])
        )
        if not targets:
            targets = set(limit_node.all_relations or [])

        left_relations = set(getattr(join_node, "left_relation_names", []) or [])
        right_relations = set(getattr(join_node, "right_relation_names", []) or [])

        new_targets: Optional[Set[str]] = None

        if join_type == "left outer":
            new_targets = targets & left_relations
        elif join_type == "right outer":
            new_targets = targets & right_relations
        elif join_type == "cross join":
            left_size = getattr(join_node, "left_size", float("inf"))
            right_size = getattr(join_node, "right_size", float("inf"))
            left_choice = targets & left_relations
            right_choice = targets & right_relations
            if left_choice and right_choice:
                new_targets = left_choice if left_size <= right_size else right_choice
            elif left_choice:
                new_targets = left_choice
            elif right_choice:
                new_targets = right_choice
        else:
            return False

        if not new_targets:
            return False

        limit_node.pushdown_targets = new_targets
        limit_node.all_relations = set(new_targets)
        return True

    def _place_before_node(
        self, limit_node: LogicalPlanNode, _: LogicalPlanNode, context: OptimizerContext
    ) -> None:
        if limit_node.nid in context.optimized_plan:
            context.optimized_plan.remove_node(limit_node.nid, heal=True)
        context.optimized_plan.insert_node_after(limit_node.nid, limit_node, context.node_id)
        limit_node.columns = []
        limit_node.pushdown_targets = set(limit_node.all_relations or [])
        self.statistics.optimization_limit_pushdown += 1
