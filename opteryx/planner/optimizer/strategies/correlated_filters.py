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
from orso.types import OrsoTypes

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
    highest_literal = _literal_node_from_statistics(
        left_column.schema_column.highest_value,
        left_column.schema_column.type,
    )
    if highest_literal is not None:
        a_side = right_column
        new_filter = LogicalPlanNode(
            node_type=LogicalPlanStepType.Filter,
            condition=Node(
                NodeType.COMPARISON_OPERATOR, value="LtEq", left=a_side, right=highest_literal
            ),
            columns=[right_column],
            relations={right_column.source},
            all_relations={right_column.source},
        )
        new_filters.append(new_filter)

    lowest_literal = _literal_node_from_statistics(
        left_column.schema_column.lowest_value,
        left_column.schema_column.type,
    )
    if lowest_literal is not None:
        a_side = right_column
        new_filter = LogicalPlanNode(
            node_type=LogicalPlanStepType.Filter,
            condition=Node(
                NodeType.COMPARISON_OPERATOR, value="GtEq", left=a_side, right=lowest_literal
            ),
            columns=[right_column],
            relations={right_column.source},
            all_relations={right_column.source},
        )
        new_filters.append(new_filter)
    return new_filters


def _literal_node_from_statistics(stat_value, column_type):
    if stat_value is None:
        return None
    literal_value = stat_value
    if column_type == OrsoTypes.VARCHAR:
        literal_value = _decode_string_prefix(stat_value)
    elif column_type == OrsoTypes.BLOB:
        literal_value = _decode_string_prefix(stat_value, as_bytes=True)
    return build_literal_node(literal_value, suggested_type=column_type)


def _decode_string_prefix(value, *, as_bytes=False):
    raw_bytes = int(value).to_bytes(8, "big", signed=True)
    payload = raw_bytes[1:]
    if as_bytes:
        return payload
    # Strip the padding zero bytes that were added when encoding the prefix.
    return payload.rstrip(b"\x00").decode("utf-8", errors="ignore")


class CorrelatedFiltersStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Join and node.type in ("inner", "nested loop"):
            # We need exactly two distinct relation sources for this optimization.
            # `node.all_relations` can contain relation names and aliases, so
            # compare the logical sources on the join columns instead.
            left_column = node.on.left
            right_column = node.on.right
            distinct_sources = {left_column.source, right_column.source}
            if len(distinct_sources) != 2:
                return context
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
                new_filters.extend(
                    _write_filters(left_column=right_column, right_column=left_column)
                )
            # If we generated any filter candidates, record that the optimization
            # was considered. We count filter candidates here so tests that assert
            # the optimization was invoked can observe it even if insertion is
            # skipped for safety reasons.
            # Insert each new filter only on the reader(s) for the side it targets.
            # Using `insert_node_before` rewires ALL incoming edges to the join which
            # caused filters to be applied to the wrong legs (and raised KeyErrors).
            for new_filter in new_filters:
                # determine which relation this filter targets from its relations set
                target_relation = next(iter(new_filter.relations)) if new_filter.relations else None

                # choose the readers to attach to based on which side contains the source
                # Use the join's reader UUID lists (left_readers/right_readers) and map
                # them to the optimized plan node ids. This avoids mistakenly matching
                # unrelated scan aliases across different subplans.

                # determine readers (UUIDs) for the side targeted by this filter
                readers = []
                if target_relation in (node.left_relation_names or []):
                    readers = node.left_readers or []
                elif target_relation in (node.right_relation_names or []):
                    readers = node.right_readers or []

                if not readers:
                    # no readers to attach to
                    continue

                # Map reader UUIDs to optimized-plan node ids (nids)
                resolved_nids = []
                nodes_list = list(context.optimized_plan.nodes())
                for nid in nodes_list:
                    plan_node = context.optimized_plan[nid]
                    if plan_node is None:
                        continue
                    node_uuid = getattr(plan_node, "uuid", None)
                    if node_uuid in readers:
                        resolved_nids.append(nid)

                if not resolved_nids:
                    # nothing we can safely attach to
                    continue

                # attach a copy of the filter between each resolved reader and the join
                for reader_nid in resolved_nids:
                    new_nid = random_string()
                    new_node = new_filter.copy() if hasattr(new_filter, "copy") else new_filter
                    if new_node is None:
                        continue
                    context.optimized_plan.insert_node_after(new_nid, new_node, reader_nid)
                    self.statistics.optimization_inner_join_correlated_filter += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
