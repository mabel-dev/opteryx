"""
Join and GroupBy Pushdown Strategy

This strategy walks the plan looking for scan/reader nodes that correspond to
SQL-backed connectors. When found, it attempts to walk up through parent nodes
collecting joins and aggregates that are safe to push. If a contiguous sub-plan
of pushable operators (joins, aggregates, filters, projections, limits) exists
that terminate at a single SQL-backed scan, the strategy annotates that scan
with `pushed_sql` and `pushed_params` for the connector to execute directly.

This is intentionally conservative: it stops when encountering
- multiple sources (different connectors)
- non-deterministic functions
- operators that cannot be expressed in SQL easily

Currently this is a skeleton that emits simple SELECT ... FROM ... WHERE
fragments for single-table pushes and marks the scan node with pushed_sql.
"""

from typing import List

from opteryx.managers.expression import NodeType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class JoinGroupByPushdownStrategy(OptimizationStrategy):
    def __init__(self, statistics):
        super().__init__(statistics)

    def should_i_run(self, plan) -> bool:
        # Only run if SQL pushable connectors exist in the plan
        # Conservative: always run; strategy will bail early if nothing to do
        return True

    def visit(self, node, context: OptimizerContext) -> OptimizerContext:
        # Ensure we have a working optimized_plan copy to avoid replacing
        # the plan with an empty one accidentally.
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        # Look for scan nodes that are SQL-backed and attempt push
        try:
            _ = node.node_type
        except Exception:
            return context

        # Identify SQL-backed scanners by attribute `is_scan` and connector type
        if getattr(node, "is_scan", False) and hasattr(node, "connector"):
            connector = getattr(node, "connector")
            # we only attempt push for connectors that expose can_push
            if hasattr(connector, "can_push"):
                # perform a conservative single-table push: create a simple
                # SELECT * FROM <dataset> WHERE <predicates>
                pushed_sql = None
                pushed_params = {}

                # If the connector already has pushed predicates/limit/projection
                # set by earlier strategies we prefer them
                # Build a minimal SQL if no pushed_sql exists
                dataset = getattr(connector, "dataset", None)
                if dataset:
                    pushed_sql = f"SELECT * FROM {dataset}"
                    # If the scan node has a 'predicates' parameter (set by
                    # predicate pushdown) we attempt to append them as a WHERE.
                    predicates = node.parameters.get("predicates") or []
                    if predicates:
                        # For now only support simple comparison predicates
                        wheres: List[str] = []
                        param_idx = 0
                        for pred in predicates:
                            if pred.node_type == NodeType.COMPARISON_OPERATOR:
                                left = pred.left.source_column
                                op = getattr(pred, "value", None)
                                right = pred.right.value
                                # very small translation map
                                wop = "=" if op == "Eq" else "="
                                pname = f"p{param_idx}"
                                pushed_params[pname] = right
                                wheres.append(f"{left} {wop} :{pname}")
                                param_idx += 1
                        if wheres:
                            pushed_sql += " WHERE " + " AND ".join(wheres)

                if pushed_sql:
                    # annotate the connector so read_dataset will use it
                    setattr(connector, "pushed_sql", pushed_sql)
                    setattr(connector, "pushed_params", pushed_params)
                    # record a statistic if available
                    if hasattr(self.statistics, "optimization_join_groupby_pushdown"):
                        self.statistics.optimization_join_groupby_pushdown = (
                            getattr(self.statistics, "optimization_join_groupby_pushdown", 0) + 1
                        )

        return context

    def complete(self, plan, context: OptimizerContext):
        # If for any reason the optimized plan is empty, fall back to the
        # original pre-optimized tree to avoid breaking later strategies.
        if not context.optimized_plan or len(getattr(context.optimized_plan, "_nodes", {})) == 0:
            return context.pre_optimized_tree
        return context.optimized_plan
