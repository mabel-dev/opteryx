# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Compaction

Type: Heuristic
Goal: Compact multiple predicates on the same column into simplified ranges

This strategy reduces predicate complexity by consolidating multiple conditions
on the same column into a single simplified range or predicate.

Example:
    col > 5 AND col < 10 AND col > 7 AND col < 9
    => col > 7 AND col < 9 (only the most restrictive bounds)

    col > 10 AND col < 5
    => FALSE (contradictory condition)

This enables better predicate pushdown by simplifying the filter expression.
"""

from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


@dataclass
class Limit:
    """Represents a single bound in a value range."""

    value: Optional[int]  # None indicates unbounded
    inclusive: bool  # Whether inclusive (<=, >=, =) or exclusive (<, >)


@dataclass
class ValueRange:
    """Tracks valid range for a column based on multiple predicates."""

    lower: Optional[Limit] = None  # Lower limit of the range
    upper: Optional[Limit] = None  # Upper limit of the range
    untrackable: bool = False  # True if non-numeric predicates mixed in

    def update_with_predicate(self, operator: str, value) -> bool:
        """
        Update range with a new predicate.

        Args:
            operator: One of "=", ">=", "<=", ">", "<"
            value: The literal value to compare against

        Returns:
            True if range is still valid, False if contradiction detected
        """
        # Only handle numeric comparisons
        if self.untrackable or operator not in ("=", ">=", "<=", ">", "<"):
            self.untrackable = True
            return True

        # Create new limit
        new_limit = Limit(value, inclusive=operator in ("=", ">=", "<="))

        # Update lower bound (for >, >=, =)
        if operator in ("=", ">=", ">"):
            if (
                self.lower is None
                or new_limit.value > self.lower.value
                or (
                    new_limit.value == self.lower.value
                    and self.lower.inclusive
                    and not new_limit.inclusive
                )
            ):
                self.lower = new_limit

        # Update upper bound (for <, <=, =)
        if operator in ("=", "<=", "<"):
            if (
                self.upper is None
                or new_limit.value < self.upper.value
                or (
                    new_limit.value == self.upper.value
                    and self.upper.inclusive
                    and not new_limit.inclusive
                )
            ):
                self.upper = new_limit

        # Check for contradictions
        return self._is_valid()

    def _is_valid(self) -> bool:
        """Check if the range is logically valid (no contradictions)."""
        if self.lower is None or self.upper is None:
            return True
        if self.lower.value > self.upper.value:
            return False
        if self.lower.value == self.upper.value:
            # Both bounds at same value - both must be inclusive
            return self.lower.inclusive and self.upper.inclusive
        return True

    def is_equality(self) -> bool:
        """Check if range represents a single value (equality)."""
        if self.lower is None or self.upper is None:
            return False
        return (
            self.lower.value == self.upper.value and self.lower.inclusive and self.upper.inclusive
        )

    def __bool__(self) -> bool:
        """Returns False if range is contradictory."""
        return self._is_valid()

    def __str__(self) -> str:
        """String representation of the range."""
        if self.untrackable:
            return "Unsupported Conditions"
        if not self:
            return "Invalid Range (Contradiction)"

        if self.is_equality():
            return f"= {self.lower.value}"

        _range = ""
        if self.lower is not None:
            _range += f" >{'=' if self.lower.inclusive else ''} {self.lower.value}"
        if self.upper is not None:
            _range += f" <{'=' if self.upper.inclusive else ''} {self.upper.value}"
        return _range.strip()


@dataclass
class PredicateOccurrence:
    """Record of a predicate instance within the logical plan."""

    filter_nid: str
    predicate: Node
    operator: str
    value: Any


@dataclass
class BoundCandidate:
    """Potential lower or upper bound for a predicate range."""

    value: Any
    inclusive: bool
    occurrence: PredicateOccurrence


@dataclass
class ColumnAnalysisResult:
    """Outcome of analyzing predicates for a single column."""

    status: str
    required: Optional[List[PredicateOccurrence]] = None


class PredicateCompactionStrategy(OptimizationStrategy):  # pragma: no cover
    """
    Compact multiple predicates on the same column into simplified ranges.

    This strategy identifies predicates on the same column and consolidates them
    into a single simplified range by keeping only the most restrictive bounds.

    Example:
        Input:  col > 5 AND col < 10 AND col > 7 AND col < 9
        Output: col > 7 AND col < 9

        Input:  col > 10 AND col < 5
        Output: FALSE (contradiction)
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """Collect filter predicates for later analysis."""
        if node.node_type != LogicalPlanStepType.Filter:
            return context

        state = context.bag.setdefault(
            "predicate_compaction",
            {"filters": {}, "column_occurrences": {}},
        )

        predicates = self._extract_and_predicates(node.condition)
        state["filters"][context.node_id] = {"predicates": predicates}

        for predicate in predicates:
            info = self._extract_comparison_info(predicate)
            if not info:
                continue
            column_id, operator, value = info
            occurrences: List[PredicateOccurrence] = state["column_occurrences"].setdefault(
                column_id, []
            )
            occurrences.append(
                PredicateOccurrence(
                    filter_nid=context.node_id,
                    predicate=predicate,
                    operator=operator,
                    value=value,
                )
            )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """Analyze collected predicates, removing redundant filters and detecting contradictions."""
        optimized_plan = context.optimized_plan
        if len(optimized_plan) == 0:
            optimized_plan = context.pre_optimized_tree.copy()
            context.optimized_plan = optimized_plan

        state = context.bag.get("predicate_compaction")
        if not state:
            return optimized_plan

        column_occurrences: Dict[str, List[PredicateOccurrence]] = state.get(
            "column_occurrences", {}
        )
        filters_state = state.get("filters", {})

        drop_keys: Set[Tuple[str, int]] = set()
        filters_to_false: Set[str] = set()

        for occurrences in column_occurrences.values():
            analysis = self._analyze_column_predicates(occurrences)
            status = analysis.status

            if status == "contradiction":
                filters_to_false.update(occ.filter_nid for occ in occurrences)
                self.statistics.optimization_predicate_compaction += 1
                self.statistics.optimization_predicate_compaction_range_simplified += 1
                continue

            if status != "compacted" or not analysis.required:
                continue

            required_keys = {(occ.filter_nid, id(occ.predicate)) for occ in analysis.required}
            for occ in occurrences:
                key = (occ.filter_nid, id(occ.predicate))
                if key not in required_keys:
                    drop_keys.add(key)

            if len(analysis.required) < len(occurrences):
                removed = len(occurrences) - len(analysis.required)
                self.statistics.optimization_predicate_compaction += removed
                self.statistics.optimization_predicate_compaction_range_simplified += 1

        for filter_nid, filter_info in filters_state.items():
            if filter_nid not in optimized_plan:
                continue

            if filter_nid in filters_to_false:
                filter_node = optimized_plan[filter_nid]
                filter_node.condition = Node(NodeType.LITERAL, value=False)
                filter_node.columns = []
                filter_node.relations = set()
                optimized_plan[filter_nid] = filter_node
                continue

            predicates: List[Node] = filter_info.get("predicates", [])
            new_predicates: List[Node] = []
            for predicate in predicates:
                key = (filter_nid, id(predicate))
                if key in drop_keys:
                    continue
                new_predicates.append(predicate.copy())

            if not new_predicates:
                optimized_plan.remove_node(filter_nid, heal=True)
                continue

            new_condition = self._rebuild_filter(new_predicates)
            if new_condition is None:
                optimized_plan.remove_node(filter_nid, heal=True)
                continue

            filter_node = optimized_plan[filter_nid]
            filter_node.condition = new_condition

            identifiers = get_all_nodes_of_type(new_condition, (NodeType.IDENTIFIER,))
            filter_node.columns = identifiers

            relations: Set[str] = set()
            for identifier in identifiers:
                if identifier.source:
                    relations.add(identifier.source)
                schema_column = getattr(identifier, "schema_column", None)
                if schema_column and getattr(schema_column, "origin", None):
                    relations.update(schema_column.origin)
            filter_node.relations = relations

            optimized_plan[filter_nid] = filter_node

        return optimized_plan

    def _analyze_column_predicates(
        self, occurrences: List[PredicateOccurrence]
    ) -> ColumnAnalysisResult:
        """Determine the minimal set of predicates required for a column."""
        if len(occurrences) <= 1:
            return ColumnAnalysisResult(status="unchanged")

        value_range = ValueRange()
        best_lower: Optional[BoundCandidate] = None
        best_upper: Optional[BoundCandidate] = None
        equality_occurrences: List[PredicateOccurrence] = []

        for occurrence in occurrences:
            mapped = self._map_operator(occurrence.operator)
            if mapped is None:
                return ColumnAnalysisResult(status="unsupported")

            if mapped == "=":
                equality_occurrences.append(occurrence)

            if mapped in ("=", ">", ">="):
                candidate = BoundCandidate(
                    value=occurrence.value,
                    inclusive=mapped in ("=", ">="),
                    occurrence=occurrence,
                )
                if self._is_better_lower(candidate, best_lower):
                    best_lower = candidate

            if mapped in ("=", "<", "<="):
                candidate = BoundCandidate(
                    value=occurrence.value,
                    inclusive=mapped in ("=", "<="),
                    occurrence=occurrence,
                )
                if self._is_better_upper(candidate, best_upper):
                    best_upper = candidate

            if not value_range.update_with_predicate(mapped, occurrence.value):
                return ColumnAnalysisResult(status="contradiction")

            if value_range.untrackable:
                return ColumnAnalysisResult(status="unsupported")

        if not value_range:
            return ColumnAnalysisResult(status="contradiction")

        if equality_occurrences:
            equality_value = equality_occurrences[0].value
            matching = [occ for occ in equality_occurrences if occ.value == equality_value]
            if len(matching) == 0:
                return ColumnAnalysisResult(status="contradiction")
            required = [matching[0]]
            status = "compacted" if len(occurrences) > len(required) else "unchanged"
            return ColumnAnalysisResult(status=status, required=required)

        required: List[PredicateOccurrence] = []
        if best_lower:
            required.append(best_lower.occurrence)
        if best_upper and (not required or best_upper.occurrence not in required):
            required.append(best_upper.occurrence)

        if not required or len(required) == len(occurrences):
            return ColumnAnalysisResult(status="unchanged", required=required or None)

        return ColumnAnalysisResult(status="compacted", required=required)

    @staticmethod
    def _is_better_lower(candidate: BoundCandidate, current: Optional[BoundCandidate]) -> bool:
        if current is None:
            return True
        if candidate.value > current.value:
            return True
        if candidate.value < current.value:
            return False
        if candidate.inclusive == current.inclusive:
            return False
        return (not candidate.inclusive) and current.inclusive

    @staticmethod
    def _is_better_upper(candidate: BoundCandidate, current: Optional[BoundCandidate]) -> bool:
        if current is None:
            return True
        if candidate.value < current.value:
            return True
        if candidate.value > current.value:
            return False
        if candidate.inclusive == current.inclusive:
            return False
        return (not candidate.inclusive) and current.inclusive

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Only run if there are FILTER clauses in the plan."""
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        return len(candidates) > 0

    def _extract_and_predicates(self, node: LogicalPlanNode) -> list:
        """
        Extract all AND-ed predicates from an expression.

        e.g., (A AND B AND C) => [A, B, C]
        """
        if node is None:
            return []

        if node.node_type != NodeType.AND:
            return [node]

        left = self._extract_and_predicates(node.left)
        right = self._extract_and_predicates(node.right)
        return left + right

    def _extract_comparison_info(self, node: LogicalPlanNode) -> Optional[Tuple[str, str, any]]:
        """
        Extract column ID, operator, and value from a comparison node.

        Returns:
            (column_id, operator, value) tuple or None if not a simple comparison
        """
        if node.node_type != NodeType.COMPARISON_OPERATOR:
            return None

        # Must be simple: column OP literal
        if node.left.node_type != NodeType.IDENTIFIER:
            return None

        if node.right.node_type != NodeType.LITERAL:
            return None

        col_id = node.left.schema_column.identity
        operator = node.value
        value = node.right.value

        return (col_id, operator, value)

    def _map_operator(self, sql_operator: str) -> Optional[str]:
        """Map SQL operator to range operator."""
        mapping = {
            "Eq": "=",
            "NotEq": None,  # Can't compact inequality
            "Gt": ">",
            "GtEq": ">=",
            "Lt": "<",
            "LtEq": "<=",
        }
        return mapping.get(sql_operator)

    def _rebuild_filter(self, predicates: list) -> LogicalPlanNode:
        """
        Rebuild filter expression from a list of predicates.

        Args:
            predicates: List of predicate nodes

        Returns:
            AND chain of predicates
        """
        if not predicates:
            return None
        if len(predicates) == 1:
            return predicates[0]

        result = predicates[0]
        for pred in predicates[1:]:
            result = Node(NodeType.AND, left=result, right=pred)
        return result
