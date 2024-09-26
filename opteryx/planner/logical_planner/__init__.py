"""
                    ┌───────────┐
                    │   USER    │
       ┌────────────┤           ◄────────────┐
       │SQL         └───────────┘            │
───────┼─────────────────────────────────────┼──────
       │                                     │
 ┌─────▼─────┐                               │
 │ SQL       │                               │
 │ Rewriter  │                               │
 └─────┬─────┘                               │
       │SQL                                  │Results
 ┌─────▼─────┐                         ┌─────┴─────┐
 │           │                         │           │
 │ Parser    │                         │ Executor  │
 └─────┬─────┘                         └─────▲─────┘
       │AST                                  │Plan
 ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
 │ AST       │      │           │      │ Physical  │
 │ Rewriter  │      │ Catalogue │      │ Planner   │
 └─────┬─────┘      └───────────┘      └─────▲─────┘
       │AST               │Schemas           │Plan
 ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
 │ Logical   │ Plan │           │ Plan │           │
 │   Planner ├──────► Binder    ├──────► Optimizer │
 └───────────┘      └───────────┘      └───────────┘

"""

from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner.logical_planner import apply_visibility_filters
from opteryx.planner.logical_planner.logical_planner import do_logical_planning_phase
from opteryx.planner.logical_planner.logical_planner_builders import build

__all__ = (
    "apply_visibility_filters",
    "LogicalPlan",
    "LogicalPlanNode",
    "LogicalPlanStepType",
    "do_logical_planning_phase",
    "build",
)
