# isort: skip

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Physical Execution Operators

This module contains all physical execution operators used by Opteryx's query engine.
Operators form a tree structure where each node processes data and passes results to
its parent node, implementing the iterator pattern for memory-efficient processing.

Operator Categories:

Data Sources:
- ReaderNode: Reads data from connectors (files, databases, etc.)
- AsyncReaderNode: Async version for improved I/O performance
- NullReaderNode: Returns empty table with correct schema (for contradictory predicates)
- FunctionDatasetNode: Generates data from function calls

Joins:
- InnerJoinNode: Standard inner joins
- OuterJoinNode: Left, right, and full outer joins
- CrossJoinNode: Cartesian product joins
- NestedLoopJoinNode: Nested loop algorithm for joins
- NonEquiJoinNode: Non-equi joins (!=, >, >=, <, <=) using draken
- UnnestJoinNode: Specialized join for array unnesting

Filtering and Selection:
- FilterNode: Applies WHERE clause predicates
- FilterJoinNode: Optimized filter for join conditions
- ProjectionNode: Column selection and expression evaluation
- DistinctNode: Removes duplicate rows

Aggregation:
- AggregateNode: Standard aggregation (SUM, COUNT, etc.)
- AggregateAndGroupNode: GROUP BY with aggregation
- SimpleAggregateNode: Single-group aggregation optimization
- SimpleAggregateAndGroupNode: Optimized single-group with GROUP BY

Sorting and Limiting:
- SortNode: ORDER BY implementation using TimSort
- HeapSortNode: Heap-based sorting for TOP-N queries
- LimitNode: LIMIT/OFFSET clause implementation

Set Operations:
- UnionNode: UNION and UNION ALL operations

Control and Meta:
- ExitNode: Query execution termination
- ExplainNode: Query plan explanation
- ShowColumnsNode: SHOW COLUMNS implementation
- ShowCreateNode: SHOW CREATE VIEW implementation
- ShowValueNode: SHOW variable value display
- SetVariableNode: SET variable operations

Base Classes:
- BasePlanNode: Base class for all operators
- JoinNode: Base class for join operators

Operator Development:
1. Inherit from BasePlanNode or appropriate base class
2. Implement execute() method that processes data morsels
3. Handle memory management and resource cleanup
4. Support statistics collection for optimization
5. Add comprehensive tests

Example:
    class MyOperatorNode(BasePlanNode):
        def execute(self, morsel):
            # Process the data morsel
            processed_data = self.process_data(morsel)
            yield processed_data

Performance Considerations:
- Use vectorized operations with PyArrow when possible
- Implement memory pooling for large data processing
- Consider async operations for I/O bound operators
- Profile memory usage and optimize accordingly
"""

from .base_plan_node import BasePlanNode, JoinNode  # isort: skip

from .aggregate_and_group_node import AggregateAndGroupNode  # Group is always followed by aggregate
from .aggregate_node import AGGREGATORS
from .aggregate_node import AggregateNode  # aggregate data
from .async_read_node import AsyncReaderNode
from .null_reader_node import NullReaderNode  # empty table for contradictory predicates
from .simple_aggregate_node import SimpleAggregateNode  # aggregate data
from .simple_aggregate_and_group_node import SimpleAggregateAndGroupNode  # aggregate data

# from .build_statistics_node import BuildStatisticsNode  # Analyze Tables
from .cross_join_node import CrossJoinNode  # CROSS JOIN
from .unnest_join_node import UnnestJoinNode  # CROSS JOIN UNNEST
from .distinct_node import DistinctNode  # remove duplicate records
from .exit_node import ExitNode
from .explain_node import ExplainNode  # EXPLAIN queries
from .filter_join_node import FilterJoinNode  # filter unwanted rows
from .filter_node import FilterNode  # filter unwanted rows
from .function_dataset_node import FunctionDatasetNode  # Dataset Constructors
from .heap_sort_node import HeapSortNode  # Heap

# from .information_schema_node import InformationSchemaNode  # information_schema
from .inner_join_node import InnerJoinNode
from .nested_loop_join_node import NestedLoopJoinNode
from .non_equi_join_node import NonEquiJoinNode

from .limit_node import LimitNode  # select the first N records

from .outer_join_node import OuterJoinNode
from .projection_node import ProjectionNode  # remove unwanted columns including renames
from .read_node import ReaderNode
from .set_variable_node import SetVariableNode
from .show_columns_node import ShowColumnsNode  # column details
from .show_create_node import ShowCreateNode  # SHOW CREATE VIEW

# from .show_databases_node import ShowDatabasesNode  # SHOW DATABASES
# from .show_functions_node import ShowFunctionsNode  # supported functions
from .show_value_node import ShowValueNode  # display node for SHOW
from .sort_node import SortNode  # order by selected columns
from .union_node import UnionNode


__all__ = [
    "BasePlanNode",
    "JoinNode",
    "AggregateAndGroupNode",
    "AGGREGATORS",
    "AggregateNode",
    "AsyncReaderNode",
    "NullReaderNode",
    "SimpleAggregateNode",
    "SimpleAggregateAndGroupNode",
    "CrossJoinNode",
    "UnnestJoinNode",
    "DistinctNode",
    "ExitNode",
    "ExplainNode",
    "FilterJoinNode",
    "FilterNode",
    "FunctionDatasetNode",
    "HeapSortNode",
    "InnerJoinNode",
    "NestedLoopJoinNode",
    "NonEquiJoinNode",
    "LimitNode",
    "OuterJoinNode",
    "ProjectionNode",
    "ReaderNode",
    "SetVariableNode",
    "ShowColumnsNode",
    "ShowCreateNode",
    "ShowValueNode",
    "SortNode",
    "UnionNode",
    "is_aggregator",
    "aggregators",
]


def is_aggregator(name):
    return name in AGGREGATORS


def aggregators():
    return list(AGGREGATORS.keys())
