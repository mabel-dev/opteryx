# Optimization Strategy Architecture

## Current Optimizer Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Logical Plan Input                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    OptimizerVisitor (Orchestrator)                  │
│                                                                     │
│  Applies 14 strategies sequentially in visitor pattern:            │
│  - Traverses plan tree top-down (projection → scanners)            │
│  - Each strategy can modify nodes or collect state                 │
│  - Context maintains optimization state across strategies          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     PHASE 1: Expression Simplification              │
├─────────────────────────────────────────────────────────────────────┤
│ 1. ConstantFoldingStrategy                                          │
│    └─ Evaluate constant expressions once                            │
│    └─ Simplify arithmetic (x * 0 → 0, x * 1 → x)                   │
│                                                                     │
│ 2. BooleanSimplificationStrategy                                    │
│    └─ Apply De Morgan's laws                                        │
│    └─ Invert comparisons (NOT x = y → x != y)                      │
│                                                                     │
│ 3. SplitConjunctivePredicatesStrategy                               │
│    └─ Break AND conditions into separate filters                    │
│    └─ Enables individual predicate pushdown                         │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   PHASE 2: Predicate Optimization                   │
├─────────────────────────────────────────────────────────────────────┤
│ 4. CorrelatedFiltersStrategy                                        │
│    └─ Handle correlated subqueries                                  │
│                                                                     │
│ 5. PredicateRewriteStrategy                                         │
│    └─ Normalize predicates (STARTS_WITH → LIKE)                    │
│    └─ Rewrite ANY/ALL conditions                                    │
│                                                                     │
│ 6. PredicatePushdownStrategy ⭐⭐⭐                                    │
│    └─ Push filters to scans (reduce rows early)                    │
│    └─ Push filters into joins (reduce join size)                   │
│    └─ Push to connectors when supported                            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  PHASE 3: Projection Optimization                   │
├─────────────────────────────────────────────────────────────────────┤
│ 7. ProjectionPushdownStrategy ⭐⭐                                    │
│    └─ Push column selection to scans                               │
│    └─ Eliminate unused columns early                               │
│    └─ Push to connectors when supported                            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     PHASE 4: Join Optimization                      │
├─────────────────────────────────────────────────────────────────────┤
│ 8. JoinRewriteStrategy                                              │
│    └─ Convert CROSS JOIN to INNER JOIN when possible               │
│                                                                     │
│ 9. JoinOrderingStrategy ⭐                                           │
│    └─ Put smaller table on left (for hash join)                    │
│    └─ Choose nested loop vs hash join                              │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 PHASE 5: Specialized Optimizations                  │
├─────────────────────────────────────────────────────────────────────┤
│ 10. DistinctPushdownStrategy                                        │
│     └─ Push DISTINCT into CROSS JOIN UNNEST                        │
│                                                                     │
│ 11. OperatorFusionStrategy                                          │
│     └─ Fuse ORDER BY + LIMIT → HeapSort                            │
│                                                                     │
│ 12. LimitPushdownStrategy ⭐                                         │
│     └─ Push LIMIT past projections                                 │
│     └─ Push to connectors when supported                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PHASE 6: Final Cleanup                         │
├─────────────────────────────────────────────────────────────────────┤
│ 13. PredicateOrderingStrategy                                       │
│     └─ Combine adjacent filters into single DNF                    │
│     └─ Order predicates by execution cost                          │
│     └─ Rewrite AND'd ANY conditions to ArrayContainsAll            │
│                                                                     │
│ 14. RedundantOperationsStrategy                                     │
│     └─ Remove redundant projections                                │
│     └─ Remove subquery wrapper nodes                               │
│                                                                     │
│ 15. ConstantFoldingStrategy (2nd pass)                              │
│     └─ Fold any new constants from optimization rewrites           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Optimized Logical Plan                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Proposed Future Architecture

### New Strategies to Add

```
┌─────────────────────────────────────────────────────────────────────┐
│                   PROPOSED: Early Analysis Phase                    │
│                        (NEW - Before Phase 1)                       │
├─────────────────────────────────────────────────────────────────────┤
│ NEW: CardinalityEstimationStrategy ⭐⭐⭐                             │
│      └─ Collect/estimate row counts for all operators              │
│      └─ Build selectivity estimates for predicates                 │
│      └─ Provide data for cost-based decisions                      │
│      └─ Enable partition pruning decisions                         │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│               PROPOSED: Enhanced Predicate Phase                    │
│                        (Insert after Phase 2)                       │
├─────────────────────────────────────────────────────────────────────┤
│ NEW: PredicateCompactionStrategy ⭐⭐⭐                               │
│      └─ Compact overlapping ranges (x > 5 AND x > 10 → x > 10)    │
│      └─ Detect impossible conditions (x > 10 AND x < 5 → FALSE)   │
│      └─ Consolidate IN-lists to ranges when beneficial             │
│                                                                     │
│ NEW: PartitionPruningStrategy ⭐⭐⭐                                  │
│      └─ Use predicates to eliminate partitions                     │
│      └─ Skip entire files/directories when possible                │
│      └─ 50-90% reduction in data scanned                           │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 PROPOSED: Aggregate Optimization                    │
│                        (Insert after Phase 3)                       │
├─────────────────────────────────────────────────────────────────────┤
│ NEW: AggregatePushdownStrategy ⭐⭐                                   │
│      └─ Push aggregations to SQL databases                         │
│      └─ Push GROUP BY when connector supports it                   │
│      └─ 10-50x speedup for remote aggregations                     │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│               PROPOSED: Advanced Join Optimization                  │
│                      (Enhance Phase 4)                              │
├─────────────────────────────────────────────────────────────────────┤
│ ENHANCED: JoinOrderingStrategy                                      │
│           └─ Use cardinality estimates (not just size)             │
│           └─ Consider join selectivity                             │
│           └─ Multi-way join enumeration (3-6 tables)               │
│                                                                     │
│ NEW: SubqueryDecorrelationStrategy ⭐                                │
│      └─ Convert correlated to non-correlated subqueries            │
│      └─ Transform to joins when possible                           │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  PROPOSED: CTE & Expression Phase                   │
│                        (Insert after Phase 5)                       │
├─────────────────────────────────────────────────────────────────────┤
│ NEW: CTEOptimizationStrategy ⭐⭐                                     │
│      └─ Inline simple CTEs (single reference)                      │
│      └─ Materialize complex CTEs (multiple references)             │
│      └─ Push predicates into CTE definitions                       │
│                                                                     │
│ NEW: CommonSubexpressionEliminationStrategy ⭐                       │
│      └─ Detect duplicate expressions                               │
│      └─ Compute once and reuse                                     │
│                                                                     │
│ NEW: SortPushdownStrategy ⭐                                         │
│      └─ Push ORDER BY to connectors                                │
│      └─ Leverage pre-sorted data                                   │
│      └─ Avoid full sort for ORDER BY + LIMIT                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Optimization Decision Flow

```
Query Plan
    │
    ▼
┌─────────────────────────┐
│ Collect Statistics      │ ◄──── NEW: Cardinality Estimation
│ - Row counts            │
│ - Column stats          │
│ - Partition info        │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Simplify Expressions    │ ◄──── Constant Folding, Boolean Simplification
│ - Fold constants        │
│ - Remove redundancy     │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Optimize Predicates     │ ◄──── Predicate Pushdown, Compaction, Pruning
│ - Push to scans         │
│ - Compact ranges        │
│ - Prune partitions      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Optimize Projections    │ ◄──── Projection Pushdown, Column Pruning
│ - Push to scans         │
│ - Eliminate unused cols │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Optimize Joins          │ ◄──── Join Ordering, Rewriting
│ - Reorder tables        │       NEW: Cardinality-based ordering
│ - Choose algorithm      │
│ - Convert join types    │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Optimize Aggregates     │ ◄──── NEW: Aggregate Pushdown
│ - Push to connectors    │
│ - Optimize GROUP BY     │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Optimize CTEs           │ ◄──── NEW: CTE Optimization
│ - Inline or materialize │
│ - Push predicates       │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Optimize Operators      │ ◄──── Operator Fusion, Limit Pushdown
│ - Fuse operations       │       NEW: Sort Pushdown
│ - Push limits           │
│ - Push sorts            │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Final Cleanup           │ ◄──── Predicate Ordering, Redundant Removal
│ - Remove redundancy     │       NEW: Common Subexpression Elimination
│ - Order predicates      │
│ - Eliminate duplicates  │
└───────────┬─────────────┘
            │
            ▼
    Optimized Plan
```

## Cost Model Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Cost Estimation                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Operator Costs:                                                    │
│  ┌─────────────┬──────────────────────────────────────────┐        │
│  │ Scan        │ rows * cost_per_row + seek_cost         │        │
│  │ Filter      │ rows * predicate_evaluation_cost        │        │
│  │ Project     │ rows * column_count * cost_per_column   │        │
│  │ Hash Join   │ left_rows + right_rows + hash_build     │        │
│  │ Nested Loop │ left_rows * right_rows * cost_per_row   │        │
│  │ Sort        │ rows * log(rows) * cost_per_comparison  │        │
│  │ Aggregate   │ rows * aggregation_cost                 │        │
│  └─────────────┴──────────────────────────────────────────┘        │
│                                                                     │
│  Selectivity Estimation:                                            │
│  ┌─────────────┬──────────────────────────────────────────┐        │
│  │ col = val   │ 1 / distinct_count                       │        │
│  │ col > val   │ histogram or 0.33 default                │        │
│  │ col IN (...)│ list_size / distinct_count               │        │
│  │ col LIKE 'x%'│ 0.01 (prefix match)                     │        │
│  │ a AND b     │ selectivity(a) * selectivity(b)          │        │
│  │ a OR b      │ min(1.0, sel(a) + sel(b))                │        │
│  └─────────────┴──────────────────────────────────────────┘        │
│                                                                     │
│  Cardinality Estimation:                                            │
│  ┌─────────────┬──────────────────────────────────────────┐        │
│  │ Filter      │ input_rows * selectivity                │        │
│  │ Join        │ left * right / max(distinct_left, right) │        │
│  │ Aggregate   │ input_rows / group_count                 │        │
│  │ DISTINCT    │ distinct_count_estimate                  │        │
│  └─────────────┴──────────────────────────────────────────┘        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Metrics & Statistics

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Optimization Statistics                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Existing Metrics:                                                  │
│  • optimization_predicate_pushdown: 127                            │
│  • optimization_projection_pushdown: 45                            │
│  • optimization_constant_fold_expression: 23                       │
│  • optimization_inner_join_smallest_table_left: 8                  │
│  • optimization_limit_pushdown: 12                                 │
│  • optimization_fuse_operators_heap_sort: 3                        │
│  • optimization_remove_redundant_operators_project: 15             │
│                                                                     │
│  Proposed New Metrics:                                              │
│  • optimization_cardinality_estimation_used                        │
│  • optimization_partitions_pruned                                  │
│  • optimization_partition_pruning_bytes_saved                      │
│  • optimization_predicate_compaction                               │
│  • optimization_cte_inlined                                        │
│  • optimization_cte_materialized                                   │
│  • optimization_aggregate_pushdown                                 │
│  • optimization_common_subexpression_eliminated                    │
│  • optimization_sort_pushdown                                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Strategy Classification

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Optimization Strategy Types                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  🔵 Heuristic (Rule-Based)                                          │
│     - Apply known good transformations                              │
│     - No cost model required                                        │
│     - Examples: Predicate pushdown, Constant folding               │
│                                                                     │
│  🟢 Cost-Based (Data-Driven)                                        │
│     - Use statistics to make decisions                              │
│     - Compare alternative plans                                     │
│     - Examples: Join ordering, Predicate ordering                  │
│                                                                     │
│  🟡 Hybrid                                                           │
│     - Use heuristics with cost-based refinement                     │
│     - Fall back to heuristics when stats unavailable               │
│     - Examples: CTE optimization, Aggregate pushdown               │
│                                                                     │
│  🔴 Adaptive (Runtime)                                               │
│     - Adjust plan during execution                                  │
│     - Monitor actual vs estimated cardinality                       │
│     - Example: Adaptive join algorithm selection (future)          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

**Visual Architecture Version:** 1.0  
**Last Updated:** 2025-10-23  
**Related Documents:**
- [QUERY_OPTIMIZATION_STRATEGIES.md](./QUERY_OPTIMIZATION_STRATEGIES.md) - Detailed strategies
- [OPTIMIZATION_QUICK_REFERENCE.md](./OPTIMIZATION_QUICK_REFERENCE.md) - Quick reference guide
