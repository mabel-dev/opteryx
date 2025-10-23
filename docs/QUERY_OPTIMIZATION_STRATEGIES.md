# Query Plan Optimization Strategies - Roadmap and Recommendations

## Executive Summary

This document provides a comprehensive analysis of current query optimization strategies in Opteryx and recommends future optimization opportunities. The recommendations are categorized by priority and expected impact, with specific implementation guidance for each strategy.

**Current State:** Opteryx implements 14 optimization strategies covering predicate pushdown, projection pushdown, join ordering, constant folding, and operator fusion.

**Recommended Focus Areas:**
1. **Cost-Based Optimization** - Enhance join ordering and introduce cardinality estimation
2. **Advanced Predicate Optimization** - Implement range predicates and predicate compaction
3. **Materialization Strategies** - Introduce common table expression (CTE) optimization
4. **Parallel Execution** - Add partition-aware optimizations
5. **Storage-Specific Optimizations** - Leverage connector capabilities more effectively

---

## 1. Current Optimization Strategies (Baseline)

### 1.1 Implemented Strategies

| Strategy | Type | Primary Goal | Status |
|----------|------|--------------|--------|
| **ConstantFoldingStrategy** | Heuristic | Evaluate once | ✅ Active (runs 2x) |
| **BooleanSimplificationStrategy** | Heuristic | Simplify expressions | ✅ Active |
| **SplitConjunctivePredicatesStrategy** | Heuristic | Enable pushdown | ✅ Active |
| **CorrelatedFiltersStrategy** | Heuristic | Handle correlations | ✅ Active |
| **PredicateRewriteStrategy** | Heuristic | Normalize predicates | ✅ Active |
| **PredicatePushdownStrategy** | Heuristic | Filter early | ✅ Active |
| **ProjectionPushdownStrategy** | Heuristic | Limit columns | ✅ Active |
| **JoinRewriteStrategy** | Heuristic | Optimize join types | ✅ Active |
| **JoinOrderingStrategy** | Cost-Based | Faster joins | ✅ Active |
| **DistinctPushdownStrategy** | Heuristic | Reduce rows | ✅ Active |
| **OperatorFusionStrategy** | Heuristic | Efficient implementation | ✅ Active |
| **LimitPushdownStrategy** | Heuristic | Reduce rows | ✅ Active |
| **PredicateOrderingStrategy** | Cost-Based | Faster execution | ✅ Active |
| **RedundantOperationsStrategy** | Heuristic | Remove redundancy | ✅ Active |

### 1.2 Strengths of Current Implementation

1. **Well-Structured Architecture** - Clean visitor pattern with clear separation of concerns
2. **Comprehensive Coverage** - Good coverage of basic optimization techniques
3. **Statistics Tracking** - Extensive metrics for optimization effectiveness
4. **Connector Integration** - Good support for pushing operations to data sources
5. **Iterative Refinement** - Some strategies run multiple times (e.g., constant folding)

### 1.3 Areas for Improvement

1. **Limited Cost Modeling** - Basic cost estimates, primarily for predicate ordering
2. **No Cardinality Estimation** - Missing statistics-based optimization decisions
3. **Limited Join Optimization** - Only considers table size, not selectivity
4. **No Common Subexpression Elimination** - Duplicate computations not detected
5. **Limited Materialization Control** - No CTE optimization or result caching strategies

---

## 2. High-Priority Optimization Strategies (Next 6-12 Months)

### 2.1 Cardinality Estimation Framework ⭐⭐⭐

**Priority:** CRITICAL | **Complexity:** High | **Impact:** Very High

#### Overview
Implement a cardinality estimation framework to make better cost-based decisions throughout the optimizer.

#### Current Gap
- Join ordering only considers table sizes
- No selectivity estimates for predicates
- Cannot estimate result set sizes for subqueries

#### Proposed Implementation

```python
class CardinalityEstimationStrategy(OptimizationStrategy):
    """
    Estimate row counts for operations to enable better cost-based decisions.
    
    Techniques:
    1. Histogram-based estimation for range predicates
    2. Distinct value counts for equality predicates
    3. Correlation detection between columns
    4. Sampling for complex predicates
    """
    
    def estimate_filter_selectivity(self, predicate, statistics):
        """
        Estimate fraction of rows passing filter.
        
        Rules:
        - Equality on key: 1/distinct_count
        - Range predicates: use histograms
        - OR: sum selectivities (capped at 1.0)
        - AND: multiply selectivities
        """
        pass
    
    def estimate_join_cardinality(self, left_rows, right_rows, join_type, join_keys):
        """
        Estimate rows after join.
        
        For inner joins:
        - If join on foreign key: estimate = left_rows (if right is unique)
        - Otherwise: (left_rows * right_rows) / max(distinct_left, distinct_right)
        """
        pass
```

#### Integration Points
- **JoinOrderingStrategy**: Use cardinality estimates to reorder multi-way joins
- **PredicatePushdownStrategy**: Prioritize pushing high-selectivity predicates
- **New strategies**: Enable histogram pruning, partition pruning

#### Implementation Steps
1. Add statistics collection framework (min, max, distinct counts, histograms)
2. Implement basic selectivity estimation for common operators
3. Update JoinOrderingStrategy to use cardinality estimates
4. Add statistics persistence (optional, for repeated queries)

#### Expected Benefits
- **20-50% improvement** in join-heavy queries
- Better decisions on which predicates to push
- Foundation for partition pruning

---

### 2.2 Advanced Predicate Optimization ⭐⭐⭐

**Priority:** HIGH | **Complexity:** Medium | **Impact:** High

#### 2.2.1 Predicate Range Compaction

**Status:** Partially implemented in `/bench/predicate_compaction_strategy.py`

The existing prototype shows promise but needs productionization:

```python
class PredicateCompactionStrategy(OptimizationStrategy):
    """
    Compact overlapping range predicates into minimal set.
    
    Examples:
    - WHERE x > 5 AND x > 10  →  WHERE x > 10
    - WHERE x >= 5 AND x <= 10 AND x = 7  →  WHERE x = 7
    - WHERE x > 10 AND x < 5  →  WHERE FALSE (empty result)
    """
    
    def visit(self, node: LogicalPlanNode, context: OptimizerContext):
        if node.node_type == LogicalPlanStepType.Filter:
            # Extract predicates on same column
            ranges_by_column = self._build_value_ranges(node.condition)
            
            # Compact ranges
            for column_id, value_range in ranges_by_column.items():
                if not value_range:  # Invalid range (x > 10 AND x < 5)
                    # Replace entire filter with FALSE
                    node.condition = build_literal_node(False)
                    self.statistics.optimization_predicate_compaction_impossible += 1
                elif value_range.is_compactable():
                    # Replace multiple predicates with compacted form
                    node.condition = value_range.to_predicate()
                    self.statistics.optimization_predicate_compaction += 1
            
            context.optimized_plan[context.node_id] = node
        return context
```

#### 2.2.2 IN-List to Range Conversion

```python
# Current: WHERE x IN (1, 2, 3, 4, 5)
# Optimized: WHERE x >= 1 AND x <= 5
```

This enables better pushdown to connectors that support range predicates but not IN-lists.

#### 2.2.3 Predicate Implication Analysis

```python
"""
Detect implied predicates from existing ones:

Given: WHERE a = b AND b > 10
Derive: a > 10

Benefits:
- Additional predicates for pushdown
- Better selectivity estimates
- Enables more aggressive optimization
"""
```

---

### 2.3 Common Table Expression (CTE) Optimization ⭐⭐

**Priority:** HIGH | **Complexity:** Medium | **Impact:** Medium-High

#### Overview
Currently, CTEs are likely materialized every time they're referenced. Implement smart CTE optimization.

#### Proposed Strategy

```python
class CTEOptimizationStrategy(OptimizationStrategy):
    """
    Optimize Common Table Expressions (WITH clauses).
    
    Decisions:
    1. Inline simple CTEs (single reference, cheap computation)
    2. Materialize complex CTEs (multiple references, expensive computation)
    3. Push predicates into CTE definitions when possible
    """
    
    def should_materialize_cte(self, cte_node, reference_count, complexity):
        """
        Decide whether to materialize CTE.
        
        Materialize if:
        - Referenced 2+ times AND (complex aggregation OR expensive join)
        - Result set is small (< 10K rows estimated)
        
        Inline if:
        - Single reference
        - Simple projection/filter (no aggregation)
        - Can push predicates from parent into CTE
        """
        if reference_count == 1:
            return False
        
        if reference_count >= 2 and (complexity.has_aggregation or complexity.has_joins):
            return True
            
        return False
```

#### Implementation Considerations
1. Track CTE reference counts during planning
2. Estimate CTE result size for materialization decisions
3. Consider memory constraints (large CTEs may need spilling)
4. Add cache invalidation for CTEs in iterative queries

---

### 2.4 Partition Pruning ⭐⭐⭐

**Priority:** CRITICAL (for large datasets) | **Complexity:** High | **Impact:** Very High

#### Overview
Leverage partition information to skip reading irrelevant data.

#### Proposed Implementation

```python
class PartitionPruningStrategy(OptimizationStrategy):
    """
    Skip partitions that cannot contain relevant data.
    
    Works with:
    - Parquet partition keys
    - Iceberg partition specifications
    - Time-based partitions (most common)
    """
    
    def visit(self, node: LogicalPlanNode, context: OptimizerContext):
        if node.node_type == LogicalPlanStepType.Scan:
            if hasattr(node.connector, 'partition_info'):
                partition_info = node.connector.partition_info()
                
                # Extract predicates on partition columns
                partition_predicates = self._extract_partition_predicates(
                    context.collected_predicates,
                    partition_info.columns
                )
                
                # Determine which partitions to skip
                excluded_partitions = self._prune_partitions(
                    partition_info,
                    partition_predicates
                )
                
                # Update scan to skip partitions
                node.excluded_partitions = excluded_partitions
                self.statistics.optimization_partitions_pruned += len(excluded_partitions)
                
                context.optimized_plan[context.node_id] = node
        
        return context
```

#### Integration Requirements
1. Connectors must expose partition metadata
2. Predicate extraction must recognize partition columns
3. File format readers must support partition skipping

#### Expected Benefits
- **50-90% reduction in data scanned** for time-series queries
- Critical for large Parquet/Iceberg datasets

---

### 2.5 Aggregate Pushdown Enhancement ⭐⭐

**Priority:** MEDIUM-HIGH | **Complexity:** Medium | **Impact:** Medium

#### Current Gap
Aggregations are not pushed to connectors that support them (e.g., SQL databases).

#### Proposed Implementation

```python
class AggregatePushdownStrategy(OptimizationStrategy):
    """
    Push aggregations to connectors that support them.
    
    Pushable aggregations:
    - COUNT, SUM, AVG, MIN, MAX
    - Simple GROUP BY (single table, no complex expressions)
    """
    
    def visit(self, node: LogicalPlanNode, context: OptimizerContext):
        if node.node_type == LogicalPlanStepType.AggregateAndGroup:
            # Check if we're aggregating over a single scan
            scan_node = self._find_single_scan_child(node, context)
            
            if scan_node and self._can_push_aggregate(scan_node, node):
                # Move aggregation into scan
                scan_node.aggregations = node.aggregations
                scan_node.groups = node.groups
                
                # Remove aggregate node
                context.optimized_plan.remove_node(context.node_id, heal=True)
                self.statistics.optimization_aggregate_pushdown += 1
        
        return context
    
    def _can_push_aggregate(self, scan_node, agg_node):
        """
        Check if connector supports aggregate pushdown.
        """
        if not hasattr(scan_node.connector, 'AggregatePushable'):
            return False
        
        # Only simple aggregations (no complex expressions in GROUP BY)
        for group in agg_node.groups:
            if not self._is_simple_column_reference(group):
                return False
        
        return True
```

#### Expected Benefits
- **10-50x speedup** for aggregations on remote databases
- Reduced network transfer for large datasets

---

## 3. Medium-Priority Optimizations (12-24 Months)

### 3.1 Subquery Decorrelation ⭐

**Priority:** MEDIUM | **Complexity:** High | **Impact:** High (when applicable)

#### Overview
Transform correlated subqueries into joins or semi-joins.

```sql
-- Correlated (slow)
SELECT * FROM orders o 
WHERE price > (SELECT AVG(price) FROM orders WHERE customer_id = o.customer_id)

-- Decorrelated (fast)
SELECT o.* FROM orders o
JOIN (SELECT customer_id, AVG(price) as avg_price FROM orders GROUP BY customer_id) avg_prices
  ON o.customer_id = avg_prices.customer_id
WHERE o.price > avg_prices.avg_price
```

---

### 3.2 Join Order Enumeration (Dynamic Programming) ⭐⭐

**Priority:** MEDIUM | **Complexity:** High | **Impact:** Very High (for 4+ way joins)

#### Current Gap
Current `JoinOrderingStrategy` only handles pairwise joins (left-deep trees).

#### Proposed Enhancement
Implement dynamic programming approach for multi-way join ordering:

```python
class MultiWayJoinOptimizationStrategy(OptimizationStrategy):
    """
    Use dynamic programming to find optimal join order.
    
    Algorithm:
    1. For each subset of tables, compute best plan
    2. Consider all possible join orders
    3. Use cardinality estimates to prune bad plans
    4. Build up from size 2 to n
    
    Limitations:
    - Only run for 3-6 table joins (exponential complexity)
    - Use heuristics for 7+ tables
    """
```

---

### 3.3 Column Pruning Enhancement ⭐

**Priority:** MEDIUM | **Complexity:** Low-Medium | **Impact:** Medium

#### Current State
`ProjectionPushdownStrategy` already does good column pruning, but can be enhanced:

#### Enhancements
1. **Prune columns in JOIN predicates** - Only keep columns needed for join condition
2. **Prune columns in complex expressions** - Decompose expressions to identify unused columns
3. **Prune DISTINCT columns** - If DISTINCT is on subset of columns, only read those

---

### 3.4 Expression Reuse (Common Subexpression Elimination) ⭐

**Priority:** MEDIUM | **Complexity:** Medium | **Impact:** Medium

```python
class CommonSubexpressionEliminationStrategy(OptimizationStrategy):
    """
    Detect and eliminate duplicate expression evaluations.
    
    Example:
    SELECT 
        price * 1.1 as price_with_tax,
        price * 1.1 * 0.9 as discounted_price
    
    Optimized:
    - Compute price * 1.1 once
    - Reuse for both columns
    """
```

---

### 3.5 Sort Pushdown ⭐

**Priority:** MEDIUM | **Complexity:** Medium | **Impact:** Medium

```python
class SortPushdownStrategy(OptimizationStrategy):
    """
    Push ORDER BY to:
    1. Connectors that support it (SQL databases)
    2. Past projections (if columns are available)
    3. Into partitioned reads (read partitions in order)
    """
```

Expected benefits:
- Avoid full sort for ORDER BY + LIMIT
- Leverage database indexes for sorting

---

## 4. Advanced Optimizations (Future Research)

### 4.1 Adaptive Query Execution ⭐⭐

**Priority:** LOW (research) | **Complexity:** Very High | **Impact:** High

Monitor query execution and adjust plan dynamically:
- Switch join algorithms based on actual row counts
- Add filters discovered during execution
- Reorder operations if estimates were wrong

### 4.2 Materialized View Matching

Automatically use materialized views when available:
- Detect when query can be answered from materialized view
- Rewrite query to use view
- Handle partial matches (view contains superset of data needed)

### 4.3 Bloom Filter Injection

Create and use Bloom filters to:
- Filter join build side before shuffle
- Prune partitions that don't contain join keys
- Reduce data transfer in distributed queries

### 4.4 Multi-Query Optimization

Detect and optimize multiple queries together:
- Share scans across queries
- Share intermediate results
- Batch execution of similar queries

---

## 5. Improvements to Existing Strategies

### 5.1 Constant Folding Enhancements

**Current:** Good coverage of arithmetic and boolean simplifications

**Enhancements:**
```python
# Add to constant_folding.py:

# 1. String operations
# CONCAT(CONCAT(a, 'x'), 'y') -> CONCAT(a, 'xy')

# 2. CASE statement simplification
# CASE WHEN TRUE THEN a ELSE b END -> a
# CASE WHEN FALSE THEN a ELSE b END -> b

# 3. Date arithmetic with constants
# date_col + INTERVAL '1 day' + INTERVAL '2 days' -> date_col + INTERVAL '3 days'

# 4. Function idempotency
# UPPER(UPPER(col)) -> UPPER(col)

# 5. NULL propagation improvements
# col IS NOT NULL AND col > 5 -> col > 5 (second condition implies first)
```

---

### 5.2 Predicate Pushdown Enhancements

**Current:** Comprehensive, but can be improved

**Enhancements:**

```python
# 1. Push predicates past UNION ALL
# Currently stops at UNION, but can push if predicate applies to all branches

# 2. Push predicates into derived tables more aggressively
# SELECT * FROM (SELECT a, b FROM t WHERE x > 5) WHERE b < 10
# -> SELECT * FROM (SELECT a, b FROM t WHERE x > 5 AND b < 10)

# 3. Split predicates for OR conditions
# WHERE (a = 1 AND b = 2) OR (a = 3 AND c = 4)
# Can push a IN (1, 3) to scan, then apply full filter

# 4. Push predicates into CASE expressions
# WHERE CASE WHEN x > 5 THEN y ELSE z END > 10
# -> WHERE (x > 5 AND y > 10) OR (x <= 5 AND z > 10)
```

---

### 5.3 Join Ordering Improvements

**Current:** Basic size-based ordering

**Enhancements:**

```python
class ImprovedJoinOrderingStrategy(JoinOrderingStrategy):
    """
    Enhanced join ordering with:
    1. Selectivity-aware ordering (use cardinality estimates)
    2. Join predicate strength analysis
    3. Multi-way join consideration
    4. Index awareness (prefer indexed joins)
    """
    
    def compute_join_cost(self, left_node, right_node, join_condition):
        """
        More sophisticated cost model:
        
        Cost = (left_rows * right_rows) / selectivity_factor + overhead
        
        Where selectivity_factor considers:
        - Join on primary/foreign key: high selectivity
        - Join on indexed columns: medium selectivity  
        - Join on non-indexed: low selectivity
        - Range joins: very low selectivity
        """
        pass
```

---

### 5.4 Limit Pushdown Enhancements

**Current:** Pushes past projections, stops at aggregations

**Enhancements:**

```python
# 1. Push LIMIT into sorted reads
# ORDER BY x LIMIT 10 -> Read only first 10 from sorted data (if pre-sorted)

# 2. Push LIMIT past UNION ALL
# (SELECT ... LIMIT 10) UNION ALL (SELECT ... LIMIT 10) LIMIT 10
# -> Can stop after finding 10 total rows

# 3. Combine OFFSET + LIMIT for better pushdown
# OFFSET 100 LIMIT 10 -> Push "skip 100, take 10" to connector
```

---

## 6. Testing and Validation Framework

### 6.1 Optimization Test Suite

**Create comprehensive test suite for optimizations:**

```python
# tests/unit/planner/test_optimization_strategies.py

OPTIMIZATION_TEST_CASES = [
    # Cardinality Estimation
    {
        "name": "estimate_equality_selectivity",
        "query": "SELECT * FROM table WHERE unique_key = 1",
        "expected_selectivity": 1.0 / distinct_count,
    },
    
    # Predicate Compaction
    {
        "name": "compact_redundant_ranges",
        "query": "SELECT * FROM t WHERE x > 5 AND x > 10 AND x < 20",
        "optimized_predicates": ["x > 10", "x < 20"],
        "flag": "optimization_predicate_compaction"
    },
    
    # Partition Pruning
    {
        "name": "prune_time_partitions",
        "query": "SELECT * FROM partitioned_table WHERE date = '2024-01-01'",
        "expected_partitions_read": 1,
        "flag": "optimization_partitions_pruned"
    },
]
```

---

### 6.2 Optimization Benchmarks

**Create benchmark suite to measure optimization impact:**

```python
# benchmarks/optimizer_benchmarks.py

class OptimizationBenchmark:
    """
    Benchmark optimization strategies.
    
    Metrics:
    - Query execution time (with/without optimization)
    - Data scanned (bytes)
    - Memory usage
    - Network transfer (for federated queries)
    """
    
    def benchmark_strategy(self, strategy_name, test_queries):
        """Run queries with and without specific optimization."""
        pass
```

**Key benchmark scenarios:**
1. Join ordering with 3, 4, 5 tables
2. Predicate pushdown to different connectors
3. Partition pruning on large Parquet datasets
4. CTE materialization with varying reference counts

---

### 6.3 Regression Testing

**Ensure optimizations don't break existing functionality:**

```python
# tests/integration/test_optimization_correctness.py

def test_optimization_produces_same_results():
    """
    Run queries with and without optimizer.
    Results should be identical.
    """
    queries = load_test_queries()
    
    for query in queries:
        # Run with optimizer
        result_optimized = opteryx.query(query)
        
        # Run without optimizer (DISABLE_OPTIMIZER=True)
        result_unoptimized = opteryx.query(query, disable_optimizer=True)
        
        # Results should match
        assert_results_equal(result_optimized, result_unoptimized)
```

---

## 7. Implementation Roadmap

### Phase 1: Foundation (Months 1-3)
- [ ] Implement cardinality estimation framework
- [ ] Add statistics collection to connectors
- [ ] Create optimization test harness
- [ ] Productionize predicate compaction strategy

### Phase 2: Core Optimizations (Months 4-6)
- [ ] Partition pruning
- [ ] CTE optimization
- [ ] Aggregate pushdown enhancement
- [ ] Enhanced join ordering with cardinality

### Phase 3: Advanced Features (Months 7-12)
- [ ] Multi-way join optimization
- [ ] Subquery decorrelation
- [ ] Common subexpression elimination
- [ ] Sort pushdown

### Phase 4: Refinement (Months 13-18)
- [ ] Adaptive query execution (research)
- [ ] Enhanced constant folding
- [ ] Materialized view matching
- [ ] Performance tuning and benchmarking

---

## 8. Success Metrics

### 8.1 Quantitative Metrics

1. **Query Performance**
   - Average query time reduction: Target 20-30%
   - P95 query time reduction: Target 40-50%
   - Queries with 4+ joins: Target 50-70% improvement

2. **Resource Utilization**
   - Data scanned reduction: Target 30-50%
   - Memory usage reduction: Target 10-20%
   - Network transfer reduction: Target 40-60% (federated queries)

3. **Optimization Statistics**
   - % queries benefiting from each optimization: Track per strategy
   - Average optimizations per query: Current ~5-8, target 10-15
   - Optimization time overhead: Keep < 5% of execution time

### 8.2 Qualitative Metrics

1. **Code Quality**
   - Test coverage for optimization strategies: Target 90%+
   - Documentation completeness: All strategies documented
   - Benchmark coverage: All strategies benchmarked

2. **User Experience**
   - Reduction in manual query tuning needed
   - Improved EXPLAIN output readability
   - Better error messages for optimization failures

---

## 9. Risks and Mitigation

### 9.1 Risks

1. **Optimization Overhead**
   - Risk: Complex optimizations take longer than they save
   - Mitigation: Set time budgets, use heuristics for complex cases

2. **Correctness Issues**
   - Risk: Optimizations produce wrong results
   - Mitigation: Comprehensive test suite, regression testing

3. **Memory Usage**
   - Risk: Statistics and caching increase memory footprint
   - Mitigation: Configurable limits, LRU eviction

4. **Connector Compatibility**
   - Risk: Pushdown optimizations break with some connectors
   - Mitigation: Capability detection, conservative defaults

### 9.2 Mitigation Strategies

1. **Gradual Rollout**
   - Add feature flags for new optimizations
   - A/B test with real workloads
   - Provide rollback path

2. **Monitoring**
   - Track optimization success rates
   - Monitor query failures after optimization
   - Add telemetry for optimization decisions

3. **Documentation**
   - Document each optimization strategy
   - Provide troubleshooting guide
   - Create optimization best practices guide

---

## 10. Conclusion

This roadmap provides a comprehensive path forward for query optimization in Opteryx. The priorities are:

**Immediate (Next 3-6 Months):**
1. ⭐⭐⭐ Cardinality Estimation Framework
2. ⭐⭐⭐ Partition Pruning
3. ⭐⭐⭐ Predicate Range Compaction
4. ⭐⭐ CTE Optimization
5. ⭐⭐ Aggregate Pushdown

**Near-Term (6-12 Months):**
1. ⭐⭐ Enhanced Join Ordering (multi-way)
2. ⭐ Subquery Decorrelation
3. ⭐ Sort Pushdown
4. ⭐ Common Subexpression Elimination

**Long-Term (12+ Months):**
1. Adaptive Query Execution (research)
2. Materialized View Matching
3. Multi-Query Optimization

The existing optimization framework is well-designed and provides a solid foundation. The recommended strategies build on this foundation to deliver significant performance improvements while maintaining code quality and correctness.

**Next Steps:**
1. Review and prioritize recommendations with team
2. Create detailed design docs for Phase 1 optimizations
3. Set up optimization benchmark infrastructure
4. Begin implementation of cardinality estimation framework

---

## Appendix A: Statistics to Track

Add these statistics to `QueryStatistics` for new optimizations:

```python
# Cardinality Estimation
optimization_cardinality_estimation_used = 0
optimization_cardinality_estimation_accurate = 0  # within 10% of actual

# Partition Pruning
optimization_partitions_pruned = 0
optimization_partition_pruning_bytes_saved = 0

# Predicate Compaction
optimization_predicate_compaction = 0
optimization_predicate_compaction_impossible = 0

# CTE Optimization
optimization_cte_inlined = 0
optimization_cte_materialized = 0

# Aggregate Pushdown
optimization_aggregate_pushdown = 0
optimization_aggregate_pushdown_rows_reduced = 0

# Common Subexpression Elimination
optimization_common_subexpression_eliminated = 0

# Sort Pushdown
optimization_sort_pushdown = 0
```

---

## Appendix B: Reference Implementations

### Cost-Based Join Ordering (PostgreSQL-style)

```python
def calculate_join_cost(left_card, right_card, join_type, join_selectivity):
    """
    Simplified cost model inspired by PostgreSQL.
    
    Cost = startup_cost + per_row_cost * num_rows
    """
    if join_type == "nested_loop":
        # Nested loop: for each left row, scan right
        startup = 0
        per_row = right_card
        total_rows = left_card * right_card * join_selectivity
        
    elif join_type == "hash":
        # Hash join: build hash table + probe
        startup = right_card  # Build hash table
        per_row = 1  # Probe is O(1)
        total_rows = left_card * right_card * join_selectivity
        
    elif join_type == "merge":
        # Merge join: sort both sides + merge
        startup = left_card * math.log2(left_card) + right_card * math.log2(right_card)
        per_row = 1
        total_rows = (left_card + right_card) * join_selectivity
    
    return startup + per_row * total_rows
```

### Selectivity Estimation

```python
def estimate_selectivity(operator, left_value, right_value, stats):
    """
    Estimate selectivity of predicate.
    
    Based on PostgreSQL selectivity estimation.
    """
    if operator == "Eq":
        # col = value
        if stats.distinct_values > 0:
            return 1.0 / stats.distinct_values
        return 0.1  # Default guess
    
    elif operator in ("Lt", "LtEq", "Gt", "GtEq"):
        # col > value
        if stats.histogram:
            # Use histogram to estimate fraction
            return stats.histogram.estimate_range(operator, right_value)
        return 0.33  # Default guess
    
    elif operator == "Like":
        # col LIKE pattern
        if right_value.startswith("%"):
            return 0.1  # Leading wildcard: expensive
        elif right_value.endswith("%"):
            # Use distinct count of prefix
            return 0.01
        else:
            return 0.001  # Exact match
    
    elif operator == "InList":
        # col IN (v1, v2, ...)
        list_size = len(right_value)
        if stats.distinct_values > 0:
            return min(1.0, list_size / stats.distinct_values)
        return min(1.0, list_size * 0.1)
    
    return 0.1  # Unknown operator
```

---

**Document Version:** 1.0  
**Last Updated:** 2025-10-23  
**Author:** Query Optimization Team  
**Status:** DRAFT for Review
