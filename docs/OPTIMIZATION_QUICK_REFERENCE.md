# Query Optimization Strategies - Quick Reference

> 📖 **Full Document:** See [QUERY_OPTIMIZATION_STRATEGIES.md](./QUERY_OPTIMIZATION_STRATEGIES.md) for complete details, code examples, and implementation guidance.

## Current State

Opteryx implements **14 active optimization strategies**:
- 12 Heuristic strategies (rule-based)
- 2 Cost-based strategies (data-driven)

## Top Recommendations

### 🔥 Critical Priority (Implement First)

| Strategy | Impact | Complexity | Expected Benefit |
|----------|--------|------------|------------------|
| **Cardinality Estimation Framework** | ⭐⭐⭐ Very High | High | 20-50% improvement in join-heavy queries |
| **Partition Pruning** | ⭐⭐⭐ Very High | High | 50-90% reduction in data scanned |
| **Predicate Range Compaction** | ⭐⭐⭐ High | Medium | Consolidate redundant predicates |

### ⚡ High Priority (Next Phase)

| Strategy | Impact | Complexity | Expected Benefit |
|----------|--------|------------|------------------|
| **CTE Optimization** | ⭐⭐ Medium-High | Medium | Smart materialization decisions |
| **Aggregate Pushdown** | ⭐⭐ Medium | Medium | 10-50x speedup on remote databases |
| **Multi-way Join Ordering** | ⭐⭐ Very High | High | 50-70% improvement for 4+ way joins |

### 📊 Medium Priority (Future Phases)

- **Subquery Decorrelation** - Transform correlated to non-correlated
- **Common Subexpression Elimination** - Reuse computed values
- **Sort Pushdown** - Leverage database indexes and pre-sorted data
- **Column Pruning Enhancement** - More aggressive column elimination

## Key Improvements to Existing Strategies

### Constant Folding
- Add string operation simplification (CONCAT consolidation)
- CASE statement simplification (eliminate trivial cases)
- Date arithmetic folding
- Function idempotency detection (UPPER(UPPER(x)) → UPPER(x))

### Predicate Pushdown
- Push past UNION ALL when applicable
- More aggressive pushdown into derived tables
- Split OR conditions for partial pushdown
- Push predicates into CASE expressions

### Join Ordering
- Add selectivity-aware ordering (not just size-based)
- Implement join predicate strength analysis
- Consider index availability
- Multi-way join enumeration (dynamic programming)

### Limit Pushdown
- Push into sorted reads (avoid full sort)
- Push past UNION ALL
- Combine OFFSET + LIMIT for better pushdown

## Implementation Roadmap

### Phase 1: Foundation (Months 1-3)
```
✓ Cardinality estimation framework
✓ Statistics collection infrastructure
✓ Predicate compaction productionization
✓ Optimization test harness
```

### Phase 2: Core Optimizations (Months 4-6)
```
□ Partition pruning
□ CTE optimization
□ Aggregate pushdown enhancement
□ Enhanced join ordering with cardinality
```

### Phase 3: Advanced Features (Months 7-12)
```
□ Multi-way join optimization
□ Subquery decorrelation
□ Common subexpression elimination
□ Sort pushdown
```

### Phase 4: Research & Refinement (Months 13-18)
```
□ Adaptive query execution
□ Materialized view matching
□ Bloom filter injection
□ Multi-query optimization
```

## Success Metrics

### Performance Targets
- **Average query time:** 20-30% reduction
- **P95 query time:** 40-50% reduction
- **Join-heavy queries (4+ tables):** 50-70% improvement
- **Data scanned:** 30-50% reduction
- **Network transfer (federated):** 40-60% reduction

### Quality Targets
- **Test coverage:** 90%+ for optimization strategies
- **Optimization time overhead:** < 5% of total execution time
- **Documentation:** All strategies fully documented

## Testing Strategy

### Test Categories
1. **Correctness Tests** - Ensure optimized queries produce same results
2. **Performance Benchmarks** - Measure optimization impact
3. **Regression Tests** - Prevent optimization from breaking functionality
4. **Integration Tests** - Test with various connectors and data sources

### Key Test Scenarios
```python
# Cardinality Estimation
"SELECT * FROM large_table JOIN small_table ON id"

# Partition Pruning  
"SELECT * FROM partitioned_data WHERE date = '2024-01-01'"

# Predicate Compaction
"SELECT * FROM t WHERE x > 5 AND x > 10 AND x < 20"

# CTE Materialization
"WITH cte AS (...) SELECT * FROM cte UNION SELECT * FROM cte"

# Aggregate Pushdown
"SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"
```

## Statistics to Track

Add to `QueryStatistics`:
```python
# New optimizations
optimization_cardinality_estimation_used
optimization_partitions_pruned
optimization_partition_pruning_bytes_saved
optimization_predicate_compaction
optimization_cte_inlined
optimization_cte_materialized
optimization_aggregate_pushdown
optimization_common_subexpression_eliminated
optimization_sort_pushdown
```

## Reference Implementations

### Selectivity Estimation Rules
- **Equality on unique column:** 1 / distinct_count
- **Range predicates:** Use histograms or 33% default
- **LIKE with leading %:** 10% selectivity
- **IN-list:** min(1.0, list_size / distinct_count)
- **AND conditions:** Multiply selectivities
- **OR conditions:** Sum selectivities (capped at 1.0)

### Join Cost Model (Simplified)
```
Nested Loop: left_rows * right_rows * selectivity
Hash Join:   right_rows + (left_rows * selectivity)
Merge Join:  left_rows * log(left_rows) + right_rows * log(right_rows)
```

## Getting Started

### For Implementers
1. Read full document: `docs/QUERY_OPTIMIZATION_STRATEGIES.md`
2. Review existing strategies: `opteryx/planner/optimizer/strategies/`
3. Check prototype implementations: `opteryx/planner/optimizer/bench/`
4. Start with Phase 1 foundations

### For Reviewers
1. Prioritize strategies based on workload characteristics
2. Consider connector capabilities for pushdown strategies
3. Evaluate memory/performance tradeoffs
4. Review expected benefits vs implementation complexity

### For Users
1. Monitor query statistics to see which optimizations apply
2. Use EXPLAIN to understand query plans
3. Report queries that don't benefit from optimization
4. Provide feedback on performance improvements

## Next Steps

1. **Review & Prioritize** - Team review of recommendations
2. **Design Documents** - Detailed designs for Phase 1 items
3. **Benchmark Infrastructure** - Set up performance testing framework
4. **Begin Implementation** - Start with cardinality estimation framework

---

**Quick Reference Version:** 1.0  
**Last Updated:** 2025-10-23  
**Status:** Active

For detailed information, code examples, and implementation guidance, see the full document: [QUERY_OPTIMIZATION_STRATEGIES.md](./QUERY_OPTIMIZATION_STRATEGIES.md)
