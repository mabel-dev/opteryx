# ClickBench Performance Investigation

**Date:** 2025-10-23  
**Status:** Investigation Required  
**Reporter:** @joocer (maintainer)

## Issue

The maintainer (@joocer) has indicated that ClickBench queries show performance degradation **even when warm**. This suggests the performance issue is not just about cold start overhead, but may include algorithmic or implementation problems.

## What We Know

### Initial Analysis (Completed)
✅ **Cold Start Issue**: Confirmed 72.3x slowdown on first query  
✅ **Simple Query Performance**: Excellent on small virtual datasets ($planets, $satellites)  
- COUNT: 3.6ms
- SELECT: 3.4ms  
- WHERE: 5.8ms
- Aggregations: 5.4ms
- GROUP BY: 4.9ms
- JOINs: 8.3ms

### Gap in Analysis
⚠️ **Not Tested**: Complex queries on larger datasets (ClickBench)  
⚠️ **Not Compared**: Performance vs v0.24 release  
⚠️ **Not Profiled**: Slow ClickBench queries specifically

## ClickBench Background

ClickBench is a standard analytical database benchmark featuring:
- Real-world web analytics queries
- Complex aggregations and GROUP BY operations
- COUNT DISTINCT operations
- String operations
- LIKE patterns
- Date filtering
- Multi-column grouping

**Dataset Size**: testdata/clickbench_tiny (subset of full ClickBench)

**Query Count**: 43 queries of varying complexity

## Hypothesis: Why ClickBench Might Be Slower

1. **COUNT DISTINCT Implementation**
   - ClickBench has many COUNT DISTINCT queries
   - May use less efficient algorithm than competitors
   - Possible O(n²) behavior or poor hash table implementation

2. **String Operations**
   - Many LIKE patterns in ClickBench
   - String comparisons and regex operations
   - Possible inefficient string handling

3. **GROUP BY with Multiple Columns**
   - Complex multi-column grouping
   - Hash table or sorting performance issues
   - Memory allocation patterns

4. **Not Using Compiled Extensions**
   - Only 18 of 50 Cython files compiled
   - Falling back to slower Python implementations
   - list_ops extensions not compiled

5. **Data Access Patterns**
   - Larger dataset → more I/O
   - Cache misses
   - Memory allocation overhead

6. **Query Optimizer Issues**
   - Suboptimal execution plans
   - Missing query optimizations
   - Predicate pushdown not working

## Investigation Steps

### Step 1: Run ClickBench Benchmark

The existing ClickBench test suite has been enhanced with warm query testing:

```bash
# Run with multiple iterations to test warm performance
python tests/performance/benchmarks/clickbench.py --warm

# Or with custom iteration count
python tests/performance/benchmarks/clickbench.py --warm --iterations 5
```

This will:
- Measure warm performance for each query
- Identify slow queries (>500ms)
- Report variance and consistency
- Output detailed timing data

### Step 2: Profile Slow Queries

For queries identified as slow:

```bash
python tools/analysis/detailed_profiler.py --query "SELECT ... FROM testdata.clickbench_tiny ..."
```

This will show:
- Which functions consume the most time
- How many times functions are called
- Call stacks for hot paths

### Step 3: Compare with v0.24 (If Available)

```bash
# Checkout v0.24
git checkout v0.24.0  # or appropriate tag
pip install -e . --force-reinstall

# Benchmark v0.24
python tests/performance/benchmarks/clickbench.py --warm > clickbench-v0.24-results.txt

# Switch back to current
git checkout main
pip install -e . --force-reinstall

# Benchmark current
python tests/performance/benchmarks/clickbench.py --warm > clickbench-current-results.txt

# Compare
diff clickbench-v0.24-results.txt clickbench-current-results.txt
```

### Step 4: Check Compiled Extensions Usage

```bash
# Verify extensions are compiled
find opteryx/compiled -name '*.so' | wc -l

# Check which list_ops are not compiled
for f in opteryx/compiled/list_ops/*.pyx; do
    so="${f%.pyx}.cpython-312-x86_64-linux-gnu.so"
    if [ ! -f "$so" ]; then
        echo "Not compiled: $(basename $f)"
    fi
done
```

### Step 5: Analyze Query Plans

For slow queries, check the execution plan:

```python
import opteryx
conn = opteryx.connect()
cursor = conn.cursor()

# For a slow query
cursor.execute("EXPLAIN <slow_query>")
plan = cursor.fetchall()
print(plan)
```

## Expected Outcomes

### Scenario 1: COUNT DISTINCT is Slow
**Finding**: Queries with COUNT DISTINCT are 10x+ slower  
**Fix**: Optimize COUNT DISTINCT implementation (use better hash table, HyperLogLog approximation)  
**Impact**: High - affects many analytical queries

### Scenario 2: String Operations are Slow
**Finding**: LIKE and string comparisons take majority of time  
**Fix**: Compile list_ops/list_in_string.pyx and related string ops  
**Impact**: Medium - affects text search queries

### Scenario 3: GROUP BY is Inefficient
**Finding**: Multi-column GROUP BY shows O(n²) behavior  
**Fix**: Optimize grouping algorithm, improve hash table  
**Impact**: High - core analytical operation

### Scenario 4: Cython Extensions Not Used
**Finding**: Profiling shows Python implementations being called  
**Fix**: Ensure compiled extensions are properly loaded  
**Impact**: High - quick win if fixable

### Scenario 5: Data Access Overhead
**Finding**: I/O or data loading dominates execution time  
**Fix**: Optimize data reading, caching, vectorization  
**Impact**: Medium to High

## Tracking Progress

- [ ] Run ClickBench benchmark suite
- [ ] Identify 5 slowest queries
- [ ] Profile those queries in detail
- [ ] Compare with v0.24 if possible
- [ ] Verify compiled extensions are used
- [ ] Document specific bottlenecks found
- [ ] Propose targeted fixes
- [ ] Implement and re-test

## Success Criteria

1. **Identify** specific slow queries (with timings)
2. **Profile** to find bottleneck functions
3. **Compare** with v0.24 baseline (if available)
4. **Document** root causes
5. **Estimate** fix effort for each issue
6. **Prioritize** fixes by impact

## Next Actions

**Immediate:** Run `python tests/performance/benchmarks/clickbench.py --warm` to get baseline data

**Report Back:** Document which queries are slow and by how much

**Deep Dive:** Profile the slowest queries to understand why

## Notes

- ClickBench is widely used for database benchmarking
- Performance on this benchmark affects Opteryx's perceived competitiveness
- Even if cold start is fixed, slow warm queries will impact users
- May need algorithmic improvements, not just implementation tweaks
- Compare against DuckDB, ClickHouse results for context
