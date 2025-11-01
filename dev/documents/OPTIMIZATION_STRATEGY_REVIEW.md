# Opteryx Optimization Strategies - Comprehensive Review & Improvement Opportunities

## Executive Summary

This document reviews all 17 optimization strategies in Opteryx and identifies opportunities for enhancement and new optimizations. The current implementation is comprehensive but has several areas where we can add additional pattern matching, edge cases, and performance improvements.

**Current Strategies**: 17 active optimizers  
**Enhancement Opportunities**: 24+ identified cases  
**Priority Areas**: Boolean simplification, constant folding, predicate rewriting

---

## I. Current Strategy Overview

### Active Strategies (in execution order)

1. **boolean_simplication.py** - Demorgan's laws, double negation, constant folding for AND/OR
2. **constant_folding.py** - Pre-evaluate expressions with no identifiers
3. **correlated_filters.py** - Handle correlated subqueries
4. **distinct_pushdown.py** - Push DISTINCT operators closer to scans
5. **join_ordering.py** - Reorder joins for better selectivity
6. **join_rewriter.py** - Rewrite join types (INNER→SEMI, INNER→ANTI, etc.)
7. **limit_pushdown.py** - Push LIMIT operators toward scans
8. **operator_fusion.py** - Combine adjacent operators
9. **optimization_strategy.py** - Base class and framework
10. **predicate_ordering.py** - Order predicates by cost
11. **predicate_pushdown.py** - Push filters toward scans
12. **predicate_rewriter.py** - Rewrite predicates to more efficient forms
13. **projection_pushdown.py** - Push column selection toward scans
14. **redundant_operators.py** - Remove redundant projections and subqueries
15. **split_conjunctive_predicates.py** - Split AND conditions into separate filters

---

## II. Enhancement Opportunities by Strategy

### A. Boolean Simplification Strategy (CURRENT - Recently Enhanced)

**Current Capabilities:**
- De Morgan's laws: `NOT(A OR B) => (NOT A) AND (NOT B)`
- Double negation: `NOT(NOT(A)) => A`
- Operator inversion: `NOT(A = B) => A != B`
- AND chain flattening: `((A AND B) AND C) => (A AND (B AND C))`
- Constant folding: `A AND TRUE => A`, `A AND FALSE => FALSE`
- Redundant condition removal: `A AND A => A`

**Additional Cases to Add:**

1. **OR Simplification Cases** (Medium Priority)
   ```
   A OR FALSE => A
   A OR TRUE => TRUE
   A OR A => A
   A OR NOT A => TRUE  (Law of Excluded Middle)
   ```

2. **XOR Simplification Cases** (Medium Priority)
   ```
   A XOR FALSE => A
   A XOR TRUE => NOT A
   A XOR A => FALSE
   A XOR NOT A => TRUE
   ```

3. **Combined AND/OR Simplification** (High Priority)
   ```
   (A OR B) AND A => A  (Absorption Law)
   (A AND B) OR A => A  (Absorption Law)
   A AND (A OR B) => A  (Absorption Law)
   A OR (A AND B) => A  (Absorption Law)
   A AND (B OR C) with constant folding
   ```

4. **Distributive Laws** (Medium Priority - Complex)
   ```
   A AND (B OR C) => (A AND B) OR (A AND C)  (when beneficial)
   A OR (B AND C) => (A OR B) AND (A OR C)  (when beneficial)
   ```

5. **Commutative Reordering for Better Pushdown** (Low Priority)
   ```
   Move constants to right: const OP col => col OP const
   Move simpler expressions first for early termination
   ```

6. **De Morgan's Generalization** (High Priority)
   ```
   NOT(A AND B AND C) => NOT A OR NOT B OR NOT C
   NOT(A OR B OR C) => NOT A AND NOT B AND NOT C
   Currently only handles binary AND/OR
   ```

### B. Constant Folding Strategy

**Current Capabilities:**
- Evaluate expressions with no identifiers
- Handle arithmetic: `2+2 => 4`
- Handle NULL operations: `x * 0 => 0` (with NULL checks)
- Runs twice (beginning and end)

**Additional Cases to Add:**

1. **String Operations** (Medium Priority)
   ```
   CONCAT('a', 'b') => 'ab'
   CONCAT_WS('-', 'a', 'b') => 'a-b'
   UPPER('hello') => 'HELLO'
   LOWER('HELLO') => 'hello'
   LENGTH('abc') => 3
   SUBSTRING('hello', 1, 3) => 'hel'
   ```

2. **Date/Time Operations** (High Priority)
   ```
   DATE '2024-01-01' + INTERVAL 1 DAY => DATE '2024-01-02'
   EXTRACT(YEAR FROM DATE '2024-06-15') => 2024
   CURRENT_DATE (when deterministic)
   ```

3. **Numeric Functions** (Medium Priority)
   ```
   ABS(-5) => 5
   ROUND(3.14159, 2) => 3.14
   CEIL(3.2) => 4
   FLOOR(3.8) => 3
   SQRT(16) => 4
   ```

4. **Logical Constant Folding** (High Priority)
   ```
   IF(true, a, b) => a
   IF(false, a, b) => b
   CASE WHEN true THEN a ELSE b END => a
   COALESCE(NULL, NULL, value, ...) => value with early termination
   ```

5. **Type Coercion Optimization** (Medium Priority)
   ```
   CAST('123' AS INTEGER) + 1 => CAST('123' AS INTEGER) + CAST(1 AS INTEGER)
   Reduce redundant casts in arithmetic
   ```

### C. Predicate Rewriter Strategy

**Current Capabilities:**
- IN with single value → Equality
- LIKE without wildcards → Equality
- LIKE '%pattern%' → INSTR
- CASE WHEN optimizations
- CONCAT → operators
- LIKE OR LIKE → REGEX
- Equals OR Equals → IN
- STARTS_WITH/ENDS_WITH → LIKE

**Additional Cases to Add:**

1. **Range Predicate Optimization** (High Priority)
   ```
   (col > 5 AND col < 10) => col BETWEEN 5 AND 10
   (col >= 5 AND col <= 10) => col BETWEEN 5 AND 10
   (col < 5 OR col > 10) => col NOT BETWEEN 5 AND 10
   (col < date1 OR col > date2) => col NOT BETWEEN date1 AND date2
   ```

2. **IN to OR Optimization** (Low Priority - Inverse)
   ```
   col IN (a, b, c) => col = a OR col = b OR col = c  (when small list)
   col NOT IN (x) => col != x  (single value)
   ```

3. **Comparison Chain Optimization** (High Priority)
   ```
   col > 5 AND col > 10 => col > 10  (keep most restrictive)
   col < 5 AND col < 10 => col < 5
   col = 5 AND col = 5 => col = 5
   col != 5 AND col != 5 => col != 5
   ```

4. **String Pattern Optimization** (Medium Priority)
   ```
   col LIKE 'a%b%c%' => complex pattern → REGEX if faster
   col LIKE '%a' AND col LIKE 'b%' => col LIKE 'b%a'  (combine)
   INSTR(col, pattern1) > 0 AND INSTR(col, pattern2) > 0 => nested INSTR
   ```

5. **NULL-Safe Comparison** (Medium Priority)
   ```
   (col IS NULL OR col = value) => col IS NOT DISTINCT FROM value  (SQL standard)
   (col IS NOT NULL AND col = value) => (col IS DISTINCT FROM NULL AND col = value)
   ```

6. **Math Expression Normalization** (Medium Priority)
   ```
   col + 5 > 10 => col > 5  (push constant to other side)
   col * 2 < 100 => col < 50
   col / 10 >= 1 => col >= 10
   ```

### D. Predicate Ordering Strategy

**Current Capabilities:**
- Cost-based predicate ordering using approximate type costs
- Groups AND'ed ANY_OP EQ conditions into ArrayContainsAll

**Additional Cases to Add:**

1. **Selectivity-Aware Ordering** (High Priority - Requires Stats)
   ```
   Track column cardinality/selectivity
   Order: Most selective first (eliminates more rows early)
   Example: WHERE active = true AND rare_status = 'pending'
   => Order by rare_status first if it has lower cardinality
   ```

2. **Predicate Dependency Analysis** (High Priority)
   ```
   Detect when predicates are independent vs dependent
   Order independent predicates before dependent ones
   Example: WHERE col1 > 10 AND col2 IN (SELECT...)
   => Execute col1 > 10 first if independent
   ```

3. **Short-Circuit Optimization** (High Priority)
   ```
   For AND: Order most likely to be FALSE first
   For OR: Order most likely to be TRUE first
   Requires runtime statistics collection
   ```

4. **Predicate Complexity Scoring** (Medium Priority)
   ```
   Simple comparisons first: =, !=, <, >
   Then range: BETWEEN, IN
   Then functions: LIKE, CONTAINS, INSTR
   Then subqueries: ANY, ALL, EXISTS
   ```

### E. Predicate Pushdown Strategy

**Current Capabilities:**
- Push filters toward scans
- Push into JOIN conditions
- Respect correlation and aggregation boundaries

**Additional Cases to Add:**

1. **Cross-Table Predicate Analysis** (High Priority)
   ```
   Detect pushable predicates: WHERE t1.col > 5 AND t2.col < 10
   => Push t1.col > 5 before join, t2.col < 10 after
   (Already partially done, enhance edge cases)
   ```

2. **Join Type Specific Pushdown** (High Priority)
   ```
   LEFT JOIN: Can only push into right table with caution
   INNER JOIN: Can push both sides
   RIGHT JOIN: Similar to LEFT JOIN mirror
   FULL OUTER JOIN: Cannot push at all
   Current: May not handle all edge cases
   ```

3. **Aggregation-Aware Pushdown** (Medium Priority)
   ```
   WHERE col1 > 5 GROUP BY col1 HAVING count > 10
   => Push col1 > 5 before aggregation
   Currently may not push when HAVING exists
   ```

4. **Derived Table Pushdown** (Medium Priority)
   ```
   SELECT * FROM (SELECT a, b FROM t WHERE x > 5) WHERE a = 10
   => Push a = 10 into derived table WHERE clause
   ```

### F. Limit Pushdown Strategy

**Current Capabilities:**
- Push LIMIT toward scans
- Respect ordering requirements
- Handle OFFSET correctly

**Additional Cases to Add:**

1. **LIMIT with ORDER BY Optimization** (High Priority)
   ```
   SELECT * FROM t ORDER BY col LIMIT 10
   => Push to source as "top-10" if available
   vs SELECT * FROM t LIMIT 10 ORDER BY col
   => Cannot push LIMIT (must order first)
   ```

2. **LIMIT with Aggregation** (Medium Priority)
   ```
   SELECT col, count FROM (SELECT col, COUNT(*) as count FROM t GROUP BY col) LIMIT 10
   => May be able to limit groups before aggregation in some cases
   ```

3. **LIMIT in Subqueries** (High Priority)
   ```
   SELECT * FROM (SELECT * FROM large_table LIMIT 10000)
   => Could push outer LIMIT into subquery
   Current: May not handle nested LIMIT correctly
   ```

### G. Operator Fusion Strategy

**Current Capabilities:**
- Combine adjacent compatible operators

**Additional Cases to Add:**

1. **Filter + Project Fusion** (High Priority)
   ```
   Filter(col1 > 5) -> Project(col1, col2)
   => Fuse into single FilterProject operator
   Reduces intermediate row sets
   ```

2. **Project + Project Fusion** (High Priority)
   ```
   Project(a, b, c) -> Project(a, c)
   => Fuse into single Project(a, c)
   Already in redundant_operators, but could be in fusion
   ```

3. **Scan + Filter Fusion** (Medium Priority)
   ```
   Scan -> Filter(col > 5)
   => Fuse into FilteredScan if connector supports
   Requires connector capability checking
   ```

4. **Aggregate + Filter Fusion** (Medium Priority - Complex)
   ```
   Aggregate -> Filter(count > 5)
   => Keep separate (Filter is HAVING, must come after)
   Current: Correctly keeps separate, but could document edge case
   ```

### H. Join Ordering & Rewriting Strategies

**Current Capabilities:**
- join_ordering.py: Estimates costs and reorders
- join_rewriter.py: Converts INNER→SEMI, INNER→ANTI, etc.

**Additional Cases to Add:**

1. **Self-Join Detection & Optimization** (High Priority)
   ```
   SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.parent_id
   => Could use windowing or recursive query
   Or flatten if possible
   ```

2. **Chain Join Reordering** (High Priority)
   ```
   t1 JOIN t2 JOIN t3 JOIN t4
   => Reorder by selectivity: small → large
   Current: May only do pairwise optimization
   ```

3. **Semi Join Expansion** (Medium Priority)
   ```
   col IN (SELECT id FROM t WHERE condition)
   => Convert to SEMI JOIN with predicate pushdown
   Already done, but could expand to more patterns
   ```

4. **Anti Join Expansion** (Medium Priority)
   ```
   col NOT IN (SELECT id FROM t WHERE condition)
   => Convert to ANTI JOIN
   Already done, could expand patterns
   ```

5. **Multi-Table JOIN Predicate Extraction** (High Priority)
   ```
   WHERE t1.col > 5 AND t2.col < 10 AND t1.id = t2.id
   => Extract join condition, order predicates, push down
   Current: May not handle complex multi-table WHERE optimal
   ```

### I. Distinct Pushdown Strategy

**Current Capabilities:**
- Push DISTINCT toward scans

**Additional Cases to Add:**

1. **DISTINCT with GROUP BY Optimization** (High Priority)
   ```
   SELECT DISTINCT col FROM t GROUP BY col, col2
   => Reduce GROUP BY: GROUP BY col (one already distinct)
   ```

2. **DISTINCT with ORDER BY** (Medium Priority)
   ```
   SELECT DISTINCT col FROM t ORDER BY col2
   => May need to include col2 in DISTINCT for ORDER BY
   vs complex rewrite with window functions
   ```

3. **DISTINCT on Primary Key** (High Priority)
   ```
   SELECT DISTINCT id, ... FROM t WHERE id IN (...)
   => Remove DISTINCT (id guarantees uniqueness)
   Requires column lineage tracking
   ```

### J. Redundant Operators Strategy

**Current Capabilities:**
- Remove redundant projections
- Remove subquery nodes that don't affect execution

**Additional Cases to Add:**

1. **Redundant Sort Removal** (Medium Priority)
   ```
   ORDER BY col1 -> ORDER BY col1, col2
   => First ORDER BY is redundant
   Current: Not handled
   ```

2. **Redundant DISTINCT Removal** (High Priority)
   ```
   DISTINCT -> DISTINCT
   => Keep only outermost
   Current: May not detect
   ```

3. **Redundant LIMIT Removal** (Medium Priority)
   ```
   LIMIT 1000 -> LIMIT 10
   => Keep only innermost (more restrictive)
   ```

4. **Redundant Filter Removal** (High Priority)
   ```
   WHERE col > 5 -> WHERE col > 5 AND col > 3
   => Remove col > 3 (superseded by col > 5)
   Current: Not detected
   ```

### K. Split Conjunctive Predicates Strategy

**Current Capabilities:**
- Split AND conditions into separate filters

**Additional Cases to Add:**

1. **Smart Splitting Order** (Medium Priority)
   ```
   WHERE a > 5 AND b < 10 AND c IS NOT NULL
   => Split and order: b < 10, c IS NOT NULL, a > 5
   (by estimated selectivity)
   Current: Splits but may not optimize order
   ```

2. **Predicate Dependency Preservation** (High Priority)
   ```
   WHERE t1.id = t2.id AND t1.col > 5
   => Keep join condition together, split selective condition
   Current: May split aggressively
   ```

3. **Function-Based Predicate Grouping** (Medium Priority)
   ```
   WHERE LOWER(col) = 'value' AND col2 > 5
   => Group LOWER(col) = 'value' together
   Current: Splits everything
   ```

---

## III. New Strategy Opportunities

### 1. **Expression Simplification Strategy** (HIGH PRIORITY)

**Goal**: Simplify complex nested expressions

**Examples**:
```
(col IS NOT NULL OR col IS NULL) => TRUE
(col IS NULL AND col IS NOT NULL) => FALSE
(a = a) => TRUE
(a != a) => FALSE
CASE WHEN col > 5 THEN col > 5 ELSE FALSE END => (col > 5)
```

### 2. **Materialization Elimination Strategy** (MEDIUM PRIORITY)

**Goal**: Remove unnecessary materialization nodes (UNION ALL, temporary results)

**Examples**:
```
UNION ALL with single branch => Remove UNION
SELECT from (SELECT * FROM t) => Remove intermediate
Consecutive JOINs could use streaming instead of buffering
```

### 3. **Column Statistics Strategy** (HIGH PRIORITY - Requires Infrastructure)

**Goal**: Track and use column statistics for better decisions

**Benefits**:
- Selectivity-aware predicate ordering
- Cardinality estimation for join ordering
- NULL fraction for NULL-handling optimization

### 4. **Index Awareness Strategy** (MEDIUM PRIORITY - Connector Dependent)

**Goal**: Leverage index information if available

**Examples**:
```
WHERE indexed_col > 5 => Use index range scan
WHERE indexed_col IN (1,2,3) => Use index seek
WHERE indexed_col LIKE 'prefix%' => Use index prefix scan
```

### 5. **Window Function Optimization Strategy** (MEDIUM PRIORITY)

**Goal**: Optimize window function execution

**Examples**:
```
ROW_NUMBER() OVER (ORDER BY col) with LIMIT => Early termination
PARTITION BY col + WHERE col = value => Reduce partitions
Multiple window functions => Single scan vs multiple
```

### 6. **Subquery Unnesting Strategy** (HIGH PRIORITY - Complex)

**Goal**: Convert correlated subqueries to JOINs when possible

**Examples**:
```
SELECT * FROM t WHERE col IN (SELECT id FROM s WHERE s.t_id = t.id)
=> Convert to JOIN when beneficial

SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.t_id = t.id)
=> Convert to SEMI JOIN
```

### 7. **Expression Vectorization Strategy** (MEDIUM PRIORITY)

**Goal**: Identify vectorizable expressions and batch them

**Examples**:
```
col1 + col2 + col3 + col4 => Vectorize multi-column arithmetic
CASE WHEN conditions => Vectorize conditional logic
Multiple LIKE patterns => Batch pattern matching
```

### 8. **Common Subexpression Elimination** (MEDIUM PRIORITY)

**Goal**: Detect and reuse common subexpressions

**Examples**:
```
SELECT col + 5, col + 5, col + 5 FROM t
=> Compute once, reference 3 times

WHERE (col1 + col2) > 10 AND (col1 + col2) < 20
=> Compute (col1 + col2) once
```

---

## IV. Edge Cases & Improvements by Risk Level

### Low Risk (Safe to Implement Soon)

1. ✅ **OR Simplification** - Same pattern as AND simplification
2. ✅ **String Constant Folding** - No side effects, deterministic
3. ✅ **Absorption Laws** - Simple boolean logic
4. ✅ **Comparison Chain Reduction** - Deterministic logic
5. ✅ **Range Predicate Combination** - Well-defined boundaries

### Medium Risk (Need Testing)

1. 🔄 **Predicate Dependency Analysis** - May affect join correctness
2. 🔄 **LEFT JOIN Pushdown Rules** - Null-handling subtleties
3. 🔄 **Common Subexpression Elimination** - Cache invalidation, state tracking
4. 🔄 **Distributive Laws** - May increase expression size
5. 🔄 **Selectivity-Based Ordering** - Requires accurate stats

### High Risk (Requires Careful Design)

1. ⚠️ **Subquery Unnesting** - Complex equivalence rules
2. ⚠️ **Statistical Inference** - Requires infrastructure
3. ⚠️ **Index Awareness** - Connector-specific, may not apply universally
4. ⚠️ **Window Function Optimization** - Complex state management
5. ⚠️ **Expression Vectorization** - May conflict with evaluation order

---

## V. Implementation Roadmap

### Phase 1: Quick Wins (Week 1)
- [ ] OR/XOR constant simplification (boolean_simplication.py)
- [ ] String constant folding (constant_folding.py)
- [ ] Absorption laws (boolean_simplication.py)
- [ ] Comparison chain reduction (predicate_rewriter.py)
- [ ] Add tests for all new cases

### Phase 2: Medium Effort (Week 2)
- [ ] Range predicate combination (predicate_rewriter.py)
- [ ] De Morgan's generalization (boolean_simplication.py)
- [ ] Expression simplification strategy (NEW)
- [ ] Common subexpression detection (NEW)
- [ ] Math expression normalization (predicate_rewriter.py)

### Phase 3: Complex Optimizations (Week 3-4)
- [ ] Selectivity-aware ordering (requires stats infrastructure)
- [ ] Subquery unnesting (complex logic)
- [ ] Predicate dependency analysis
- [ ] Enhanced join predicate extraction

### Phase 4: Infrastructure (Ongoing)
- [ ] Column statistics collection
- [ ] Cardinality estimation
- [ ] Index capability checking

---

## VI. Testing Strategy

For each new simplification, add:

1. **Unit Test**: Direct optimizer test
   ```python
   # Test in tests/unit/planner/test_optimizations_invoked.py
   ("SELECT * FROM $t WHERE (a OR b OR c) OR (a OR b)", 
    "optimization_boolean_rewrite_or_simplify"),
   ```

2. **Integration Test**: Edge case with real queries
   ```python
   # Test in tests/integration/sql_battery/test_shapes_edge_cases.py
   ("SELECT * FROM $planets WHERE id BETWEEN 1 AND 5", 
    9, 20, None),  # should still return same rows
   ```

3. **Correctness Verification**: Same results before/after
   ```python
   # Ensure optimization doesn't change results
   before = opteryx.query("SELECT * FROM t WHERE a = 5 AND a = 5")
   after = opteryx.query("SELECT * FROM t WHERE a = 5")
   assert before.row_count == after.row_count
   ```

4. **Performance Benchmark** (optional)
   ```python
   # Verify improvement for large datasets
   %timeit result = opteryx.query(optimized_query)
   ```

---

## VII. Metrics & Success Criteria

### For New Simplifications

✅ **Success Criteria**:
- [ ] All existing tests still pass
- [ ] New test cases cover the simplification
- [ ] Statistics counter increments correctly
- [ ] No performance regression
- [ ] Documentation updated

### For Strategy Enhancement

✅ **Success Criteria**:
- [ ] Identifies edge cases correctly
- [ ] Doesn't over-optimize (safety first)
- [ ] Clear statistics tracking
- [ ] Comprehensive test coverage
- [ ] Documented with examples

---

## VIII. Code Examples

### Example 1: Add OR Simplification to boolean_simplication.py

```python
# Additional AND simplifications for predicate pushdown
if node.node_type == NodeType.OR:
    # A OR TRUE => TRUE
    if _is_literal_true(node.right) or _is_literal_true(node.left):
        statistics.optimization_boolean_rewrite_or_true += 1
        return build_literal_node(True)
    
    # A OR FALSE => A
    if _is_literal_false(node.right):
        statistics.optimization_boolean_rewrite_or_false += 1
        return node.left
    if _is_literal_false(node.left):
        statistics.optimization_boolean_rewrite_or_false += 1
        return node.right
    
    # A OR A => A (remove redundancy)
    if node.left.uuid == node.right.uuid:
        statistics.optimization_boolean_rewrite_or_redundant += 1
        return node.left
```

### Example 2: Add Range Predicate Combination to predicate_rewriter.py

```python
def rewrite_and_predicates_to_between(predicate):
    """
    Rewrite: (col > 5 AND col < 10) => col BETWEEN 5 AND 10
    """
    # Check if we have col comparison AND col comparison pattern
    if (predicate.node_type != NodeType.AND or
        predicate.left.node_type != NodeType.COMPARISON_OPERATOR or
        predicate.right.node_type != NodeType.COMPARISON_OPERATOR):
        return predicate
    
    left_comp = predicate.left
    right_comp = predicate.right
    
    # Check if both reference same column
    if (left_comp.left.schema_column.identity != 
        right_comp.left.schema_column.identity):
        return predicate
    
    # Check for (col > a AND col < b) or similar patterns
    if (left_comp.value in ('Gt', 'GtEq') and 
        right_comp.value in ('Lt', 'LtEq')):
        # Create BETWEEN node
        between_node = Node(NodeType.COMPARISON_OPERATOR)
        between_node.value = "Between"
        between_node.left = left_comp.left
        between_node.right = Node(NodeType.EXPRESSION_LIST)
        between_node.right.parameters = [left_comp.right, right_comp.right]
        return between_node
    
    return predicate
```

---

## IX. Summary of Recommendations

| Priority | Category | Effort | Impact |
|----------|----------|--------|--------|
| HIGH | OR/XOR Simplification | 30min | High |
| HIGH | Range Predicate Optimization | 1hr | High |
| HIGH | Comparison Chain Reduction | 30min | Medium |
| HIGH | De Morgan's Generalization | 1hr | Medium |
| MEDIUM | String Constant Folding | 2hrs | Medium |
| MEDIUM | Selectivity-Based Ordering | 2hrs | High (needs stats) |
| MEDIUM | Expression Simplification Strategy | 2hrs | Medium |
| LOW | Math Expression Normalization | 1hr | Low |
| LOW | Absorption Laws | 30min | Low |

**Total Estimated Effort**: 15-20 hours for all improvements  
**Expected Performance Improvement**: 5-15% additional optimization for typical queries

---

## X. References & Related Files

- `boolean_simplication.py` - Core boolean logic
- `constant_folding.py` - Expression evaluation
- `predicate_rewriter.py` - Predicate transformations
- `predicate_ordering.py` - Execution order
- `predicate_pushdown.py` - Filter placement
- `tests/unit/planner/test_optimizations_invoked.py` - Optimizer tests
- `tests/integration/sql_battery/test_shapes_edge_cases.py` - Integration tests
