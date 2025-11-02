# Quick Implementation Plan: High-Impact Boolean & Predicate Optimizations

## Overview

This document outlines the 5 highest-priority, highest-impact optimizations that can be implemented quickly with minimal risk.

**Total Estimated Time**: 6-8 hours  
**Expected Query Performance Impact**: 8-15% for typical queries  
**Difficulty**: Low to Medium

---

## Priority 1: OR Simplification (30 minutes) ⭐⭐⭐⭐⭐

### Why It Matters
OR expressions are as common as AND expressions in queries. Current implementation only handles AND simplification.

### Changes
**File**: `opteryx/planner/optimizer/strategies/boolean_simplication.py`

Add after AND simplification block:

```python
# Additional OR simplifications for predicate pushdown
if node.node_type == NodeType.OR:
    # A OR TRUE => TRUE (entire expression is satisfied)
    if _is_literal_true(node.right) or _is_literal_true(node.left):
        statistics.optimization_boolean_rewrite_or_true += 1
        return build_literal_node(True)
    
    # A OR FALSE => A (FALSE is no-op in OR)
    if _is_literal_false(node.right):
        statistics.optimization_boolean_rewrite_or_false += 1
        return node.left
    if _is_literal_false(node.left):
        statistics.optimization_boolean_rewrite_or_false += 1
        return node.right
    
    # A OR A => A (remove redundancy)
    if (hasattr(node.left, 'uuid') and hasattr(node.right, 'uuid') and
        node.left.uuid == node.right.uuid):
        statistics.optimization_boolean_rewrite_or_redundant += 1
        return node.left
```

### Test Case
```python
("SELECT * FROM $planets WHERE id > 5 OR id > 5", 9, 20, None),
("SELECT * FROM $planets WHERE id > 100 OR 1=1", 9, 20, None),  # OR TRUE => all rows
("SELECT * FROM $planets WHERE id > 100 OR 1=0", 0, 20, None),  # Only keeps first part
```

### Expected Impact
- Eliminates redundant OR expressions
- Simplifies queries with duplicate conditions
- Prepares for predicate pushdown

---

## Priority 2: Comparison Chain Reduction (30 minutes) ⭐⭐⭐⭐

### Why It Matters
Common pattern: `WHERE col > 5 AND col > 10` should reduce to `col > 10` (more restrictive).

### Changes
**File**: `opteryx/planner/optimizer/strategies/predicate_rewriter.py`

Add new function:

```python
def reduce_comparison_chains(predicate, statistics):
    """
    Reduce redundant comparisons of same column.
    
    Examples:
        col > 5 AND col > 10 => col > 10 (keep most restrictive)
        col < 5 AND col < 10 => col < 5
        col = 5 AND col = 5 => col = 5
    """
    if predicate.node_type != NodeType.AND:
        return predicate
    
    left = predicate.left
    right = predicate.right
    
    # Both must be comparisons on same column
    if (left.node_type != NodeType.COMPARISON_OPERATOR or
        right.node_type != NodeType.COMPARISON_OPERATOR or
        left.left.schema_column.identity != right.left.schema_column.identity):
        return predicate
    
    col_id = left.left.schema_column.identity
    left_op = left.value
    left_val = left.right.value
    right_op = right.value
    right_val = right.right.value
    
    # Rule 1: Both are > or >=, keep the larger value
    if left_op in ('Gt', 'GtEq') and right_op in ('Gt', 'GtEq'):
        if left_val >= right_val:
            statistics.optimization_predicate_rewriter_chain_reduction += 1
            return left  # col > 5 AND col > 10 => col > 10 is returned as right
        else:
            statistics.optimization_predicate_rewriter_chain_reduction += 1
            return right
    
    # Rule 2: Both are < or <=, keep the smaller value
    if left_op in ('Lt', 'LtEq') and right_op in ('Lt', 'LtEq'):
        if left_val <= right_val:
            statistics.optimization_predicate_rewriter_chain_reduction += 1
            return left
        else:
            statistics.optimization_predicate_rewriter_chain_reduction += 1
            return right
    
    # Rule 3: Same equality conditions
    if left_op == 'Eq' and right_op == 'Eq':
        if left_val == right_val:
            statistics.optimization_predicate_rewriter_chain_reduction += 1
            return left  # col = 5 AND col = 5 => col = 5
        # If different values, contradiction (col = 5 AND col = 10 => FALSE)
        statistics.optimization_predicate_rewriter_chain_reduction += 1
        return build_literal_node(False)
    
    return predicate
```

### Test Cases
```python
("SELECT * FROM $planets WHERE id > 5 AND id > 10", 0, 20, None),  # Keep id > 10
("SELECT * FROM $planets WHERE id < 100 AND id < 50", 50, 20, None),  # Keep id < 50
("SELECT * FROM $planets WHERE id = 3 AND id = 3", 1, 20, None),  # Reduce redundancy
("SELECT * FROM $planets WHERE id = 3 AND id = 5", 0, 20, None),  # Contradiction => 0 rows
```

### Expected Impact
- Reduces filter complexity
- Eliminates impossible queries early
- Improves query plan clarity

---

## Priority 3: Range Predicate Combination (45 minutes) ⭐⭐⭐⭐

### Why It Matters
Common query pattern: `WHERE col > 5 AND col < 10` can be expressed as BETWEEN which is often faster.

### Changes
**File**: `opteryx/planner/optimizer/strategies/predicate_rewriter.py`

Add new function:

```python
def combine_range_predicates(predicate, statistics):
    """
    Rewrite: (col > 5 AND col < 10) => col BETWEEN 5 AND 10
    
    Handles:
        col > a AND col < b => col BETWEEN a AND b
        col >= a AND col <= b => col BETWEEN a AND b
        col > a AND col <= b => col BETWEEN a AND b
    """
    if predicate.node_type != NodeType.AND:
        return predicate
    
    left = predicate.left
    right = predicate.right
    
    # Both must be comparisons
    if (left.node_type != NodeType.COMPARISON_OPERATOR or
        right.node_type != NodeType.COMPARISON_OPERATOR):
        return predicate
    
    # Both must reference same column
    if (left.left.schema_column.identity != right.left.schema_column.identity):
        return predicate
    
    col = left.left
    left_op = left.value
    left_val = left.right.value if hasattr(left.right, 'value') else left.right
    right_op = right.value
    right_val = right.right.value if hasattr(right.right, 'value') else right.right
    
    # Pattern 1: (col > a AND col < b) or similar
    if ((left_op in ('Gt', 'GtEq') and right_op in ('Lt', 'LtEq')) or
        (left_op in ('Lt', 'LtEq') and right_op in ('Gt', 'GtEq'))):
        
        # Ensure left is lower bound, right is upper bound
        if left_op in ('Gt', 'GtEq'):
            lower = left
            upper = right
        else:
            lower = right
            upper = left
        
        # Create BETWEEN node
        between_node = Node(NodeType.COMPARISON_OPERATOR)
        between_node.value = "Between"
        between_node.left = col
        
        # Create list of bounds [lower, upper]
        bounds_list = Node(NodeType.EXPRESSION_LIST)
        bounds_list.parameters = [lower.right, upper.right]
        between_node.right = bounds_list
        
        statistics.optimization_predicate_rewriter_range_combination += 1
        return between_node
    
    return predicate
```

### Test Cases
```python
("SELECT * FROM $planets WHERE id > 3 AND id < 7", 3, 20, None),  # 4, 5, 6
("SELECT * FROM $planets WHERE id >= 3 AND id <= 7", 5, 20, None),  # 3, 4, 5, 6, 7
("SELECT * FROM $planets WHERE mass > 0.1 AND mass < 1", 3, 20, None),  # Between works
```

### Expected Impact
- BETWEEN is often better optimized in storage layers
- Clearer query intent
- May enable better statistics-based pushdown

---

## Priority 4: Absorption Laws (30 minutes) ⭐⭐⭐

### Why It Matters
Absorption laws simplify complex nested boolean expressions.

### Changes
**File**: `opteryx/planner/optimizer/strategies/boolean_simplication.py`

Add helper function:

```python
def _nodes_equal(node1, node2) -> bool:
    """Check if two nodes represent the same expression."""
    if node1 is None or node2 is None:
        return False
    # Simple check: same UUID means same node
    return (hasattr(node1, 'uuid') and hasattr(node2, 'uuid') and
            node1.uuid == node2.uuid)

def _apply_absorption_laws(node, statistics):
    """
    Apply absorption laws to simplify expressions.
    
    Laws:
        (A OR B) AND A => A
        (A AND B) OR A => A
        A AND (A OR B) => A
        A OR (A AND B) => A
    """
    if node.node_type != NodeType.AND:
        return None  # Handle AND absorption in parent context
    
    # Pattern: (A OR B) AND A => A
    if node.left.node_type == NodeType.OR:
        if _nodes_equal(node.left.left, node.right):
            statistics.optimization_boolean_rewrite_absorption += 1
            return node.right
        if _nodes_equal(node.left.right, node.right):
            statistics.optimization_boolean_rewrite_absorption += 1
            return node.right
    
    # Pattern: A AND (A OR B) => A
    if node.right.node_type == NodeType.OR:
        if _nodes_equal(node.left, node.right.left):
            statistics.optimization_boolean_rewrite_absorption += 1
            return node.left
        if _nodes_equal(node.left, node.right.right):
            statistics.optimization_boolean_rewrite_absorption += 1
            return node.left
    
    return None  # Not applicable
```

### Test Cases
```python
("SELECT * FROM $planets WHERE (id > 5 OR id < 3) AND id > 5", 4, 20, None),
("SELECT * FROM $planets WHERE id > 5 AND (id > 5 OR id < 3)", 4, 20, None),
```

### Expected Impact
- Simplifies overly complex WHERE clauses
- Makes intent clearer
- Reduces expression evaluation cost

---

## Priority 5: De Morgan's Law Generalization (1 hour) ⭐⭐⭐⭐

### Why It Matters
Current implementation only handles binary AND/OR. Many queries have 3+ conditions.

### Changes
**File**: `opteryx/planner/optimizer/strategies/boolean_simplication.py`

Enhance NOT handling:

```python
def _apply_demorgans_generalized(node, statistics):
    """
    Generalize De Morgan's laws to n-ary AND/OR chains.
    
    NOT(A AND B AND C) => NOT A OR NOT B OR NOT C
    NOT(A OR B OR C) => NOT A AND NOT B AND NOT C
    """
    if node.node_type != NodeType.NOT:
        return None
    
    centre = node.centre
    if centre.node_type == NodeType.NESTED:
        centre = centre.centre
    
    # Handle n-ary AND chain (including chained binary AND)
    if centre.node_type == NodeType.AND:
        # Flatten the AND chain
        conditions = _flatten_and_chain(centre, statistics)
        
        # Apply NOT to each condition
        negated = [Node(NodeType.NOT, centre=cond) for cond in conditions]
        
        # Build OR chain from negated conditions
        if len(negated) == 1:
            result = negated[0]
        else:
            result = negated[0]
            for neg_cond in negated[1:]:
                result = Node(NodeType.OR, left=result, right=neg_cond)
        
        statistics.optimization_boolean_rewrite_demorgan_n_ary += 1
        return result
    
    # Handle n-ary OR chain
    if centre.node_type == NodeType.OR:
        # Flatten the OR chain (need new helper)
        conditions = _flatten_or_chain(centre, statistics)
        
        # Apply NOT to each condition
        negated = [Node(NodeType.NOT, centre=cond) for cond in conditions]
        
        # Build AND chain from negated conditions
        if len(negated) == 1:
            result = negated[0]
        else:
            result = negated[0]
            for neg_cond in negated[1:]:
                result = Node(NodeType.AND, left=result, right=neg_cond)
        
        statistics.optimization_boolean_rewrite_demorgan_n_ary += 1
        return result
    
    return None
```

### Test Cases
```python
("SELECT * FROM $planets WHERE NOT (id > 3 AND id < 7 AND name = 'Earth')", 
 8, 20, None),  # All except {4,5,6} with Earth
```

### Expected Impact
- Handles more realistic complex WHERE clauses
- Enables better predicate pushdown for multi-condition predicates
- Prepares for complex query optimization

---

## Implementation Checklist

### Phase 1: OR Simplification
- [ ] Add OR simplification code to boolean_simplication.py
- [ ] Add statistics counters
- [ ] Add unit test in test_optimizations_invoked.py
- [ ] Add integration tests in test_shapes_edge_cases.py
- [ ] Run full test suite
- [ ] Verify no regressions

### Phase 2: Comparison Chain Reduction
- [ ] Add comparison chain reduction to predicate_rewriter.py
- [ ] Add statistics counters
- [ ] Add tests
- [ ] Run full test suite
- [ ] Verify correctness

### Phase 3: Range Predicate Combination
- [ ] Add range combination logic
- [ ] Add statistics counters
- [ ] Add comprehensive tests (handles all variants)
- [ ] Verify BETWEEN support in execution
- [ ] Run full test suite

### Phase 4: Absorption Laws
- [ ] Add absorption law detection
- [ ] Add statistics counters
- [ ] Add edge case tests
- [ ] Verify with complex expressions
- [ ] Run full test suite

### Phase 5: De Morgan's Generalization
- [ ] Add helper function _flatten_or_chain()
- [ ] Enhance NOT handling for n-ary chains
- [ ] Add statistics counters
- [ ] Add comprehensive tests
- [ ] Run full test suite

### Post-Implementation
- [ ] Update documentation
- [ ] Add examples to OPTIMIZATION_STRATEGY_REVIEW.md
- [ ] Create before/after performance benchmarks
- [ ] Update query optimization guide

---

## Success Metrics

For each optimization:
✅ Tests passing: All 350+ integration tests  
✅ Statistics incremented: Counters show optimization fired  
✅ Correctness: Same results before/after  
✅ No regressions: No performance degradation  
✅ Documentation: Clear examples and rationale  

---

## Estimated Timeline

| Phase | Time | Tasks |
|-------|------|-------|
| 1 | 30min | OR Simplification |
| 2 | 30min | Comparison Chain Reduction |
| 3 | 45min | Range Predicate Combination |
| 4 | 30min | Absorption Laws |
| 5 | 1hr | De Morgan's Generalization |
| Test & Verify | 1hr | Full test suite, edge cases |
| Documentation | 30min | Update guides, examples |
| **Total** | **~4.5 hours** | **5 high-impact optimizations** |

---

## Expected Outcomes

After implementing all 5 optimizations:

**Query Simplification**: Cleaner logical plans visible in EXPLAIN  
**Performance**: 5-10% improvement on typical analytical queries  
**Coverage**: Handles ~90% of common boolean expression patterns  
**Maintainability**: Clear test cases for future enhancements  
**Documentation**: Comprehensive guide for future optimization strategy additions

---

## Questions?

See `OPTIMIZATION_STRATEGY_REVIEW.md` for comprehensive analysis of all 17 strategies and 24+ enhancement opportunities.
