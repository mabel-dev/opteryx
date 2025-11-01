# De Morgan's Extensions Analysis

## Goal
Transform complex nested predicates into simpler AND chains that enable better predicate pushdown.

## Current De Morgan's Implementation

```
NOT(A OR B) => (NOT A) AND (NOT B)  ✅ Already implemented
NOT(A AND B) => (NOT A) OR (NOT B)  ⚠️ Exists but creates OR, doesn't help pushdown
```

## Additional De Morgan's Opportunities for Pushdown

### 1. NOT(A AND B) Handled Within Filter Context

**Current behavior**: 
```
NOT(A AND B) => (NOT A) OR (NOT B)
```
This creates an OR, which is harder to push down.

**Alternative perspective**: When this OR appears in an AND context, can expand:
```
WHERE (NOT(A AND B)) AND (other_condition)
=>
WHERE ((NOT A) OR (NOT B)) AND (other_condition)
```

This is harder to work with. Better: recognize the pattern earlier.

---

### 2. De Morgan's With AND-Heavy Predicates (n-ary)

**Current**: Binary only
```
NOT(A OR B) => (NOT A) AND (NOT B)
```

**Extension to n-ary**:
```
NOT(A OR B OR C) => (NOT A) AND (NOT B) AND (NOT C)
NOT(A OR B OR C OR D) => (NOT A) AND (NOT B) AND (NOT C) AND (NOT D)
```

**Benefit**: Creates long AND chains that are highly pushable!

**Example**:
```
Input:  WHERE NOT(id = 1 OR id = 2 OR id = 3)
Output: WHERE id != 1 AND id != 2 AND id != 3
=> Three separate pushable predicates! 🎯
```

---

### 3. De Morgan's Over Multiple Levels (Nested Application)

**Pattern**: Nested NOT(AND/OR structures)

```
Input:  WHERE NOT(NOT(A AND B) OR C)
Step 1: De Morgan on outer NOT
        => NOT(NOT(A AND B)) AND NOT(C)
Step 2: Simplify NOT(NOT(...))
        => (A AND B) AND NOT(C)
Step 3: Flatten AND chain
        => A AND B AND NOT(C)
=> Three pushable predicates! 🎯

Result: Maximum pushdown opportunity
```

---

### 4. Absorbed Contradictions (Early Detection)

When De Morgan's creates multiple conditions, detect contradictions early:

```
Input:  WHERE NOT(A OR NOT A)
Step 1: Apply De Morgan's
        => NOT(A) AND NOT(NOT(A))
Step 2: Simplify double NOT
        => NOT(A) AND A
Step 3: Recognize contradiction
        => Result is always FALSE
=> Eliminate entire filter! 🎯
```

---

### 5. IN List Expansion (De Morgan's on IN)

**Pattern**: Negated IN becomes multiple inequality predicates

```
Input:  WHERE NOT(col IN (1, 2, 3))
Current: WHERE col NOT IN (1, 2, 3)
        [Treated as single predicate, harder to push]

Better:  WHERE col != 1 AND col != 2 AND col != 3
        [Three separate pushable predicates]
```

**Benefit**: Highly compatible with predicate pushdown

---

### 6. De Morgan's Over BETWEEN

**Pattern**: NOT BETWEEN breaks into inequality range

```
Input:  WHERE NOT(col BETWEEN 5 AND 10)
Step 1: Expand BETWEEN
        => NOT(col >= 5 AND col <= 10)
Step 2: De Morgan's n-ary
        => (col < 5) OR (col > 10)
Step 3: In AND context, could be treated as ranges

Alternative:
Input:  WHERE NOT(col BETWEEN 5 AND 10) AND (other_conditions)
        => (col < 5 OR col > 10) AND (other_conditions)
```

---

### 7. De Morgan's with CASE Expressions (Nested Logic)

**Pattern**: Complex case statements with OR/AND

```
Input:  WHERE NOT(CASE WHEN A OR B THEN x ELSE y END)
Step 1: Apply logic simplification
        => Potentially creates AND chains

Note: Complex, possibly lower priority
```

---

### 8. NULL-Safe De Morgan's (Extended)

**Pattern**: Handling NULL values in De Morgan's transformations

```
Standard De Morgan's: NOT(A OR B) => (NOT A) AND (NOT B)

With NULLs:
- NOT(A OR B) still = (NOT A) AND (NOT B) in SQL three-valued logic
- But requires care with NULL handling

Input:  WHERE NOT(col = 1 OR col = 2)
        If col IS NULL, entire condition is UNKNOWN
        So result is: col != 1 AND col != 2 AND col IS NOT NULL
```

---

## Prioritized Implementation Opportunities

### ⭐⭐⭐ HIGH PRIORITY (Maximum AND Creation)

1. **De Morgan's n-ary** (NOT of multiple ORs)
   - Current: Binary only
   - Needed: Flatten OR chains, apply De Morgan's to all
   - Result: Maximum AND chain length
   - Complexity: Medium (recursive flattening)
   - Impact: Very High

2. **NOT(IN list)** expansion
   - Current: Treated as single predicate
   - Needed: Expand to multiple != predicates
   - Result: Multiple pushable predicates
   - Complexity: Low
   - Impact: High

### ⭐⭐ MEDIUM PRIORITY

3. **Nested NOT simplification** (NOT(NOT(...)))
   - Works with #1 for compound expressions
   - Enables deeper simplifications
   - Complexity: Low
   - Impact: Medium (depends on query patterns)

4. **De Morgan's with BETWEEN**
   - Convert NOT(BETWEEN) to inequality range
   - Complexity: Medium
   - Impact: Medium

### ⭐ LOWER PRIORITY

5. **NULL-safe De Morgan's** - Requires NULL handling logic
6. **Contradiction detection** - Separate concern (absorb into boolean simplification)
7. **Case expression handling** - Complex, lower ROI

---

## Code Structure for Implementation

### Location
File: `opteryx/planner/optimizer/strategies/boolean_simplication.py`

### New Helper Functions Needed

```python
def _flatten_or_chain(node: LogicalPlanNode) -> list:
    """
    Flatten nested OR chains into a list.
    ((A OR B) OR C) => [A, B, C]
    """
    if node.node_type != NodeType.OR:
        return [node]
    return _flatten_or_chain(node.left) + _flatten_or_chain(node.right)


def _rebuild_or_chain(conditions: list) -> LogicalPlanNode:
    """
    Rebuild OR chain from list (or convert to AND if inverted).
    [A, B, C] => ((A OR B) OR C)
    """
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]
    result = conditions[0]
    for cond in conditions[1:]:
        result = Node(NodeType.OR, left=result, right=cond)
    return result


def _apply_nary_demorgan(node: LogicalPlanNode, statistics: QueryStatistics) -> LogicalPlanNode:
    """
    Apply De Morgan's to n-ary OR within a NOT.
    NOT(A OR B OR C) => (NOT A) AND (NOT B) AND (NOT C)
    """
    if node.node_type != NodeType.NOT:
        return node
    
    centre = node.centre
    if centre.node_type == NodeType.NESTED:
        centre = centre.centre
    
    # Only apply if we have an OR chain with 2+ conditions
    if centre.node_type == NodeType.OR:
        conditions = _flatten_or_chain(centre)
        if len(conditions) > 1:
            # Apply NOT to each condition
            not_conditions = [
                Node(NodeType.NOT, centre=cond)
                for cond in conditions
            ]
            # Rebuild as AND chain
            result = not_conditions[0]
            for cond in not_conditions[1:]:
                result = Node(NodeType.AND, left=result, right=cond)
            statistics.optimization_boolean_rewrite_demorgan_nary += 1
            return update_expression_tree(result, statistics)
    
    return node


def _apply_demorgan_in_list(node: LogicalPlanNode, statistics: QueryStatistics) -> LogicalPlanNode:
    """
    Convert NOT IN to multiple != predicates.
    NOT(col IN (a, b, c)) => col != a AND col != b AND col != c
    """
    if node.node_type != NodeType.NOT:
        return node
    
    centre = node.centre
    if centre.node_type != NodeType.COMPARISON_OPERATOR:
        return node
    
    if centre.value == 'NotInList':
        # Extract column and values
        col = centre.left
        values = centre.right.value  # Should be tuple/list
        
        if len(values) > 1:
            # Create != predicates
            ne_predicates = [
                Node(NodeType.COMPARISON_OPERATOR, value='NotEq', left=col, right=Node(NodeType.LITERAL, value=v))
                for v in values
            ]
            # Rebuild as AND chain
            result = ne_predicates[0]
            for pred in ne_predicates[1:]:
                result = Node(NodeType.AND, left=result, right=pred)
            statistics.optimization_boolean_rewrite_demorgan_in_expansion += 1
            return result
    
    return node
```

### Integration Point

In `update_expression_tree()` function, add these handlers:

```python
def update_expression_tree(node: LogicalPlanNode, statistics: QueryStatistics):
    # ... existing NOT handling ...
    
    if node.node_type == NodeType.NOT:
        centre_node = node.centre
        
        # ... existing handlers ...
        
        # NEW: De Morgan's n-ary (NOT of OR chain)
        result = _apply_nary_demorgan(node, statistics)
        if result != node:
            return update_expression_tree(result, statistics)
        
        # NEW: De Morgan's for NOT IN
        result = _apply_demorgan_in_list(node, statistics)
        if result != node:
            return update_expression_tree(result, statistics)
    
    # ... rest of existing logic ...
```

---

## Statistics to Track

```python
# Add to QueryStatistics
optimization_boolean_rewrite_demorgan_nary: int = 0
    # NOT(A OR B OR C) => (NOT A) AND (NOT B) AND (NOT C)

optimization_boolean_rewrite_demorgan_in_expansion: int = 0
    # NOT(col IN (a,b,c)) => col != a AND col != b AND col != c
```

---

## Test Cases

### De Morgan's n-ary

```python
# Test case 1: Simple 3-condition OR
("SELECT * FROM $planets WHERE NOT(id = 1 OR id = 2 OR id = 3)", 6, 20, None),

# Test case 2: 4+ condition OR (maximum pushdown)
("SELECT * FROM $planets WHERE NOT(id = 1 OR id = 2 OR id = 3 OR id = 4)", 5, 20, None),

# Test case 3: Nested NOTs with OR
("SELECT * FROM $planets WHERE NOT(NOT(id > 5 OR id < 2))", 3, 20, None),
```

### De Morgan's IN expansion

```python
# Test case 1: NOT IN becomes multiple !=
("SELECT * FROM $planets WHERE id NOT IN (1, 2, 3)", 6, 20, None),

# Test case 2: NOT IN with more values
("SELECT * FROM $planets WHERE name NOT IN ('Mercury', 'Venus')", 7, 20, None),
```

---

## Expected Benefits

### Query: WHERE NOT(id = 1 OR id = 2 OR id = 3)

**Before** (single complex predicate):
```
Filter: NOT(OR-CHAIN)  
  → Storage layer sees complex operation
  → Can't easily push down
```

**After** (three simple AND predicates):
```
Filter: id != 1 AND id != 2 AND id != 3
  → Storage layer sees three simple predicates
  → Can push all three down
  → Massive performance improvement! 🎯
```

### Estimated Impact
- **For negated OR expressions**: 20-40% improvement (fewer rows passed through)
- **For NOT IN queries**: 15-30% improvement (better column filtering)
- **Overall**: +5-10% for typical queries (those with these patterns)

---

## Implementation Priority

1. **Start with**: De Morgan's n-ary (highest impact, clearest logic)
2. **Then**: NOT IN expansion (simpler, still high value)
3. **Consider**: Nested NOT simplification (depends on query patterns)
4. **Later**: BETWEEN handling (more complex, lower frequency)

