# Implementation Plan: Predicate Compaction Strategy

## Overview

The predicate compaction strategy prototype exists in `/opteryx/planner/optimizer/bench/predicate_compaction_strategy.py` and needs to be integrated into the main optimizer.

**What it does**: Tracks value ranges as predicates are applied, compacting redundant predicates into simplified range expressions.

---

## Prototype Analysis

### What the Prototype Implements

The prototype provides a `ValueRange` class that:
- Tracks lower and upper bounds of ranges
- Handles predicates: `=`, `>=`, `>`, `<=`, `<`
- Detects unsupported/untrackable predicates (LIKE, etc.)
- Simplifies predicate chains by keeping only the most restrictive bounds

### Example Flow
```
Initial: col
Apply >3:  col > 3
Apply <10: col > 3 AND col < 10
Apply =7:  col = 7 (most restrictive)
Apply <8:  col = 7 (equality takes precedence)
Apply <9:  col = 7 (no change)

Result: Single consolidated range expressing all constraints
```

### Key Classes

```python
@dataclass
class Limit:
    value: Optional[int]        # The bound value
    inclusive: bool             # Whether inclusive (<=, >=, =) or exclusive (<, >)

@dataclass
class ValueRange:
    lower: Limit = None         # Lower bound
    upper: Limit = None         # Upper bound
    untrackable: bool = False   # True if non-numeric predicates mixed in
```

---

## Integration Strategy

### Where to Implement

**Primary location**: Create `opteryx/planner/optimizer/strategies/predicate_compaction.py`

**Why separate file**:
- It's a distinct optimization strategy (separate from boolean simplification)
- Follows the existing pattern of one strategy per file
- Can be enabled/disabled independently
- Easier to maintain and test

### How It Works with Other Strategies

```
Boolean Simplification     → Normalizes conditions (AND TRUE, De Morgan's, etc.)
         ↓
Predicate Compaction       → Compacts numeric predicates into ranges
         ↓
Predicate Rewriter         → Converts ranges to SQL constructs (BETWEEN, etc.)
         ↓
Predicate Pushdown         → Pushes down compacted predicates
```

---

## Implementation Roadmap

### Phase 1: Foundation (Extract to Strategy Class)
1. Copy `ValueRange` and `Limit` classes to new strategy file
2. Create `PredicateCompactionStrategy` class following OptimizationStrategy pattern
3. Implement logic to:
   - Traverse filter expressions
   - Extract predicates by column
   - Build ValueRange for each column
   - Compare/simplify predicate chains
4. Add statistics tracking

### Phase 2: Integration (Hook into Optimizer)
1. Register strategy in optimizer pipeline (after boolean simplification)
2. Add to `__init__.py` in strategies directory
3. Implement `visit()` method to process Filter nodes
4. Generate optimized predicate nodes from ValueRange

### Phase 3: SQL Generation (Convert Ranges to Predicates)
1. Convert ValueRange back to predicate nodes:
   - `ValueRange(lower=3, upper=10)` → `col > 3 AND col < 10`
   - `ValueRange(lower=5, upper=5, inclusive=True)` → `col = 5`
2. Consider BETWEEN optimization for ranges
3. Preserve original predicate semantics

### Phase 4: Testing & Validation
1. Unit tests for ValueRange logic (already in prototype)
2. Integration tests for strategy invocation
3. Edge cases:
   - Mixed AND/OR conditions
   - Contradictory predicates (e.g., col > 10 AND col < 5)
   - Non-numeric columns
   - NULL handling

---

## Implementation Details

### Core Logic

```python
from opteryx.managers.expression import NodeType
from opteryx.models import Node, QueryStatistics
from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType

class PredicateCompactionStrategy(OptimizationStrategy):
    """
    Compact multiple predicates on the same column into simplified ranges.
    
    Example:
    - Input:  col > 5 AND col < 10 AND col > 7 AND col < 9
    - Output: col > 7 AND col < 9
    """
    
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Filter:
            # Extract and compact predicates
            predicates_by_column = self._extract_predicates(node.condition)
            
            for col_id, predicates in predicates_by_column.items():
                # Build value range from all predicates on this column
                value_range = ValueRange()
                for op, value in predicates:
                    value_range.update_with_predicate(op, value)
                
                # Check if compaction reduced complexity
                if len(predicates) > 1:
                    self.statistics.optimization_predicate_compaction += 1
            
            # Rebuild filter expression from compacted ranges
            node.condition = self._rebuild_filter(predicates_by_column)
            context.optimized_plan[context.node_id] = node
        
        return context
    
    def _extract_predicates(self, node: LogicalPlanNode) -> dict:
        """Extract all predicates grouped by column"""
        # Returns: {column_id: [(operator, value), ...]}
        pass
    
    def _rebuild_filter(self, predicates_by_column: dict) -> LogicalPlanNode:
        """Rebuild filter expression from compacted predicates"""
        pass
```

---

## Benefits

### Query Complexity Reduction
```
Before: col > 5 AND col < 10 AND col > 7 AND col < 9 AND col < 8
After:  col > 7 AND col < 8
```

### Plan Optimization
- Fewer predicates to evaluate in memory
- More compact representation for predicate pushdown
- Better storage layer filtering

### Performance Impact
- Estimated: 5-10% improvement for range-heavy queries
- No impact for non-numeric or single predicates
- Low risk (conservative simplification only)

---

## Testing Strategy

### Unit Tests
```python
def test_compaction_basic():
    """col > 5 AND col > 10 => col > 10"""
    # Apply predicates, verify compaction
    
def test_compaction_range():
    """col > 5 AND col < 10 => kept as-is"""
    # Verify non-overlapping bounds preserved
    
def test_compaction_contradiction():
    """col > 10 AND col < 5 => FALSE"""
    # Detect impossible conditions early
    
def test_compaction_untrackable():
    """col = 5 AND col LIKE '%pattern' => kept separate"""
    # Don't compact mixed types
```

### Integration Tests
```python
def test_with_boolean_simplification():
    """Verify strategy works well with boolean simplification"""
    # Test with De Morgan's and other simplifications
    
def test_with_predicate_pushdown():
    """Verify compacted predicates can be pushed down"""
    # End-to-end query optimization
```

---

## Risk Assessment

| Risk | Mitigation | Status |
|------|-----------|--------|
| Incorrect range logic | Comprehensive unit tests | ✅ Prototype has tests |
| Missed predicates | Systematic traversal of AST | ✅ Need implementation |
| Type errors | Only handle numeric types | ✅ Clear boundaries |
| Contradictions | Detect and mark as untrackable | ✅ Prototype does this |
| Edge cases | Test with real queries | ✅ Plan for Phase 4 |

**Overall Risk Level**: LOW (conservative logic, well-tested prototype)

---

## Files to Modify/Create

### Create (New)
- `opteryx/planner/optimizer/strategies/predicate_compaction.py` (350-400 lines)

### Modify (Existing)
- `opteryx/planner/optimizer/strategies/__init__.py` - Register new strategy
- `opteryx/models/__init__.py` or stats file - Add new statistic counter
- `tests/unit/planner/test_optimizations_invoked.py` - Add test cases

### Reference (Already Exist)
- `opteryx/planner/optimizer/bench/predicate_compaction_strategy.py` (prototype)

---

## Timeline Estimate

- **Phase 1 (Foundation)**: 1-2 hours
- **Phase 2 (Integration)**: 30-45 minutes  
- **Phase 3 (SQL Generation)**: 1 hour
- **Phase 4 (Testing)**: 1-2 hours

**Total**: 4-5.5 hours

---

## Next Steps

1. **Immediate**: Extract prototype into strategy class
2. **Implement**: Hook into optimizer pipeline
3. **Generate**: Convert ValueRange back to SQL predicates
4. **Test**: Unit + integration tests
5. **Validate**: Run full test suite

---

## Questions for Clarification

- [ ] Should we handle non-numeric types (strings, dates)?
- [ ] Priority: Implement Phase 1 & 2 first, or wait for full implementation?
- [ ] Any specific query patterns where this is particularly valuable?
- [ ] Should untrackable ranges fail safely or aggressive simplify?

