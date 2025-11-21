# FOR Clause Parsing in Opteryx

## Overview

Opteryx supports temporal filtering using FOR clauses - a non-standard SQL extension that allows querying data at specific points in time or time ranges.

## Syntax

```sql
-- Single point in time
SELECT * FROM planets FOR TODAY
SELECT * FROM planets FOR '2020-01-01'

-- Date range
SELECT * FROM planets FOR DATES BETWEEN '2020-01-01' AND '2020-12-31'
SELECT * FROM planets FOR DATES BETWEEN YESTERDAY AND TODAY

-- Named ranges
SELECT * FROM planets FOR DATES IN THIS_MONTH
SELECT * FROM planets FOR DATES IN LAST_MONTH

-- Relative ranges
SELECT * FROM planets FOR DATES SINCE '2020-01-01'
SELECT * FROM planets FOR LAST 7 DAYS

-- Multiple tables with different temporal filters
SELECT * FROM planets FOR TODAY
  INNER JOIN satellites FOR YESTERDAY  
  ON planets.id = satellites.planet_id
```

## Implementation

### Current Approach (Python)

The FOR clause is currently parsed using a Python-based approach in `opteryx/planner/sql_rewriter.py`:

1. **SQL Rewriting** (`do_sql_rewrite`):
   - Uses regex to split SQL into parts while preserving quoted strings
   - Handles special string prefixes (`b""` for binary, `r""` for raw strings)
   - Removes SQL comments

2. **Temporal Extraction** (`extract_temporal_filters`):
   - Uses a state machine to identify table references and their FOR clauses
   - Handles special cases like functions that use FROM keyword (EXTRACT, SUBSTRING, TRIM)
   - Tracks nested subqueries and multiple table references
   - Returns cleaned SQL (without FOR clauses) and a list of temporal filters

3. **AST Binding** (`temporal_range_binder` in `ast_rewriter.py`):
   - Adds temporal information back into the parsed AST
   - Binds start_date and end_date to table references
   - Handles various table reference formats (Table, table_name, parent_name, ShowCreate)

### Why Not Native Parser Support?

Adding native FOR clause support to the SQL parser (sqlparser-rs) would be ideal but faces challenges:

1. **External Dependency**: sqlparser-rs is a third-party crate. Modifying it would require either:
   - Forking the repository (maintenance burden)
   - Contributing changes upstream (slow, may not align with project goals)
   - Using local patches (fragile)

2. **Dialect Limitations**: The sqlparser-rs Dialect trait provides limited extension points:
   - `parse_infix`: For custom infix operators (not applicable)
   - `parse_prefix`: For custom prefix operators (not applicable)  
   - `parse_statement`: For custom statements (too coarse-grained)
   - No hook for extending table reference parsing

3. **AST Modifications**: Adding FOR clause support would require:
   - Extending TableFactor::Table with new fields
   - Modifying the parser's `parse_table_factor` function
   - Ensuring serialization/deserialization works with Python

### Future Directions

There are several potential paths forward:

#### Option 1: Rust Implementation of Current Approach
- Port the Python regex and state machine logic to Rust
- Expose as a function callable from Python
- Benefits: Performance, type safety, reduced Python complexity
- Challenges: Complex porting effort, need to maintain parity

**Status**: Proof-of-concept started in `src/temporal_parser.rs`

#### Option 2: Fork sqlparser-rs
- Fork the sqlparser-rs repository
- Add native FOR clause support
- Use the fork via git dependency in Cargo.toml
- Benefits: Clean parser integration, proper AST support
- Challenges: Maintenance burden, staying in sync with upstream

#### Option 3: Use Existing Extension Points  
- Convert FOR clauses to WITH hints during preprocessing
- Example: `FROM table FOR TODAY` → `FROM table WITH(__TEMPORAL__='TODAY')`
- sqlparser-rs already supports WITH hints
- Extract hints after parsing and convert to temporal filters
- Benefits: Uses standard SQL syntax, minimal changes
- Challenges: Slightly awkward, requires coordination between preprocessing and post-processing

#### Option 4: Keep Current Approach
- The current Python implementation works well
- It's well-tested and handles many edge cases
- Focus efforts on other improvements
- Benefits: No risk, proven solution
- Challenges: Python complexity remains

## Recommendations

For now, the Python implementation should remain the authoritative version because:

1. It's well-tested and handles all edge cases
2. It's proven in production
3. The complexity of a complete Rust port is significant
4. The performance benefit may not justify the porting effort

If native parser support becomes a priority:

1. Start with Option 3 (WITH hints) as a low-risk experiment
2. If successful, consider Option 2 (fork) for long-term maintainability  
3. Option 1 (Rust port) could be done incrementally as an optimization

## Related Files

- `opteryx/planner/sql_rewriter.py` - Current implementation
- `opteryx/planner/ast_rewriter.py` - AST binding logic
- `src/temporal_parser.rs` - Proof-of-concept Rust version
- `tests/unit/planner/test_temporal_extraction.py` - Test suite
