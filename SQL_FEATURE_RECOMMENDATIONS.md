# SQL Language Feature Recommendations for Opteryx

**Date:** October 2025  
**Review Base:** sqlparser-rs v0.59.0 Dialect trait  
**Reviewer:** Analysis of available SQL language features in sqlparser-rs repository

## Executive Summary

After reviewing the sqlparser-rs Dialect trait (v0.59.0) and analyzing Opteryx's current implementation, this document provides a prioritized list of SQL language features recommended for addition to Opteryx. The recommendations are based on:

1. User value and common SQL use cases
2. Alignment with Opteryx's analytical query focus
3. Implementation complexity vs. benefit
4. Compatibility with existing Python execution engine capabilities

**Conclusion:** Opteryx already has a solid SQL foundation covering core DML operations, joins, aggregations, and modern features like array operators and PartiQL. Only 5 additional features are recommended as the current language base is already quite comprehensive.

---

## Current Opteryx SQL Support

Opteryx already supports a robust set of SQL features:

### Core Features
- ✅ Basic SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
- ✅ JOIN operations (INNER, LEFT, RIGHT, CROSS)
- ✅ Aggregation functions with FILTER clause
- ✅ Set operations (UNION, INTERSECT, EXCEPT)
- ✅ Subqueries in FROM clause
- ✅ Common table expressions (WITH/CTE)

### Modern Features
- ✅ Array operations (@>, @>>)
- ✅ SELECT * EXCEPT (column)
- ✅ PartiQL-style subscripting (field['key'])
- ✅ Numeric literals with underscores (10_000_000)
- ✅ MATCH() AGAINST() for text search
- ✅ Custom operators (DIV)

---

## Recommended Feature Additions (Top 5)

### Priority 1: Window Functions with Named Window References ⭐⭐⭐

**Dialect Method:** `supports_window_clause_named_window_reference`

**SQL Example:**
```sql
SELECT *, 
       ROW_NUMBER() OVER w1,
       AVG(price) OVER w1
FROM products
WINDOW w1 AS (PARTITION BY category ORDER BY price DESC);
```

**Rationale:**
- Critical for analytical queries (ranking, running totals, lag/lead)
- Commonly used in business intelligence and reporting
- Named windows improve query readability and reduce duplication
- Opteryx's current code shows window function infrastructure exists but named window references are not dialect-enabled
- High user demand for analytical features

**Implementation Impact:** MEDIUM
- Parser already supports window functions
- Need to enable dialect flag and test
- May require minor planner updates

**Use Cases:**
- Customer purchase ranking within categories
- Running totals and moving averages
- Time-series analysis with lag/lead
- Top-N queries per group

---

### Priority 2: Lambda Functions (Higher-Order Functions) ⭐⭐⭐

**Dialect Method:** `supports_lambda_functions`

**SQL Examples:**
```sql
-- Transform array elements
SELECT TRANSFORM(array_col, x -> x * 2) FROM table;

-- Filter array elements
SELECT FILTER(scores, s -> s > 70) FROM students;

-- Reduce/aggregate array
SELECT REDUCE(prices, 0, (acc, x) -> acc + x) FROM products;
```

**Rationale:**
- Modern SQL feature available in BigQuery, Snowflake, DuckDB
- Powerful for array/list transformations without UDFs
- Aligns with Opteryx's support for arrays and complex types
- Reduces need for complex procedural code
- Enhances expressiveness for data transformations

**Implementation Impact:** HIGH
- Requires parser support (available in sqlparser-rs)
- Needs lambda expression evaluation in Python execution engine
- Would unlock powerful array manipulation capabilities
- Consider starting with simple lambda functions on arrays

**Use Cases:**
- Complex array transformations
- Filtering nested data structures
- Map/reduce operations on arrays
- Data cleaning and normalization

---

### Priority 3: Dictionary/Map Literal Syntax ⭐⭐

**Dialect Methods:** `supports_dictionary_syntax` OR `support_map_literal_syntax`

**SQL Examples:**
```sql
-- Dictionary syntax (BigQuery style)
SELECT {'key': 'value', 'num': 123, 'active': true} AS config;

-- Map syntax (Snowflake style)
SELECT Map {1: 'one', 2: 'two', 3: 'three'} AS lookup;

-- Use in WHERE clause
SELECT * FROM events
WHERE metadata = {'source': 'web', 'campaign': 'summer2025'};
```

**Rationale:**
- Opteryx supports STRUCT types and complex data
- Dictionary/map literals complement existing JSON/struct support
- Common in modern analytical databases (BigQuery, Snowflake)
- Useful for ad-hoc data structure creation
- Aligns with PartiQL support already enabled

**Implementation Impact:** MEDIUM
- Parser support available in sqlparser-rs
- Need to map to Python dict/map structures
- Integrates with existing complex type handling

**Use Cases:**
- Configuration objects in queries
- Lookup tables without joins
- Metadata filtering
- Ad-hoc key-value pair creation

---

### Priority 4: GROUP BY Expression Enhancements ⭐⭐

**Dialect Methods:** 
- `supports_group_by_expr` (ROLLUP, CUBE, GROUPING SETS)
- `supports_order_by_all`

**SQL Examples:**
```sql
-- ROLLUP for hierarchical subtotals
SELECT region, product, SUM(sales) as total_sales
FROM sales
GROUP BY ROLLUP(region, product);
-- Generates: (region, product), (region), ()

-- CUBE for all combinations
SELECT year, quarter, product, SUM(revenue)
FROM sales
GROUP BY CUBE(year, quarter, product);

-- GROUPING SETS for specific combinations
SELECT country, city, SUM(sales)
FROM orders
GROUP BY GROUPING SETS ((country, city), (country), ());

-- ORDER BY ALL
SELECT * FROM large_table ORDER BY ALL;
```

**Rationale:**
- ROLLUP/CUBE are standard OLAP operations
- Useful for generating subtotals and cross-tabulations
- ORDER BY ALL simplifies sorting entire result sets
- Opteryx focuses on analytical queries - these are core features
- Reduces complexity of multi-level aggregation queries

**Implementation Impact:** MEDIUM-HIGH
- Parser support exists
- ROLLUP/CUBE require expansion of GROUP BY execution logic
- ORDER BY ALL is simpler - just orders all columns
- Both align well with Opteryx's aggregation capabilities

**Use Cases:**
- Hierarchical reporting (totals, subtotals, grand totals)
- Multi-dimensional analytics
- Pivot table-style aggregations
- OLAP cube operations

---

### Priority 5: IN () Empty List Support ⭐

**Dialect Method:** `supports_in_empty_list`

**SQL Example:**
```sql
-- Returns empty result set instead of error
SELECT * FROM table WHERE column IN ();

-- Useful in dynamic query generation
SELECT * FROM products WHERE category IN ($categories);
-- When $categories is empty, returns no rows instead of failing
```

**Rationale:**
- Handles edge cases in dynamic query generation
- Prevents query errors when parameter lists are empty
- Common issue in programmatically generated SQL
- Simple to implement with high practical value
- Low risk, high convenience feature

**Implementation Impact:** LOW
- Minimal parser changes needed
- Execution engine just returns empty result
- Good candidate for quick win

**Use Cases:**
- Dynamic filtering with optional parameters
- ORM-generated queries
- API-driven query construction
- Batch processing with variable filters

---

## Features NOT Recommended

While sqlparser-rs supports many additional features, the following are NOT recommended for Opteryx at this time as they provide limited value given Opteryx's current solid SQL foundation:

| Feature | Reason Not Recommended |
|---------|------------------------|
| `supports_connect_by` | Hierarchical queries are a niche use case; can be handled with CTEs |
| `supports_match_recognize` | Pattern matching is very complex and rarely used |
| `supports_outer_join_operator` | Oracle's (+) syntax is legacy; standard JOIN syntax is preferred |
| `supports_execute_immediate` | Dynamic SQL execution raises security concerns |
| `supports_dollar_placeholder` | `$1`, `$2` style parameters; prefer named parameters |
| Most dialect-specific syntaxes | Opteryx aims for portable SQL across vendors |
| `supports_table_sample_before_alias` | Minor syntax variation with limited value |
| `supports_user_host_grantee` | MySQL-specific; not relevant to Opteryx's use case |

---

## Implementation Roadmap

### Phase 1: Quick Wins (Low Effort, High Value)
1. ✅ **IN () Empty List Support** - Enable `supports_in_empty_list`
   - Estimated effort: 1-2 days
   - High value for programmatic query generation

### Phase 2: Core Analytical Features (Medium Effort, High Value)
2. ✅ **Named Window References** - Enable `supports_window_clause_named_window_reference`
   - Estimated effort: 1-2 weeks
   - Critical for advanced analytics
   
3. ✅ **Dictionary/Map Literals** - Enable `supports_dictionary_syntax` or `support_map_literal_syntax`
   - Estimated effort: 2-3 weeks
   - Complements existing complex type support

### Phase 3: Advanced Features (High Effort, High Value)
4. ✅ **GROUP BY Enhancements** - Enable `supports_group_by_expr` and `supports_order_by_all`
   - Estimated effort: 3-4 weeks
   - OLAP operations for business intelligence

5. ✅ **Lambda Functions** - Enable `supports_lambda_functions`
   - Estimated effort: 4-6 weeks
   - Most complex but very powerful for array operations

---

## Testing Strategy

For each new feature:

1. **Unit Tests:** Test parser recognizes syntax correctly
2. **Integration Tests:** Verify end-to-end query execution
3. **Edge Cases:** Test boundary conditions and error handling
4. **Performance:** Benchmark against existing alternatives
5. **Documentation:** Update user documentation with examples

---

## References

- [sqlparser-rs Dialect Trait](https://github.com/apache/datafusion-sqlparser-rs/blob/main/src/dialect/mod.rs)
- [Opteryx Dialect Implementation](src/opteryx_dialect.rs)
- [SQL:2023 Standard](https://www.iso.org/standard/76583.html)
- [Modern SQL Features Survey](https://modern-sql.com/)

---

## Detailed Analysis Location

The full detailed analysis with rationale, examples, and implementation notes has been added to the source code in:

**File:** `src/opteryx_dialect.rs`  
**Lines:** 13-160

This keeps the recommendations close to the implementation for easy reference during development.
