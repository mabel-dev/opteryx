# REGEXP_REPLACE Performance Optimization

## Overview

This document describes the optimization made to the `REGEXP_REPLACE` function to address performance issues identified in Clickbench query #29.

## Problem

The original implementation of `REGEXP_REPLACE` used PyArrow's `compute.replace_substring_regex` function, which was extremely slow for queries that:
1. Process large datasets
2. Use the same regex pattern multiple times (e.g., in both SELECT and GROUP BY clauses)
3. Have complex regex patterns with capture groups

Clickbench query #29 is a prime example:
```sql
SELECT 
    REGEXP_REPLACE(Referer, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') AS k, 
    AVG(length(Referer)) AS l, 
    COUNT(*) AS c, 
    MIN(Referer) 
FROM testdata.clickbench_tiny 
WHERE Referer <> '' 
GROUP BY REGEXP_REPLACE(Referer, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') 
HAVING COUNT(*) > 100000 
ORDER BY l DESC 
LIMIT 25;
```

## Solution

A high-performance Cython implementation was created with the following optimizations:

### 1. Pattern Compilation and Caching
- **Pattern Compilation**: The regex pattern is compiled once and reused for all strings in the array
- **Pattern Caching**: Compiled patterns are cached globally, so when the same pattern is used multiple times in a query (e.g., in SELECT and GROUP BY), it's compiled only once
- **Cache Size Limit**: The cache is limited to 100 patterns to prevent unbounded memory growth

### 2. Efficient C-Level Loops
The Cython implementation uses:
- Static typing with `cdef` declarations
- Disabled bounds checking (`boundscheck=False`)
- Disabled none checking (`nonecheck=False`)
- Direct memory access via NumPy arrays

### 3. Reduced Overhead
- Avoids PyArrow array conversion overhead
- Processes data directly as NumPy arrays
- Minimal Python object creation in the hot loop

### 4. Graceful Fallback
The implementation includes automatic fallback to PyArrow if the compiled version is not available, ensuring compatibility.

## Performance Impact

### Expected Improvements
- **10-100x faster** for large datasets with constant patterns
- **Additional 2-3x improvement** when patterns are reused in the same query
- Particularly significant for queries like Clickbench #29 that use REGEXP_REPLACE in multiple places

### Benchmarking
Performance tests are included in:
- `tests/compiled/test_list_regex_replace.py` - Unit tests for the Cython function
- `tests/performance/test_regexp_replace_performance.py` - Performance benchmarks

To run benchmarks:
```bash
pytest tests/performance/test_regexp_replace_performance.py -v
```

## Implementation Details

### File Structure
- **Implementation**: `opteryx/compiled/list_ops/list_regex_replace.pyx`
- **Integration**: `opteryx/functions/string_functions.py` (updated `regex_replace` function)
- **Tests**: `tests/compiled/test_list_regex_replace.py`
- **Benchmarks**: `tests/performance/test_regexp_replace_performance.py`

### Key Features
1. **Bytes and String Support**: Handles both bytes and string patterns/data
2. **None Handling**: Gracefully handles None values in the data
3. **Error Resilience**: Returns original value if regex replacement fails
4. **Backreference Support**: Fully supports regex backreferences (\\1, \\2, etc.)

### Building
The Cython extension is automatically compiled during the build process:
```bash
make compile
```

Or manually:
```bash
python setup.py build_ext --inplace
```

## Usage

The optimization is transparent to users. Existing queries using `REGEXP_REPLACE` will automatically use the optimized implementation:

```sql
-- This now uses the optimized Cython implementation
SELECT REGEXP_REPLACE(url, '^https?://([^/]+)/.*$', '\\1') 
FROM my_table;
```

## Cache Management

The pattern cache is managed automatically, but can be cleared if needed:

```python
# After the module is built, the function is available from:
from opteryx.compiled.list_ops.function_definitions import clear_regex_cache

# Or if using the compiled module directly:
from opteryx.compiled.list_ops import clear_regex_cache

# Clear the cache
clear_regex_cache()
```

Note: The cache is thread-safe and limited to 100 patterns by default.

## Future Enhancements

Potential future improvements:
1. **Parallel Processing**: Use multiple threads for very large arrays
2. **SIMD Optimization**: Leverage SIMD instructions for pattern matching
3. **Custom Regex Engine**: Consider vendoring a faster regex engine (e.g., RE2 or Hyperscan)
4. **Query-Level Caching**: Cache results for identical inputs within a query execution

## Testing

Run the full test suite:
```bash
# Unit tests
pytest tests/compiled/test_list_regex_replace.py -v

# Performance tests
pytest tests/performance/test_regexp_replace_performance.py -v

# Integration tests
pytest tests/integration/sql_battery/test_shapes_joins_subqueries.py -k REGEXP_REPLACE -v
```

## References

- Original Issue: Clickbench #29 performance problem
- PyArrow regex implementation: Uses RE2 internally
- Python `re` module: Used in the Cython implementation for maximum compatibility
