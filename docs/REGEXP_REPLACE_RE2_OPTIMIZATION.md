# REGEXP_REPLACE Performance Optimization with google-re2

## Problem

Clickbench query #29 was extremely slow (3 minutes for 100M rows) due to REGEXP_REPLACE using Python's standard `re` module, which made 66M+ calls to `expand_template` and 81M+ calls to `_subx`.

## Solution

Replaced the regex engine with **google-re2**, which provides Python bindings to Google's RE2 C++ library.

### Why google-re2?

1. **Much faster than Python's re**: RE2 is a C++ library optimized for speed and safety
2. **Used by DuckDB**: DuckDB achieves excellent performance with RE2
3. **Same features**: Supports backreferences and all regex features needed
4. **Proven**: Google uses RE2 in production at massive scale

## Implementation

The optimization is in `opteryx/functions/string_functions.py`:

```python
def regex_replace(array, _pattern, _replacement):
    """
    Optimized regex replacement using google-re2.
    Falls back to PyArrow if google-re2 is not available.
    """
    try:
        import re2
        # ... use re2 for fast pattern matching ...
    except ImportError:
        # Fallback to PyArrow
        return compute.replace_substring_regex(array, _pattern[0], _replacement[0])
```

### Key Features

- **Pattern compiled once**: The regex pattern is compiled once per function call
- **No extra overhead**: Direct processing without unnecessary conversions
- **Graceful fallback**: Uses PyArrow if google-re2 is not installed
- **Handles bytes and strings**: Works with both data types correctly

## Performance Impact

Based on profiling data from @joocer:
- **Before**: 3 minutes for 100M rows (66M template expansions)
- **Expected**: 10-50x faster with RE2 (similar to DuckDB performance)

The bottleneck was the Python `re` module's `expand_template` and `_subx` functions, which are eliminated by using RE2's native C++ implementation.

## Installation

google-re2 is now a core dependency in `pyproject.toml`:

```toml
dependencies = [..., 'google-re2', ...]
```

Install with:
```bash
pip install google-re2
```

## Testing

Run tests with:
```bash
pytest tests/integration/sql_battery/test_regexp_replace_re2.py -v
```

## Benchmarking

To benchmark performance improvement:

```python
import time
import opteryx

query = """
SELECT 
    REGEXP_REPLACE(Referer, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') AS k,
    COUNT(*) AS c
FROM testdata.clickbench_tiny
WHERE Referer <> ''
GROUP BY k
"""

start = time.time()
result = opteryx.query(query)
_ = list(result)
elapsed = time.time() - start
print(f"Query completed in {elapsed:.2f}s")
```

## References

- [google-re2 PyPI](https://pypi.org/project/google-re2/)
- [RE2 documentation](https://github.com/google/re2)
- [DuckDB regex implementation](https://github.com/duckdb/duckdb) (uses RE2)
