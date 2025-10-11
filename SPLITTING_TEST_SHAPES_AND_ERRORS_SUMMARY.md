# Test File Splitting - Quick Summary

## Question
How can we split `test_shapes_and_errors_battery.py` while keeping it as a quick test (`make t`) that runs in ~10 seconds?

## Answer
See **[SPLITTING_TEST_SHAPES_AND_ERRORS.md](SPLITTING_TEST_SHAPES_AND_ERRORS.md)** for the full analysis and recommendations.

## TL;DR - Recommended Approach

**Split into 7 modular test files** organized by feature area:

| File | Tests | Time | Focus |
|------|-------|------|-------|
| `test_shapes_basic.py` | ~300 | 1-2s | Basic queries, dataset shapes |
| `test_shapes_operators_expressions.py` | ~600 | 2-3s | Operators, expressions, infix calculations |
| `test_shapes_aliases_distinct.py` | ~700 | 3-4s | Aliases, DISTINCT operations |
| `test_shapes_functions_aggregates.py` | ~300 | 1-2s | Functions, aggregates, HAVING |
| `test_shapes_joins_subqueries.py` | ~150 | 1s | JOINs, subqueries, CTEs, UNIONs |
| `test_shapes_data_sources.py` | ~200 | 1-2s | Iceberg, Parquet, NUMPY types |
| `test_shapes_edge_cases.py` | ~200 | 1s | Edge cases, errors, regressions |

**Total**: ~2,450 tests in ~10 seconds (same as current)

## Key Benefits
- ✅ Easier to navigate and maintain
- ✅ Can run specific test subsets during development
- ✅ Better git history (changes don't touch entire file)
- ✅ Enables parallel test execution
- ✅ Same total execution time
- ✅ `make t` still works (with simple runner script)

## Implementation
Create a common infrastructure file (`shapes_battery_common.py`) and split tests into focused files. Update `make test-quick` to run all files in sequence or use a simple runner script.

See the full document for:
- Detailed implementation examples
- Alternative approaches (data split, pytest markers)
- Migration steps
- Makefile updates
