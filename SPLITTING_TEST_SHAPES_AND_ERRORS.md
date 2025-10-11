# Suggestions for Splitting test_shapes_and_errors_battery.py

## Current State
- **File**: `tests/integration/sql_battery/test_shapes_and_errors_battery.py`
- **Size**: 2,647 lines
- **Test Cases**: ~2,123 test statements
- **Logical Sections**: ~73 comment-delineated sections
- **Current Execution**: ~10 seconds (via `make t` / `make test-quick`)

## Problem
The file has grown very large and is becoming difficult to maintain. We need to split it while:
1. Keeping execution time fast (~10 seconds)
2. Maintaining the quick test capability (`make t`)
3. Preserving logical organization

## Analysis of Content

### Largest Test Sections
```
636 tests: Test Aliases
369 tests: Test infix calculations
188 tests: NUMPY types not handled by sqlalchemy
140 tests: Aggregate Functions with HAVING Clause
124 tests: Tests with comments in different parts of the query
 86 tests: TEST VIEWS
 84 tests: Some tests of the same query in different formats
 60 tests: V2 New Syntax Checks
 60 tests: We rewrite expressions like this, make sure all variations work
 49 tests: AGG (FUNCTION)
 45 tests: Randomly generated queries (repeated 3x for $planets, testdata, iceberg)
```

## Recommended Approach: Modular Test Files

### Option 1: Split by Feature Area (Recommended)

Split into 5-7 focused test files that can run independently:

1. **test_shapes_basic.py** (~300 tests, ~1-2 seconds)
   - Dataset shape validation
   - Basic SELECT, WHERE, ORDER BY
   - Simple operators and comparisons
   - Error detection validation

2. **test_shapes_operators_expressions.py** (~600 tests, ~2-3 seconds)
   - Test infix calculations (369 tests)
   - Operator variations
   - Expression rewriting tests
   - Comments in queries (124 tests)

3. **test_shapes_aliases_distinct.py** (~700 tests, ~3-4 seconds)
   - Test Aliases (636 tests)
   - DISTINCT operations
   - Column aliasing edge cases

4. **test_shapes_functions_aggregates.py** (~300 tests, ~1-2 seconds)
   - TEST FUNCTIONS (27 tests)
   - AGG (FUNCTION) (49 tests)
   - Aggregate Functions with HAVING (140 tests)
   - Function filtering

5. **test_shapes_joins_subqueries.py** (~150 tests, ~1 second)
   - JOIN variations
   - Subqueries
   - CTEs and views
   - UNION operations

6. **test_shapes_data_sources.py** (~200 tests, ~1-2 seconds)
   - Iceberg connector tests
   - Parquet tests
   - Multiple data source variations
   - NUMPY types (188 tests)

7. **test_shapes_edge_cases.py** (~200 tests, ~1 second)
   - V2 Negative Tests
   - Edge cases and error conditions
   - Specific bug regression tests
   - Temporal filtering edge cases

### Implementation Strategy

```python
# Create a common test infrastructure file
# tests/integration/sql_battery/shapes_battery_common.py

import os
import pytest
import sys
from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import *
from opteryx.connectors import IcebergConnector, DiskConnector, SqlConnector
from opteryx.managers.schemes import MabelPartitionScheme

def setup_test_environment():
    """Common setup for all shape tests"""
    from tests import set_up_iceberg
    iceberg = set_up_iceberg()
    opteryx.register_store("iceberg", connector=IcebergConnector, catalog=iceberg)
    opteryx.register_store(
        "testdata.partitioned", DiskConnector, partition_scheme=MabelPartitionScheme
    )
    opteryx.register_store(
        "sqlite",
        SqlConnector,
        remove_prefix=True,
        connection="sqlite:///testdata/sqlite/database.db",
    )

@pytest.fixture(scope="module", autouse=True)
def setup_stores():
    """Pytest fixture for environment setup"""
    setup_test_environment()
    yield

def run_shape_test(statement: str, rows: int, columns: int, exception: Optional[Exception]):
    """Common test execution logic"""
    try:
        result = opteryx.query_to_arrow(statement, memberships=["Apollo 11", "opteryx"])
        actual_rows, actual_columns = result.shape
        assert rows == actual_rows, f"Query returned {actual_rows} rows but {rows} were expected.\n{statement}"
        assert columns == actual_columns, f"Query returned {actual_columns} cols but {columns} were expected.\n{statement}"
        assert exception is None, f"Exception {exception} not raised but expected\n{statement}"
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{statement}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error
```

Then each test file follows this pattern:

```python
# tests/integration/sql_battery/test_shapes_basic.py
import pytest
from typing import Optional
from .shapes_battery_common import run_shape_test

# fmt:off
STATEMENTS = [
    # Are the datasets the shape we expect?
    ("SELECT * FROM $satellites", 177, 8, None),
    ("SELECT * FROM $planets", 9, 20, None),
    # ... rest of basic tests
]
# fmt:on

@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_shapes_basic(statement: str, rows: int, columns: int, exception: Optional[Exception]):
    """Test basic query shapes"""
    run_shape_test(statement, rows, columns, exception)

if __name__ == "__main__":
    # Same standalone execution pattern as current file
    import shutil
    import time
    from tests import trunc_printable
    from opteryx.utils.formatter import format_sql
    from shapes_battery_common import setup_test_environment
    
    setup_test_environment()
    # ... existing main execution code
```

### Option 2: Keep Single File, Split Test Data

Keep one test file but move test data to separate files:

```
tests/integration/sql_battery/
├── test_shapes_and_errors_battery.py  (main test runner)
└── shapes_test_data/
    ├── __init__.py
    ├── basic_queries.py
    ├── operators.py
    ├── aliases.py
    ├── functions.py
    ├── joins.py
    ├── data_sources.py
    └── edge_cases.py
```

Each data file exports a `STATEMENTS` list, and the main test file imports and concatenates them.

### Option 3: Use pytest markers for selective execution

Keep the current file but add pytest markers:

```python
@pytest.mark.basic
@pytest.mark.parametrize("statement, rows, columns, exception", BASIC_STATEMENTS)
def test_shapes_basic(...):
    ...

@pytest.mark.operators
@pytest.mark.parametrize("statement, rows, columns, exception", OPERATOR_STATEMENTS)
def test_shapes_operators(...):
    ...
```

Then run subsets with: `pytest -m basic` or `pytest -m "basic or operators"`

## Maintaining `make t` / `make test-quick`

### For Option 1 (Recommended):
Update Makefile:

```makefile
test-quick: ## Run quick test (alias: t)
	@clear
	@echo "Running shape tests battery..."
	@$(PYTHON) -c "import subprocess, sys; \
	    files = ['test_shapes_basic.py', 'test_shapes_operators_expressions.py', \
	             'test_shapes_aliases_distinct.py', 'test_shapes_functions_aggregates.py', \
	             'test_shapes_joins_subqueries.py', 'test_shapes_data_sources.py', \
	             'test_shapes_edge_cases.py']; \
	    for f in files: \
	        ret = subprocess.call([sys.executable, f'tests/integration/sql_battery/{f}']); \
	        if ret != 0: sys.exit(ret)"
```

Or create a simple test runner script:

```python
# tests/integration/sql_battery/run_shapes_battery.py
import subprocess
import sys
import time

test_files = [
    'test_shapes_basic.py',
    'test_shapes_operators_expressions.py',
    'test_shapes_aliases_distinct.py',
    'test_shapes_functions_aggregates.py',
    'test_shapes_joins_subqueries.py',
    'test_shapes_data_sources.py',
    'test_shapes_edge_cases.py',
]

start = time.time()
failed = []

for test_file in test_files:
    print(f"\n▶ Running {test_file}...")
    ret = subprocess.call([sys.executable, f'tests/integration/sql_battery/{test_file}'])
    if ret != 0:
        failed.append(test_file)

print(f"\n✅ Completed in {time.time() - start:.2f}s")
if failed:
    print(f"❌ Failed: {', '.join(failed)}")
    sys.exit(1)
```

Then update Makefile:
```makefile
test-quick: ## Run quick test (alias: t)
	@clear
	@$(PYTHON) tests/integration/sql_battery/run_shapes_battery.py
```

## Benefits of Option 1 (Modular Files)

1. **Easier Navigation**: Each file focuses on specific functionality
2. **Parallel Testing**: Files can be run in parallel with pytest-xdist
3. **Selective Testing**: Developers can run just the relevant subset
4. **Better Organization**: Related tests are grouped together
5. **Easier Maintenance**: Smaller files are easier to review and update
6. **Same Speed**: Total execution time remains ~10 seconds
7. **Git History**: Changes to specific areas don't pollute the whole file's history

## Migration Steps

1. Create `shapes_battery_common.py` with shared infrastructure
2. Create individual test files starting with smallest sections
3. Move test cases from original file to new files
4. Verify each new file runs correctly standalone
5. Update Makefile or create runner script
6. Run full battery to ensure nothing is missed
7. Delete original file once all tests are migrated and verified
8. Update documentation

## Backward Compatibility

- Keep the same pytest test discovery pattern
- Ensure `pytest tests/integration/sql_battery/` still finds all tests
- Maintain the same test names for CI/CD integration
- Keep standalone execution capability (`python test_file.py`)

## Recommended Choice

**Option 1 (Modular Files)** is the most maintainable long-term solution. It provides:
- Clear separation of concerns
- Easy selective testing
- Better git history
- Parallelization opportunities
- Matches the pattern of other test files in the repository (e.g., `test_run_only_battery.py`)

The split can be done incrementally, moving one logical section at a time, which reduces risk and allows for validation at each step.
