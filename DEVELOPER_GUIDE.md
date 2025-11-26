# Opteryx Developer Guide

Welcome to the Opteryx developer guide! This document provides a comprehensive overview of Opteryx's architecture, development patterns, and best practices for contributors.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Development Setup](#development-setup)
- [Development Patterns](#development-patterns)
- [Testing Strategy](#testing-strategy)
- [Performance Guidelines](#performance-guidelines)
- [Common Development Tasks](#common-development-tasks)
- [Code Style and Standards](#code-style-and-standards)
- [Debugging and Profiling](#debugging-and-profiling)

## Architecture Overview

Opteryx follows a modular query processing pipeline that transforms SQL queries into optimized execution plans:

```
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │ Rewriter  │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │      │ Physical  │
   │ Rewriter  │      │ Catalogue │      │ Planner   │
   └─────┬─────┘      └───────────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │           │
   │ Planner   ├─────►│ Optimizer ├─────►│ Physical  │
   └───────────┘      └───────────┘      │ Planner   │
                                         └───────────┘
```

### Query Processing Flow

1. **SQL Rewriter** - Preprocesses SQL for optimization and standardization
2. **Parser** - Converts SQL to Abstract Syntax Tree (AST) using SqlOxide
3. **AST Rewriter** - Applies AST-level transformations and optimizations
4. **Logical Planner** - Creates a logical query plan from the AST
5. **Optimizer** - Applies rule-based and cost-based optimizations
6. **Physical Planner** - Creates an executable physical plan with specific operators
7. **Executor** - Executes the plan using a tree of operators

## Core Components

### Query Planning (`opteryx.planner`)

The planner module is responsible for converting SQL queries into executable plans:

- **`binder/`** - Binds identifiers and validates query semantics
- **`logical_planner/`** - Creates logical query plans from AST
- **`optimizer/`** - Applies various optimization rules
- **`physical_planner.py`** - Converts logical plans to physical execution plans
- **`sql_rewriter.py`** - Preprocesses SQL queries
- **`ast_rewriter.py`** - Transforms AST nodes

### Execution Operators (`opteryx.operators`)

Physical execution operators that form the execution tree:

- **Data Sources**: `ReaderNode`, `AsyncReaderNode`
- **Joins**: `InnerJoinNode`, `OuterJoinNode`, `CrossJoinNode`, `NestedLoopJoinNode`
- **Filters**: `FilterNode`, `FilterJoinNode`
- **Aggregation**: `AggregateNode`, `AggregateAndGroupNode`, `SimpleAggregateNode`
- **Sorting**: `SortNode`, `HeapSortNode`
- **Utilities**: `ProjectionNode`, `LimitNode`, `DistinctNode`, `UnionNode`

All operators inherit from `BasePlanNode` and implement the iterator pattern.

### Data Connectors (`opteryx.connectors`)

Connectors abstract different data sources behind a common interface:

- **`base/`** - Base connector classes and interfaces
- **File Systems**: `disk_connector.py`, `aws_s3_connector.py`, `gcp_cloudstorage_connector.py`
- **Databases**: `sql_connector.py`, `mongodb_connector.py`, `cql_connector.py`
- **Formats**: `arrow_connector.py`, `file_connector.py`
- **Special**: `iceberg_connector.py`, `virtual_data.py`

### Functions (`opteryx.functions`)

SQL function implementations organized by category:

- **String Functions**: Text manipulation and processing
- **Date Functions**: Temporal operations and calculations
- **Math Functions**: Mathematical operations and calculations
- **Aggregate Functions**: Aggregation operations (SUM, COUNT, etc.)
- **Other Functions**: Utility and specialized functions

Functions are registered in the `FUNCTIONS` dictionary with metadata about return types and cost estimates.

### Managers (`opteryx.managers`)

Resource management and system components:

- **`cache/`** - Query result and metadata caching
- **`buffers/`** - Memory buffer management
- **`expression/`** - Expression evaluation engine

## Development Setup

### Prerequisites

- Python 3.11+
- Rust toolchain (for Rust extensions)
- Git

### Initial Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/mabel-dev/opteryx.git
   cd opteryx
   ```

2. **Install dependencies:**
   ```bash
   pip install -e .
   pip install -r tests/requirements.txt
   ```

3. **Build Rust extensions:**
   ```bash
   make build
   ```

4. **Verify installation:**
   ```bash
   python -c "import opteryx; print(opteryx.query('SELECT 1'))"
   ```

### Development Workflow

```bash
# Run tests
make test

# Check code quality
make lint

# Build Rust components
make build

# Clean build artifacts
make clean
```


### macOS support

We no longer build or support Intel (x86_64) macOS binaries. Apple stopped adding support for Intel macs and our CI and releases build ARM64 mac wheels only.

To build mac wheel locally for Apple Silicon (arm64):

```bash
ARCHFLAGS="-arch arm64" python setup.py bdist_wheel
```
## Development Patterns

### Adding a New Operator

1. **Create the operator class:**
   ```python
   # opteryx/operators/my_operator_node.py
   from opteryx.operators.base_plan_node import BasePlanNode
   
   class MyOperatorNode(BasePlanNode):
       def __init__(self, **config):
           super().__init__(**config)
           # Initialize operator-specific state
       
       def execute(self, morsel):
           # Implement operator logic
           # Return transformed morsel
           pass
   ```

2. **Register in `__init__.py`:**
   ```python
   # Add import
   from .my_operator_node import MyOperatorNode
   
   # Update __all__
   __all__ = [..., "MyOperatorNode"]
   ```

3. **Add tests:**
   ```python
   # tests/unit/operators/test_my_operator_node.py
   def test_my_operator():
       # Test operator functionality
       pass
   ```

### Adding a New Connector

1. **Implement the connector:**
   ```python
   # opteryx/connectors/my_connector.py
   from opteryx.connectors.base import BaseConnector
   
   class MyConnector(BaseConnector):
       def read_dataset(self, dataset, **kwargs):
           # Implement data reading logic
           return pyarrow_table
       
       def get_dataset_schema(self, dataset):
           # Return schema information
           return pyarrow_schema
   ```

2. **Register the connector:**
   ```python
   # In opteryx/connectors/__init__.py
   # Add to _storage_prefixes or register dynamically
   ```

3. **Add integration tests:**
   ```python
   # tests/integration/test_my_connector.py
   def test_my_connector_integration():
       # Test end-to-end functionality
       pass
   ```

### Adding a New Function

1. **Implement the function:**
   ```python
   # In appropriate module under opteryx/functions/
   def my_function(column_data):
       # Function implementation
       return result
   ```

2. **Register in `FUNCTIONS` dictionary:**
   ```python
   # opteryx/functions/__init__.py
   FUNCTIONS = {
       ...
       "MY_FUNCTION": (my_function, "RETURN_TYPE", 1.0),
   }
   ```

3. **Add tests:**
   ```python
   # tests/unit/functions/test_my_function.py
   def test_my_function():
       # Test function behavior
       pass
   ```

## Testing Strategy

### Test Structure

Tests are organized to mirror the source code structure:

```
tests/
├── unit/           # Unit tests for individual components
│   ├── core/       # Core functionality tests
│   ├── operators/  # Operator tests
│   ├── functions/  # Function tests
│   └── planner/    # Planning tests
├── integration/    # End-to-end integration tests
├── performance/    # Performance and benchmark tests
├── fuzzing/        # Fuzz testing
└── verifiers/      # Data verification tests
```

### Test Categories

1. **Unit Tests** - Test individual components in isolation
2. **Integration Tests** - Test complete query workflows
3. **Performance Tests** - Verify performance characteristics
4. **Regression Tests** - Prevent regressions in functionality

### Running Tests

```bash
# All tests
make test

# Specific test files
pytest tests/unit/operators/test_join_node.py

# Performance tests
pytest tests/performance/ -v

# Integration tests
pytest tests/integration/ -v
```

### Test Data

Use the test data generator for consistent test datasets:

```bash
# Generate test data
python tools/testing/data_generators.py --type business --size small
```

## Performance Guidelines

### Memory Management

- Use PyArrow for columnar operations
- Implement memory pooling where appropriate
- Monitor memory usage in operators
- Consider streaming for large datasets

### Optimization Patterns

- **Predicate Pushdown** - Push filters to data sources
- **Column Pruning** - Select only needed columns
- **Vectorization** - Use PyArrow compute functions
- **Parallel Processing** - Leverage async operations where possible

### Profiling

Use the built-in profiling tools:

```bash
# Profile query performance
python tools/analysis/query_profiler.py --query "SELECT * FROM my_table"

# Run benchmarks
python tools/analysis/query_profiler.py --benchmark
```

## Common Development Tasks

### Debugging Query Issues

1. **Enable debug mode:**
   ```bash
   export OPTERYX_DEBUG=1
   ```

2. **Use EXPLAIN:**
   ```sql
   EXPLAIN SELECT * FROM my_table WHERE condition = 'value'
   ```

3. **Check query plans:**
   ```python
   result = opteryx.query("SELECT * FROM table")
   print(result.explain())
   ```

### Adding New SQL Syntax

1. **Update SQL parser** (if needed - usually handled by SqlOxide)
2. **Add AST handling** in the logical planner
3. **Implement operator support**
4. **Add optimizer rules**
5. **Update documentation and tests**

### Performance Optimization

1. **Profile the query:**
   ```python
   import opteryx
   opteryx.config.OPTERYX_DEBUG = True
   result = opteryx.query("YOUR_QUERY")
   ```

2. **Check operator timings**
3. **Verify predicate pushdown**
4. **Optimize data access patterns**

## Code Style and Standards

### Python Style

- Follow PEP 8 guidelines
- Use Black for code formatting (`make format`)
- Use type hints for function signatures
- Write descriptive docstrings

### Code Organization

- One class per file for operators and major components
- Group related functionality in modules
- Use clear, descriptive names
- Keep functions focused and small

### Documentation

- Add docstrings to all public functions and classes
- Include examples in docstrings where helpful
- Update this guide when adding major features
- Document complex algorithms and optimizations

### Error Handling

- Use specific exception types
- Provide helpful error messages
- Include context in error messages
- Log errors appropriately

## Debugging and Profiling

### Debug Mode

Enable debug mode for detailed execution information:

```python
import opteryx
opteryx.config.OPTERYX_DEBUG = True

# Or set environment variable
# export OPTERYX_DEBUG=1
```

### Query Profiling

Use the profiler to identify performance bottlenecks:

```python
from tools.analysis.query_profiler import QueryProfiler

profiler = QueryProfiler()
with profiler.profile_query("My Query"):
    result = opteryx.query("SELECT * FROM large_table")
```

### Memory Debugging

Monitor memory usage during development:

```python
import psutil
import os

process = psutil.Process(os.getpid())
memory_before = process.memory_info().rss / 1024 / 1024
# ... run query ...
memory_after = process.memory_info().rss / 1024 / 1024
print(f"Memory used: {memory_after - memory_before:.1f} MB")
```

## Contributing

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Ensure all tests pass
5. Update documentation
6. Submit pull request

### Review Checklist

- [ ] Tests added for new functionality
- [ ] Documentation updated
- [ ] Code follows style guidelines
- [ ] Performance impact considered
- [ ] Error handling implemented
- [ ] Edge cases covered

## Getting Help

- **Documentation**: https://opteryx.dev/
- **Issues**: https://github.com/mabel-dev/opteryx/issues
- **Discussions**: https://github.com/mabel-dev/opteryx/discussions
- **Contributing**: See CONTRIBUTING.md

---

This guide is a living document. Please help keep it up to date as Opteryx evolves!