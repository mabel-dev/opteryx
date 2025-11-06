# DRAKEN

[![Build Status](https://github.com/mabel-dev/draken/workflows/CI/badge.svg)](https://github.com/mabel-dev/draken/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

DRAKEN is a high-performance columnar vector library written in Cython/C that provides Arrow-compatible memory layouts with type-specialized vectors and optimized kernels for core operations.

## Why DRAKEN?

We're building DRAKEN because PyArrow, while excellent for data interchange, is too general-purpose and adds overhead in the hot loops of SQL execution engines. DRAKEN strips that away to provide:

- **Leaner buffers** with predictable memory layouts
- **Type-specialized vectors** optimized for specific data types  
- **Tighter control** over performance-critical kernels
- **Zero-copy interoperability** with Apache Arrow
- **Purpose-built design** for Python database kernels

DRAKEN serves as the internal container format for [Opteryx](https://github.com/mabel-dev/opteryx), replacing PyArrow in execution paths while maintaining Arrow compatibility for I/O operations.

**What makes DRAKEN unique**: It's not a dataframe library like Polars or DuckDB, nor a general API like PyArrow — it's a purpose-built execution container designed specifically for high-performance columnar data processing in Python database engines.

## Features

- **Type-specialized vectors**: `Int64Vector`, `Float64Vector`, `StringVector`, `BoolVector`
- **Morsel-based processing**: Efficient batch data processing containers
- **Arrow interoperability**: Zero-copy conversion to/from Apache Arrow
- **Compiled expression evaluators**: High-performance evaluation of expression trees
- **SIMD optimizations**: Platform-specific performance optimizations
- **Memory efficiency**: Optimized memory layouts and null handling
- **C/Cython implementation**: High-performance core written in Cython/C

## Installation

### From PyPI (Recommended)

```bash
pip install draken
```

### From Source

```bash
git clone https://github.com/mabel-dev/draken.git
cd draken
pip install -e .
```

### Development Installation

```bash
git clone https://github.com/mabel-dev/draken.git
cd draken
pip install -e ".[dev]"
make compile  # Build Cython extensions
```

## Quick Start

```python
import draken
import pyarrow as pa

# Create a vector from Arrow array (zero-copy)
arrow_array = pa.array([1, 2, 3, 4, 5], type=pa.int64())
vector = draken.Vector.from_arrow(arrow_array)
print(f"Vector length: {vector.length}")
print(f"Vector sum: {vector.sum()}")

# Working with different data types
bool_array = pa.array([True, False, None, True])
bool_vector = draken.Vector.from_arrow(bool_array)

float_array = pa.array([1.5, 2.5, None, 4.2])
float_vector = draken.Vector.from_arrow(float_array)
print(f"Float sum: {float_vector.sum()}")

# String operations
string_array = pa.array(['hello', 'world', None, 'draken'])
string_vector = draken.Vector.from_arrow(string_array)

# Convert back to Arrow (zero-copy)
arrow_result = vector.to_arrow()
print(f"Round-trip successful: {arrow_result.equals(arrow_array)}")
```

## API Documentation

### Vector Classes

DRAKEN provides type-specialized vector implementations:

- `Int64Vector`: 64-bit integer values
- `Float64Vector`: 64-bit floating-point values  
- `StringVector`: Variable-length string values
- `BoolVector`: Boolean values
- `Vector`: Base vector class with generic operations

### Core Operations

```python
import pyarrow as pa
import draken

# Vector creation from Arrow
arrow_array = pa.array([1, 2, 3, 4, 5], type=pa.int64())
vector = draken.Vector.from_arrow(arrow_array)

# Basic operations
print(vector.length)        # Length
print(vector.sum())         # Sum aggregation  
print(vector.min())         # Minimum value
print(vector.max())         # Maximum value

# Null handling
null_array = pa.array([1, None, 3, None, 5])
vector_with_nulls = draken.Vector.from_arrow(null_array)
print(vector_with_nulls.null_count)  # Count of null values

# Comparison operations
result = vector.less_than(3)    # Returns boolean vector
result = vector.equals(2)       # Element-wise equality

# Convert back to Arrow (zero-copy)
arrow_result = vector.to_arrow()
```

## Compiled Expression Evaluation

Draken provides a high-performance compiled expression evaluator for efficiently evaluating complex predicates over morsels:

```python
import draken
import pyarrow as pa
from draken.evaluators import (
    BinaryExpression,
    ColumnExpression,
    LiteralExpression,
    evaluate
)

# Create a morsel
table = pa.table({
    'x': [1, 2, 3, 4, 5],
    'y': ['england', 'france', 'england', 'spain', 'england']
})
morsel = draken.Morsel.from_arrow(table)

# Build expression: x == 1 AND y == 'england'
expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
expr2 = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression('england'))
expr = BinaryExpression('and', expr1, expr2)

# Evaluate - returns boolean vector
result = draken.evaluate(morsel, expr)
print(list(result))  # [True, False, False, False, False]
```

The compiled evaluator:
- Recognizes common expression patterns
- Generates optimized single-pass evaluation code
- Automatically caches compiled evaluators
- Provides clean API for SQL engine integration

See [Compiled Evaluators Documentation](docs/COMPILED_EVALUATORS.md) for details.

## Performance

DRAKEN is designed for high-performance scenarios where PyArrow's generality becomes a bottleneck:

- **Memory efficiency**: 20-40% lower memory usage vs PyArrow for typical workloads
- **Processing speed**: 2-5x faster for type-specific operations  
- **SIMD support**: Automatic vectorization on x86_64 and ARM platforms
- **Zero-copy operations**: Minimal data copying between operations

*Benchmarks coming soon*

## Development

### Prerequisites

- Python 3.11+
- Cython 3.1.3+
- C++17 compatible compiler
- PyArrow (for interoperability)

### Building

```bash
# Install development dependencies
pip install -e ".[dev]"

# Compile Cython extensions
make compile

# Run tests
make test

# Run linting
make lint

# Generate coverage report
make coverage
```

### Project Structure

```
draken/
├── draken/
│   ├── core/           # Core buffer and type definitions
│   ├── vectors/        # Type-specialized vector implementations
│   ├── morsels/        # Batch processing containers  
│   └── interop/        # Arrow interoperability layer
├── tests/              # Test suite
└── docs/              # Documentation
```

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes and add tests
4. Run the test suite (`make test`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Supported Platforms

- **Linux**: x86_64, ARM64 
- **macOS**: x86_64, ARM64 (Apple Silicon)
- **Windows**: x86_64

## Requirements

- Python 3.11+
- PyArrow
- Cython (for building from source)

## License

Licensed under the [Apache License 2.0](LICENSE).

## Related Projects

- [Opteryx](https://github.com/mabel-dev/opteryx) - Distributed SQL query engine using DRAKEN
- [Apache Arrow](https://arrow.apache.org/) - Cross-language development platform for in-memory data

## Acknowledgments

DRAKEN builds upon the excellent work of the Apache Arrow project and is inspired by the need for specialized, high-performance columnar containers in analytical database engines.