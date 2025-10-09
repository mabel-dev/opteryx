# Development Tools

This directory contains utilities and tools for Opteryx development, testing, and analysis.

## Contents

### Testing Tools (`testing/`)
- Performance testing utilities
- Test data generators
- Benchmarking tools

### Analysis Tools (`analysis/`)
- Query profilers
- Memory usage analyzers
- Performance monitoring utilities

## Usage

These tools are designed to help developers:
- Profile query performance
- Analyze memory usage patterns
- Generate test data for development
- Monitor system behavior

## Examples

```bash
# Run performance analysis
python tools/analysis/query_profiler.py

# Generate test data
python tools/testing/data_generators.py
```

## Contributing

When adding new tools:
1. Place them in the appropriate subdirectory
2. Include usage documentation
3. Add command-line interfaces where appropriate
4. Update this README