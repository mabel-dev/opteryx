# Opteryx Examples

This directory contains examples demonstrating various features and use cases of Opteryx.

## Getting Started

First, ensure Opteryx is installed:
```bash
pip install opteryx
```

## Categories

### Basic Examples (`basic/`)
- `simple_queries.py` - Basic SQL queries and operations
- `data_types.py` - Working with different data types
- `csv_parquet.py` - Querying CSV and Parquet files
- `dbapi_quickstart.py` - Using `opteryx.connect()` with cursors and pandas

### Connector Examples (`connectors/`)
- `custom_connector.py` - Building custom data source connectors
- `cloud_storage.py` - Working with cloud storage (AWS S3, GCS)
- `database_examples.py` - Database connector usage

### Advanced Examples (`advanced/`)
- `query_optimization.py` - Performance optimization techniques
- `embedded_usage.py` - Embedding Opteryx in applications
- `dbapi_pandas_integration.py` - Using cursor.pandas() for analytics

### Feature Examples
- `non_equi_join_example.py` - Non-equi join operations (!=, >, >=, <, <=) using draken
- `simd_quote_finding.py` - SIMD-optimized quote finding
- `disk_reader_usage.py` - Disk reader functionality

## Running Examples

Each example is self-contained and can be run directly:
```bash
python examples/basic/simple_queries.py
```

## Sample Data

Examples use test data from the `testdata/` directory or generate sample data as needed.

## Contributing

When adding new examples:
1. Include clear comments explaining the functionality
2. Add sample data or data generation code
3. Test that examples run successfully
4. Update this README with the new example
