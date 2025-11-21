# Substrait Support Module

This module provides Substrait import and export capabilities for Opteryx query plans.

## Overview

[Substrait](https://substrait.io/) is a cross-language specification for data compute operations. This module enables Opteryx to:

- **Export** logical query plans to Substrait protobuf format
- **Import** Substrait plans and convert them to Opteryx logical plans

This enables interoperability with other query engines and data processing systems that support Substrait.

## Files

- `__init__.py` - Public API exports
- `exporter.py` - Logic for converting Opteryx plans to Substrait
- `importer.py` - Logic for converting Substrait plans to Opteryx

## Usage

### Exporting Plans

```python
from opteryx.planner.substrait import export_to_substrait

# Export to protobuf binary format
substrait_bytes = export_to_substrait(logical_plan, output_format="proto")

# Export to JSON format
substrait_json = export_to_substrait(logical_plan, output_format="json")
```

### Importing Plans

```python
from opteryx.planner.substrait import import_from_substrait

# Import from protobuf binary format
logical_plan = import_from_substrait(substrait_bytes, input_format="proto")

# Import from JSON format
logical_plan = import_from_substrait(json_bytes, input_format="json")
```

## Supported Operations

### Logical Plan Operations
- Scan (ReadRel)
- Project (ProjectRel)
- Filter (FilterRel)
- Join (JoinRel) - all types
- Aggregate (AggregateRel)
- Limit (FetchRel)
- Sort (SortRel)

### Expression Types
- Literals (boolean, integer, double, string)
- Field references
- Scalar functions (basic support)
- Aggregate functions (basic support)

## Dependencies

This module requires the optional `substrait` package:

```bash
pip install substrait protobuf
```

Or install Opteryx with the substrait extra:

```bash
pip install opteryx[substrait]
```

## Architecture

### Exporter Flow
1. Traverse Opteryx logical plan graph
2. For each node, call appropriate handler based on node type
3. Convert node properties to Substrait relation properties
4. Build expression trees recursively
5. Serialize to protobuf or JSON

### Importer Flow
1. Parse Substrait protobuf or JSON
2. Traverse Substrait relation tree
3. For each relation, call appropriate handler
4. Create Opteryx logical plan nodes
5. Build graph structure with edges between nodes
6. Return complete logical plan

## Extension Handling

Unsupported operations are represented as extension relations in Substrait. This preserves the plan structure while indicating custom Opteryx-specific operations.

## Limitations

- Not all Opteryx-specific optimizations are preserved
- Function mapping requires extension function registry (placeholder currently)
- Some type precision may be lost in conversion
- Complex nested expressions have partial support

## Testing

Tests are located in:
- `tests/integration/test_substrait_integration.py` - Integration tests
- `tests/unit/planner/test_substrait.py` - Unit tests (requires compilation)

Run tests with:
```bash
pytest tests/integration/test_substrait_integration.py -v
```

## Examples

See `examples/substrait_example.py` for usage examples.

## Contributing

To extend Substrait support:

1. Add new operation handlers in `exporter.py` and `importer.py`
2. Update type mappings for new data types
3. Extend expression builders for complex expressions
4. Add corresponding tests

## References

- [Substrait Specification](https://substrait.io/specification/)
- [Substrait Python Bindings](https://github.com/substrait-io/substrait-python)
- [Opteryx Documentation](https://opteryx.dev/)
