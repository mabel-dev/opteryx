# Substrait Support in Opteryx

Opteryx provides support for importing and exporting query plans in [Substrait](https://substrait.io/) format, enabling interoperability with other query engines.

## Overview

Substrait is a cross-language specification for data compute operations. It provides a common representation for query plans that can be shared between different query engines and data processing systems.

Opteryx's Substrait support includes:
- **Export**: Convert Opteryx logical plans to Substrait protobuf format
- **Import**: Parse Substrait plans and convert them to Opteryx logical plans

## Installation

Substrait support requires the optional `substrait` dependency:

```bash
pip install opteryx[substrait]
```

Or install the substrait package directly:

```bash
pip install substrait protobuf
```

## Exporting Plans to Substrait

You can export an Opteryx logical plan to Substrait format:

```python
from opteryx.planner.substrait import export_to_substrait

# After creating a logical plan through Opteryx's planning process
# (this is typically done internally when executing a query)
substrait_bytes = export_to_substrait(logical_plan, output_format="proto")

# Or export to JSON format
substrait_json = export_to_substrait(logical_plan, output_format="json")

# Save to file
with open("query_plan.substrait", "wb") as f:
    f.write(substrait_bytes)
```

## Importing Plans from Substrait

You can import a Substrait plan and convert it to an Opteryx logical plan:

```python
from opteryx.planner.substrait import import_from_substrait

# Load from file
with open("query_plan.substrait", "rb") as f:
    substrait_bytes = f.read()

# Import the plan
logical_plan = import_from_substrait(substrait_bytes, input_format="proto")

# Or import from JSON
logical_plan = import_from_substrait(json_bytes, input_format="json")
```

## Supported Operations

The following Opteryx logical plan operations are supported for import/export:

### Fully Supported
- **Scan** (ReadRel) - Reading from tables/datasets
- **Project** (ProjectRel) - Column selection and expressions
- **Filter** (FilterRel) - Row filtering with predicates
- **Join** (JoinRel) - All join types (INNER, LEFT, RIGHT, OUTER)
- **Aggregate** (AggregateRel) - Grouping and aggregations
- **Limit** (FetchRel) - Limiting result rows
- **Sort** (SortRel) - Ordering results

### Partial Support
- **Expressions** - Literals, field references, and basic functions
- **Types** - Common data types (INTEGER, VARCHAR, TIMESTAMP, etc.)

### Not Yet Supported
- Window functions
- Set operations (UNION, INTERSECT, EXCEPT)
- Common Table Expressions (CTEs)
- Complex nested expressions

Unsupported operations are represented as extension relations in Substrait, preserving the plan structure while indicating custom Opteryx-specific operations.

## Example: Round-Trip Conversion

```python
from opteryx.planner.substrait import export_to_substrait, import_from_substrait
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

# Create a simple logical plan
original_plan = LogicalPlan()
scan_node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
scan_node.relation = "my_table"
original_plan.add_node(scan_node)

# Export to Substrait
substrait_bytes = export_to_substrait(original_plan, output_format="proto")

# Import back to Opteryx
imported_plan = import_from_substrait(substrait_bytes, input_format="proto")

# The imported plan can now be used in Opteryx's execution pipeline
```

## Use Cases

### Query Federation
Share query plans between different systems:
```python
# Generate plan in System A (Opteryx)
plan_bytes = export_to_substrait(opteryx_plan)

# Execute in System B (another Substrait-compatible engine)
# The other system imports and executes the plan
```

### Plan Optimization Across Systems
Leverage different optimization capabilities:
```python
# Create initial plan in Opteryx
initial_plan = create_plan_from_sql(query)
substrait_plan = export_to_substrait(initial_plan)

# Optimize using external optimizer
optimized_substrait = external_optimizer.optimize(substrait_plan)

# Import back and execute
final_plan = import_from_substrait(optimized_substrait)
```

### Plan Storage and Analysis
Store and analyze query plans:
```python
# Export and store plan
plan_bytes = export_to_substrait(logical_plan, output_format="json")
with open("plans/query_123.json", "wb") as f:
    f.write(plan_bytes)

# Later: analyze plan structure
import json
with open("plans/query_123.json", "rb") as f:
    plan_data = json.loads(f.read())
    # Analyze plan structure, complexity, etc.
```

## Limitations and Considerations

### Type Mapping
Not all Opteryx types have direct Substrait equivalents. The implementation uses best-effort mapping, with some precision loss possible for:
- Custom types
- Nested complex types
- Decimal precision

### Function Mapping
Substrait requires an extension function registry for custom functions. Currently, Opteryx functions are exported with placeholder references. Future versions will include a complete function mapping.

### Plan Fidelity
While basic plan structure is preserved, some Opteryx-specific optimizations and metadata may be lost during export/import.

## API Reference

### `export_to_substrait(logical_plan, output_format="proto")`

Export an Opteryx logical plan to Substrait format.

**Parameters:**
- `logical_plan`: The Opteryx logical plan (Graph object) to export
- `output_format`: Format of the output ("proto" for binary protobuf, "json" for JSON)

**Returns:**
- Serialized Substrait plan as bytes

**Raises:**
- `ImportError`: If substrait package is not installed
- `ValueError`: If output_format is not "proto" or "json"
- `NotImplementedError`: If the plan contains unsupported operations (future versions)

### `import_from_substrait(substrait_plan, input_format="proto")`

Import a Substrait plan and convert it to an Opteryx logical plan.

**Parameters:**
- `substrait_plan`: The Substrait plan as bytes
- `input_format`: Format of the input ("proto" for binary protobuf, "json" for JSON)

**Returns:**
- An Opteryx logical plan (Graph object)

**Raises:**
- `ImportError`: If substrait package is not installed
- `ValueError`: If input_format is not "proto" or "json"
- `NotImplementedError`: If the plan contains unsupported operations (future versions)

## Contributing

To contribute to Substrait support:

1. **Add Operation Support**: Extend the handlers in `exporter.py` and `importer.py`
2. **Improve Type Mapping**: Enhance type conversion in both directions
3. **Function Registry**: Help build the extension function registry
4. **Test Coverage**: Add tests for new operations and edge cases

See the [contribution guide](../CONTRIBUTING.md) for more information.

## Related Resources

- [Substrait Specification](https://substrait.io/specification/)
- [Substrait Python Bindings](https://github.com/substrait-io/substrait-python)
- [Opteryx Query Planning](https://opteryx.dev/latest/architecture/query-planning/)
