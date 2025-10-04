# List Operations Module

This directory contains Cython implementations of optimized list operations.

## Compilation

All functions in this directory are compiled into a **single consolidated module**: `list_ops.pyx`

The individual `.pyx` files (e.g., `list_in_list.pyx`, `list_anyop_eq.pyx`, etc.) are kept for 
reference and development purposes but are no longer compiled separately. Instead, all functions 
are combined in `list_ops.pyx` which compiles to a single `.so` file.

## Usage

### Direct import from consolidated module:
```python
from opteryx.compiled.list_ops.list_ops import (
    list_in_list,
    list_anyop_eq,
    cython_arrow_op,
    # ... other functions
)
```

### Backward-compatible import (via __init__.py):
```python
from opteryx.compiled.list_ops.list_in_list import list_in_list
from opteryx.compiled.list_ops.list_anyop_eq import list_anyop_eq
# ... etc
```

Both import styles are supported for backward compatibility.

## Build Process

The `setup.py` file builds a single Extension module:
- **Module name**: `opteryx.compiled.list_ops.list_ops`
- **Sources**: `list_ops.pyx` + `src/cpp/simd_search.cpp`
- **Output**: Single `.so` file instead of ~20 separate files

## Available Functions

All functions from the individual files are available in the consolidated module:
- `list_allop_eq`, `list_allop_neq`
- `list_anyop_eq`, `list_anyop_neq`, `list_anyop_gt`, `list_anyop_gte`, `list_anyop_lt`, `list_anyop_lte`
- `list_arrow_op`, `list_long_arrow_op`, `cython_arrow_op`, `cython_long_arrow_op`
- `list_in_list`, `list_in_list_int64`
- `list_in_string`, `list_in_string_case_insensitive`
- `list_contains_any`, `list_contains_all`
- `list_cast_*` functions
- `list_encode_utf8`, `list_get_element`, `list_length`
