# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Import all functions from the consolidated list_ops module.

The individual .pyx files are kept separate for maintainability,
but they are compiled into a single .so file via include directives.
"""

import sys
from types import ModuleType

# Import all functions from the single compiled module
from opteryx.compiled.list_ops.list_ops import (
    list_allop_eq,
    list_allop_neq,
    list_anyop_eq,
    list_anyop_gt,
    list_anyop_gte,
    list_anyop_lt,
    list_anyop_lte,
    list_anyop_neq,
    list_arrow_op,
    list_cast_int64_to_bytes,
    list_cast_int64_to_ascii,
    list_cast_ascii_to_int,
    list_cast_bytes_to_int,
    list_cast_uint64_to_bytes,
    list_cast_uint64_to_ascii,
    list_contains_all,
    list_contains_any,
    list_encode_utf8,
    list_get_element,
    list_in_list,
    list_in_list_int64,
    list_in_string,
    list_in_string_case_insensitive,
    list_length,
    list_long_arrow_op,
    cython_arrow_op,
    cython_long_arrow_op,
)

# Create submodules for backward compatibility with code that imports like:
# from opteryx.compiled.list_ops.list_in_list import list_in_list
_SUBMODULE_FUNCTIONS = {
    'list_allop_eq': ['list_allop_eq'],
    'list_allop_neq': ['list_allop_neq'],
    'list_anyop_eq': ['list_anyop_eq'],
    'list_anyop_gt': ['list_anyop_gt'],
    'list_anyop_gte': ['list_anyop_gte'],
    'list_anyop_lt': ['list_anyop_lt'],
    'list_anyop_lte': ['list_anyop_lte'],
    'list_anyop_neq': ['list_anyop_neq'],
    'list_arrow_op': ['list_arrow_op'],
    'list_cast_int64_to_string': ['list_cast_int64_to_bytes', 'list_cast_int64_to_ascii'],
    'list_cast_string_to_int': ['list_cast_ascii_to_int', 'list_cast_bytes_to_int'],
    'list_cast_uint64_to_string': ['list_cast_uint64_to_bytes', 'list_cast_uint64_to_ascii'],
    'list_contains_all': ['list_contains_all'],
    'list_contains_any': ['list_contains_any'],
    'list_encode_utf8': ['list_encode_utf8'],
    'list_get_element': ['list_get_element'],
    'list_in_list': ['list_in_list', 'list_in_list_int64'],
    'list_in_string': ['list_in_string', 'list_in_string_case_insensitive'],
    'list_length': ['list_length'],
    'list_long_arrow_op': ['list_long_arrow_op'],
}

# Dynamically create submodules for backward compatibility
for module_name, func_names in _SUBMODULE_FUNCTIONS.items():
    submodule = ModuleType(f'opteryx.compiled.list_ops.{module_name}')
    for func_name in func_names:
        setattr(submodule, func_name, globals()[func_name])
    sys.modules[f'opteryx.compiled.list_ops.{module_name}'] = submodule

