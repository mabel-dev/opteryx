# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Consolidated list_ops module that includes all individual list operation files.
This allows keeping the source files separate for maintainability while compiling
them into a single .so file.
"""

# Include all individual list operation implementations
include "list_allop_eq.pyx"
include "list_allop_neq.pyx"
include "list_anyop_eq.pyx"
include "list_anyop_gt.pyx"
include "list_anyop_gte.pyx"
include "list_anyop_lt.pyx"
include "list_anyop_lte.pyx"
include "list_anyop_neq.pyx"
include "list_arrow_op.pyx"
include "list_cast_int64_to_string.pyx"
include "list_cast_string_to_int.pyx"
include "list_cast_uint64_to_string.pyx"
include "list_contains_all.pyx"
include "list_contains_any.pyx"
include "list_encode_utf8.pyx"
include "list_get_element.pyx"
include "list_in_list.pyx"
include "list_in_string.pyx"
include "list_length.pyx"
include "list_long_arrow_op.pyx"

# Aliases for backward compatibility
cython_arrow_op = list_arrow_op
cython_long_arrow_op = list_long_arrow_op
