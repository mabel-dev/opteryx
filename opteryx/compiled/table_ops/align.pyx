"""
This is marginally faster than the pyarrow/python version
"""

# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

import pyarrow
cimport pyarrow
from pyarrow cimport lib as arrow
from libc.stdint cimport int64_t

ctypedef arrow.Table Table
ctypedef arrow.Array Array
ctypedef arrow.Schema Schema
ctypedef arrow.Field Field
#ctypedef arrow.DataType DataType
#ctypedef arrow.ChunkedArray ChunkedArray


cpdef Table align_tables(
    Table source_table,
    Table append_table,
    int64_t[:] source_indices,
    int64_t[:] append_indices
):
    cdef:
        Schema combined_schema
        list source_cols, all_fields, all_cols
        list[Field] missing_fields = []
        list[Array] missing_cols = []
        set source_names
        Field field

    if len(source_indices) == 0 or len(append_indices) == 0:
        combined_schema = pyarrow.schema(
            list(source_table.schema)
            + [f for f in append_table.schema if f.name not in source_table.schema.names]
        )
        return pyarrow.Table.from_arrays(
            [pyarrow.array([], type=f.type) for f in combined_schema], schema=combined_schema
        )

    source_cols = source_table.take(numpy.asarray(source_indices, dtype=numpy.int64)).columns
    source_names = set(source_table.schema.names)

    for field in append_table.schema:
        if field.name not in source_names:
            missing_fields.append(field)
            if append_is_all_nulls:
                missing_cols.append(pyarrow.nulls(len(source_indices), type=field.type))
            else:
                missing_cols.append(append_table.column(field.name).take(numpy.asarray(append_indices, dtype=numpy.int64)))

    all_fields = list(source_table.schema) + missing_fields
    all_cols = source_cols + missing_cols

    return pyarrow.Table.from_arrays(all_cols, schema=pyarrow.schema(all_fields))
