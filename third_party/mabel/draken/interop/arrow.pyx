# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Arrow interoperability helpers for Draken columnar buffers.

This module provides:
- Functions to expose DrakenFixedBuffer as ArrowArray and ArrowSchema
- Memory management utilities for Arrow C Data Interface structs
- Conversion helpers for zero-copy Arrow integration

Used to enable efficient interchange between Draken and Apache Arrow for analytics and data science workflows.
"""

from libc.stdlib cimport free
from libc.stdlib cimport malloc

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DrakenType
from opteryx.draken.interop.arrow_c_data_interface cimport ARROW_FLAG_NULLABLE
from opteryx.draken.interop.arrow_c_data_interface cimport ArrowArray
from opteryx.draken.interop.arrow_c_data_interface cimport ArrowSchema
from opteryx.draken.vectors.bool_vector cimport from_arrow as bool_from_arrow
from opteryx.draken.vectors.float64_vector cimport from_arrow as float64_from_arrow
from opteryx.draken.vectors.int64_vector cimport from_arrow as int64_from_arrow
from opteryx.draken.vectors.string_vector cimport from_arrow as string_from_arrow
from opteryx.draken.vectors.string_vector cimport from_arrow_struct as string_from_arrow_struct
from opteryx.draken.vectors.date32_vector cimport from_arrow as date32_from_arrow
from opteryx.draken.vectors.timestamp_vector cimport from_arrow as timestamp_from_arrow
from opteryx.draken.vectors.time_vector cimport from_arrow as time_from_arrow
from opteryx.draken.vectors.interval_vector cimport (
    from_arrow_interval as interval_from_arrow_interval,
)
from opteryx.draken.vectors.interval_vector cimport (
    from_arrow_binary as interval_from_arrow_binary,
)
from opteryx.draken.vectors.array_vector cimport from_arrow as array_from_arrow

from opteryx.draken.vectors.arrow_vector import from_arrow as arrow_from_arrow

cdef void release_arrow_array(ArrowArray* arr) noexcept:
    free(<void*>arr.buffers)
    free(arr)

cdef void release_arrow_schema(ArrowSchema* schema) noexcept:
    free(schema)

cdef void expose_draken_fixed_as_arrow(
    DrakenFixedBuffer* vec,
    ArrowArray** out_array,
    ArrowSchema** out_schema,
):
    cdef ArrowArray* arr = <ArrowArray*>malloc(sizeof(ArrowArray))
    cdef ArrowSchema* schema = <ArrowSchema*>malloc(sizeof(ArrowSchema))
    out_array[0] = arr
    out_schema[0] = schema

    # Fill ArrowArray
    arr.length = vec.length
    arr.null_count = -1
    arr.offset = 0
    arr.n_buffers = 2
    arr.n_children = 0
    arr.children = NULL
    arr.dictionary = NULL
    arr.release = release_arrow_array
    arr.private_data = NULL

    arr.buffers = <const void**>malloc(2 * sizeof(void*))
    arr.buffers[0] = <const void*>vec.null_bitmap
    arr.buffers[1] = vec.data

    # Fill ArrowSchema
    schema.format = b"l"
    schema.name = NULL
    schema.metadata = NULL
    schema.flags = ARROW_FLAG_NULLABLE if vec.null_bitmap != NULL else 0
    schema.n_children = 0
    schema.children = NULL
    schema.dictionary = NULL
    schema.release = release_arrow_schema
    schema.private_data = NULL


cpdef object vector_from_arrow(object array):
    import pyarrow as pa
    
    if hasattr(array, "combine_chunks"):
        array = array.combine_chunks()

    pa_type = array.type
    if pa_type.equals(pa.int64()):
        return int64_from_arrow(array)
    if pa.types.is_interval(pa_type):
        return interval_from_arrow_interval(array)
    if pa.types.is_fixed_size_binary(pa_type) and pa_type.byte_width == 16:
        return interval_from_arrow_binary(array)
    if pa_type.equals(pa.string()) or pa_type.equals(pa.binary()):
        return string_from_arrow(array)
    if pa_type.equals(pa.float64()):
        return float64_from_arrow(array)
    if pa_type.equals(pa.bool_()):
        return bool_from_arrow(array)
    if pa.types.is_date32(pa_type):
        return date32_from_arrow(array)
    if pa.types.is_timestamp(pa_type):
        return timestamp_from_arrow(array)
    if pa.types.is_time32(pa_type) or pa.types.is_time64(pa_type):
        return time_from_arrow(array)
    if pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        return array_from_arrow(array)
    if isinstance(pa_type, pa.StructType):
        return string_from_arrow_struct(array)

    # fall back implementation (just wrap pyarrow compute)
    return arrow_from_arrow(array)


cpdef DrakenType arrow_type_to_draken(object dtype):
    """
    Convert a PyArrow DataType to a DrakenType enum.
    Raises TypeError if unsupported.
    """
    import pyarrow as pa
    
    if pa.types.is_int8(dtype):
        return DrakenType.DRAKEN_INT8
    elif pa.types.is_int16(dtype):
        return DrakenType.DRAKEN_INT16
    elif pa.types.is_int32(dtype):
        return DrakenType.DRAKEN_INT32
    elif pa.types.is_int64(dtype):
        return DrakenType.DRAKEN_INT64
    elif pa.types.is_float32(dtype):
        return DrakenType.DRAKEN_FLOAT32
    elif pa.types.is_float64(dtype):
        return DrakenType.DRAKEN_FLOAT64
    elif pa.types.is_date32(dtype):
        return DrakenType.DRAKEN_DATE32
    elif pa.types.is_timestamp(dtype):
        return DrakenType.DRAKEN_TIMESTAMP64
    elif pa.types.is_interval(dtype):
        return DrakenType.DRAKEN_INTERVAL
    elif pa.types.is_boolean(dtype):
        return DrakenType.DRAKEN_BOOL
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return DrakenType.DRAKEN_STRING
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return DrakenType.DRAKEN_ARRAY
    elif pa.types.is_fixed_size_binary(dtype) and dtype.byte_width == 16:
        return DrakenType.DRAKEN_INTERVAL

    return DrakenType.DRAKEN_NON_NATIVE
