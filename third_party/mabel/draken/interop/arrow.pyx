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
from libc.stdint cimport int64_t, uint8_t

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
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.float64_vector cimport from_sequence as float64_from_sequence
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.bool_vector cimport from_sequence as bool_from_sequence

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
    
    # Handle chunked arrays: single chunk is OK, multiple chunks not supported
    if hasattr(array, "num_chunks"):
        num_chunks = array.num_chunks
        if num_chunks > 1:
            raise ValueError(
                f"vector_from_arrow received ChunkedArray with {num_chunks} chunks. "
                f"Use Morsel.iter_from_arrow() to process tables with chunked columns, "
                f"or call table.combine_chunks() before conversion."
            )
        elif num_chunks == 1:
            # Single chunk - extract it
            array = array.chunk(0)
        # num_chunks == 0: empty array, proceed with it as-is

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


cpdef object vector_from_sequence(object data, object dtype=None):
    """
    Create a Draken Vector from a typed memoryview or Python sequence.
    
    For fixed-width numeric/boolean types, accepts typed memoryviews for zero-copy wrapping.
    Falls back to Arrow conversion for other types (including varchar/varbinary).
    
    Args:
        data: int64[::1], double[::1], uint8[::1] (bool), or Python sequence
        dtype: Optional type hint (for future use)
    
    Returns:
        Vector: Appropriate Draken Vector subclass
    
    Note:
        For varchar/varbinary types, use pa.array() + vector_from_arrow() instead.
        This function is optimized for fixed-width numeric types only.
    """
    cdef int64_t[::1] int64_view
    cdef double[::1] float64_view
    cdef uint8_t[::1] bool_view
    import pyarrow as pa
    
    # Check if it's a typed memoryview by attempting casts
    try:
        # Try int64 memoryview
        int64_view = data
        return int64_from_sequence(int64_view)
    except (TypeError, ValueError):
        pass
    
    try:
        # Try float64 memoryview
        float64_view = data
        return float64_from_sequence(float64_view)
    except (TypeError, ValueError):
        pass
    
    try:
        # Try bool/uint8 memoryview
        bool_view = data
        return bool_from_sequence(bool_view)
    except (TypeError, ValueError):
        pass
    
    # Fallback: convert to Arrow then to Vector
    # This handles varchar, varbinary, and other complex types
    arrow_array = pa.array(data)
    return vector_from_arrow(arrow_array)


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
