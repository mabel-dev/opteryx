# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True
# cython: initializedcheck=False

"""
High-performance table alignment for Draken Morsels.

This module provides optimized Cython implementations for aligning two morsels
based on index arrays. The primary use case is for join operations where we need
to combine columns from two tables based on matched row indices.

Key optimizations:
- Direct memory access to avoid Python overhead
- Minimized allocations by reusing buffers where possible
- Vectorized operations where applicable
- No intermediate conversions or copies
- Fast path for common cases (all nulls, single chunk, etc.)
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.string cimport strlen, memcpy

from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.vectors.interval_vector cimport IntervalVector
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.array_vector cimport ArrayVector
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.core.buffers cimport DrakenMorsel, DrakenType


cdef inline Vector _take_vector_fast(Vector vec, int32_t[::1] indices):
    """
    Fast path for taking a vector by indices.
    
    This dispatches to the appropriate typed vector's take method for maximum performance.
    """
    cdef DrakenType dtype = vec.dtype
    
    # Fast dispatch based on type
    if dtype == DrakenType.DRAKEN_INT64:
        return (<Int64Vector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_FLOAT64:
        return (<Float64Vector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_BOOL:
        return (<BoolVector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_STRING:
        return (<StringVector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_DATE32:
        return (<Date32Vector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_TIMESTAMP64:
        return (<TimestampVector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_INTERVAL:
        return (<IntervalVector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_TIME32 or dtype == DrakenType.DRAKEN_TIME64:
        return (<TimeVector>vec).take(indices)
    elif dtype == DrakenType.DRAKEN_ARRAY:
        return (<ArrayVector>vec).take(indices)
    else:
        # Fallback to Python implementation for non-native types
        return vec.take(indices)


cdef inline bint _check_all_nulls(int32_t[::1] indices) nogil:
    """
    Check if all indices are -1 (null marker).
    
    Returns True if all indices are null, False otherwise.
    """
    cdef Py_ssize_t i, n = indices.shape[0]
    for i in range(n):
        if indices[i] != -1:
            return False
    return True


cpdef Morsel align_tables(
    Morsel source_morsel,
    Morsel append_morsel,
    int32_t[::1] source_indices,
    int32_t[::1] append_indices
):
    """
    Align two morsels based on index arrays and combine them.
    
    This is the primary entry point for morsel alignment. It takes two morsels
    and their corresponding index arrays, performs efficient "take" operations,
    and combines them into a single output morsel.
    
    Args:
        source_morsel: Left morsel to align
        append_morsel: Right morsel to align  
        source_indices: Index array for source morsel (int32, -1 for nulls)
        append_indices: Index array for append morsel (int32, -1 for nulls)
        
    Returns:
        Morsel: Combined morsel with all columns from both inputs
        
    Performance notes:
        - Zero-copy operations where possible
        - Direct memory access to avoid Python overhead
        - Fast paths for common cases (empty, all nulls)
        - No intermediate Arrow conversions
    """
    cdef Py_ssize_t len_src = source_indices.shape[0]
    cdef Py_ssize_t len_app = append_indices.shape[0]
    cdef Py_ssize_t i, j
    cdef Py_ssize_t num_src_cols, num_app_cols, num_total_cols
    cdef Vector vec, taken_vec
    cdef list result_columns = []
    cdef list result_names = []
    cdef bytes encoded_name
    cdef const char* col_name
    cdef set source_names = set()
    cdef bint source_all_nulls = False
    cdef bint append_all_nulls = False
    cdef Morsel result
    cdef DrakenMorsel* result_ptr
    
    # Fast path: empty result
    if len_src == 0 or len_app == 0:
        # Return empty morsel with combined schema
        result = Morsel()
        result_ptr = <DrakenMorsel*>PyMem_Malloc(sizeof(DrakenMorsel))
        if result_ptr == NULL:
            raise MemoryError()
            
        num_src_cols = source_morsel.ptr.num_columns
        num_app_cols = append_morsel.ptr.num_columns
        
        # Collect source column names
        for i in range(num_src_cols):
            col_name = source_morsel.ptr.column_names[i]
            encoded_name = col_name[:strlen(col_name)]
            source_names.add(encoded_name)
        
        # Count unique columns
        num_total_cols = num_src_cols
        for i in range(num_app_cols):
            col_name = append_morsel.ptr.column_names[i]
            encoded_name = col_name[:strlen(col_name)]
            if encoded_name not in source_names:
                num_total_cols += 1
        
        result_ptr.num_columns = num_total_cols
        result_ptr.num_rows = 0
        result_ptr.columns = <void**>PyMem_Malloc(sizeof(void*) * num_total_cols)
        result_ptr.column_names = <const char**>PyMem_Malloc(sizeof(const char*) * num_total_cols)
        result_ptr.column_types = <DrakenType*>PyMem_Malloc(sizeof(DrakenType) * num_total_cols)
        
        if result_ptr.columns == NULL or result_ptr.column_names == NULL or result_ptr.column_types == NULL:
            PyMem_Free(result_ptr)
            raise MemoryError()
        
        result.ptr = result_ptr
        result._columns = [None] * num_total_cols
        result._encoded_names = [None] * num_total_cols
        
        # Add source columns (empty vectors)
        j = 0
        for i in range(num_src_cols):
            vec = <Vector>source_morsel.ptr.columns[i]
            # Create empty vector of same type
            taken_vec = vec.take(source_indices)  # Will create empty vector
            result._columns[j] = taken_vec
            result._encoded_names[j] = source_morsel._encoded_names[i]
            result_ptr.columns[j] = <void*>taken_vec
            result_ptr.column_names[j] = source_morsel.ptr.column_names[i]
            result_ptr.column_types[j] = source_morsel.ptr.column_types[i]
            j += 1
        
        # Add non-overlapping append columns
        for i in range(num_app_cols):
            col_name = append_morsel.ptr.column_names[i]
            encoded_name = col_name[:strlen(col_name)]
            if encoded_name not in source_names:
                vec = <Vector>append_morsel.ptr.columns[i]
                taken_vec = vec.take(append_indices)
                result._columns[j] = taken_vec
                result._encoded_names[j] = encoded_name
                result_ptr.columns[j] = <void*>taken_vec
                result_ptr.column_names[j] = col_name
                result_ptr.column_types[j] = append_morsel.ptr.column_types[i]
                j += 1
        
        result._rebuild_name_to_index()
        return result
    
    # Validate lengths match
    if len_src != len_app:
        raise ValueError(f"Index arrays must have same length: {len_src} vs {len_app}")
    
    # Check for all-null cases (common optimization)
    source_all_nulls = _check_all_nulls(source_indices)
    append_all_nulls = _check_all_nulls(append_indices)
    
    # Get source column names for deduplication
    num_src_cols = source_morsel.ptr.num_columns
    num_app_cols = append_morsel.ptr.num_columns
    
    for i in range(num_src_cols):
        col_name = source_morsel.ptr.column_names[i]
        encoded_name = col_name[:strlen(col_name)]
        source_names.add(encoded_name)
    
    # Process source columns first
    for i in range(num_src_cols):
        vec = <Vector>source_morsel.ptr.columns[i]
        
        if source_all_nulls:
            # Create null vector of appropriate length
            # Most vector types support creating empty and then we can use take with empty indices
            # For now, use take which should handle this
            taken_vec = vec.take(source_indices)
        else:
            # Fast path: dispatch to typed take
            taken_vec = _take_vector_fast(vec, source_indices)
        
        result_columns.append(taken_vec)
        result_names.append(source_morsel._encoded_names[i])
    
    # Process append columns (skip duplicates)
    for i in range(num_app_cols):
        col_name = append_morsel.ptr.column_names[i]
        encoded_name = col_name[:strlen(col_name)]
        
        # Skip if column already exists in source
        if encoded_name in source_names:
            continue
        
        vec = <Vector>append_morsel.ptr.columns[i]
        
        if append_all_nulls:
            taken_vec = vec.take(append_indices)
        else:
            taken_vec = _take_vector_fast(vec, append_indices)
        
        result_columns.append(taken_vec)
        result_names.append(encoded_name)
    
    # Build result morsel
    num_total_cols = len(result_columns)
    result = Morsel()
    result_ptr = <DrakenMorsel*>PyMem_Malloc(sizeof(DrakenMorsel))
    if result_ptr == NULL:
        raise MemoryError()
    
    result_ptr.num_columns = num_total_cols
    result_ptr.num_rows = len_src
    result_ptr.columns = <void**>PyMem_Malloc(sizeof(void*) * num_total_cols)
    result_ptr.column_names = <const char**>PyMem_Malloc(sizeof(const char*) * num_total_cols)
    result_ptr.column_types = <DrakenType*>PyMem_Malloc(sizeof(DrakenType) * num_total_cols)
    
    if result_ptr.columns == NULL or result_ptr.column_names == NULL or result_ptr.column_types == NULL:
        PyMem_Free(result_ptr)
        raise MemoryError()
    
    result.ptr = result_ptr
    result._columns = result_columns
    result._encoded_names = result_names
    
    # Populate the C struct
    for i in range(num_total_cols):
        vec = result_columns[i]
        encoded_name = result_names[i]
        
        result_ptr.columns[i] = <void*>vec
        result_ptr.column_names[i] = <const char*>encoded_name
        result_ptr.column_types[i] = vec.dtype
    
    result._rebuild_name_to_index()
    
    return result


cpdef Morsel align_tables_pyarray(
    Morsel source_morsel,
    Morsel append_morsel,
    object source_indices,
    object append_indices
):
    """
    Convenience wrapper that accepts PyArrow arrays or Python lists.
    
    This converts the input indices to int32 memoryviews before calling
    the optimized align_tables function.
    
    Args:
        source_morsel: Left morsel to align
        append_morsel: Right morsel to align
        source_indices: Index array for source (list, numpy, or pyarrow)
        append_indices: Index array for append (list, numpy, or pyarrow)
        
    Returns:
        Morsel: Combined morsel
    """
    import numpy as np
    
    # Convert to numpy int32 arrays
    if not isinstance(source_indices, np.ndarray):
        source_indices = np.asarray(source_indices, dtype=np.int32)
    else:
        source_indices = source_indices.astype(np.int32, copy=False)
        
    if not isinstance(append_indices, np.ndarray):
        append_indices = np.asarray(append_indices, dtype=np.int32)
    else:
        append_indices = append_indices.astype(np.int32, copy=False)
    
    # Get memoryviews
    cdef int32_t[::1] src_view = source_indices
    cdef int32_t[::1] app_view = append_indices
    
    return align_tables(source_morsel, append_morsel, src_view, app_view)
