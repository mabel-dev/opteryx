# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

import pyarrow

from libc.stdint cimport uint32_t

cpdef numpy.ndarray[numpy.uint32_t, ndim=1] list_length(object array):

    cdef Py_ssize_t n
    cdef numpy.ndarray[numpy.uint32_t, ndim=1] result
    cdef uint32_t[::1] result_view
    cdef object val
    cdef uint32_t i
    cdef numpy.ndarray[numpy.int32_t, ndim=1] offsets

    # PyArrow fast path (uses offsets buffer)
    if isinstance(array, (pyarrow.Array, pyarrow.ChunkedArray, pyarrow.lib.StringArray)):
        if isinstance(array, pyarrow.ChunkedArray):
            return numpy.concatenate([list_length(chunk) for chunk in array.chunks])

        n = len(array)

        try:
            offsets_buffer = array.buffers()[1]
            offsets = numpy.frombuffer(offsets_buffer, dtype=numpy.int32, count=n + 1)
            # Avoid negative indices when wraparound is disabled by using explicit slice bounds
            offsets_end = offsets[1 : n + 1]
            offsets_start = offsets[:n]
            return (offsets_end - offsets_start).astype(numpy.uint32)
        except Exception:
            pass  # fallback if offsets unavailable

    n = array.shape[0]
    result = numpy.empty(n, dtype=numpy.uint32)
    result_view = result

    for i in range(n):
        val = array[i]
        if isinstance(val, (str, bytes, list, numpy.ndarray)):
            result_view[i] = len(val)
        else:
            result_view[i] = 0

    return result
