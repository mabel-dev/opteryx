# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

from opteryx.third_party.mrabarnett import regex

cdef numpy.dtype dtype_obj = numpy.dtype('O')

cpdef numpy.ndarray list_regex_replace(object[::1] arr, object pattern, object replacement):
    """
    Perform regex replacement on a NumPy array of strings.

    Parameters:
        arr: numpy.ndarray
            Input NumPy array containing string objects.
        pattern: bytes
            Regex pattern to match.
        replacement: bytes
            Replacement string.

    Returns:
        numpy.ndarray
            A new NumPy array with modified strings.
    """
    cdef Py_ssize_t n = arr.shape[0]
    cdef numpy.ndarray result = numpy.empty(n, dtype=dtype_obj)
    cdef object[::1] result_view = result

    compiled_pattern = regex.compile(pattern).sub

    cdef Py_ssize_t i
    for i in range(n):
        _value = arr[i]
        if _value is None:
            result_view[i] = None
        else:
            result_view[i] = compiled_pattern(replacement, _value)

    return result
