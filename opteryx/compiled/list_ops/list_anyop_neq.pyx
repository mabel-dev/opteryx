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

cpdef numpy.ndarray[numpy.npy_bool, ndim=1] list_anyop_neq(object literal, numpy.ndarray arr):
    cdef:
        Py_ssize_t i, j
        numpy.ndarray[numpy.npy_bool, ndim=1] result = numpy.ones(arr.shape[0], dtype=bool)  # Default to True
        numpy.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is None:
            result[i] = False
            continue

        if row.size == 0:
            continue  # Keep result[i] as True for empty rows

        for j in range(row.size):
            if row[j] == literal:
                result[i] = False  # Found a match, set to False
                break  # No need to check the rest of the elements in this row

    return result
