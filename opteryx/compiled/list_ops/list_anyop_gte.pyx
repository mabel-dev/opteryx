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

cpdef numpy.ndarray[numpy.npy_bool, ndim=1] list_anyop_gte(object literal, numpy.ndarray arr):
    cdef:
        Py_ssize_t i, j
        numpy.ndarray[numpy.npy_bool, ndim=1] result = numpy.zeros(arr.shape[0], dtype=bool)
        numpy.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is not None:
            for j in range(row.shape[0]):
                if row[j] >= literal:
                    result[i] = True
                    break

    return result
