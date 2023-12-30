# cython: language_level=3
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

import cython
import numpy

cimport numpy as cnp

from cython import Py_ssize_t
from cython.parallel import prange

from numpy cimport int64_t, ndarray

cnp.import_array()


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_allop_eq(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.empty(arr.shape[0], dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        if len(row) == 0:
            result[i] = False
            continue

        result[i] = True
        for j in range(row.shape[0]):
            if row[j] != literal:
                result[i] = False
                break

    return result


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_allop_neq(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.empty(arr.shape[0], dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]

        result[i] = True
        for j in range(row.shape[0]):
            if row[j] == literal:
                result[i] = False
                break

    return result

