# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t

from opteryx.draken.vectors.vector cimport Vector


cpdef void hash_into(
    Vector vector,
    uint64_t[::1] out_buf,
    Py_ssize_t offset=0
):
    """
    Python-visible shim for invoking Vector.hash_into.

    This exists solely for test helpers; production code should call the cdef
    method directly from Cython.
    """
    vector.hash_into(out_buf, offset)
