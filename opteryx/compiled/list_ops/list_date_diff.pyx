# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t
from cpython.array cimport array, clone

# Conversion factors from microseconds → target unit
cdef const int64_t MICROSECONDS = 1
cdef const int64_t MILLISECONDS = 1000
cdef const int64_t SECONDS = 1000000
cdef const int64_t MINUTES = 60 * 1000000
cdef const int64_t HOURS = 3600 * 1000000
cdef const int64_t DAYS = 86400 * 1000000

cdef inline int64_t get_divisor(str part) noexcept:
    """
    Map unit string → microsecond divisor.
    """
    if part == "microseconds":
        return MICROSECONDS
    if part == "milliseconds":
        return MILLISECONDS
    if part == "seconds":
        return SECONDS
    if part == "minutes":
        return MINUTES
    if part == "hours":
        return HOURS
    if part == "days":
        return DAYS
    return -1


cpdef int64_t[:] list_date_diff(
    int64_t[:] start,
    int64_t[:] end,
    str part
):
    """
    Compute (end - start) in the given unit, assuming both are int64 microsecond timestamps.

    Parameters
    ----------
    start, end : int64_t memoryview
        Microsecond timestamps (same length)
    part : str
        One of 'microseconds', 'milliseconds', 'seconds', 'minutes', 'hours', 'days'

    Returns
    -------
    int64_t memoryview
    """

    cdef Py_ssize_t n = start.size
    if n != end.size:
        raise ValueError("Mismatched array lengths")

    cdef int64_t divisor = get_divisor(part)
    if divisor == -1:
        raise ValueError(f"Unsupported unit: {part}")

    # Allocate result array using Python's array module
    cdef array template = array('q')  # 'q' is for signed long long (int64_t)
    cdef array result = clone(template, n, zero=False)
    cdef int64_t[:] res_view = result

    cdef Py_ssize_t i
    for i in range(n):
        res_view[i] = (end[i] - start[i]) // divisor

    return res_view
