# cython: language_level=3
import cython
import numpy

cimport numpy as cnp

from cython import Py_ssize_t

from numpy cimport int64_t, ndarray, uint64_t

cnp.import_array()

@cython.boundscheck(False)
@cython.nonecheck(False)
@cython.wraparound(False)
def cython_inner_join(
        const int64_t[:] left_idxs, const int64_t[:] right_idxs, 
        const uint64_t[:] left_counts, const uint64_t[:] right_counts, 
        const uint64_t[:] left_bidxs, const uint64_t[:] right_bidxs):

    """ this is referred to as the HASH join in Opteryx """

    cdef:
        Py_ssize_t i, li, ri, rows = 0, p = 0
        int64_t cats, lbi, rbi, lc, rc, lp, rp
        ndarray[int64_t] left_align, right_align
    
    cats = left_counts.shape[0]
    with nogil:
        for i in range(cats):
            lc = left_counts[i]
            rc = right_counts[i]
            rows += lc * rc

    left_align, right_align = numpy.empty(rows, dtype=numpy.int64), numpy.empty(rows, dtype=numpy.int64)

    with nogil:
        for i in range(cats):
            lc = left_counts[i]
            rc = right_counts[i]

            lbi = left_bidxs[i]
            for li in range(lc):
                rbi = right_bidxs[i]
                for ri in range(rc):
                    lp = left_idxs[lbi]
                    rp = right_idxs[rbi]
                    left_align[p] = lp
                    right_align[p] = rp
                    rbi += 1
                    p += 1
                lbi += 1
    return left_align, right_align
                            

@cython.boundscheck(False)
def cython_left_join(
        const int64_t[:] left_idxs, const int64_t[:] right_idxs, 
        const int64_t[:] left_counts, const int64_t[:] right_counts, 
        const int64_t[:] left_bidxs, const int64_t[:] right_bidxs):

    """ this is referred to as the HASH join in Opteryx """

    cdef:
        Py_ssize_t i, li, ri, rows = 0, p = 0
        int64_t cats, lbi, rbi, lc, rc, lp, rp
        ndarray[int64_t] left_align, right_align
    
    cats = left_counts.shape[0]
    with nogil:
        for i in range(cats):
            lc = left_counts[i]
            rc = right_counts[i]
            if rc:
                rows += lc * rc
            else:
                rows += lc

    left_align, right_align = numpy.empty(rows, dtype=numpy.int64), numpy.empty(rows, dtype=numpy.int64)

    with nogil:
        for i in range(cats):
            lc = left_counts[i]
            rc = right_counts[i]
            if lc > 0:
                lbi = left_bidxs[i]
                for li in range(lc):
                    if rc > 0:
                        rbi = right_bidxs[i]
                        for ri in range(rc):
                            lp = left_idxs[lbi]
                            rp = right_idxs[rbi]
                            left_align[p] = lp
                            right_align[p] = rp
                            rbi += 1
                            p += 1
                    else:
                        lp = left_idxs[lbi]
                        left_align[p] = lp
                        # we can't set the numpy array value to None
                        right_align[p] = -1
                        p += 1
                    lbi += 1

    # We need to set -1s to None
    py_right_align = right_align.tolist()
    for i in range(p):
        if right_align[i] < 0:
            py_right_align[i] = None

    return left_align, py_right_align

