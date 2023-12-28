import cython
import numpy as np
from cython.parallel import prange


@cython.boundscheck(False)
def cython_inner_join(
        const int64_t[:] left_idxs, const int64_t[:] right_idxs, 
        const int64_t[:] left_counts, const int64_t[:] right_counts, 
        const int64_t[:] left_bidxs, const int64_t[:] right_bidxs):

    cdef:
        Py_ssize_t i, li, ri, rows = 0, p = 0
        int64_t cats, lbi, rbi, lc, rc, lp, rp
        int64_t[:] left_align, right_align
        int64_t max_rows = 0

    cats = left_counts.shape[0]
    for i in range(cats):
        max_rows += left_counts[i] * right_counts[i]

    left_align = np.empty(max_rows, dtype=np.int64)
    right_align = np.empty(max_rows, dtype=np.int64)

    with nogil, cython.parallel.parallel():
        for i in prange(cats):
            lc = left_counts[i]
            rc = right_counts[i]
            if lc > 0 and rc > 0:
                lbi = left_bidxs[i]
                rbi_start = right_bidxs[i]
                for li in range(lc):
                    for ri in range(rc):
                        left_align[p] = left_idxs[lbi]
                        right_align[p] = right_idxs[rbi_start + ri]
                        p += 1
                    lbi += 1

    return np.asarray(left_align[:p]), np.asarray(right_align[:p])
