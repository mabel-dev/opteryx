import cython
from cython import Py_ssize_t
import numpy as np

cimport numpy as cnp
from numpy cimport ndarray, int64_t
cnp.import_array()

@cython.boundscheck(False)
def inner_join(
        const int64_t[:] left_idxs, const int64_t[:] right_idxs, 
        const int64_t[:] left_counts, const int64_t[:] right_counts, 
        const int64_t[:] left_bidxs, const int64_t[:] right_bidxs):
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

    left_align, right_align = np.empty(rows, dtype=np.int64), np.empty(rows, dtype=np.int64)

    with nogil:
        for i in range(cats):
            lc = left_counts[i]
            rc = right_counts[i]
            if lc > 0 and rc > 0:
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
                            

