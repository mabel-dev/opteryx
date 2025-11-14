# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport int64_t, uint64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.draken.morsels.morsel cimport Morsel


cpdef tuple distinct(Morsel morsel, FlatHashSet seen_hashes=None, list columns=None):
    """
    Compute distinct indices using Draken morsel hashing.

    Parameters:
        morsel: draken.Morsel
            The morsel to compute distinct rows for
        seen_hashes: FlatHashSet, optional
            Existing hash set to check against (for streaming distinct)
        columns: list of bytes, optional
            Column names (as bytes) to use for hashing. If None, uses all columns.

    Returns:
        tuple: (indices_to_keep, updated_hash_set)
    """
    cdef uint64_t[::1] row_hashes

    # Get row hashes
    if columns is None:
        row_hashes = morsel.hash()
    else:
        row_hashes = morsel.hash(columns=columns)

    if seen_hashes is None:
        seen_hashes = FlatHashSet()
        seen_hashes.reserve(2048)

    cdef Py_ssize_t num_rows = row_hashes.shape[0]
    cdef IntBuffer keep = IntBuffer(<size_t>(num_rows))
    cdef Py_ssize_t row_idx

    if num_rows == 0:
        return keep.to_numpy(), seen_hashes

    for row_idx in range(num_rows):
        if seen_hashes.insert(row_hashes[row_idx]):
            keep.append(<int64_t>row_idx)

    return keep.to_numpy(), seen_hashes
