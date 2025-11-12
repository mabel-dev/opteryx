# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False


from libc.stdint cimport uint64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.table_ops.hash_ops cimport process_column
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

    if columns is None:
        row_hashes = morsel.hash()
    else:
        # Convert column names to bytes if they're strings
        # morsel.hash() accepts both strings and bytes
        row_hashes = morsel.hash(columns=columns)

    return distinct_from_hashes(row_hashes, seen_hashes)


cdef inline tuple distinct_from_hashes(uint64_t[::1] row_hashes, FlatHashSet seen_hashes=None):
    """DISTINCT using precomputed row hash values."""

    cdef Py_ssize_t num_rows = row_hashes.shape[0]
    cdef Py_ssize_t row_idx
    cdef IntBuffer keep = IntBuffer()

    if seen_hashes is None:
        seen_hashes = FlatHashSet()
        # Pre-allocate to reduce rehashing during insertion
        seen_hashes.reserve(num_rows)

    for row_idx in range(num_rows):
        if seen_hashes.insert(row_hashes[row_idx]):
            keep.append(row_idx)

    return keep.to_numpy(), seen_hashes


# Test wrapper for distinct_from_hashes (only for testing)
cpdef tuple _test_distinct_from_hashes(uint64_t[::1] row_hashes, FlatHashSet seen_hashes=None):
    """
    Test wrapper for distinct_from_hashes.
    DO NOT USE IN PRODUCTION CODE - use distinct_with_draken instead.
    """
    return distinct_from_hashes(row_hashes, seen_hashes)
