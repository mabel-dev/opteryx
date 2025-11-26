# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, uint64_t, int32_t

from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from opteryx.third_party.abseil.containers cimport FlatHashSet
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

    if num_rows == 0:
        return IntBuffer(0), seen_hashes

    cdef uint64_t* hashes_ptr = &row_hashes[0]
    cdef Py_ssize_t count = 0
    cdef Int32Buffer buffer32
    cdef int32_t* buffer32_ptr
    cdef IntBuffer buffer64
    cdef int64_t* buffer64_ptr

    # Use Int32Buffer if indices fit in 32 bits (approx 2 billion rows)
    if num_rows < 2147483647:
        buffer32 = Int32Buffer(num_rows)
        buffer32.c_buffer.resize(num_rows)
        buffer32_ptr = buffer32.c_buffer.mutable_data()

        with nogil:
            count = seen_hashes.find_new_indices_out_32(hashes_ptr, num_rows, buffer32_ptr)

        if count == 0:
            return Int32Buffer(0), seen_hashes

        buffer32.c_buffer.resize(count)
        return buffer32, seen_hashes
    else:
        # Fallback to Int64Buffer for huge datasets
        buffer64 = IntBuffer(num_rows)
        buffer64.c_buffer.resize(num_rows)
        buffer64_ptr = buffer64.c_buffer.mutable_data()

        with nogil:
            count = seen_hashes.find_new_indices_out(hashes_ptr, num_rows, buffer64_ptr)

        if count == 0:
            return IntBuffer(0), seen_hashes

        buffer64.c_buffer.resize(count)
        return buffer64, seen_hashes
