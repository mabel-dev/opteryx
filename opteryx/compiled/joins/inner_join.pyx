# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow
import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t, uintptr_t
from cpython.object cimport PyObject_Hash
from cpython.bytes cimport PyBytes_AsString, PyBytes_Size

from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64
from opteryx.third_party.abseil.containers cimport FlatHashMap
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.structures.hash_table cimport HashTable

cdef:
    int64_t NULL_HASH = <int64_t>0xBADF00D
    int64_t EMPTY_HASH = <int64_t>0xBADC0FFEE
    uint64_t SEED = <uint64_t>0x9e3779b97f4a7c15


cpdef HashTable probe_side_hash_map(object relation, list join_columns):
    """
    Build a hash table for the join operations (probe-side) using buffer-level hashing.
    """
    cdef HashTable ht = HashTable()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t[::1] non_null_indices
    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef Py_ssize_t i

    non_null_indices = non_null_row_indices(relation, join_columns)

    # Compute hash of each row on the buffer level
    compute_row_hashes(relation, join_columns, row_hashes)

    # Insert into HashTable using row index + buffer-computed hash
    for i in range(non_null_indices.shape[0]):
        ht.insert(row_hashes[non_null_indices[i]], non_null_indices[i])

    return ht


cpdef FlatHashMap build_side_hash_map(object relation, list join_columns):
    cdef FlatHashMap ht = FlatHashMap()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t[::1] non_null_indices
    cdef uint64_t[::1] row_hashes = numpy.empty(num_rows, dtype=numpy.uint64)
    cdef Py_ssize_t i

    non_null_indices = non_null_row_indices(relation, join_columns)

    compute_row_hashes(relation, join_columns, row_hashes)

    for i in range(non_null_indices.shape[0]):
        ht.insert(row_hashes[non_null_indices[i]], non_null_indices[i])

    return ht

cpdef tuple nested_loop_join(left_relation, right_relation, list left_columns, list right_columns):
    """
    A buffer-aware nested loop join using direct Arrow buffer access and hash computation.
    Only intended for small relations (<1000 rows), primarily used for correctness testing or fallbacks.
    """
    # determine the rows we're going to try to join on
    cdef int64_t[::1] left_non_null_indices = non_null_row_indices(left_relation, left_columns)
    cdef int64_t[::1] right_non_null_indices = non_null_row_indices(right_relation, right_columns)

    cdef int64_t nl = left_non_null_indices.shape[0]
    cdef int64_t nr = right_non_null_indices.shape[0]
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    cdef int64_t left_non_null_idx, right_non_null_idx, left_record_idx, right_record_idx

    cdef uint64_t[::1] left_hashes = numpy.empty(nl, dtype=numpy.uint64)
    cdef uint64_t[::1] right_hashes = numpy.empty(nr, dtype=numpy.uint64)

    # remove the rows from the relations
    left_relation = left_relation.select(sorted(set(left_columns))).drop_null()
    right_relation = right_relation.select(sorted(set(right_columns))).drop_null()

    # build hashes for the columns we're joining on
    compute_row_hashes(left_relation, left_columns, left_hashes)
    compute_row_hashes(right_relation, right_columns, right_hashes)

    # Compare each pair of rows (naive quadratic approach)
    for left_non_null_idx in range(nl):
        for right_non_null_idx in range(nr):
            # if we have a match, look up the offset in the original table
            if left_hashes[left_non_null_idx] == right_hashes[right_non_null_idx]:
                left_record_idx = left_non_null_indices[left_non_null_idx]
                right_record_idx = right_non_null_indices[right_non_null_idx]
                left_indexes.append(left_record_idx)
                right_indexes.append(right_record_idx)

    return (left_indexes.to_numpy(), right_indexes.to_numpy())


cdef void process_column(object column, uint64_t[::1] row_hashes):
    """Process column using type-specific handlers"""
    cdef:
        Py_ssize_t row_offset = 0
        object chunk

    for chunk in column.chunks if isinstance(column, pyarrow.ChunkedArray) else [column]:
        dtype = chunk.type
        if pyarrow.types.is_string(dtype) or pyarrow.types.is_binary(dtype):
            process_string_chunk(chunk, row_hashes, row_offset)
        elif pyarrow.types.is_integer(dtype) or pyarrow.types.is_floating(dtype) or pyarrow.types.is_temporal(dtype):
            process_primitive_chunk(chunk, row_hashes, row_offset)
        elif pyarrow.types.is_list(dtype):
            process_list_chunk(chunk, row_hashes, row_offset)
        elif pyarrow.types.is_boolean(dtype):
            process_boolean_chunk(chunk, row_hashes, row_offset)
        else:
            process_generic_chunk(chunk, row_hashes, row_offset)

        row_offset += len(chunk)


# String Chunk Handler
cdef inline void process_string_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    cdef:
        const int32_t* offsets
        const char* data
        Py_ssize_t i, row_count, buffer_length
        Py_ssize_t arr_offset
        uint64_t hash_val
        list buffers = chunk.buffers()
        size_t str_len
        int start, end

    # Handle potential missing buffers
    offsets = <int32_t*><uintptr_t>(buffers[1].address) if len(buffers) > 1 else NULL
    data = <const char*><uintptr_t>buffers[2].address if len(buffers) > 2 else NULL
    row_count = len(chunk)
    buffer_length = len(data) if data else 0
    arr_offset = chunk.offset  # Account for non-zero offset in chunk

    for i in range(row_count):

        # Calculate the position in offsets array
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        str_len = end - start

        # Validate string length and boundaries
        if str_len < 0 or (start + str_len) > buffer_length:
            hash_val = EMPTY_HASH
        else:
            # Hash the string using xxhash3_64
            hash_val = <int64_t>cy_xxhash3_64(data + start, str_len)

        update_row_hash(row_hashes, row_offset + i, hash_val)


# Primitive Numeric Handler (Int/Float)
cdef inline void process_primitive_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    cdef:
        const uint8_t* data
        Py_ssize_t i, length, item_size
        Py_ssize_t arr_offset
        uint64_t hash_val
        list buffers = chunk.buffers()

    data = <uint8_t*><uintptr_t>(buffers[1].address)
    length = len(chunk)
    item_size = chunk.type.bit_width // 8
    arr_offset = chunk.offset  # Account for non-zero offset in chunk

    for i in range(length):
        if item_size == <uint64_t>8:
            hash_val = (<uint64_t*>(data + ((arr_offset + i) * 8)))[0]
        else:
            # Calculate the correct position in data buffer
            hash_val = cy_xxhash3_64(data + ((arr_offset + i) * item_size), item_size)

        update_row_hash(row_hashes, row_offset + i, hash_val)


# Composite Type Handler (List)
cdef inline void process_list_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    """
    Processes a ListArray chunk by slicing the child array correctly,
    combining each sub-element's hash, and mixing it into `row_hashes`.
    """

    cdef:
        const int32_t* offsets
        Py_ssize_t i, j, length, data_size
        Py_ssize_t start, end, sub_length
        Py_ssize_t arr_offset, child_offset
        object child_array, sublist
        uint64_t hash_val
        list buffers = chunk.buffers()
        uint64_t c1 = <uint64_t>0xbf58476d1ce4e5b9
        uint64_t c2 = <uint64_t>0x94d049bb133111eb
        cdef char* data_ptr

    # Obtain addresses of validity bitmap and offsets buffer
    offsets = <int32_t*><uintptr_t>(buffers[1].address)

    # The child array holds the sub-elements of the list
    child_array = chunk.values

    # Number of "top-level" list entries in this chunk
    length = len(chunk)

    # Arrow can slice a chunk, so account for chunk.offset
    arr_offset = chunk.offset

    # Child array can also be offset
    child_offset = child_array.offset

    for i in range(length):
        # Properly compute start/end using arr_offset
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        sub_length = end - start

        # Initialize hash with a seed
        hash_val = SEED

        # Handle empty list
        if sub_length == 0:
            hash_val = EMPTY_HASH
        else:
            # Correctly slice child array by adding child_offset
            sublist = child_array.slice(start + child_offset, sub_length)

            # Combine each element in the sublist
            for j in range(sub_length):
                # Convert to Python string, then to UTF-8 bytes
                element = sublist[j].as_py().encode("utf-8")
                data_ptr = PyBytes_AsString(element)
                data_size = PyBytes_Size(element)

                # Combine each element's hash with a simple mix
                hash_val = cy_xxhash3_64(<const void*>data_ptr, data_size) ^ hash_val

                # Optionally apply SplitMix64 finalizer (commented out for now)
                hash_val = (hash_val ^ (hash_val >> 30)) * c1
                hash_val = (hash_val ^ (hash_val >> 27)) * c2
                hash_val = hash_val ^ (hash_val >> 31)

        # Merge this row's final list-hash into row_hashes
        update_row_hash(row_hashes, row_offset + i, hash_val)


# Add boolean chunk handler
cdef inline void process_boolean_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    cdef:
        const uint8_t* data
        Py_ssize_t i, length
        uint64_t hash_val
        list buffers = chunk.buffers()

    data = <uint8_t*><uintptr_t>(buffers[1].address) if buffers[1] else NULL
    length = len(chunk)

    for i in range(length):
        hash_val = (data[i >> 3] & (1 << (i & 7))) != 0
        update_row_hash(row_hashes, row_offset + i, hash_val)


cdef inline void process_generic_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    """Fallback handler for types without a specific handler"""
    cdef:
        Py_ssize_t i, length
        uint64_t hash_val

    length = len(chunk)

    for i in range(length):
        hash_val = PyObject_Hash(chunk[i])
        update_row_hash(row_hashes, row_offset + i, hash_val)


cdef inline void update_row_hash(uint64_t[::1] row_hashes, Py_ssize_t row_idx, uint64_t col_hash) noexcept nogil:
    """Combine column hashes using a stronger mixing function (MurmurHash3 finalizer)"""
    cdef uint64_t h = row_hashes[row_idx] ^ col_hash

    # Use uint64_t constants to ensure proper C-level handling
    cdef uint64_t c1 = <uint64_t>0xff51afd7ed558ccd
    cdef uint64_t c2 = <uint64_t>0xc4ceb9fe1a85ec53

    # MurmurHash3 finalizer
    h ^= h >> 33
    h *= c1
    h ^= h >> 33
    h *= c2
    h ^= h >> 33

    row_hashes[row_idx] = h


cdef inline void compute_row_hashes(object table, list columns, uint64_t[::1] row_hashes):
    row_hashes[:] = 0
    for col_name in columns:
        process_column(table.column(col_name), row_hashes)


cdef inline int64_t[::1] non_null_row_indices(object relation, list column_names):
    """
    Compute indices of rows where all `column_names` in `relation` are non-null.
    Returns a memoryview of row indices (int64).
    """
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        numpy.ndarray[uint8_t, ndim=1] combined_nulls_np = numpy.ones(num_rows, dtype=numpy.uint8)
        uint8_t[::1] combined_nulls = combined_nulls_np
        object column, bitmap_buffer
        uint8_t[::1] bitmap_view
        numpy.ndarray[int64_t, ndim=1] indices = numpy.empty(num_rows, dtype=numpy.int64)
        int64_t[::1] indices_view = indices
        Py_ssize_t i, count = 0
        uint8_t byte, bit

    for column_name in column_names:
        column = relation.column(column_name)

        if column.null_count > 0:
            bitmap_buffer = column.combine_chunks().buffers()[0]
            if bitmap_buffer is not None:
                bitmap_view = numpy.frombuffer(bitmap_buffer, dtype=numpy.uint8)

                for i in range(num_rows):
                    byte = bitmap_view[i >> 3]
                    bit = (byte >> (i & 7)) & 1
                    combined_nulls[i] &= bit

    for i in range(num_rows):
        if combined_nulls[i]:
            indices_view[count] = i
            count += 1

    return indices_view[:count]
