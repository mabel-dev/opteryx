# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow

from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t, uintptr_t
from cpython.object cimport PyObject_Hash
from cpython.bytes cimport PyBytes_AsString, PyBytes_Size
import array

from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64

cdef:
    uint64_t NULL_HASH = <uint64_t>0x4c3f95a36ab8eccaU   # xxhash(null)
    uint64_t EMPTY_HASH = <uint64_t>0xab52d8afc1448992U  # xxhash(empty)
    uint64_t TRUE_HASH = <uint64_t>0x4f112caa54efa882U   # xxhash(true)
    uint64_t FALSE_HASH = <uint64_t>0xc2fd8b2343f83ce7U  # xxhash(false)
    uint64_t SEED = <uint64_t>0x9e3779b97f4a7c15U


cdef void process_column(object column, uint64_t[::1] row_hashes):
    """Process column using type-specific handlers"""
    cdef Py_ssize_t row_offset = 0
    cdef object chunk
    cdef object dtype
    cdef bint is_chunked = isinstance(column, pyarrow.ChunkedArray)
    cdef list chunks = column.chunks if is_chunked else [column]
    for chunk in chunks:
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


cdef inline void compute_row_hashes(object table, list columns, uint64_t[::1] row_hashes):
    cdef Py_ssize_t i, n
    cdef object col_name
    n = row_hashes.shape[0]
    for i in range(n):
        row_hashes[i] = 0
    for col_name in columns:
        process_column(table.column(col_name), row_hashes)


# String Chunk Handler
cdef void process_string_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    cdef const uint8_t* validity
    cdef const int32_t* offsets
    cdef const char* data
    cdef Py_ssize_t i, row_count, buffer_length
    cdef Py_ssize_t arr_offset, offset_in_bits, offset_in_bytes, byte_index, bit_index
    cdef uint64_t hash_val
    cdef list buffers = chunk.buffers()
    cdef size_t str_len
    cdef int start, end

    # Handle potential missing buffers
    validity = <uint8_t*><uintptr_t>(buffers[0].address) if len(buffers) > 0 and buffers[0] else NULL
    offsets = <int32_t*><uintptr_t>(buffers[1].address) if len(buffers) > 1 else NULL
    data = <const char*><uintptr_t>buffers[2].address if len(buffers) > 2 else NULL
    row_count = len(chunk)
    buffer_length = buffers[2].size if len(buffers) > 2 and buffers[2] is not None else 0
    arr_offset = chunk.offset  # Account for non-zero offset in chunk

    # Calculate the byte and bit offset for validity
    offset_in_bits = arr_offset & 7
    offset_in_bytes = arr_offset >> 3

    for i in range(row_count):
        # locate validity bit for this row
        byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
        bit_index = (offset_in_bits + i) & 7

        # Check validity bit
        if validity and not (validity[byte_index] & (1 << bit_index)):
            hash_val = NULL_HASH
        else:
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
cdef void process_primitive_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    cdef const uint8_t* validity
    cdef const uint8_t* data
    cdef Py_ssize_t i
    cdef Py_ssize_t length
    cdef Py_ssize_t item_size
    cdef Py_ssize_t arr_offset
    cdef Py_ssize_t offset_in_bits
    cdef Py_ssize_t offset_in_bytes
    cdef Py_ssize_t bit_offset
    cdef Py_ssize_t byte_index
    cdef Py_ssize_t bit_index
    cdef uint64_t hash_val
    cdef list buffers = chunk.buffers()
    cdef object validity_buf = buffers[0]
    cdef object data_buf = buffers[1]

    validity = <const uint8_t*><uintptr_t>(validity_buf.address) if validity_buf else NULL
    data = <const uint8_t*><uintptr_t>(data_buf.address)
    length = len(chunk)
    item_size = chunk.type.bit_width // 8
    arr_offset = chunk.offset

    offset_in_bits = arr_offset & 7
    offset_in_bytes = arr_offset >> 3

    if validity is NULL:
        if item_size == 8:
            for i in range(length):
                hash_val = (<uint64_t*>(data + ((arr_offset + i) << 3)))[0]
                update_row_hash(row_hashes, row_offset + i, hash_val)
        else:
            for i in range(length):
                hash_val = cy_xxhash3_64(data + ((arr_offset + i) * item_size), item_size)
                update_row_hash(row_hashes, row_offset + i, hash_val)
    else:
        if item_size == 8:
            for i in range(length):
                bit_offset = offset_in_bits + i
                byte_index = offset_in_bytes + (bit_offset >> 3)
                bit_index = bit_offset & 7
                if not (validity[byte_index] & (1 << bit_index)):
                    hash_val = NULL_HASH
                else:
                    hash_val = (<uint64_t*>(data + ((arr_offset + i) << 3)))[0]
                update_row_hash(row_hashes, row_offset + i, hash_val)
        else:
            for i in range(length):
                bit_offset = offset_in_bits + i
                byte_index = offset_in_bytes + (bit_offset >> 3)
                bit_index = bit_offset & 7
                if not (validity[byte_index] & (1 << bit_index)):
                    hash_val = NULL_HASH
                else:
                    hash_val = cy_xxhash3_64(data + ((arr_offset + i) * item_size), item_size)
                update_row_hash(row_hashes, row_offset + i, hash_val)


# Composite Type Handler (List)
cdef void process_list_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    """
    Processes a ListArray chunk by slicing the child array correctly,
    combining each sub-element's hash, and mixing it into `row_hashes`.
    """

    cdef:
        const uint8_t* validity
        const int32_t* offsets
        Py_ssize_t i, j, length, data_size
        Py_ssize_t start, end, sub_length
        Py_ssize_t arr_offset, child_offset
        object child_array, sublist
        uint64_t hash_val
        list buffers = chunk.buffers()
        uint64_t c1 = <uint64_t>0xbf58476d1ce4e5b9U
        uint64_t c2 = <uint64_t>0x94d049bb133111ebU
        cdef char* data_ptr

    # Obtain addresses of validity bitmap and offsets buffer
    validity = <uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
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
        # Check validity for the i-th list in this chunk
        if validity and not (validity[i >> 3] & (1 << (i & 7))):
            hash_val = NULL_HASH
        else:
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
                    hash_val = (hash_val ^ (hash_val >> 30)) * c1
                    hash_val = (hash_val ^ (hash_val >> 27)) * c2
                    hash_val = hash_val ^ (hash_val >> 31)

        # Merge this row's final list-hash into row_hashes
        update_row_hash(row_hashes, row_offset + i, hash_val)


cdef void process_boolean_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    cdef const uint8_t* validity
    cdef const uint8_t* data
    cdef Py_ssize_t i, length, arr_offset
    cdef Py_ssize_t bit_index, byte_index, bit_in_byte
    cdef uint64_t hash_val
    cdef list buffers = chunk.buffers()

    validity = <const uint8_t*><uintptr_t>(buffers[0].address) if len(buffers) > 0 and buffers[0] else NULL
    data = <const uint8_t*><uintptr_t>(buffers[1].address) if len(buffers) > 1 and buffers[1] else NULL
    length = len(chunk)
    arr_offset = chunk.offset

    for i in range(length):
        bit_index = arr_offset + i
        byte_index = bit_index >> 3
        bit_in_byte = bit_index & 7

        if validity and not (validity[byte_index] & (1 << bit_in_byte)):
            hash_val = NULL_HASH
        elif data and (data[byte_index] & (1 << bit_in_byte)):
            hash_val = TRUE_HASH
        else:
            hash_val = FALSE_HASH

        update_row_hash(row_hashes, row_offset + i, hash_val)


cdef void process_generic_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    """
    Fallback handler for types without a specific buffer-aware handler.
    This function requires the GIL because it calls Python APIs (PyObject_Hash, exception handling).
    """
    cdef const uint8_t* validity
    cdef Py_ssize_t i, length, arr_offset
    cdef uint64_t hash_val
    cdef list buffers = chunk.buffers()
    cdef Py_ssize_t bit_index, byte_index, bit_in_byte

    validity = <const uint8_t*><uintptr_t>(buffers[0].address) if len(buffers) > 0 and buffers[0] else NULL
    length = len(chunk)
    arr_offset = chunk.offset

    # This loop must run with the GIL held
    for i in range(length):
        bit_index = arr_offset + i
        byte_index = bit_index >> 3
        bit_in_byte = bit_index & 7

        if validity and not (validity[byte_index] & (1 << bit_in_byte)):
            hash_val = NULL_HASH
        else:
            # Fall back to Python object hashing
            try:
                hash_val = <uint64_t>PyObject_Hash(chunk[i]) & 0xFFFFFFFFFFFFFFFFU
            except Exception:
                # Defensive fallback if object is unhashable or throws
                hash_val = EMPTY_HASH

        update_row_hash(row_hashes, row_offset + i, hash_val)


cdef inline void update_row_hash(uint64_t[::1] row_hashes, Py_ssize_t row_idx, uint64_t col_hash) noexcept nogil:
    """
    Combine column hashes using a mixing function (xxhash finalizer)
    """
    cdef uint64_t h
    h = row_hashes[row_idx]
    h = (h ^ col_hash) * <uint64_t>0x9e3779b97f4a7c15U
    h ^= h >> 32
    row_hashes[row_idx] = h

cpdef uint64_t[::1] compute_hashes(object table, list columns):
    """
    Python wrapper for compute_row_hashes that returns an array.array of uint64.
    """
    cdef Py_ssize_t n = table.num_rows
    cdef object hashes_array = array.array("Q", [0] * n)  # 'Q' is for unsigned long long (uint64)
    cdef uint64_t[::1] mv = hashes_array  # memoryview over array.array
    compute_row_hashes(table, columns, mv)
    return hashes_array
