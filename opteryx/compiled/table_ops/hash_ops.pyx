# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow

from libc.stdint cimport int32_t, uint8_t, uint64_t, uintptr_t
from cpython.object cimport PyObject_Hash
from libc.string cimport memcpy
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
    cdef Py_ssize_t str_len
    cdef Py_ssize_t start, end

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
                hash_val = <uint64_t>cy_xxhash3_64(data + start, <size_t>str_len)

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
    cdef uint64_t tmp_val
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
                # Safe load into local to avoid unaligned access
                memcpy(&tmp_val, data + ((arr_offset + i) << 3), 8)
                hash_val = tmp_val
                update_row_hash(row_hashes, row_offset + i, hash_val)
        else:
            for i in range(length):
                hash_val = cy_xxhash3_64(data + ((arr_offset + i) * item_size), <size_t>item_size)
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
                    memcpy(&tmp_val, data + ((arr_offset + i) << 3), 8)
                    hash_val = tmp_val
                update_row_hash(row_hashes, row_offset + i, hash_val)
        else:
            for i in range(length):
                bit_offset = offset_in_bits + i
                byte_index = offset_in_bytes + (bit_offset >> 3)
                bit_index = bit_offset & 7
                if not (validity[byte_index] & (1 << bit_index)):
                    hash_val = NULL_HASH
                else:
                    hash_val = cy_xxhash3_64(data + ((arr_offset + i) * item_size), <size_t>item_size)
                update_row_hash(row_hashes, row_offset + i, hash_val)


# Composite Type Handler (List)
cdef void process_list_chunk(object chunk, uint64_t[::1] row_hashes, Py_ssize_t row_offset):
    """
    Processes a ListArray chunk by slicing the child array correctly,
    combining each sub-element's hash, and mixing it into `row_hashes`.
    """

    # New buffer-aware fast paths for primitive and string child types
    # Declare all C variables at function scope (Cython requirement)
    cdef:
        const uint8_t* list_validity
        const int32_t* list_offsets
        const uint8_t* child_validity
        const uint8_t* child_data_bytes
        const char* child_data_chars
        const int32_t* child_offsets
        Py_ssize_t i, j, length
        Py_ssize_t start, end, sub_length
        Py_ssize_t arr_offset, child_offset
        Py_ssize_t item_size, child_idx, bit
        Py_ssize_t child_offset_bytes, child_buffer_len
        object child_array, sublist
        list buffers
        list child_buffers
        uint64_t hash_val, elem_hash, tmp_val
        uint64_t c1 = <uint64_t>0xbf58476d1ce4e5b9U
        uint64_t c2 = <uint64_t>0x94d049bb133111ebU

    buffers = chunk.buffers()
    # Obtain addresses of validity bitmap and offsets buffer for the list
    list_validity = <uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
    list_offsets = <int32_t*><uintptr_t>(buffers[1].address) if len(buffers) > 1 else NULL

    # Child array and its metadata
    child_array = chunk.values
    length = len(chunk)
    arr_offset = chunk.offset
    child_offset = child_array.offset

    # Default initializations
    child_validity = NULL
    child_data_bytes = NULL
    child_data_chars = NULL
    child_offsets = NULL
    child_buffers = []
    elem_hash = 0
    tmp_val = 0

    # Decide on fast path based on child type once per chunk
    if pyarrow.types.is_integer(child_array.type) or pyarrow.types.is_floating(child_array.type) or pyarrow.types.is_temporal(child_array.type):
        # Buffer-aware primitive child fast-path (no Python objects in inner loop)
        child_buffers = child_array.buffers()
        child_validity = <const uint8_t*><uintptr_t>(child_buffers[0].address) if child_buffers[0] else NULL
        child_data_bytes = <const uint8_t*><uintptr_t>(child_buffers[1].address)
        item_size = child_array.type.bit_width // 8
        child_offset_bytes = child_offset * item_size

        for i in range(length):
            # list validity
            if list_validity and not (list_validity[(arr_offset + i) >> 3] & (1 << ((arr_offset + i) & 7))):
                hash_val = NULL_HASH
            else:
                start = list_offsets[arr_offset + i]
                end = list_offsets[arr_offset + i + 1]
                sub_length = end - start
                hash_val = SEED
                if sub_length == 0:
                    hash_val = EMPTY_HASH
                else:
                    # iterate child elements directly from child_data
                    for j in range(sub_length):
                        child_idx = start + j
                        if child_validity:
                            bit = (child_offset + child_idx)
                            if not (child_validity[bit >> 3] & (1 << (bit & 7))):
                                elem_hash = NULL_HASH
                                # mix and continue
                                hash_val = elem_hash ^ hash_val
                                hash_val = (hash_val ^ (hash_val >> 30)) * c1
                                hash_val = (hash_val ^ (hash_val >> 27)) * c2
                                hash_val = hash_val ^ (hash_val >> 31)
                                continue

                        if item_size == 8:
                            memcpy(&tmp_val, child_data_bytes + child_offset_bytes + (child_idx << 3), 8)
                            elem_hash = tmp_val
                        else:
                            elem_hash = <uint64_t>cy_xxhash3_64(child_data_bytes + child_offset_bytes + (child_idx * item_size), <size_t>item_size)

                        # mix element hash
                        hash_val = elem_hash ^ hash_val
                        hash_val = (hash_val ^ (hash_val >> 30)) * c1
                        hash_val = (hash_val ^ (hash_val >> 27)) * c2
                        hash_val = hash_val ^ (hash_val >> 31)

            update_row_hash(row_hashes, row_offset + i, hash_val)

    elif pyarrow.types.is_string(child_array.type) or pyarrow.types.is_binary(child_array.type):
        # Buffer-aware string child fast-path
        child_buffers = child_array.buffers()
        child_validity = <const uint8_t*><uintptr_t>(child_buffers[0].address) if child_buffers[0] else NULL
        child_offsets = <const int32_t*><uintptr_t>(child_buffers[1].address)
        child_data_chars = <const char*><uintptr_t>(child_buffers[2].address)
        child_buffer_len = child_buffers[2].size if child_buffers[2] is not None else 0

        for i in range(length):
            if list_validity and not (list_validity[(arr_offset + i) >> 3] & (1 << ((arr_offset + i) & 7))):
                hash_val = NULL_HASH
            else:
                start = list_offsets[arr_offset + i]
                end = list_offsets[arr_offset + i + 1]
                sub_length = end - start
                hash_val = SEED
                if sub_length == 0:
                    hash_val = EMPTY_HASH
                else:
                    for j in range(sub_length):
                        child_idx = start + j + child_offset
                        # check child validity
                        if child_validity and not (child_validity[child_idx >> 3] & (1 << (child_idx & 7))):
                            elem_hash = NULL_HASH
                        else:
                            s = child_offsets[child_idx]
                            e = child_offsets[child_idx + 1]
                            ln = e - s
                            if ln <= 0 or (s + ln) > child_buffer_len:
                                elem_hash = EMPTY_HASH
                            else:
                                elem_hash = <uint64_t>cy_xxhash3_64(child_data_chars + s, <size_t>ln)

                        # mix
                        hash_val = elem_hash ^ hash_val
                        hash_val = (hash_val ^ (hash_val >> 30)) * c1
                        hash_val = (hash_val ^ (hash_val >> 27)) * c2
                        hash_val = hash_val ^ (hash_val >> 31)

            update_row_hash(row_hashes, row_offset + i, hash_val)

    else:
        # Fallback: per-element Python handling (kept minimal and only for unsupported child types)
        for i in range(length):
            if list_validity and not (list_validity[(arr_offset + i) >> 3] & (1 << ((arr_offset + i) & 7))):
                hash_val = NULL_HASH
            else:
                start = list_offsets[arr_offset + i]
                end = list_offsets[arr_offset + i + 1]
                sub_length = end - start
                hash_val = SEED
                if sub_length == 0:
                    hash_val = EMPTY_HASH
                else:
                    sublist = child_array.slice(start + child_offset, sub_length)
                    for j in range(sub_length):
                        try:
                            elem_hash = <uint64_t>PyObject_Hash(sublist[j]) & 0xFFFFFFFFFFFFFFFFU
                        except Exception:
                            elem_hash = EMPTY_HASH

                        hash_val = elem_hash ^ hash_val
                        hash_val = (hash_val ^ (hash_val >> 30)) * c1
                        hash_val = (hash_val ^ (hash_val >> 27)) * c2
                        hash_val = hash_val ^ (hash_val >> 31)

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
