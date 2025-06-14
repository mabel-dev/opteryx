# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
from cpython.object cimport PyObject_Hash
from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t, uintptr_t
from cpython.object cimport PyObject_Hash
from cpython.bytes cimport PyBytes_AsString, PyBytes_Size

from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64
from opteryx.third_party.abseil.containers cimport FlatHashSet

import pyarrow

cdef:
    int64_t NULL_HASH = <int64_t>0xBADF00D
    int64_t EMPTY_HASH = <int64_t>0xBADC0FFEE
    uint64_t SEED = <uint64_t>0x9e3779b97f4a7c15

cpdef FlatHashSet count_distinct(column, FlatHashSet seen_hashes):

    """Process column using type-specific handlers"""
    cdef object chunk

    for chunk in column.chunks if isinstance(column, pyarrow.ChunkedArray) else [column]:
        dtype = chunk.type
        if pyarrow.types.is_string(dtype) or pyarrow.types.is_binary(dtype):
            process_string_chunk(chunk, seen_hashes)
        elif pyarrow.types.is_integer(dtype) or pyarrow.types.is_floating(dtype) or pyarrow.types.is_temporal(dtype):
            process_primitive_chunk(chunk, seen_hashes)
        elif pyarrow.types.is_list(dtype):
            process_list_chunk(chunk, seen_hashes)
        elif pyarrow.types.is_boolean(dtype):
            process_boolean_chunk(chunk, seen_hashes)
        else:
            process_generic_chunk(chunk, seen_hashes)

    return seen_hashes


# String Chunk Handler
cdef void process_string_chunk(chunk, FlatHashSet seen_hashes):
    cdef:
        const uint8_t* validity
        const int32_t* offsets
        const char* data
        Py_ssize_t i, row_count, buffer_length
        Py_ssize_t arr_offset, offset_in_bits, offset_in_bytes, byte_index, bit_index
        uint64_t hash_val
        list buffers = chunk.buffers()
        size_t str_len
        int start, end

    # Handle potential missing buffers
    validity = <uint8_t*><uintptr_t>(buffers[0].address) if len(buffers) > 0 and buffers[0] else NULL
    offsets = <int32_t*><uintptr_t>(buffers[1].address) if len(buffers) > 1 else NULL
    data = <const char*><uintptr_t>buffers[2].address if len(buffers) > 2 else NULL
    row_count = len(chunk)
    buffer_length = len(data) if data else 0
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

        seen_hashes.just_insert(hash_val)


# Primitive Numeric Handler (Int/Float)
cdef void process_primitive_chunk(chunk, FlatHashSet seen_hashes):
    cdef:
        const uint8_t* validity
        const uint8_t* data
        Py_ssize_t i, length, item_size
        Py_ssize_t arr_offset, offset_in_bits, offset_in_bytes, byte_index, bit_index
        uint64_t hash_val
        list buffers = chunk.buffers()

    validity = <uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
    data = <uint8_t*><uintptr_t>(buffers[1].address)
    length = len(chunk)
    item_size = chunk.type.bit_width // 8
    arr_offset = chunk.offset  # Account for non-zero offset in chunk

    # Calculate the byte and bit offset for validity
    offset_in_bits = arr_offset & 7
    offset_in_bytes = arr_offset >> 3

    for i in range(length):
        # Correctly locate validity bit for this row
        byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
        bit_index = (offset_in_bits + i) & 7

        # Check validity bit, considering chunk offset
        if validity and not (validity[byte_index] & (1 << bit_index)):
            hash_val = NULL_HASH
        elif item_size == <uint64_t>8:
            # cast 8-byte values directly into hash_val
            hash_val = (<uint64_t*>(data + ((arr_offset + i) * 8)))[0]
        else:
            hash_val = cy_xxhash3_64(data + ((arr_offset + i) * item_size), item_size)

        seen_hashes.just_insert(hash_val)


# Composite Type Handler (List)
cdef void process_list_chunk(chunk, FlatHashSet seen_hashes):
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
        uint64_t c1 = <uint64_t>0xbf58476d1ce4e5b9
        uint64_t c2 = <uint64_t>0x94d049bb133111eb
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

                    # Optionally apply SplitMix64 finalizer (commented out for now)
                    hash_val = (hash_val ^ (hash_val >> 30)) * c1
                    hash_val = (hash_val ^ (hash_val >> 27)) * c2
                    hash_val = hash_val ^ (hash_val >> 31)

        seen_hashes.just_insert(hash_val)

# Add boolean chunk handler
cdef void process_boolean_chunk(chunk, FlatHashSet seen_hashes):
    cdef:
        const uint8_t* validity
        const uint8_t* data
        Py_ssize_t i, length
        uint64_t hash_val
        list buffers = chunk.buffers()

    validity = <uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
    data = <uint8_t*><uintptr_t>(buffers[1].address) if buffers[1] else NULL
    length = len(chunk)

    for i in range(length):
        if validity and not (validity[i >> 3] & (1 << (i & 7))):
            hash_val = NULL_HASH
        else:
            # Booleans are bit-packed - use bitwise ops to extract values
            hash_val = (data[i >> 3] & (1 << (i & 7))) != 0

        seen_hashes.just_insert(hash_val)


cdef void process_generic_chunk(chunk, FlatHashSet seen_hashes):
    """Fallback handler for types without a specific handler"""
    cdef:
        const uint8_t* validity
        Py_ssize_t i, length
        uint64_t hash_val
        list buffers = chunk.buffers()

    validity = <uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
    length = len(chunk)

    for i in range(length):
        if validity and not (validity[i >> 3] & (1 << (i & 7))):
            hash_val = NULL_HASH
        else:
            hash_val = PyObject_Hash(chunk[i])

        seen_hashes.just_insert(hash_val)
