# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint32_t, int8_t
from libc.stdlib cimport strtol
from libc.string cimport strlen
from cpython cimport PyUnicode_AsUTF8String

import numpy
cimport numpy
numpy.import_array()

cdef inline uint32_t ip_to_int(const char* ip):

    # Check if the input string is at least 7 characters long
    if strlen(ip) < 7:
        raise ValueError("Invalid IP address: too short")

    cdef uint32_t result = 0
    cdef uint32_t num = 0
    cdef int8_t shift = 24  # Start with the leftmost byte
    cdef char* end

    # Convert each part of the IP to an integer
    for _ in range(4):
        num = strtol(ip, &end, 10)  # Convert substring to long
        if num > 255 or ip == end or (end[0] not in (b'.', b'\0') and _ < 3):  # Validate octet and check for non-digit characters
            raise ValueError("Invalid IP address: invalid part")
        result += num << shift
        shift -= 8
        if end[0] == b'\0':  # Check if end of string
            break
        ip = end + 1  # Move to the next part

    if shift != -8 or end[0] != b'\0':  # Ensure exactly 4 octets and end of string
        raise ValueError("Invalid IP address: not enough parts")

    return result


cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_ip_in_cidr(numpy.ndarray ip_addresses, str cidr):
    """
    Check if IP addresses are within a CIDR block.

    Parameters:
        ip_addresses: Array of IP address strings
        cidr: CIDR notation string (e.g., "192.168.1.0/24")

    Returns:
        Boolean array indicating which IPs are in the CIDR block
    """

    # CIDR validation...
    cdef int slash_idx = cidr.find('/')
    if slash_idx == -1:
        raise ValueError("Invalid CIDR notation")
    cdef int mask_size = int(cidr[slash_idx+1:])
    if mask_size < 0 or mask_size > 32:
        raise ValueError("Invalid CIDR notation")

    cdef str base_ip_str = cidr[:slash_idx]
    cdef uint32_t netmask = (0xFFFFFFFF << (32 - mask_size)) & 0xFFFFFFFF
    cdef uint32_t base_ip = ip_to_int(PyUnicode_AsUTF8String(base_ip_str))

    cdef bytes ip_bytes

    cdef Py_ssize_t arr_len = ip_addresses.shape[0]
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(arr_len, dtype=numpy.uint8)

    # Use memoryview for input if possible
    cdef Py_ssize_t i
    cdef object ip_address
    cdef uint32_t ip_int

    for i in range(arr_len):
        ip_address = ip_addresses[i]
        if ip_address is not None:
            ip_bytes = PyUnicode_AsUTF8String(ip_address)
            ip_int = ip_to_int(ip_bytes)
            result[i] = (ip_int & netmask) == base_ip

    return result
