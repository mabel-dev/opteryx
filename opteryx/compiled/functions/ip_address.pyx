# cython: language_level=3

from libc.stdint cimport uint32_t
from libc.stdlib cimport strtol
from libc.string cimport strchr
from libc.string cimport strlen
from libc.string cimport memset
import numpy as np
cimport numpy as cnp

import cython

@cython.boundscheck(False)
@cython.wraparound(False)
cdef uint32_t ip_to_int(char* ip):

    # Check if the input string is at least 7 characters long
    if strlen(ip) < 7:
        raise ValueError("Invalid IP address: too short")

    cdef uint32_t result = 0
    cdef uint32_t num = 0
    cdef int shift = 24  # Start with the leftmost byte
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

@cython.boundscheck(False)
@cython.wraparound(False)
def ip_in_cidr(cnp.ndarray ip_addresses, str cidr):

    # CIDR validation...
    if '/' not in cidr or not 0 <= int(cidr.split('/')[1]) <= 32:
        raise ValueError("Invalid CIDR notation")

    cdef uint32_t base_ip, netmask, ip_int
    cdef int mask_size
    cdef str base_ip_str
    cdef list cidr_parts = cidr.split('/')

    base_ip_str, mask_size = cidr_parts[0], int(cidr_parts[1])
    netmask = (0xFFFFFFFF << (32 - mask_size)) & 0xFFFFFFFF

    base_ip = ip_to_int(base_ip_str.encode('utf-8'))

    cdef unsigned char[:] result = np.zeros(ip_addresses.shape[0], dtype=np.bool_)

    for i in range(ip_addresses.shape[0]):
        ip_int = ip_to_int(ip_addresses[i].encode('utf-8'))
        result[i] = (ip_int & netmask) == base_ip

    return np.asarray(result, dtype=bool)
