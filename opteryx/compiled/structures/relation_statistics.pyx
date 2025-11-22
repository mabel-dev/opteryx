# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

from libc.stdint cimport uint8_t, int64_t, uint64_t
from libc.math cimport isnan
from libcpp.unordered_map cimport unordered_map
from cython.operator cimport dereference, preincrement
from libcpp.string cimport string
from libc.string cimport memcpy, memset
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.unicode cimport PyUnicode_AsUTF8AndSize, PyUnicode_Check

import datetime
from decimal import Decimal
from cython cimport cast

cdef uint64_t _INT64_HIGH_BIT = (<uint64_t>1) << 63
cdef int64_t NULL_FLAG = -cast(int64_t, _INT64_HIGH_BIT)              # -9223372036854775808
cdef int64_t MIN_SIGNED_64BIT = NULL_FLAG + 1               # -9223372036854775807
cdef int64_t MAX_SIGNED_64BIT = cast(int64_t, _INT64_HIGH_BIT - 1)    # 9223372036854775807

cdef inline bint map_contains(unordered_map[string, int64_t]& m, string key):
    return m.find(key) != m.end()

cdef inline int64_t map_get(unordered_map[string, int64_t]& m, string key, int64_t default_val=0):
    cdef unordered_map[string, int64_t].iterator it = m.find(key)
    if it != m.end():
        return m[key]
    return default_val

cdef inline int64_t _ensure_64bit_range(object val):
    if val < MIN_SIGNED_64BIT:
        return MIN_SIGNED_64BIT
    if val > MAX_SIGNED_64BIT:
        return MAX_SIGNED_64BIT
    return val

cdef inline int64_t decode_signed_big_endian_bytes(const char* buf):
    cdef uint64_t result = 0
    cdef int i
    for i in range(8):
        result = (result << 8) | <uint8_t>buf[i]
    return <int64_t>result

cpdef int64_t to_int(object value):
    """
    Convert a value to a signed 64-bit int for order-preserving comparisons.
    Returns MIN_SIGNED_64BIT to MAX_SIGNED_64BIT or raises ValueError.
    """
    cdef int64_t result
    cdef type value_type = type(value)

    cdef char* raw
    cdef const char* _raw
    cdef Py_ssize_t length
    cdef char buf[8]

    if value_type == int:
        return _ensure_64bit_range(value)

    if value_type == float:
        if value == float("inf"):
            return MAX_SIGNED_64BIT
        if value == float("-inf"):
            return MIN_SIGNED_64BIT
        if isnan(value):
            return NULL_FLAG
        return _ensure_64bit_range(<int64_t>round(value))

    if value_type == datetime.datetime:
        return _ensure_64bit_range(<int64_t>round(value.timestamp()))

    if value_type == datetime.date:
        # Converts to days since epoch (1970-01-01)
        timestamp = int(value.strftime("%s"))
        return _ensure_64bit_range(<int64_t>timestamp)

    if value_type == datetime.time:
        result = value.hour * 3600 + value.minute * 60 + value.second
        return _ensure_64bit_range(result)

    if value_type == Decimal:
        return _ensure_64bit_range(<int64_t>round(value))

    if value_type == str:
        # Keep the first byte as 0 to ensure the order of the 64bit int
        # is preserved by creating negative numbers
        memset(buf, 0, 8)

        if PyUnicode_Check(value):
            _raw = PyUnicode_AsUTF8AndSize(value, &length)
            memcpy(&buf[1], _raw, min(length, 7))
        else:
            PyBytes_AsStringAndSize(value, &raw, &length)
            memcpy(&buf[1], raw, min(length, 7))

        return _ensure_64bit_range(decode_signed_big_endian_bytes(buf))

    if value_type == bytes:
        # Keep the first byte as 0 to ensure order is preserved
        memset(buf, 0, 8)
        PyBytes_AsStringAndSize(value, &raw, &length)
        memcpy(&buf[1], raw, min(length, 7))
        return _ensure_64bit_range(decode_signed_big_endian_bytes(buf))

    return NULL_FLAG

cdef inline void write_int64(char* buf, int offset, int64_t value):
    cdef int i
    for i in range(8):
        buf[offset + 7 - i] = <char>((value >> (i * 8)) & 0xFF)

cdef inline int64_t read_int64(const char* buf, int offset):
    cdef int64_t result = 0
    cdef int i
    for i in range(8):
        result = (result << 8) | (<unsigned char>buf[offset + i])
    return result


cdef inline void write_map(unordered_map[string, int64_t]& m, bytearray out):

    cdef int64_t size = <int64_t>m.size()
    cdef unordered_map[string, int64_t].iterator it = m.begin()
    cdef string key
    cdef int64_t value

    out += (<int64_t>size).to_bytes(4, "big")

    # Iterate through all entries
    while it != m.end():
        # Access current key-value pair
        key = dereference(it).first
        value = dereference(it).second

        out.append(len(key))
        out += key
        out += (<int64_t>value).to_bytes(8, "big", signed=True)

        preincrement(it)


cdef inline void read_map(const char* buf, Py_ssize_t* offset, unordered_map[string, int64_t]& m):
    cdef int64_t size = 0
    cdef int i

    for i in range(4):
        size = (size << 8) | (<unsigned char>buf[offset[0] + i])
    offset[0] = offset[0] + 4
    for i in range(size):
        key_len = <unsigned char>buf[offset[0]]
        offset[0] = offset[0] + 1
        key = string(buf + offset[0], key_len)
        offset[0] = offset[0] + key_len
        val = read_int64(buf, offset[0])
        offset[0] = offset[0] + 8
        m[key] = val

cdef class RelationStatistics:
    cdef public int64_t record_count
    cdef public int64_t record_count_estimate
    cdef public unordered_map[string, int64_t] null_count
    cdef public unordered_map[string, int64_t] lower_bounds
    cdef public unordered_map[string, int64_t] upper_bounds
    cdef public unordered_map[string, int64_t] cardinality_estimate

    def __cinit__(self):
        self.record_count = 0
        self.record_count_estimate = 0
        self.null_count = {}
        self.lower_bounds = {}
        self.upper_bounds = {}
        self.cardinality_estimate = {}

    cpdef void update_lower(self, str column, object value, object index=None):
        cdef int64_t int_value = to_int(value)
        cdef string skey = column.encode()

        if int_value != NULL_FLAG:
            if not map_contains(self.lower_bounds, skey) or int_value < self.lower_bounds[skey]:
                self.lower_bounds[skey] = int_value

    cpdef void update_upper(self, str column, object value, object index=None):
        cdef int64_t int_value = to_int(value)
        cdef string skey = column.encode()

        if int_value != NULL_FLAG:
            if not map_contains(self.upper_bounds, skey) or int_value > self.upper_bounds[skey]:
                self.upper_bounds[skey] = int_value

    cpdef void add_null(self, str column, int nulls):
        cdef string skey = column.encode()

        self.null_count[skey] = map_get(self.null_count, skey, 0) + nulls

    cpdef void set_cardinality_estimate(self, str column, int cardinality):
        cdef string skey = column.encode()

        self.cardinality_estimate[skey] = cardinality

    cpdef bytes to_bytes(self):
        cdef bytearray out = bytearray()
        out += (<int64_t>self.record_count).to_bytes(8, "big")
        out += (<int64_t>self.record_count_estimate).to_bytes(8, "big")
        write_map(self.null_count, out)
        write_map(self.lower_bounds, out)
        write_map(self.upper_bounds, out)
        write_map(self.cardinality_estimate, out)
        return bytes(out)

    @classmethod
    def from_bytes(cls, bytes data):
        cdef const char* buf
        buf = data
        cdef Py_ssize_t offset = 0
        cdef RelationStatistics inst = cls()
        inst.record_count = read_int64(buf, offset)
        offset += 8
        inst.record_count_estimate = read_int64(buf, offset)
        offset += 8
        read_map(buf, &offset, inst.null_count)
        read_map(buf, &offset, inst.lower_bounds)
        read_map(buf, &offset, inst.upper_bounds)
        read_map(buf, &offset, inst.cardinality_estimate)
        return inst

    cpdef void merge(self, RelationStatistics other):
        """
        Merge another RelationStatistics into this one.

        Parameters:
            other: RelationStatistics
                The statistics object to merge into this one.

        Behavior:
            - Sums record counts
            - Takes minimum of lower bounds
            - Takes maximum of upper bounds
            - Sums null counts
            - Takes maximum of cardinality estimates
        """
        cdef unordered_map[string, int64_t].iterator it
        cdef string key
        cdef int64_t value

        # Add record counts
        self.record_count += other.record_count
        self.record_count_estimate += other.record_count_estimate

        # Merge lower bounds (take minimum)
        it = other.lower_bounds.begin()
        while it != other.lower_bounds.end():
            key = dereference(it).first
            value = dereference(it).second
            if not map_contains(self.lower_bounds, key) or value < self.lower_bounds[key]:
                self.lower_bounds[key] = value
            preincrement(it)

        # Merge upper bounds (take maximum)
        it = other.upper_bounds.begin()
        while it != other.upper_bounds.end():
            key = dereference(it).first
            value = dereference(it).second
            if not map_contains(self.upper_bounds, key) or value > self.upper_bounds[key]:
                self.upper_bounds[key] = value
            preincrement(it)

        # Merge null counts (sum)
        it = other.null_count.begin()
        while it != other.null_count.end():
            key = dereference(it).first
            value = dereference(it).second
            self.null_count[key] = map_get(self.null_count, key, 0) + value
            preincrement(it)

        # Merge cardinality estimates (take maximum)
        it = other.cardinality_estimate.begin()
        while it != other.cardinality_estimate.end():
            key = dereference(it).first
            value = dereference(it).second
            if not map_contains(self.cardinality_estimate, key) or value > self.cardinality_estimate[key]:
                self.cardinality_estimate[key] = value
            preincrement(it)

    def __deepcopy__(self, memo):
        return self
