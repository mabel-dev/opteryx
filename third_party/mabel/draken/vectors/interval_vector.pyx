# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
IntervalVector: two-component interval storage for Draken.

Draken stores INTERVAL values as a pair of int64 values representing
(months, microseconds). This diverges from Apache Arrow's internal
interval layout, so conversions to and from Arrow require light-weight
copying and normalization logic.

This module provides:
- IntervalVector class with hashing, null handling, and gather support
- Conversion helpers for Arrow month-day-nano interval arrays
- Conversion helpers for fixed-size binary (16-byte) arrays used for
  packed interval transport
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AsString

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc
from libc.string cimport memset

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DRAKEN_INTERVAL
from opteryx.draken.core.fixed_vector cimport alloc_fixed_buffer
from opteryx.draken.core.fixed_vector cimport buf_dtype
from opteryx.draken.core.fixed_vector cimport buf_itemsize
from opteryx.draken.core.fixed_vector cimport buf_length
from opteryx.draken.core.fixed_vector cimport free_fixed_buffer
from opteryx.draken.vectors.vector cimport MIX_HASH_CONSTANT, NULL_HASH, Vector, mix_hash, simd_mix_hash

DEF INTERVAL_HASH_CHUNK = 512

cdef struct IntervalValue:
    int64_t months
    int64_t microseconds

cdef struct ArrowMonthDayNanoValue:
    int32_t months
    int32_t days
    int64_t nanoseconds

cdef struct ArrowDayTimeValue:
    int32_t days
    int32_t milliseconds

cdef int64_t MICROSECONDS_PER_SECOND = 1000000
cdef int64_t MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
cdef int64_t MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
cdef int64_t MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
cdef int64_t MICROSECONDS_PER_MILLISECOND = 1000
cdef int64_t NANOSECONDS_PER_MICROSECOND = 1000
cdef size_t INTERVAL_ITEMSIZE = sizeof(IntervalValue)

cdef inline bint _is_valid(DrakenFixedBuffer* ptr, Py_ssize_t idx) nogil:
    if ptr.null_bitmap == NULL:
        return True
    return (ptr.null_bitmap[idx >> 3] >> (idx & 7)) & 1

cdef void _copy_arrow_null_bitmap(DrakenFixedBuffer* dest, object array):
    cdef Py_ssize_t length = len(array)
    if length == 0:
        dest.null_bitmap = NULL
        return

    cdef object bufs = array.buffers()
    cdef object null_buf = bufs[0]
    if null_buf is None:
        dest.null_bitmap = NULL
        return

    cdef size_t nbytes = (length + 7) >> 3
    if nbytes == 0:
        dest.null_bitmap = NULL
        return

    dest.null_bitmap = <uint8_t*> malloc(nbytes)
    if dest.null_bitmap == NULL:
        raise MemoryError()
    memset(dest.null_bitmap, 0, nbytes)
    cdef bytes null_bytes = null_buf.to_pybytes()
    cdef const uint8_t* src = <const uint8_t*> PyBytes_AsString(null_bytes)
    cdef Py_ssize_t offset = array.offset
    cdef Py_ssize_t bit_index
    cdef Py_ssize_t i
    for i in range(length):
        bit_index = offset + i
        if (src[bit_index >> 3] >> (bit_index & 7)) & 1:
            dest.null_bitmap[i >> 3] |= (1 << (i & 7))

cdef inline void _divmod_microseconds(int64_t total, int64_t* out_days, int64_t* out_remainder) noexcept nogil:
    cdef int64_t q = total / MICROSECONDS_PER_DAY
    cdef int64_t r = total - q * MICROSECONDS_PER_DAY
    if r < 0:
        q -= 1
        r += MICROSECONDS_PER_DAY
    out_days[0] = q
    out_remainder[0] = r

cdef class IntervalVector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        self._arrow_data_buf = None
        self._arrow_null_buf = None
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INTERVAL, length, INTERVAL_ITEMSIZE)
            self.owns_data = True

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    @property
    def length(self):
        return buf_length(self.ptr)

    @property
    def itemsize(self):
        return buf_itemsize(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenFixedBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        if not _is_valid(ptr, i):
            return None
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        return (data[i].months, data[i].microseconds)

    def to_arrow(self):
        return self.to_arrow_interval()

    cpdef object to_arrow_interval(self):
        import pyarrow as pa

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef list rows = [None] * n
        cdef Py_ssize_t i
        cdef int64_t days
        cdef int64_t remainder
        for i in range(n):
            if not _is_valid(ptr, i):
                continue
            _divmod_microseconds(data[i].microseconds, &days, &remainder)
            rows[i] = (
                data[i].months,
                days,
                remainder * NANOSECONDS_PER_MICROSECOND,
            )
        return pa.array(rows, type=pa.month_day_nano_interval())

    cpdef object to_arrow_binary(self):
        import pyarrow as pa

        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        cdef intptr_t data_addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(data_addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(
                pa.foreign_buffer(
                    <intptr_t> self.ptr.null_bitmap,
                    (self.ptr.length + 7) // 8,
                    base=self,
                )
            )
        else:
            buffers.append(None)
        buffers.append(data_buf)
        cdef object binary_factory = getattr(pa, "fixed_size_binary", None)
        cdef object binary_type
        if binary_factory is not None:
            binary_type = binary_factory(<int>INTERVAL_ITEMSIZE)
        else:
            binary_type = pa.binary(<int>INTERVAL_ITEMSIZE)

        return pa.Array.from_buffers(
            binary_type,
            buf_length(self.ptr),
            buffers,
        )

    cpdef IntervalVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t n = indices.shape[0]
        cdef Py_ssize_t i
        cdef IntervalVector out = IntervalVector(<size_t> n)
        cdef IntervalValue* src = <IntervalValue*> self.ptr.data
        cdef IntervalValue* dst = <IntervalValue*> out.ptr.data
        cdef size_t nbytes = 0
        for i in range(n):
            dst[i] = src[indices[i]]

        if self.ptr.null_bitmap != NULL:
            nbytes = (n + 7) >> 3
            if nbytes:
                out.ptr.null_bitmap = <uint8_t*> malloc(nbytes)
                if out.ptr.null_bitmap == NULL:
                    raise MemoryError()
                memset(out.ptr.null_bitmap, 0, nbytes)
                for i in range(n):
                    if _is_valid(self.ptr, indices[i]):
                        out.ptr.null_bitmap[i >> 3] |= (1 << (i & 7))
        return out

    cpdef int8_t[::1] is_null(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte
        cdef uint8_t bit
        if buf == NULL:
            raise MemoryError()
        if ptr.null_bitmap == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1
        return <int8_t[:n]> buf

    @property
    def null_count(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t count = 0
        if ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            if not _is_valid(ptr, i):
                count += 1
        return count

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        for i in range(n):
            if not _is_valid(ptr, i):
                out.append(None)
            else:
                out.append((data[i].months, data[i].microseconds))
        return out

    cpdef uint64_t[::1] hash(self):
        cdef Py_ssize_t n = self.ptr.length
        cdef uint64_t* buf = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
        cdef Py_ssize_t i
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 0
        cdef uint64_t[::1] view = <uint64_t[:n]> buf
        self.hash_into(view, 0)
        return view

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("IntervalVector.hash_into: output buffer too small")

        cdef Py_ssize_t i
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef uint64_t value
        cdef uint64_t partial
        cdef uint64_t* dst = &out_buf[offset]
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint64_t[INTERVAL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if not has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > INTERVAL_HASH_CHUNK:
                    block = INTERVAL_HASH_CHUNK
                for j in range(block):
                    partial = mix_hash(0, <uint64_t>data[i + j].months)
                    scratch[j] = mix_hash(partial, <uint64_t>data[i + j].microseconds)
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block
            return

        for i in range(n):
            if not _is_valid(ptr, i):
                value = NULL_HASH
            else:
                partial = mix_hash(0, <uint64_t>data[i].months)
                value = mix_hash(partial, <uint64_t>data[i].microseconds)
            dst[i] = mix_hash(dst[i], value)

    def __str__(self):
        cdef list preview = []
        cdef Py_ssize_t i, n = buf_length(self.ptr)
        cdef Py_ssize_t limit = n if n < 10 else 10
        for i in range(limit):
            preview.append(self[i])
        return f"<IntervalVector len={n} values={preview}>"

cdef inline object _maybe_call_factory(object factory):
    if factory is None:
        return None
    return factory()


cdef IntervalVector from_arrow_interval(object array):
    import pyarrow as pa
    import pyarrow.lib as pa_lib

    cdef object pa_type = array.type
    if not pa.types.is_interval(pa_type):
        raise TypeError("Provided array is not an Arrow interval type")

    cdef int type_id = -1
    try:
        type_id = pa_type.id
    except AttributeError:
        type_id = -1

    cdef object mdn_type = _maybe_call_factory(getattr(pa, "month_day_nano_interval", None))
    cdef object month_type = _maybe_call_factory(getattr(pa, "month_interval", None))
    cdef object day_time_type = _maybe_call_factory(getattr(pa, "day_time_interval", None))

    cdef bint is_mdn = False
    cdef bint is_month_only = False
    cdef bint is_day_time = False

    if mdn_type is not None:
        is_mdn = pa_type == mdn_type
    elif type_id == getattr(pa_lib, "Type_INTERVAL_MONTH_DAY_NANO", -1):
        is_mdn = True

    if month_type is not None:
        is_month_only = pa_type == month_type
    elif type_id == getattr(pa_lib, "Type_INTERVAL_MONTHS", -2):
        is_month_only = True

    if day_time_type is not None:
        is_day_time = pa_type == day_time_type
    elif type_id == getattr(pa_lib, "Type_INTERVAL_DAY_TIME", -3):
        is_day_time = True

    cdef Py_ssize_t length = len(array)
    cdef IntervalVector vec = IntervalVector(<size_t> length)
    if length == 0:
        return vec

    _copy_arrow_null_bitmap(vec.ptr, array)

    cdef object bufs = array.buffers()
    cdef intptr_t data_addr = <intptr_t> bufs[1].address
    cdef Py_ssize_t offset = array.offset
    cdef IntervalValue* dst = <IntervalValue*> vec.ptr.data
    cdef ArrowMonthDayNanoValue* src_mdn = NULL
    cdef int32_t* src_months = NULL
    cdef ArrowDayTimeValue* src_dt = NULL
    cdef Py_ssize_t i

    if is_mdn:
        src_mdn = <ArrowMonthDayNanoValue*>(data_addr + offset * sizeof(ArrowMonthDayNanoValue))
        for i in range(length):
            dst[i].months = <int64_t> src_mdn[i].months
            dst[i].microseconds = (
                <int64_t>src_mdn[i].days * MICROSECONDS_PER_DAY
                + src_mdn[i].nanoseconds // NANOSECONDS_PER_MICROSECOND
            )
        return vec

    if is_month_only:
        src_months = <int32_t*>(data_addr + offset * sizeof(int32_t))
        for i in range(length):
            dst[i].months = <int64_t> src_months[i]
            dst[i].microseconds = 0
        return vec

    if is_day_time:
        src_dt = <ArrowDayTimeValue*>(data_addr + offset * sizeof(ArrowDayTimeValue))
        for i in range(length):
            dst[i].months = 0
            dst[i].microseconds = (
                <int64_t>src_dt[i].days * MICROSECONDS_PER_DAY
                + <int64_t>src_dt[i].milliseconds * MICROSECONDS_PER_MILLISECOND
            )
        return vec

    raise TypeError(f"Unsupported Arrow interval subtype: {pa_type}")

cdef IntervalVector from_arrow_binary(object array):
    import pyarrow as pa

    cdef object pa_type = array.type
    if not pa.types.is_fixed_size_binary(pa_type):
        raise TypeError("IntervalVector requires a fixed_size_binary(16) array")
    if pa_type.byte_width != INTERVAL_ITEMSIZE:
        raise ValueError("Fixed-size binary width must be 16 bytes for IntervalVector")

    cdef IntervalVector vec = IntervalVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_INTERVAL
    vec.ptr.itemsize = INTERVAL_ITEMSIZE
    vec.ptr.length = <size_t> len(array)
    vec.ptr.data = <void*> (base_ptr + offset * INTERVAL_ITEMSIZE)

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        vec.ptr.null_bitmap = <uint8_t*> nb_addr
    else:
        vec.ptr.null_bitmap = NULL

    return vec
