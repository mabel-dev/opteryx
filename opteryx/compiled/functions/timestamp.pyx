# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.time cimport tm, time_t
from libc.string cimport memset
from libc.stdint cimport int64_t, int32_t, int16_t, uint8_t, int8_t
from cpython.bytes cimport PyBytes_AS_STRING

import datetime

cdef extern from "time.h":
    time_t timegm(tm *timeptr)

cdef const int64_t MICROS_PER_SEC = 1_000_000
cdef int64_t MICROS_PER_MIN = 60 * MICROS_PER_SEC
cdef int64_t MICROS_PER_HOUR = 60 * MICROS_PER_MIN
cdef int64_t MICROS_PER_DAY = 24 * MICROS_PER_HOUR
cdef const unsigned char CHAR_SPACE = 32
cdef const unsigned char CHAR_T = 84
cdef const unsigned char CHAR_COLON = 58
cdef const unsigned char CHAR_DOT = 46
cdef const unsigned char CHAR_Z = 90
cdef const unsigned char CHAR_PLUS = 43
cdef const unsigned char CHAR_MINUS = 45
cdef const unsigned char CHAR_0 = 48
cdef const unsigned char CHAR_9 = 57

cdef struct ParsedDateTime:
    int16_t year
    uint8_t month
    uint8_t day
    uint8_t hour
    uint8_t minute
    uint8_t second
    int32_t micros
    int8_t offset_sign
    uint8_t offset_hour
    uint8_t offset_minute

cdef inline int parse_2digit(const char* s):
    return (s[0] - CHAR_0) * 10 + (s[1] - CHAR_0)

cdef inline int parse_4digit(const char* s):
    return (s[0] - CHAR_0) * 1000 + (s[1] - CHAR_0) * 100 + (s[2] - CHAR_0) * 10 + (s[3] - CHAR_0)

cdef inline bint is_digit(char c):
    return CHAR_0 <= c <= CHAR_9

cdef inline bint is_digit_range(const char* s, Py_ssize_t start, Py_ssize_t end) nogil:
    cdef const char* ptr = s + start
    cdef Py_ssize_t i = end - start
    while i > 0:
        if ptr[0] < CHAR_0 or ptr[0] > CHAR_9:
            return False
        ptr += 1
        i -= 1
    return True

cdef inline int days_in_month(int year, int month):
    if month == 2:
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            return 29
        return 28
    elif month in (1, 3, 5, 7, 8, 10, 12):
        return 31
    else:
        return 30


cdef inline ParsedDateTime _parse_iso_parts(bytes bts):
    cdef Py_ssize_t n = len(bts)
    cdef const char* s = PyBytes_AS_STRING(bts)
    cdef ParsedDateTime out
    cdef int i = 0
    cdef int digit_count = 0

    out.year = parse_4digit(s)
    out.month = parse_2digit(s + 5)
    out.day = parse_2digit(s + 8)
    out.hour = 0
    out.minute = 0
    out.second = 0
    out.micros = 0
    out.offset_sign = 0
    out.offset_hour = 0
    out.offset_minute = 0

    # fewer than 10, more than 26 and the first separators not '-'
    if n < 10 or n > 26 or s[4] != 45 or s[7] != 45:
        raise ValueError("Invalid ISO timestamp")

    if n == 10:
        if not (1 <= out.month <= 12):
            raise ValueError("Month must be between 1 and 12")
        if not (1 <= out.day <= days_in_month(out.year, out.month)):
            raise ValueError("Invalid day for given month/year")
        return out

    if n >= 16 and (s[10] == CHAR_T or s[10] == CHAR_SPACE):
        out.hour = parse_2digit(s + 11)
        out.minute = parse_2digit(s + 14)
        i = 16

        if n >= 19 and s[16] == CHAR_COLON:
            out.second = parse_2digit(s + 17)
            i = 19

        if i < n and s[i] == CHAR_DOT:
            i += 1
            while i < n and is_digit(s[i]) and digit_count < 6:
                out.micros = out.micros * 10 + (s[i] - CHAR_0)
                i += 1
                digit_count += 1
            while digit_count < 6:
                out.micros *= 10
                digit_count += 1

        if i < n:
            if s[i] == CHAR_Z:
                i += 1
            elif (s[i] == CHAR_PLUS or s[i] == CHAR_MINUS) and (i + 6 <= n):
                out.offset_sign = 1 if s[i] == CHAR_PLUS else -1
                out.offset_hour = parse_2digit(s + i + 1)
                out.offset_minute = parse_2digit(s + i + 4)
                i += 6

        if i != n:
            raise ValueError("Unexpected characters at end of timestamp")
    else:
        raise ValueError("Invalid ISO timestamp")

    # Range checks
    if not (1 <= out.month <= 12):
        raise ValueError("Month must be between 1 and 12")
    if not (1 <= out.day <= days_in_month(out.year, out.month)):
        raise ValueError("Invalid day for given month/year")
    if not (0 <= out.hour < 24):
        raise ValueError("Hour must be between 0 and 23")
    if not (0 <= out.minute < 60):
        raise ValueError("Minute must be between 0 and 59")
    if not (0 <= out.second < 60):
        raise ValueError("Second must be between 0 and 59")

    return out

cpdef int64_t parse_iso_timestamp(bytes bts):
    """
    Parse a ISO format timestamp string to microseconds after the
    linux epoch (including negative values)
    """
    cdef ParsedDateTime p = _parse_iso_parts(bts)
    cdef tm t
    memset(&t, 0, sizeof(tm))
    t.tm_year = p.year - 1900
    t.tm_mon = p.month - 1
    t.tm_mday = p.day
    t.tm_hour = p.hour
    t.tm_min = p.minute
    t.tm_sec = p.second

    cdef time_t epoch = timegm(&t)

    if epoch == -1:
        # Dates before 1970-01-01 need manual handling
        dt = datetime.datetime(p.year, p.month, p.day, p.hour, p.minute, p.second, p.micros)
        delta = dt - datetime.datetime(1970, 1, 1)
        return int(delta.total_seconds() * MICROS_PER_SEC)

    cdef int64_t result = <int64_t>epoch * MICROS_PER_SEC + p.micros
    if p.offset_sign:
        result -= p.offset_sign * (
            (<int64_t>p.offset_hour * 60 + p.offset_minute) * MICROS_PER_MIN
        )
    return result


def parse_iso(bytes bts) -> datetime.datetime:

    cdef ParsedDateTime p

    try:
        p = _parse_iso_parts(bts)
        dt = datetime.datetime(
            p.year, p.month, p.day,
            p.hour, p.minute, p.second,
            p.micros
        )
        if p.offset_sign != 0:
            offset = datetime.timedelta(
                hours=p.offset_sign * p.offset_hour,
                minutes=p.offset_sign * p.offset_minute
            )
            dt = dt - offset  # convert to UTC
        return dt
    except Exception:
        return None
