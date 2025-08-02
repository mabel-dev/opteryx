# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.time cimport tm, time_t
from libc.string cimport memset
from libc.stdint cimport int64_t
from cpython.bytes cimport PyBytes_AS_STRING

import datetime

cdef int64_t MICROS_PER_SEC = 1_000_000
cdef int64_t MICROS_PER_MIN = 60 * MICROS_PER_SEC
cdef int64_t MICROS_PER_HOUR = 60 * MICROS_PER_MIN
cdef int64_t MICROS_PER_DAY = 24 * MICROS_PER_HOUR
cdef int CHAR_SPACE = 32
cdef int CHAR_T = 84
cdef int CHAR_COLON = 58
cdef int CHAR_DOT = 46
cdef int CHAR_Z = 90
cdef int CHAR_PLUS = 43
cdef int CHAR_MINUS = 45
cdef int CHAR_0 = 48
cdef int CHAR_9 = 57

cdef extern from "time.h":
    time_t timegm(tm *timeptr)

cdef inline int parse_2digit(const char* s):
    return (s[0] - 48) * 10 + (s[1] - 48)

cdef inline int parse_4digit(const char* s):
    return (s[0] - 48) * 1000 + (s[1] - 48) * 100 + (s[2] - 48) * 10 + (s[3] - 48)

cdef inline bint is_digit(char c):
    return CHAR_0 <= c <= CHAR_9

cdef inline bint is_digit_range(const char* s, int start, int end):
    cdef int i
    for i in range(start, end):
        if not is_digit(s[i]):
            return False
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

cpdef inline bint is_likely_iso_timestamp(bytes bts):
    cdef Py_ssize_t n = len(bts)
    cdef const char* s = PyBytes_AS_STRING(bts)
    cdef int i, digit_count

    if n < 10:
        return False

    # YYYY-MM-DD
    if not (
        is_digit_range(s, 0, 4)
        and s[4] == 45            # '-'
        and is_digit_range(s, 5, 7)
        and s[7] == 45            # '-'
        and is_digit_range(s, 8, 10)
    ):
        return False

    if n == 10:
        return True  # date only

    if n >= 16 and (s[10] == CHAR_T or s[10] == CHAR_SPACE):
        # HH:MM
        if not (
            is_digit_range(s, 11, 13)
            and s[13] == CHAR_COLON
            and is_digit_range(s, 14, 16)
        ):
            return False

        if n == 16:
            return True  # ends with HH:MM

        # optional HH:MM:SS
        if n >= 19 and s[16] == CHAR_COLON and is_digit_range(s, 17, 19):
            i = 19
        else:
            return False
    else:
        return False

    # optional .ffffff
    if i < n and s[i] == CHAR_DOT:
        i += 1
        digit_count = 0
        while i < n and is_digit(s[i]) and digit_count < 6:
            i += 1
            digit_count += 1
        while digit_count < 6:
            digit_count += 1  # allow zero-padded

    # optional timezone
    if i == n:
        return True
    elif s[i] == CHAR_Z:
        return i + 1 == n
    elif (s[i] == CHAR_PLUS or s[i] == CHAR_MINUS) and (i + 6 <= n):
        if not (
            is_digit_range(s, i + 1, i + 3)
            and s[i + 3] == CHAR_COLON
            and is_digit_range(s, i + 4, i + 6)
        ):
            return False
        i += 6
        return i == n

    return False

cpdef int64_t parse_iso_timestamp(bytes bts):
    cdef Py_ssize_t n = len(bts)
    cdef const char* s = PyBytes_AS_STRING(bts)
    cdef int year, month, day, hour = 0, minute = 0, second = 0
    cdef int micros = 0, i = 0, digit_count
    cdef int offset_sign = 0, offset_hour = 0, offset_min = 0

    year = parse_4digit(s)
    month = parse_2digit(s + 5)
    day = parse_2digit(s + 8)

    cdef tm t
    memset(&t, 0, sizeof(tm))
    t.tm_year = year - 1900
    t.tm_mon = month - 1
    t.tm_mday = day

    if n == 10:
        pass  # date only
    elif n >= 16 and (s[10] == CHAR_T or s[10] == CHAR_SPACE):
        hour = parse_2digit(s + 11)
        minute = parse_2digit(s + 14)
        t.tm_hour = hour
        t.tm_min = minute

        i = 16
        if n >= 19 and s[16] == CHAR_COLON:
            second = parse_2digit(s + 17)
            t.tm_sec = second
            i = 19
        else:
            t.tm_sec = 0

        # fractional seconds
        if i < n and s[i] == CHAR_DOT:
            i += 1
            while i < n and CHAR_0 <= s[i] <= CHAR_9 and digit_count < 6:
                micros = micros * 10 + (s[i] - CHAR_0)
                i += 1
                digit_count += 1
            while digit_count < 6:
                micros *= 10
                digit_count += 1

        # timezone
        if i < n:
            if s[i] == CHAR_Z:
                i += 1
            elif (s[i] == CHAR_PLUS or s[i] == CHAR_MINUS) and (i + 6 <= n):
                offset_sign = 1 if s[i] == CHAR_PLUS else -1
                offset_hour = parse_2digit(s + i + 1)
                offset_min = parse_2digit(s + i + 4)
                i += 6
            if i != n:
                raise ValueError("Unexpected characters at end of timestamp")
    else:
        raise ValueError("Invalid format – expected date-only or full timestamp")

    # Range checks
    if not (1 <= month <= 12):
        raise ValueError("Month must be between 1 and 12")
    if not (1 <= day <= days_in_month(year, month)):
        raise ValueError("Invalid day for given month/year")
    if not (0 <= hour < 24):
        raise ValueError("Hour must be between 0 and 23")
    if not (0 <= minute < 60):
        raise ValueError("Minute must be between 0 and 59")
    if not (0 <= second < 60):
        raise ValueError("Second must be between 0 and 59")

    cdef time_t epoch = timegm(&t)
    cdef int64_t result = <int64_t>epoch * MICROS_PER_SEC + micros
    result -= offset_sign * (offset_hour * MICROS_PER_HOUR + offset_min * MICROS_PER_MIN)
    return result


def parse_iso(bytes bts) -> datetime.datetime:
    if not is_likely_iso_timestamp(bts):
        return None

    cdef int64_t micros = parse_iso_timestamp(bts)
    try:
        return datetime.datetime.utcfromtimestamp(micros / 1_000_000)
    except Exception:
        return None
