# cython: language_level=3, c_string_type=unicode, c_string_encoding=ascii
# This implementation has evolved from this version:
# https://github.com/yougov/fuzzy/blob/master/src/fuzzy.pyx
# Various bug fixes and restructure of the code has been made from the
# original version.

cdef extern from "string.h":
    int strlen(char *s)

cdef extern from "stdlib.h":
    void * malloc(int i)
    void free(void * buf)

cdef char* soundex_map = "01230120022455012623010202"
cdef int SOUNDEX_LENGTH = 4;

cpdef soundex(char* s):

    cdef char *cs
    cdef int ls
    cdef int i
    cdef int written
    cdef char *out
    cdef char c

    cs = s
    ls = strlen(cs)

    # If the string is empty, return an empty string
    if ls == 0:
        return ""

    written = 0
    out = <char *>malloc(SOUNDEX_LENGTH + 1)

    for i from 0<= i < ls:
        c = cs[i]
        if c >= 97 and c <= 122:
            c = c - 32
        if c >= 65 and c <= 90:
            if written == 0:
                out[written] = c
                written = written + 1
            elif soundex_map[c - 65] != 48 and (written == 1 or out[written - 1] != soundex_map[c - 65]):
                out[written] = soundex_map[c - 65]
                written = written + 1
        if written == SOUNDEX_LENGTH:
            break
    for i from written <= i < SOUNDEX_LENGTH:
        out[i] = 48
    out[SOUNDEX_LENGTH] = 0

    # Changed for Opteryx - appears to be a bug in the original code, the object is
    # freed before it's returned which returns an empty string. This frees it after
    # it's been returned.
    try:
        return out
    finally:
        free(out)
