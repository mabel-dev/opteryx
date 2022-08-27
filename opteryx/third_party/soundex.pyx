# cython: language_level=3, c_string_type=unicode, c_string_encoding=ascii
# https://github.com/yougov/fuzzy/blob/master/src/fuzzy.pyx

cdef extern from "string.h":
    int strlen(char *s)

cdef extern from "stdlib.h":
    void * malloc(int i)
    void free(void * buf)

cdef class Soundex:
    cdef int size
    cdef char *map

    def __init__(self, size):
        self.size = size
        self.map = "01230120022455012623010202"

    def __call__(self, s):
        cdef char *cs
        cdef int ls
        cdef int i
        cdef int written
        cdef char *out
        cdef char c

        written = 0

        out = <char *>malloc(self.size + 1)
        cs = s
        ls = strlen(cs)

        for i from 0<= i < ls:
            c = cs[i]
            if c >= 97 and c <= 122:
                c = c - 32
            if c >= 65 and c <= 90:
                if written == 0:
                    out[written] = c
                    written = written + 1
                elif self.map[c - 65] != 48 and (written == 1 or
                            out[written - 1] != self.map[c - 65]):
                    out[written] = self.map[c - 65]
                    written = written + 1
            if written == self.size:
                break
        for i from written <= i < self.size:
            out[i] = 48
        out[self.size] = 0

        # Changed for Opteryx - appears to be a bug in the original code, the object is
        # freed before it's returned which returns an empty string. This frees it after
        # it's been returned.
        try:
            return out
        finally:
            free(out)
