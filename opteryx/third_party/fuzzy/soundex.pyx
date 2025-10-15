# cython: language_level=3, c_string_type=unicode, c_string_encoding=ascii
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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
cdef int SOUNDEX_LENGTH = 4

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
    cdef char prev_code = 0  # Track the previous soundex code from original input

    for i from 0<= i < ls:
        c = cs[i]
        # Convert to uppercase
        if c >= 97 and c <= 122:
            c = c - 32
        # Only process alphabetic characters
        if c >= 65 and c <= 90:
            if written == 0:
                # First character is always the first letter
                out[written] = c
                written = written + 1
                prev_code = soundex_map[c - 65]  # Remember the code for the first letter
            else:
                # Get the soundex code for this character
                code = soundex_map[c - 65]

                if code != 48:  # Not a vowel/ignored letter
                    # Only add if not the same as previous soundex code
                    if code != prev_code:
                        out[written] = code
                        written = written + 1
                    prev_code = code
                else:  # code == 48 (vowel or H/W)
                    # A, E, I, O, U, Y reset the previous code
                    # H and W act as separators but don't reset prev_code
                    if c == 72 or c == 87:  # H=72, W=87 - separators only
                        pass  # Don't reset prev_code, just continue
                    else:  # True vowels (A, E, I, O, U, Y)
                        prev_code = 48  # Reset previous code
        # Stop if we've filled the soundex code
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
