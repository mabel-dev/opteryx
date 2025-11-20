# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

cdef extern from "base64.h":
    void* b64tobin(void* dest, const char* src)
    char* bintob64(char* dest, const void* src, size_t size)
    void* b64tobin_len(void* dest, const char* src, size_t len)
    int b64_has_neon()
    int b64_has_avx2()