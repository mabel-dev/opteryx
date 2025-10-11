# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

from libc.stddef cimport size_t
from cpython.unicode cimport (
    PyUnicode_AsUTF8AndSize,
    PyUnicode_Check,
    PyUnicode_FromStringAndSize,
)

cdef extern from *:
    """
    #if defined(__APPLE__)
    #include <CommonCrypto/CommonDigest.h>
    typedef CC_MD5_CTX MD5_CTX;
    typedef CC_SHA1_CTX SHA_CTX;
    typedef CC_SHA256_CTX SHA256_CTX;
    typedef CC_SHA512_CTX SHA512_CTX;
    #define MD5_Init CC_MD5_Init
    #define MD5_Update CC_MD5_Update
    #define MD5_Final CC_MD5_Final
    #define SHA1_Init CC_SHA1_Init
    #define SHA1_Update CC_SHA1_Update
    #define SHA1_Final CC_SHA1_Final
    #define SHA256_Init CC_SHA256_Init
    #define SHA256_Update CC_SHA256_Update
    #define SHA256_Final CC_SHA256_Final
    #define SHA512_Init CC_SHA512_Init
    #define SHA512_Update CC_SHA512_Update
    #define SHA512_Final CC_SHA512_Final
    #else
    #include <openssl/md5.h>
    #include <openssl/sha.h>
    #endif
    """

    ctypedef struct MD5_CTX:
        pass
    ctypedef struct SHA_CTX:
        pass
    ctypedef struct SHA256_CTX:
        pass
    ctypedef struct SHA512_CTX:
        pass

    int MD5_Init(MD5_CTX *c) nogil
    int MD5_Update(MD5_CTX *c, const void *data, size_t len) nogil
    int MD5_Final(unsigned char *md, MD5_CTX *c) nogil

    int SHA1_Init(SHA_CTX *c) nogil
    int SHA1_Update(SHA_CTX *c, const void *data, size_t len) nogil
    int SHA1_Final(unsigned char *md, SHA_CTX *c) nogil

    int SHA256_Init(SHA256_CTX *c) nogil
    int SHA256_Update(SHA256_CTX *c, const void *data, size_t len) nogil
    int SHA256_Final(unsigned char *md, SHA256_CTX *c) nogil

    int SHA512_Init(SHA512_CTX *c) nogil
    int SHA512_Update(SHA512_CTX *c, const void *data, size_t len) nogil
    int SHA512_Final(unsigned char *md, SHA512_CTX *c) nogil

cdef const char* _HEX_DIGITS = "0123456789abcdef"


cdef inline void _digest_to_hex(const unsigned char* digest, size_t length, char* out) noexcept nogil:
    cdef size_t i
    cdef unsigned char byte
    for i in range(length):
        byte = digest[i]
        out[2 * i] = _HEX_DIGITS[(byte >> 4) & 0x0F]
        out[2 * i + 1] = _HEX_DIGITS[byte & 0x0F]
    out[2 * length] = 0


cdef inline numpy.ndarray[object, ndim=1] _ensure_object_array_hash(object arr):
    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)
    if not isinstance(arr, numpy.ndarray):
        arr = numpy.array(arr, dtype=object)
    elif arr.dtype != object:
        arr = arr.astype(object)
    return <numpy.ndarray[object, ndim=1]>arr


cpdef numpy.ndarray list_md5(object arr):
    """
    Calculate MD5 hash for array of strings/values.

    Parameters:
        arr: Array of values to hash

    Returns:
        Array of MD5 hex digests
    """
    cdef numpy.ndarray[object, ndim=1] values = _ensure_object_array_hash(arr)
    cdef Py_ssize_t i, n = values.shape[0]
    cdef Py_ssize_t data_len = 0
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object item
    cdef object str_value
    cdef const char* data
    cdef MD5_CTX ctx
    cdef unsigned char digest[16]
    cdef char hex_digest[33]
    cdef object hex_obj

    for i in range(n):
        item = values[i]
        if item is None:
            result[i] = None
            continue

        if PyUnicode_Check(item):
            str_value = item
        else:
            str_value = str(item)

        data = PyUnicode_AsUTF8AndSize(str_value, &data_len)
        if data == NULL:
            raise ValueError("Unable to encode value as UTF-8")

        if MD5_Init(&ctx) != 1:
            raise RuntimeError("MD5_Init failed")
        if MD5_Update(&ctx, <const void*>data, <size_t>data_len) != 1:
            raise RuntimeError("MD5_Update failed")
        if MD5_Final(digest, &ctx) != 1:
            raise RuntimeError("MD5_Final failed")

        _digest_to_hex(digest, 16, hex_digest)
        hex_obj = PyUnicode_FromStringAndSize(hex_digest, 32)
        if hex_obj is None:
            raise MemoryError("Unable to allocate MD5 hex digest")
        result[i] = hex_obj

    return result


cpdef numpy.ndarray list_sha1(object arr):
    """
    Calculate SHA1 hash for array of strings/values.

    Parameters:
        arr: Array of values to hash

    Returns:
        Array of SHA1 hex digests
    """
    cdef numpy.ndarray[object, ndim=1] values = _ensure_object_array_hash(arr)
    cdef Py_ssize_t i, n = values.shape[0]
    cdef Py_ssize_t data_len = 0
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object item
    cdef object str_value
    cdef const char* data
    cdef SHA_CTX ctx
    cdef unsigned char digest[20]
    cdef char hex_digest[41]
    cdef object hex_obj

    for i in range(n):
        item = values[i]
        if item is None:
            result[i] = None
            continue

        if PyUnicode_Check(item):
            str_value = item
        else:
            str_value = str(item)

        data = PyUnicode_AsUTF8AndSize(str_value, &data_len)
        if data == NULL:
            raise ValueError("Unable to encode value as UTF-8")

        if SHA1_Init(&ctx) != 1:
            raise RuntimeError("SHA1_Init failed")
        if SHA1_Update(&ctx, <const void*>data, <size_t>data_len) != 1:
            raise RuntimeError("SHA1_Update failed")
        if SHA1_Final(digest, &ctx) != 1:
            raise RuntimeError("SHA1_Final failed")

        _digest_to_hex(digest, 20, hex_digest)
        hex_obj = PyUnicode_FromStringAndSize(hex_digest, 40)
        if hex_obj is None:
            raise MemoryError("Unable to allocate SHA1 hex digest")
        result[i] = hex_obj

    return result


cpdef numpy.ndarray list_sha256(object arr):
    """
    Calculate SHA256 hash for array of strings/values.

    Parameters:
        arr: Array of values to hash

    Returns:
        Array of SHA256 hex digests
    """
    cdef numpy.ndarray[object, ndim=1] values = _ensure_object_array_hash(arr)
    cdef Py_ssize_t i, n = values.shape[0]
    cdef Py_ssize_t data_len = 0
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object item
    cdef object str_value
    cdef const char* data
    cdef SHA256_CTX ctx
    cdef unsigned char digest[32]
    cdef char hex_digest[65]
    cdef object hex_obj

    for i in range(n):
        item = values[i]
        if item is None:
            result[i] = None
            continue

        if PyUnicode_Check(item):
            str_value = item
        else:
            str_value = str(item)

        data = PyUnicode_AsUTF8AndSize(str_value, &data_len)
        if data == NULL:
            raise ValueError("Unable to encode value as UTF-8")

        if SHA256_Init(&ctx) != 1:
            raise RuntimeError("SHA256_Init failed")
        if SHA256_Update(&ctx, <const void*>data, <size_t>data_len) != 1:
            raise RuntimeError("SHA256_Update failed")
        if SHA256_Final(digest, &ctx) != 1:
            raise RuntimeError("SHA256_Final failed")

        _digest_to_hex(digest, 32, hex_digest)
        hex_obj = PyUnicode_FromStringAndSize(hex_digest, 64)
        if hex_obj is None:
            raise MemoryError("Unable to allocate SHA256 hex digest")
        result[i] = hex_obj

    return result


cpdef numpy.ndarray list_sha512(object arr):
    """
    Calculate SHA512 hash for array of strings/values.

    Parameters:
        arr: Array of values to hash

    Returns:
        Array of SHA512 hex digests
    """
    cdef numpy.ndarray[object, ndim=1] values = _ensure_object_array_hash(arr)
    cdef Py_ssize_t i, n = values.shape[0]
    cdef Py_ssize_t data_len = 0
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object item
    cdef object str_value
    cdef const char* data
    cdef SHA512_CTX ctx
    cdef unsigned char digest[64]
    cdef char hex_digest[129]
    cdef object hex_obj

    for i in range(n):
        item = values[i]
        if item is None:
            result[i] = None
            continue

        if PyUnicode_Check(item):
            str_value = item
        else:
            str_value = str(item)

        data = PyUnicode_AsUTF8AndSize(str_value, &data_len)
        if data == NULL:
            raise ValueError("Unable to encode value as UTF-8")

        if SHA512_Init(&ctx) != 1:
            raise RuntimeError("SHA512_Init failed")
        if SHA512_Update(&ctx, <const void*>data, <size_t>data_len) != 1:
            raise RuntimeError("SHA512_Update failed")
        if SHA512_Final(digest, &ctx) != 1:
            raise RuntimeError("SHA512_Final failed")

        _digest_to_hex(digest, 64, hex_digest)
        hex_obj = PyUnicode_FromStringAndSize(hex_digest, 128)
        if hex_obj is None:
            raise MemoryError("Unable to allocate SHA512 hex digest")
        result[i] = hex_obj

    return result
