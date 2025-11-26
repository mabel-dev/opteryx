# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free

# External SIMD functions from C++
cdef extern from "simd_bitops.h":
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) nogil
    void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) nogil
    void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) nogil
    void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n) nogil
    size_t simd_popcount(const uint8_t* data, size_t n) nogil
    void simd_select_bytes(uint8_t* dest, const uint8_t* mask,
                           const uint8_t* a, const uint8_t* b, size_t n) nogil


def and_mask(bytes a, bytes b, Py_ssize_t n):
    """Return the byte-wise AND of `a` and `b` for `n` bytes as bytes.
    
    Uses SIMD acceleration:
    - AVX2: 32 bytes per iteration  
    - NEON: 16 bytes per iteration
    """
    cdef uint8_t* pa = <uint8_t*> (<char*> a)
    cdef uint8_t* pb = <uint8_t*> (<char*> b)
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    
    simd_and_mask(out, pa, pb, n)
    
    py_out = out[:n]
    free(out)
    return bytes(py_out)


def or_mask(bytes a, bytes b, Py_ssize_t n):
    """Return the byte-wise OR of `a` and `b` for `n` bytes as bytes.
    
    Uses SIMD acceleration:
    - AVX2: 32 bytes per iteration
    - NEON: 16 bytes per iteration
    """
    cdef uint8_t* pa = <uint8_t*> (<char*> a)
    cdef uint8_t* pb = <uint8_t*> (<char*> b)
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    
    simd_or_mask(out, pa, pb, n)
    
    py_out = out[:n]
    free(out)
    return bytes(py_out)


def xor_mask(bytes a, bytes b, Py_ssize_t n):
    """Return the byte-wise XOR of `a` and `b` for `n` bytes as bytes.
    
    Uses SIMD acceleration:
    - AVX2: 32 bytes per iteration
    - NEON: 16 bytes per iteration
    """
    cdef uint8_t* pa = <uint8_t*> (<char*> a)
    cdef uint8_t* pb = <uint8_t*> (<char*> b)
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    
    simd_xor_mask(out, pa, pb, n)
    
    py_out = out[:n]
    free(out)
    return bytes(py_out)


def not_mask(bytes a, Py_ssize_t n):
    """Return the byte-wise NOT of `a` for `n` bytes as bytes.
    
    Uses SIMD acceleration:
    - AVX2: 32 bytes per iteration
    - NEON: 16 bytes per iteration
    """
    cdef uint8_t* pa = <uint8_t*> (<char*> a)
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    
    simd_not_mask(out, pa, n)
    
    py_out = out[:n]
    free(out)
    return bytes(py_out)


def popcount_mask(bytes a, Py_ssize_t n):
    """Return the count of set bits in the mask.
    
    Uses SIMD acceleration with POPCNT instruction when available.
    """
    cdef uint8_t* pa = <uint8_t*> (<char*> a)
    return simd_popcount(pa, n)


def select_mask(bytes mask, bytes a, bytes b, Py_ssize_t n):
    """Select bytes from `a` or `b` based on `mask` (non-zero -> `a`).

    Uses SIMD acceleration for fast conditional selection.
    """
    cdef uint8_t* pmask = <uint8_t*> (<char*> mask)
    cdef uint8_t* pa = <uint8_t*> (<char*> a)
    cdef uint8_t* pb = <uint8_t*> (<char*> b)
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()

    simd_select_bytes(out, pmask, pa, pb, n)

    py_out = out[:n]
    free(out)
    return bytes(py_out)
    
