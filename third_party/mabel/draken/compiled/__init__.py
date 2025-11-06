"""Compiled helpers for dynamic/compiled evaluators.

This package contains a small compiled helper `maskops` implemented in Cython
that provides fast byte-wise boolean mask operations. If the compiled
extension isn't available (e.g., before build), a pure-Python fallback is
provided so imports don't fail during development.
"""

try:
    # Prefer the compiled implementations
    from .maskops import and_mask
    from .maskops import or_mask
    from .maskops import xor_mask
except ImportError:
    # Pure-Python fallback (slower) — operates on bytes-like objects
    def _py_and_mask(a: bytes, b: bytes, n: int) -> bytes:
        out = bytearray(n)
        for i in range(n):
            out[i] = a[i] & b[i]
        return bytes(out)

    def _py_or_mask(a: bytes, b: bytes, n: int) -> bytes:
        out = bytearray(n)
        for i in range(n):
            out[i] = a[i] | b[i]
        return bytes(out)

    def _py_xor_mask(a: bytes, b: bytes, n: int) -> bytes:
        out = bytearray(n)
        for i in range(n):
            out[i] = a[i] ^ b[i]
        return bytes(out)

    and_mask = _py_and_mask
    or_mask = _py_or_mask
    xor_mask = _py_xor_mask

__all__ = ["and_mask", "or_mask", "xor_mask"]
