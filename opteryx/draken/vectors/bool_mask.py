"""BoolMask: lightweight Python wrapper for boolean masks returned by vector comparisons.

This module provides BoolMask so both compiled evaluators and manual vector code
get a consistent object that supports and_vector, or_vector, xor_vector, indexing,
iteration, and conversion helpers.
"""


class BoolMask:
    def __init__(self, data):
        try:
            self._mv = memoryview(data)
        except Exception:
            self._mv = memoryview(bytes(bytearray((1 if bool(x) else 0) for x in data)))

    def __len__(self):
        # memoryview of bytes is 1-D
        return self._mv.nbytes

    def __getitem__(self, i):
        v = self._mv[i]
        return bool(v)

    def to_pylist(self):
        return [bool(x) for x in self._mv.tobytes()]

    def _coerce_other(self, other):
        if isinstance(other, BoolMask):
            return other._mv.tobytes()
        try:
            return memoryview(other).tobytes()
        except Exception:
            return bytes(bytearray((1 if bool(x) else 0) for x in other))

    def and_vector(self, other):
        rb = self._coerce_other(other)
        lb = self._mv.tobytes()
        if len(lb) != len(rb):
            raise ValueError("Boolean operands must have same length")
        out = bytearray(len(lb))
        for i in range(len(lb)):
            out[i] = 1 if (lb[i] and rb[i]) else 0
        return BoolMask(bytes(out))

    def or_vector(self, other):
        rb = self._coerce_other(other)
        lb = self._mv.tobytes()
        if len(lb) != len(rb):
            raise ValueError("Boolean operands must have same length")
        out = bytearray(len(lb))
        for i in range(len(lb)):
            out[i] = 1 if (lb[i] or rb[i]) else 0
        return BoolMask(bytes(out))

    def xor_vector(self, other):
        rb = self._coerce_other(other)
        lb = self._mv.tobytes()
        if len(lb) != len(rb):
            raise ValueError("Boolean operands must have same length")
        out = bytearray(len(lb))
        for i in range(len(lb)):
            out[i] = 1 if ((lb[i] and not rb[i]) or (not lb[i] and rb[i])) else 0
        return BoolMask(bytes(out))

    def __iter__(self):
        for b in self._mv.tobytes():
            yield bool(b)

    def __repr__(self):
        return f"<BoolMask len={len(self)} values={self.to_pylist()[:10]}>"
