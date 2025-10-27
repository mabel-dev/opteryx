# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: nonecheck=False

"""
Cython-optimized MemoryViewStream for high-performance memoryview reading.
"""

import io

cdef class MemoryViewStream:
    """
    Handle a memoryview like a stream without converting to bytes.

    Optimized Cython implementation for maximum performance.
    """
    cdef:
        const unsigned char[:] mv  # Typed memoryview for direct access
        Py_ssize_t offset
        bint _closed
        Py_ssize_t _len
        object _underlying_bytes  # bytes object if available

    def __init__(self, object mv):
        self.mv = mv
        self.offset = 0
        self._closed = False
        self._len = len(mv)
        # Check if we can use the underlying bytes object directly
        self._underlying_bytes = mv.obj if isinstance(mv.obj, bytes) else None

    cpdef read(self, Py_ssize_t n=-1):
        """Read and return up to n bytes."""
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        cdef:
            Py_ssize_t offset = self.offset
            Py_ssize_t length = self._len
            Py_ssize_t bytes_to_read

        if offset >= length:
            return b""

        if n < 0 or offset + n > length:
            bytes_to_read = length - offset
        else:
            bytes_to_read = n

        # Fast path: if backed by bytes, slice directly (no copy)
        if self._underlying_bytes is not None:
            result = self._underlying_bytes[offset : offset + bytes_to_read]
            self.offset = offset + bytes_to_read
            return result

        # Use memoryview slicing which is more efficient than tobytes()
        result = self.mv[offset : offset + bytes_to_read]
        self.offset = offset + bytes_to_read
        # Use bytes() constructor instead of tobytes() for better performance
        return bytes(result)

    cpdef Py_ssize_t readinto(self, bytearray b):
        """Read bytes into a pre-allocated buffer (zero-copy when possible)."""
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        cdef:
            Py_ssize_t n = len(b)
            Py_ssize_t bytes_available = self._len - self.offset
            Py_ssize_t bytes_to_read

        if bytes_available <= 0:
            return 0

        bytes_to_read = n if n < bytes_available else bytes_available

        # Direct memory copy for maximum performance
        cdef unsigned char[:] b_view = b
        cdef Py_ssize_t i
        for i in range(bytes_to_read):
            b_view[i] = self.mv[self.offset + i]

        self.offset += bytes_to_read
        return bytes_to_read

    cpdef read1(self, Py_ssize_t n=-1):
        """Read and return up to n bytes (same as read for this implementation)."""
        return self.read(n)

    cpdef Py_ssize_t seek(self, Py_ssize_t offset, int whence=0):
        """Change stream position."""
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        cdef Py_ssize_t new_offset

        if whence == 0:  # SEEK_SET
            new_offset = offset
        elif whence == 1:  # SEEK_CUR
            new_offset = self.offset + offset
        elif whence == 2:  # SEEK_END
            new_offset = self._len + offset
        else:
            raise ValueError(f"Invalid value for whence: {whence}")

        # Clamp to valid range
        if new_offset < 0:
            new_offset = 0
        elif new_offset > self._len:
            new_offset = self._len

        self.offset = new_offset
        return self.offset

    cpdef Py_ssize_t tell(self):
        """Return current stream position."""
        return self.offset

    def readable(self):
        """Return whether object supports reading."""
        return True

    def writable(self):
        """Return whether object supports writing."""
        return False

    def seekable(self):
        """Return whether object supports random access."""
        return True

    cpdef close(self):
        """Close the stream."""
        self._closed = True

    @property
    def closed(self):
        """Return whether the stream is closed."""
        return self._closed

    @property
    def mode(self):
        """Return the mode of the stream."""
        return "rb"

    def __len__(self):
        """Return the length of the underlying buffer."""
        return self._len

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __iter__(self):
        """Return an iterator over the memoryview."""
        return self

    def __next__(self):
        """Return the next byte."""
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if self.offset >= self._len:
            raise StopIteration()

        # Direct access to memoryview for maximum performance
        cdef unsigned char byte = self.mv[self.offset]
        self.offset += 1
        return bytes([byte])

    def fileno(self):
        """Return file descriptor (not supported)."""
        return -1

    def flush(self):
        """Flush write buffers (not supported)."""
        raise io.UnsupportedOperation()

    def isatty(self):
        """Return whether this is an interactive stream."""
        return False

    def readline(self, limit=-1):
        """Read and return a line (not supported)."""
        raise io.UnsupportedOperation()

    def readlines(self, hint=-1):
        """Read and return a list of lines (not supported)."""
        raise io.UnsupportedOperation()

    def truncate(self, size=None):
        """Truncate file to size (not supported)."""
        raise io.UnsupportedOperation()

    def write(self, s):
        """Write string to file (not supported)."""
        raise io.UnsupportedOperation()

    def writelines(self, lines):
        """Write a list of lines to stream (not supported)."""
        raise io.UnsupportedOperation()

# Additional high-performance helper functions
cdef class MemoryViewStreamOptimized(MemoryViewStream):
    """
    Further optimized version with additional performance enhancements.
    """

    cpdef const unsigned char[:] read_memoryview(self, Py_ssize_t n=-1):
        """
        Read as memoryview instead of bytes (zero-copy).

        This avoids the copy in read() but the returned memoryview
        becomes invalid if the underlying buffer changes.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        cdef:
            Py_ssize_t offset = self.offset
            Py_ssize_t length = self._len
            Py_ssize_t bytes_to_read

        if offset >= length:
            return self.mv[0:0]  # Empty memoryview

        if n < 0 or offset + n > length:
            bytes_to_read = length - offset
        else:
            bytes_to_read = n

        result = self.mv[offset : offset + bytes_to_read]
        self.offset = offset + bytes_to_read
        return result

    cpdef Py_ssize_t readinto_memoryview(self, unsigned char[:] buffer):
        """
        Read into existing memoryview (most efficient for large reads).
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        cdef:
            Py_ssize_t n = buffer.shape[0]
            Py_ssize_t bytes_available = self._len - self.offset
            Py_ssize_t bytes_to_read
            Py_ssize_t i

        if bytes_available <= 0:
            return 0

        bytes_to_read = n if n < bytes_available else bytes_available

        # Direct memory copy - fastest possible
        for i in range(bytes_to_read):
            buffer[i] = self.mv[self.offset + i]

        self.offset += bytes_to_read
        return bytes_to_read
