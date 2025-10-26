# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Ultra-fast disk reader module
"""

from cpython.buffer cimport PyBuffer_FillInfo
from libc.stdlib cimport free

cdef extern from "disk_io.h":
    int read_all_pread(const char* path, unsigned char* dst, size_t* out_len,
                       bint sequential, bint willneed, bint drop_after)
    int read_all_mmap(const char* path, unsigned char** dst, size_t* out_len)
    int unmap_memory_c(unsigned char* addr, size_t size)

cdef class MappedMemory:
    cdef unsigned char* data
    cdef size_t size
    cdef bint owned

    def __dealloc__(self):
        if self.owned and self.data != NULL:
            # Free the allocated memory (for non-mmap case)
            free(self.data)

    def __getbuffer__(self, Py_buffer* buffer, int flags):
        PyBuffer_FillInfo(buffer, self, self.data, self.size, 1, flags)

    def __len__(self):
        return self.size


def read_file(str path, bint sequential=True, bint willneed=True, bint drop_after=False):
    """
    Read an entire file into memory with optimized I/O.
    """
    import os

    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    cdef size_t size = os.path.getsize(path)
    cdef size_t out_len = 0

    path_b = path.encode("utf-8")
    cdef const char* c_path = path_b

    # Allocate buffer - use bytearray for mutable buffer
    buf = bytearray(size)
    cdef unsigned char[::1] buf_view = buf
    cdef unsigned char* dst = &buf_view[0]

    cdef int rc = read_all_pread(c_path, dst, &out_len, sequential, willneed, drop_after)

    if rc != 0:
        raise OSError(-rc, f"Failed to read file: {path}")

    return memoryview(buf)[:out_len]


def read_file_mmap(str path):
    """
    Read file using memory mapping - returns an object that provides memoryview interface
    but MUST be manually closed to avoid resource leaks.
    """
    import os

    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    path_b = path.encode("utf-8")
    cdef const char* c_path = path_b
    cdef unsigned char* mapped_data = NULL
    cdef size_t size = 0

    cdef int rc = read_all_mmap(c_path, &mapped_data, &size)

    if rc != 0:
        raise OSError(-rc, f"Failed to mmap file: {path}")

    # Create wrapper that knows how to clean up
    cdef MappedMemory wrapper = MappedMemory.__new__(MappedMemory)
    wrapper.data = mapped_data
    wrapper.size = size
    wrapper.owned = False  # This is mmap'd memory, not malloc'd

    return wrapper


def read_file_to_bytes(str path, bint sequential=True, bint willneed=True, bint drop_after=False):
    """
    Read an entire file into memory as bytes.
    """
    mv = read_file(path, sequential, willneed, drop_after)
    return bytes(mv)


def unmap_memory(mem_obj):
    """
    Explicitly unmap memory from read_file_mmap.
    MUST be called when done with the data to avoid resource leaks.
    """
    cdef int rc
    if hasattr(mem_obj, 'data') and mem_obj.data is not None:
        # Import the unmap function from your C code
        rc = unmap_memory_c(mem_obj.data, mem_obj.size)
        mem_obj.data = None
        return rc == 0
    return True
