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

import errno

from cpython.buffer cimport PyBuffer_FillInfo
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from cpython.unicode cimport PyUnicode_FromString
from libc.stddef cimport size_t

cdef extern from "disk_io.h":
    int read_all_pread(const char* path, unsigned char* dst, size_t* out_len,
                       bint sequential, bint willneed, bint drop_after)
    int read_all_mmap(const char* path, unsigned char** dst, size_t* out_len)
    int unmap_memory_c(unsigned char* addr, size_t size)

cdef extern from "directories.h":
    ctypedef struct file_info_t:
        char* name
        int is_directory
        int is_regular_file
        long long size
        long long mtime

    int list_directory_c "list_directory"(const char* path, file_info_t** files, size_t* count)
    void free_file_list(file_info_t* files, size_t count)
    int list_matching_files_c "list_matching_files"(const char* base_path, const char** extensions,
                                                    size_t ext_count, char*** files, size_t* count) nogil
    void free_file_names(char** files, size_t count)

cdef class MappedMemory:
    cdef unsigned char* data
    cdef size_t size
    cdef bint owned

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


def list_directory(str path):
    """Return directory entries using the native file system scanner."""

    path_b = path.encode("utf-8")
    cdef const char* c_path = path_b
    cdef file_info_t* files = NULL
    cdef size_t count = 0

    cdef int rc = list_directory_c(c_path, &files, &count)
    if rc != 0:
        err = -rc
        if err == errno.ENOENT:
            raise FileNotFoundError(path)
        raise OSError(err, f"Failed to list directory: {path}")

    entries = []
    cdef file_info_t entry
    cdef char* name_ptr
    cdef size_t idx
    try:
        for idx in range(count):
            entry = files[idx]
            name_ptr = entry.name
            if name_ptr == NULL:
                continue

            py_name = PyUnicode_FromString(name_ptr)
            entries.append(
                (
                    py_name,
                    bool(entry.is_directory),
                    bool(entry.is_regular_file),
                    entry.size,
                    entry.mtime,
                )
            )
    finally:
        if files != NULL:
            free_file_list(files, count)

    return entries


def list_files(str path, extensions):
    """Return a list of files under ``path`` matching provided extensions."""

    if extensions is None:
        raise ValueError("extensions must be provided")

    ext_seq = tuple(extensions)
    ext_count = len(ext_seq)

    cdef size_t c_ext_count = ext_count
    cdef const char** ext_array = NULL
    cdef char** file_array = NULL
    cdef size_t file_count = 0

    ext_bytes = [ext.encode("utf-8") if isinstance(ext, str) else ext for ext in ext_seq]

    if c_ext_count > 0:
        ext_array = <const char**>PyMem_Malloc(c_ext_count * sizeof(const char*))
        if ext_array == NULL:
            raise MemoryError("Unable to allocate extension array")
        for idx in range(c_ext_count):
            ext_array[idx] = <const char*>ext_bytes[idx]

    path_b = path.encode("utf-8")
    cdef const char* c_path = path_b

    cdef int rc
    with nogil:
        rc = list_matching_files_c(c_path, ext_array, c_ext_count, &file_array, &file_count)

    if ext_array != NULL:
        PyMem_Free(ext_array)

    if rc != 0:
        if file_array != NULL:
            free_file_names(file_array, file_count)
        err = -rc
        if err == errno.ENOENT:
            raise FileNotFoundError(path)
        raise OSError(err, f"Failed to list files: {path}")

    results = []
    try:
        for idx in range(file_count):
            py_path = PyUnicode_FromString(file_array[idx])
            if py_path is None:
                raise MemoryError("Unable to decode file path")
            results.append(py_path)
    finally:
        if file_array != NULL:
            free_file_names(file_array, file_count)

    return results


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
