# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_AsString, PyBytes_FromStringAndSize
from cpython.buffer cimport PyBUF_SIMPLE, PyObject_GetBuffer, PyBuffer_Release, Py_buffer
from threading import RLock
from libcpp.vector cimport vector
from libc.stdint cimport int64_t, int8_t

import os

cdef int64_t DEBUG_MODE = os.environ.get("OPTERYX_DEBUG", 0) != 0

cdef struct MemorySegment:
    int64_t start
    int64_t length
    int8_t latched


cdef class MemoryPool:
    cdef:
        unsigned char* pool
        public int64_t size
        public vector[MemorySegment] free_segments
        public dict[int64_t, MemorySegment] used_segments
        public str name
        public int64_t commits, failed_commits, reads, read_locks, l1_compaction, l2_compaction, releases
        object lock
        int64_t next_ref_id

    def __cinit__(self, int64_t size, str name="Memory Pool"):
        if size <= 0:
            raise ValueError("MemoryPool size must be a positive integer")

        self.size = size
        attempt_size = size
        self.next_ref_id = 1

        while attempt_size > 0:
            self.pool = <unsigned char*>malloc(attempt_size * sizeof(unsigned char))
            if self.pool:
                break
            attempt_size >>= 1  # Bit shift to halve the size and try again

        if not self.pool:
            raise MemoryError("Failed to allocate memory pool")

        self.size = attempt_size
        self.name = name
        self.free_segments = [MemorySegment(0, self.size, 0)]
        self.used_segments = {}
        self.lock = RLock()

        # Initialize statistics
        self.commits = 0
        self.failed_commits = 0
        self.reads = 0
        self.read_locks = 0
        self.l1_compaction = 0
        self.l2_compaction = 0
        self.releases = 0

    def __dealloc__(self):
        if self.pool is not NULL:
            free(self.pool)
        if DEBUG_MODE:
            print (f"Memory Pool ({self.name}) <size={self.size}, commits={self.commits} ({self.failed_commits}), reads={self.reads}, releases={self.releases}, L1={self.l1_compaction}, L2={self.l2_compaction}>")

    cpdef int64_t _find_free_segment(self, int64_t size):
        cdef int64_t i
        cdef MemorySegment segment
        for i in range(len(self.free_segments)):
            segment = self.free_segments[i]
            if segment.length >= size:
                return i
        return -1

    def _level1_compaction(self):
        """
        Level 1 compaction combines adjacent empty segments only
        """
        cdef int64_t n
        cdef MemorySegment last_segment, segment

        self.l1_compaction += 1
        n = len(self.free_segments)
        if n <= 1:
            return

        # Sort the free segments by start attribute
        self.free_segments = sorted(self.free_segments, key=lambda x: x["start"])
        new_free_segments = [self.free_segments[0]]

        for segment in self.free_segments[1:]:
            last_segment = new_free_segments[-1]
            if last_segment.start + last_segment.length == segment.start:
                # If adjacent, merge by extending the last segment
                new_free_segments[-1] = MemorySegment(last_segment.start, last_segment.length + segment.length, 0)
            else:
                # If not adjacent, just add the segment to the new list
                new_free_segments.append(segment)

        self.free_segments = new_free_segments

    def _level2_compaction(self):
        """
        Moves all used memory to the beginning and all free memory to the end.
        """
        cdef long offset = 0
        self.l2_compaction += 1

        # Sort used segments by start address
        used_segments_sorted = sorted(self.used_segments.items(), key=lambda x: x[1]["start"])

        for segment_id, segment in used_segments_sorted:
            if segment["latched"] == 0 and segment["start"] != offset:
                memcpy(self.pool + offset, self.pool + <long>segment["start"], segment["length"])
                segment["start"] = offset
                self.used_segments[segment_id] = segment
            offset += segment["length"]

        # Update free segment list
        self.free_segments = [MemorySegment(offset, self.size - offset, 0)]

    def commit(self, object data) -> int64_t:
        cdef int64_t len_data
        cdef int64_t segment_index
        cdef MemorySegment segment
        cdef int64_t ref_id = self.next_ref_id
        cdef Py_buffer view
        cdef char* raw_ptr

        # increment for the next commit
        self.next_ref_id += 1

        if isinstance(data, bytes):
            len_data = len(data)
            raw_ptr = PyBytes_AsString(data)
        else:
            # Use PyBuffer API to support memoryview, numpy, or PyArrow arrays
            if PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) == 0:
                len_data = view.len
                raw_ptr = <char*> view.buf
            else:
                raise TypeError("Unsupported data type for commit")

        if len_data == 0:
            self.used_segments[ref_id] = MemorySegment(0, 0, 0)
            self.commits += 1
            return ref_id

        total_free_space = sum(segment.length for segment in self.free_segments)
        if total_free_space < len_data:
            self.failed_commits += 1
            return -1

        with self.lock:
            segment_index = self._find_free_segment(len_data)
            if segment_index == -1:
                self._level1_compaction()
                segment_index = self._find_free_segment(len_data)
                if segment_index == -1:
                    self._level2_compaction()
                    segment_index = self._find_free_segment(len_data)
                    if segment_index == -1:
                        raise MemoryError("Unable to create segment in bufferpool")

            segment = self.free_segments[segment_index]
            self.free_segments.erase(self.free_segments.begin() + segment_index)
            if segment.length > len_data:
                self.free_segments.push_back(MemorySegment(segment.start + len_data, segment.length - len_data, 0))

            memcpy(self.pool + segment.start, raw_ptr, len_data)
            self.used_segments[ref_id] = MemorySegment(segment.start, len_data, 0)
            self.commits += 1

        # Release PyBuffer if used
        if not isinstance(data, bytes):
            PyBuffer_Release(&view)

        return ref_id

    cpdef read(self, int64_t ref_id, int zero_copy = 0, int latch = 0):
        cdef MemorySegment segment
        cdef char* char_ptr = <char*> self.pool

        self.reads += 1

        if ref_id not in self.used_segments:
            raise ValueError("Invalid reference ID.")

        segment = self.used_segments[ref_id]

        if latch != 0:
            self.used_segments[ref_id]["latched"] = 1

        if zero_copy != 0:
            return memoryview(<char[:segment.length]>(char_ptr + segment.start))

        return PyBytes_FromStringAndSize(char_ptr + segment.start, segment.length)

    cpdef read_and_release(self, int64_t ref_id, int zero_copy = 1):
        cdef MemorySegment segment
        cdef char* char_ptr = <char*> self.pool
        cdef char[:] raw_data

        with self.lock:
            self.reads += 1
            self.releases += 1

            if ref_id not in self.used_segments:
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            segment = self.used_segments.pop(ref_id)
            self.free_segments.push_back(segment)

            if zero_copy != 0:
                raw_data = <char[:segment.length]> (char_ptr + segment.start)
                return memoryview(raw_data)  # Create a memoryview from the raw data
            else:
                return PyBytes_FromStringAndSize(char_ptr + segment.start, segment.length)

    cpdef unlatch(self, int64_t ref_id):
        """
        Remove a latch from a segment
        """
        cdef MemorySegment segment

        if ref_id not in self.used_segments:
            raise ValueError(f"Invalid reference ID - {ref_id}.")
        segment = self.used_segments[ref_id]
        if segment.latched == 0:
            raise RuntimeError(f"Segment {ref_id} was not latched.")
        self.used_segments[ref_id]["latched"] = 0

    cpdef release(self, int64_t ref_id):
        with self.lock:
            self.releases += 1

            if ref_id not in self.used_segments:
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            segment = self.used_segments.pop(ref_id)
            self.free_segments.push_back(segment)

    def available_space(self) -> int64_t:
        return sum(segment.length for segment in self.free_segments)
