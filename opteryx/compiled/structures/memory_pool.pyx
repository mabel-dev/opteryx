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
from orso.tools import random_int
from libcpp.vector cimport vector

import os

cdef long DEBUG_MODE = os.environ.get("OPTERYX_DEBUG", 0) != 0

cdef struct MemorySegment:
    long start
    long length

cdef class MemoryPool:
    cdef:
        unsigned char* pool
        public long size
        public vector[MemorySegment] free_segments
        public dict[long, MemorySegment] used_segments
        public str name
        public long commits, failed_commits, reads, read_locks, l1_compaction, l2_compaction, releases
        object lock

    def __cinit__(self, long size, str name="Memory Pool"):
        if size <= 0:
            raise ValueError("MemoryPool size must be a positive integer")

        self.size = size
        attempt_size = size

        while attempt_size > 0:
            self.pool = <unsigned char*>malloc(attempt_size * sizeof(unsigned char))
            if self.pool:
                break
            attempt_size >>= 1  # Bit shift to halve the size and try again

        if not self.pool:
            raise MemoryError("Failed to allocate memory pool")

        self.size = attempt_size
        self.name = name
        self.free_segments = [MemorySegment(0, self.size)]
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

    def _find_free_segment(self, long size) -> long:
        cdef long i
        cdef MemorySegment segment
        for i in range(len(self.free_segments)):
            segment = self.free_segments[i]
            if segment.length >= size:
                return i
        return -1

    def _level1_compaction(self):
        cdef long n
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
                new_free_segments[-1] = MemorySegment(last_segment.start, last_segment.length + segment.length)
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
            if segment["start"] != offset:
                memcpy(self.pool + offset, self.pool + <long>segment["start"], segment["length"])
                segment["start"] = offset
                self.used_segments[segment_id] = segment
            offset += segment["length"]

        # Update free segment list
        self.free_segments = [MemorySegment(offset, self.size - offset)]

    def commit(self, object data) -> long:
        cdef long len_data
        cdef long segment_index
        cdef MemorySegment segment
        cdef long ref_id = random_int()
        cdef Py_buffer view
        cdef char* raw_ptr

        # Resolve collisions
        while ref_id in self.used_segments:
            ref_id = random_int()

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
            self.used_segments[ref_id] = MemorySegment(0, 0)
            self.commits += 1
            return ref_id

        total_free_space = sum(segment.length for segment in self.free_segments)
        if total_free_space < len_data:
            self.failed_commits += 1
            return None

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
                self.free_segments.push_back(MemorySegment(segment.start + len_data, segment.length - len_data))

            memcpy(self.pool + segment.start, raw_ptr, len_data)
            self.used_segments[ref_id] = MemorySegment(segment.start, len_data)
            self.commits += 1

        # Release PyBuffer if used
        if not isinstance(data, bytes):
            PyBuffer_Release(&view)

        return ref_id

    def read(self, long ref_id, int zero_copy = 1):
        cdef MemorySegment segment
        cdef char* char_ptr = <char*> self.pool

        self.reads += 1

        if ref_id not in self.used_segments:
            raise ValueError("Invalid reference ID.")

        segment = self.used_segments[ref_id]

        if zero_copy != 0:
            return memoryview(<char[:segment.length]>(char_ptr + segment.start))

        return PyBytes_FromStringAndSize(char_ptr + segment.start, segment.length)

    def read_and_release(self, long ref_id, int zero_copy = 1):
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

    def release(self, long ref_id):
        with self.lock:
            self.releases += 1

            if ref_id not in self.used_segments:
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            segment = self.used_segments.pop(ref_id)
            self.free_segments.push_back(segment)

    def available_space(self) -> int:
        return sum(segment.length for segment in self.free_segments)
