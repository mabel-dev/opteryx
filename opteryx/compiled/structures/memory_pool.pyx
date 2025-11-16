# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

"""
memory_pool.pyx

This module provides the `MemoryPool` class, which allocates a contiguous block of memory and manages it through
segmentation, compaction, and thread-safe operations. It supports committing, reading, releasing, and compacting
memory segments, with optional latching for concurrent access. Designed for use cases requiring fast, low-level
memory operations, such as database engines or data processing pipelines.

Features:
- Memory allocation and deallocation using C-level malloc/free.
- Segment management: commit, read, release, and unlatch.
- Level 1 and Level 2 compaction to reduce fragmentation.
- Thread safety via RLock.
- Debug mode for detailed statistics and diagnostics.
- Supports zero-copy reads and buffer-based commits for compatibility with numpy, memoryview, and PyArrow.

"""

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memmove
from cpython.bytes cimport PyBytes_AsString, PyBytes_FromStringAndSize
from cpython.buffer cimport PyBUF_SIMPLE, PyObject_GetBuffer, PyBuffer_Release, Py_buffer
from threading import RLock
from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uintptr_t

import os

cdef int64_t DEBUG_MODE = os.environ.get("OPTERYX_DEBUG", 0) != 0

cdef struct MemorySegment:
    int64_t start
    int64_t length
    int64_t latches
    int64_t ref_id
    bint is_free

cdef inline int64_t _align_size(int64_t size, int64_t alignment=8):
    """Align size to the specified boundary for better memory access patterns."""
    return (size + alignment - 1) & ~(alignment - 1)

cdef class MemoryPool:
    cdef:
        unsigned char* pool
        public int64_t size
        public int64_t used_size
        public vector[MemorySegment] segments
        public dict used_segments  # public view: ref_id -> {'start','length','latches','orig_length'}
        cdef dict _used_start_map  # internal: ref_id -> segment.start (or -1 for zero-length)
        public str name
        public int64_t commits, failed_commits, reads, read_locks
        public int64_t l1_compaction, l2_compaction, releases, resizes
        object lock
        int64_t next_ref_id
        int64_t alignment
        bint auto_resize

    def __cinit__(self, int64_t size, str name="Memory Pool", bint auto_resize=False, int64_t alignment=1):
        if size <= 0:
            raise ValueError("MemoryPool size must be a positive integer")

        if alignment & (alignment - 1) != 0:
            raise ValueError("Alignment must be a power of two")

        self.size = size
        self.used_size = 0
        self.alignment = alignment
        self.auto_resize = auto_resize
        self.next_ref_id = 1

        self.pool = <unsigned char*>malloc(self.size * sizeof(unsigned char))
        if not self.pool:
            raise MemoryError("Failed to allocate memory pool")

        # Initialize with one free segment covering entire pool
        cdef MemorySegment initial_segment
        initial_segment.start = 0
        initial_segment.length = self.size
        initial_segment.latches = 0
        initial_segment.is_free = True
        self.segments.push_back(initial_segment)

        self.name = name
        self.used_segments = {}
        self._used_start_map = {}
        self.lock = RLock()

        # Initialize statistics
        self.commits = 0
        self.failed_commits = 0
        self.reads = 0
        self.read_locks = 0
        self.l1_compaction = 0
        self.l2_compaction = 0
        self.releases = 0
        self.resizes = 0

    def __dealloc__(self):
        if self.pool is not NULL:
            free(self.pool)
        if DEBUG_MODE:
            self._print_stats()

    cdef void _print_stats(self):
        cdef int64_t total_free = 0
        cdef int64_t total_used = 0
        cdef double fragmentation = 0.0
        cdef int64_t free_blocks = 0
        cdef double denom

        for i in range(self.segments.size()):
            if self.segments[i].is_free:
                total_free += self.segments[i].length
                free_blocks += 1
            else:
                total_used += self.segments[i].length

        if total_free > 0 and free_blocks > 1:
            # Avoid integer division by zero when total_free < 1024.
            denom = total_free / 1024.0
            if denom > 0.0:
                fragmentation = (free_blocks - 1) * 100.0 / denom  # Simplified fragmentation metric
            else:
                fragmentation = 0.0

        print(f"Memory Pool ({self.name}) <"
              f"size={self.size}, used={total_used}, free={total_free}, "
              f"fragmentation={fragmentation:.1f}%, "
              f"commits={self.commits} (failed={self.failed_commits}), "
              f"reads={self.reads}, releases={self.releases}, "
              f"L1={self.l1_compaction}, L2={self.l2_compaction}, "
              f"resizes={self.resizes}>")

    cdef bint _resize_pool(self, int64_t new_size):
        """Resize the memory pool (expensive operation)."""
        cdef unsigned char* new_pool = <unsigned char*>realloc(self.pool, new_size)
        if not new_pool:
            return False

        self.pool = new_pool
        self.size = new_size
        self.resizes += 1
        return True

    cdef inline int64_t _find_best_fit_segment(self, int64_t size):
        """Find the best fit free segment using first-fit strategy."""
        cdef int64_t best_index = -1
        cdef int64_t best_waste = self.size + 1  # Initialize with large value
        cdef int64_t waste

        for i in range(self.segments.size()):
            if self.segments[i].is_free and self.segments[i].length >= size:
                waste = self.segments[i].length - size
                if waste < best_waste:
                    best_waste = waste
                    best_index = i
                    if waste == 0:  # Perfect fit
                        break

        return best_index

    cdef void _merge_adjacent_free_segments(self):
        """Merge adjacent free segments (Level 1 compaction)."""
        if self.segments.size() <= 1:
            return

        self.l1_compaction += 1

        cdef vector[MemorySegment] new_segments

        for i in range(self.segments.size()):
            if new_segments.size() == 0:
                new_segments.push_back(self.segments[i])
                continue

            # Access last element directly and current segment from self.segments
            if new_segments[new_segments.size() - 1].is_free and self.segments[i].is_free and \
               new_segments[new_segments.size() - 1].start + new_segments[new_segments.size() - 1].length == self.segments[i].start:
                # Merge adjacent free segments by extending the last element
                new_segments[new_segments.size() - 1].length += self.segments[i].length
            else:
                new_segments.push_back(self.segments[i])

        self.segments = new_segments

    cdef void _defragment_memory(self):
        """Defragment memory by moving segments (Level 2 compaction)."""
        if self.segments.size() <= 1:
            return

        self.l2_compaction += 1

        cdef vector[MemorySegment] new_segments
        cdef int64_t current_pos = 0
        cdef int64_t i
        cdef MemorySegment seg
        cdef int64_t original_start
        cdef dict start_to_ref = {}
        cdef object ref, info

        # Build mapping from original start -> ref so we can update refs after moves
        for ref, info in self.used_segments.items():
            start_to_ref[info["start"]] = ref

        # Iterate original segments in order and move unlatched segments leftwards
        for i in range(self.segments.size()):
            seg = self.segments[i]
            if seg.is_free:
                continue

            original_start = seg.start

            if seg.latches > 0:
                # Can't move latched segments; ensure current_pos moves past them
                # If there's a gap before the latched segment, keep it as-is
                if current_pos != seg.start:
                    # If current_pos < seg.start, we should leave the latched segment at its current start
                    current_pos = seg.start + seg.length
                else:
                    current_pos += seg.length

                # Append the latched segment unchanged
                new_segments.push_back(seg)
                # Update mapping if present (start remains same)
                if original_start in start_to_ref:
                    ref = start_to_ref[original_start]
                    self._used_start_map[ref] = seg.start
                    if ref in self.used_segments:
                        self.used_segments[ref]["start"] = seg.start
            else:
                # Move this unlatched segment to current_pos if needed
                if seg.start != current_pos:
                    memmove(self.pool + current_pos, self.pool + seg.start, seg.length)
                    seg.start = current_pos

                new_segments.push_back(seg)

                # Update mapping for this moved segment
                if original_start in start_to_ref:
                    ref = start_to_ref[original_start]
                    self._used_start_map[ref] = seg.start
                    if ref in self.used_segments:
                        self.used_segments[ref]["start"] = seg.start

                current_pos += seg.length

        # Add one large free segment at the end
        cdef MemorySegment free_segment
        if current_pos < self.size:
            free_segment.start = current_pos
            free_segment.length = self.size - current_pos
            free_segment.latches = 0
            free_segment.is_free = True
            new_segments.push_back(free_segment)

        self.segments = new_segments

    cpdef int64_t get_fragmentation(self):
        """Calculate memory fragmentation percentage."""
        cdef int64_t total_free = 0
        cdef int64_t largest_free_block = 0
        cdef int64_t free_blocks = 0

        with self.lock:
            for i in range(self.segments.size()):
                if self.segments[i].is_free:
                    total_free += self.segments[i].length
                    free_blocks += 1
                    if self.segments[i].length > largest_free_block:
                        largest_free_block = self.segments[i].length

            if total_free == 0:
                return 0
            if largest_free_block == 0:
                return 100

            return 100 - (largest_free_block * 100 // total_free)

    def commit(self, object data) -> int64_t:
        cdef int64_t len_data
        cdef int64_t segment_index
        cdef MemorySegment segment, new_segment
        cdef int64_t new_size
        cdef MemorySegment additional_space
        cdef int64_t ref_id = self.next_ref_id
        cdef Py_buffer view
        cdef char* raw_ptr
        cdef int64_t aligned_size

        self.next_ref_id += 1

        if isinstance(data, bytes):
            len_data = len(data)
            raw_ptr = PyBytes_AsString(data)
        else:
            if PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) == 0:
                len_data = view.len
                raw_ptr = <char*> view.buf
            else:
                raise TypeError("Unsupported data type for commit")

        if len_data == 0:
            with self.lock:
                self._used_start_map[ref_id] = -1  # Special marker for zero-length data
                # public view entry for tests
                self.used_segments[ref_id] = {"start": 0, "length": 0, "latches": 0}
                self.commits += 1
            return ref_id

        aligned_size = _align_size(len_data, self.alignment)

        with self.lock:
            # Try to find a suitable segment
            segment_index = self._find_best_fit_segment(aligned_size)

            if segment_index == -1:
                # Try compaction
                self._merge_adjacent_free_segments()
                segment_index = self._find_best_fit_segment(aligned_size)

                if segment_index == -1:
                    # Try defragmentation
                    self._defragment_memory()
                    segment_index = self._find_best_fit_segment(aligned_size)

                    if segment_index == -1 and self.auto_resize:
                        # Calculate new size (at least double or enough for current allocation)
                        new_size = max(self.size * 2, self.size + aligned_size * 2)
                        if self._resize_pool(new_size):
                            # Add the new free space to the last segment if it's free
                            if self.segments.size() > 0 and self.segments[self.segments.size() - 1].is_free:
                                self.segments[self.segments.size() - 1].length += new_size - self.size
                            else:
                                additional_space.start = self.size
                                additional_space.length = new_size - self.size
                                additional_space.latches = 0
                                additional_space.is_free = True
                                self.segments.push_back(additional_space)

                            segment_index = self._find_best_fit_segment(aligned_size)

            if segment_index == -1:
                self.failed_commits += 1
                if not isinstance(data, bytes):
                    PyBuffer_Release(&view)
                return -1

            segment = self.segments[segment_index]

            # Create new used segment
            new_segment.start = segment.start
            new_segment.length = aligned_size
            new_segment.latches = 0
            new_segment.is_free = False

            # Update or replace the free segment
            if segment.length > aligned_size:
                # Split the segment
                segment.start += aligned_size
                segment.length -= aligned_size
                self.segments[segment_index] = segment
                self.segments.insert(self.segments.begin() + segment_index, new_segment)
                self._used_start_map[ref_id] = new_segment.start
                self.used_segments[ref_id] = {"start": new_segment.start, "length": new_segment.length, "latches": 0, "orig_length": len_data}
            else:
                # Exact fit or very close (due to alignment)
                self.segments[segment_index] = new_segment
                self._used_start_map[ref_id] = new_segment.start
                self.used_segments[ref_id] = {"start": new_segment.start, "length": new_segment.length, "latches": 0, "orig_length": len_data}

            # Copy data
            memcpy(self.pool + new_segment.start, raw_ptr, len_data)
            self.used_size += aligned_size
            self.commits += 1

        if not isinstance(data, bytes):
            PyBuffer_Release(&view)

        return ref_id

    cpdef tuple reserve_for_write_ptr(self, int64_t size):
        """
        Reserve a segment for direct write and return (ref_id, ptr, capacity).
        The returned ptr is an integer address pointing into the internal pool.
        The segment is latched to prevent compaction while the writer fills it.
        """
        cdef int64_t len_data = size
        cdef int64_t aligned_size = _align_size(len_data, self.alignment)
        cdef int64_t segment_index
        cdef MemorySegment segment, new_segment
        cdef int64_t new_size
        cdef MemorySegment additional_space

        with self.lock:
            segment_index = self._find_best_fit_segment(aligned_size)

            if segment_index == -1:
                # Try compaction
                self._merge_adjacent_free_segments()
                segment_index = self._find_best_fit_segment(aligned_size)

                if segment_index == -1:
                    self._defragment_memory()
                    segment_index = self._find_best_fit_segment(aligned_size)

                    if segment_index == -1 and self.auto_resize:
                        new_size = max(self.size * 2, self.size + aligned_size * 2)
                        if self._resize_pool(new_size):
                            if self.segments.size() > 0 and self.segments[self.segments.size() - 1].is_free:
                                self.segments[self.segments.size() - 1].length += new_size - self.size
                            else:
                                additional_space.start = self.size
                                additional_space.length = new_size - self.size
                                additional_space.latches = 0
                                additional_space.is_free = True
                                self.segments.push_back(additional_space)

                            segment_index = self._find_best_fit_segment(aligned_size)

            if segment_index == -1:
                self.failed_commits += 1
                return (-1, 0, 0)

            segment = self.segments[segment_index]

            # Create new used segment and latch it to prevent moves
            new_segment.start = segment.start
            new_segment.length = aligned_size
            new_segment.latches = 1
            new_segment.is_free = False

            if segment.length > aligned_size:
                segment.start += aligned_size
                segment.length -= aligned_size
                self.segments[segment_index] = segment
                self.segments.insert(self.segments.begin() + segment_index, new_segment)
                self._used_start_map[self.next_ref_id] = new_segment.start
                self.used_segments[self.next_ref_id] = {"start": new_segment.start, "length": new_segment.length, "latches": 1, "orig_length": 0}
            else:
                self.segments[segment_index] = new_segment
                self._used_start_map[self.next_ref_id] = new_segment.start
                self.used_segments[self.next_ref_id] = {"start": new_segment.start, "length": new_segment.length, "latches": 1, "orig_length": 0}

            ptr_val = <uintptr_t>(self.pool + new_segment.start)
            cap = new_segment.length
            ref_id = self.next_ref_id
            self.next_ref_id += 1
            self.used_size += aligned_size
            self.commits += 1

        # Return (ref_id, pointer as integer, capacity)
        return (ref_id, <unsigned long long>ptr_val, cap)

    cpdef finalize_commit(self, int64_t ref_id, int64_t actual_length):
        """Finalize a previous `reserve_for_write_ptr` by setting the actual
        length and unlatching the segment so readers can access it."""
        cdef int64_t segment_index
        cdef MemorySegment segment

        with self.lock:
            if ref_id not in self._used_start_map:
                raise ValueError(f"Invalid reference ID - {ref_id}.")

            start = self._used_start_map[ref_id]
            # update public view
            if ref_id in self.used_segments:
                self.used_segments[ref_id]["orig_length"] = actual_length

            if start == -1:
                return

            segment_index = -1
            for i in range(self.segments.size()):
                if not self.segments[i].is_free and self.segments[i].start == start:
                    segment_index = i
                    break

            if segment_index == -1:
                raise ValueError(f"Invalid reference ID - {ref_id}.")

            segment = self.segments[segment_index]
            # unlatch the segment (writer finished)
            if segment.latches > 0:
                segment.latches -= 1
            self.segments[segment_index] = segment
            if ref_id in self.used_segments:
                self.used_segments[ref_id]["latches"] = segment.latches

    cpdef read(self, int64_t ref_id, bint zero_copy=False, bint latch=False):
        cdef int64_t segment_index
        cdef MemorySegment segment
        cdef char* char_ptr = <char*>self.pool

        with self.lock:
            if ref_id not in self._used_start_map:
                raise ValueError("Invalid reference ID.")

            start = self._used_start_map[ref_id]

            # Handle zero-length data
            if start == -1:
                if zero_copy:
                    return memoryview(b'')
                else:
                    return b''

            # find segment index by start
            segment_index = -1
            for i in range(self.segments.size()):
                if not self.segments[i].is_free and self.segments[i].start == start:
                    segment_index = i
                    break

            if segment_index == -1:
                raise ValueError("Invalid reference ID.")

            segment = self.segments[segment_index]
            self.reads += 1

            if latch:
                segment.latches += 1
                self.segments[segment_index] = segment
                # update public view
                self.used_segments[ref_id]["latches"] = segment.latches
                self.read_locks += 1

            # Return only the original data length
            orig_len = self.used_segments[ref_id].get("orig_length", segment.length)
            if zero_copy:
                return memoryview(<char[:orig_len]>(char_ptr + segment.start))
            else:
                return PyBytes_FromStringAndSize(char_ptr + segment.start, orig_len)

    cpdef unlatch(self, int64_t ref_id):
        cdef int64_t segment_index
        cdef MemorySegment segment

        with self.lock:
            if ref_id not in self._used_start_map:
                raise ValueError(f"Invalid reference ID - {ref_id}.")

            start = self._used_start_map[ref_id]
            if start == -1:  # Zero-length data
                return

            # find segment index
            segment_index = -1
            for i in range(self.segments.size()):
                if not self.segments[i].is_free and self.segments[i].start == start:
                    segment_index = i
                    break
            if segment_index == -1:
                raise ValueError(f"Invalid reference ID - {ref_id}.")

            segment = self.segments[segment_index]
            if segment.latches == 0:
                raise RuntimeError(f"Segment {ref_id} was not latched.")

            segment.latches -= 1
            self.segments[segment_index] = segment
            # update public view
            self.used_segments[ref_id]["latches"] = segment.latches

    cpdef release(self, int64_t ref_id):
        cdef int64_t segment_index
        cdef MemorySegment segment

        with self.lock:
            if ref_id not in self._used_start_map:
                raise ValueError(f"Invalid reference ID - {ref_id}.")

            self.releases += 1
            start = self._used_start_map.pop(ref_id)
            # remove public view
            if ref_id in self.used_segments:
                del self.used_segments[ref_id]

            if start == -1:  # Zero-length data
                return

            # find segment index by start
            segment_index = -1
            for i in range(self.segments.size()):
                if not self.segments[i].is_free and self.segments[i].start == start:
                    segment_index = i
                    break

            if segment_index == -1:
                raise ValueError(f"Invalid reference ID - {ref_id}.")

            segment = self.segments[segment_index]

            # Allow releasing a segment even if it is latched. Tests expect
            # release to remove the segment and then unlatch should raise ValueError.
            if segment.latches > 0:
                # clear latch count and proceed to free
                segment.latches = 0

            segment.is_free = True
            # ensure latches cleared
            segment.latches = 0
            self.segments[segment_index] = segment
            self.used_size -= segment.length

            # Try to merge adjacent free segments
            self._merge_adjacent_free_segments()

    def available_space(self) -> int64_t:
        cdef int64_t total_free = 0
        with self.lock:
            for i in range(self.segments.size()):
                if self.segments[i].is_free:
                    total_free += self.segments[i].length
        return total_free

    def get_stats(self) -> dict:
        """Get detailed statistics about the memory pool."""
        cdef int64_t total_free = 0
        cdef int64_t total_used = 0
        cdef int64_t free_blocks = 0
        cdef int64_t used_blocks = 0
        cdef int64_t largest_free = 0

        with self.lock:
            for i in range(self.segments.size()):
                if self.segments[i].is_free:
                    total_free += self.segments[i].length
                    free_blocks += 1
                    if self.segments[i].length > largest_free:
                        largest_free = self.segments[i].length
                else:
                    total_used += self.segments[i].length
                    used_blocks += 1

        return {
            'total_size': self.size,
            'used_size': total_used,
            'free_size': total_free,
            'used_blocks': used_blocks,
            'free_blocks': free_blocks,
            'largest_free_block': largest_free,
            'fragmentation': self.get_fragmentation(),
            'commits': self.commits,
            'failed_commits': self.failed_commits,
            'reads': self.reads,
            'releases': self.releases,
            'compactions': self.l1_compaction + self.l2_compaction,
            'resizes': self.resizes
        }

    def debug_info(self) -> str:
        """Get debug information about segment layout."""
        cdef str info = f"Memory Pool '{self.name}' (size: {self.size})\n"
        cdef int64_t i
        cdef MemorySegment seg

        with self.lock:
            for i in range(self.segments.size()):
                seg = self.segments[i]
                status = "FREE" if seg.is_free else f"USED (latches: {seg.latches})"
                info += f"  Segment {i}: [{seg.start:8d} - {seg.start + seg.length:8d}] "
                info += f"length: {seg.length:8d} {status}\n"

        return info

    @property
    def free_segments(self):
        """Return a list of free segments as dictionaries for tests."""
        cdef list out = []
        cdef int64_t i
        cdef MemorySegment seg
        with self.lock:
            for i in range(self.segments.size()):
                seg = self.segments[i]
                if seg.is_free:
                    out.append({"start": seg.start, "length": seg.length})
        return out

    cpdef _level1_compaction(self):
        with self.lock:
            self._merge_adjacent_free_segments()

    cpdef _level2_compaction(self):
        cdef list ordered_refs
        cdef list _ordered
        cdef int64_t i
        cdef MemorySegment seg
        cdef object ref, info

        with self.lock:
            # Build list of refs ordered by their current start without using a lambda
            _ordered = []
            for ref, info in self.used_segments.items():
                _ordered.append((info["start"], ref))
            _ordered.sort()
            ordered_refs = [t[1] for t in _ordered]

            # defragment memory (moves used segments to the front in-order)
            self._defragment_memory()

            # After defrag, assign refs to used segments in order
            idx = 0
            for i in range(self.segments.size()):
                seg = self.segments[i]
                if not seg.is_free:
                    if idx < len(ordered_refs):
                        ref = ordered_refs[idx]
                        self._used_start_map[ref] = seg.start
                        if ref in self.used_segments:
                            self.used_segments[ref]["start"] = seg.start
                        idx += 1
