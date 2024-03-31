# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from multiprocessing import Lock
from typing import Dict

from orso.tools import random_int

"""
Memory Pool is used to manage access to arbitrary blocks of memory.

This is designed to be thread-safe with non-blocking reads.
"""


class MemorySegment:
    slots = ("start", "length")

    def __init__(self, start, length):
        self.start = start
        self.length = length


class MemoryPool:
    def __init__(
        self,
        size: int,
        name: str = "Memory Pool",
    ):
        if size <= 0:
            raise ValueError("MemoryPool size must be a positive integer")
        self.lock = Lock()
        self.pool = bytearray(size)
        self.size = size
        self.free_segments = [MemorySegment(0, size)]  # the whole pool is free
        self.used_segments: Dict[int, MemorySegment] = {}
        self.name = name
        # statistics
        self.commits = 0
        self.failed_commits = 0
        self.reads = 0
        self.read_locks = 0
        self.l1_compaction = 0
        self.l2_compaction = 0
        self.releases = 0

    def _find_free_segment(self, size: int) -> int:
        for index, segment in enumerate(self.free_segments):
            if segment.length >= size:
                return index
        return -1

    def _level1_compaction(self):
        """Merges adjacent free segments (Level 1 compaction)."""
        self.l1_compaction += 1
        if not self.free_segments:
            return
        # Ensure the list is sorted
        self.free_segments.sort(key=lambda segment: segment.start)

        # Use a new list to store merged segments
        new_free_segments = [self.free_segments[0]]
        for segment in self.free_segments[1:]:
            last_segment = new_free_segments[-1]
            if last_segment.start + last_segment.length == segment.start:
                # If adjacent, merge by extending the last segment
                last_segment.length += segment.length
            else:
                # If not adjacent, just add the segment to the new list
                new_free_segments.append(segment)

        self.free_segments = new_free_segments

    def _level2_compaction(self):
        """Aggressively compacts by pushing all free memory to the end (Level 2 compaction)."""
        self.l2_compaction += 1

        total_free_space = sum(segment.length for segment in self.free_segments)
        compacted_start = self.size - total_free_space
        self.free_segments = [MemorySegment(compacted_start, total_free_space)]

        offset = 0
        for segment_id, segment in sorted(self.used_segments.items(), key=lambda x: x[1].start):
            new_start = offset

            # Apply memory views for zero-copy slice assignment
            source_view = memoryview(self.pool)[segment.start : segment.start + segment.length]
            dest_view = memoryview(self.pool)[new_start : new_start + segment.length]
            dest_view[:] = source_view

            segment.start = new_start
            offset += segment.length

    def commit(self, data: bytes) -> int:
        self.commits += 1
        len_data = len(data)
        # always acquire a lock to write
        with self.lock:
            segment_index = self._find_free_segment(len_data)
            if segment_index == -1:
                # avoid trying to compact if it won't release enough space anyway
                total_free_space = sum(segment.length for segment in self.free_segments)
                if total_free_space < len_data:
                    return None
                # avoid trying to compact, if we're already compacted
                if len(self.free_segments) <= 1:
                    return None
                # combine adjacent free space (should be pretty quick)
                self._level1_compaction()
                segment_index = self._find_free_segment(len_data)
                if segment_index == -1:
                    # move free space to the end (is slower)
                    self._level2_compaction()
                    segment_index = self._find_free_segment(len_data)
                    if segment_index == -1:
                        self.failed_commits += 1
                        # we're full, even after compaction
                        return None

            free_segment = self.free_segments[segment_index]
            start, length = free_segment.start, free_segment.length
            new_segment = MemorySegment(start, len_data)

            pool_view = memoryview(self.pool)[start : start + len_data]
            pool_view[:] = data

            if length > len_data:
                free_segment.start += len_data
                free_segment.length -= len_data
            else:
                self.free_segments.pop(segment_index)

            ref_id = random_int()
            self.used_segments[ref_id] = new_segment
            return ref_id

    def read(self, ref_id: int) -> bytes:
        """
        We're using an optimistic locking strategy where we do not acquire
        a lock, perform the read and then check that the metadata hasn't changed
        and if it's the same, we assume no writes have updated it.

        If it has changed, we acquire a lock and try again. The buffer pool is
        read heavy, so optimized reads are preferred.
        """
        if ref_id not in self.used_segments:
            raise ValueError("Invalid reference ID.")
        self.reads += 1
        segment = self.used_segments[ref_id]
        view = memoryview(self.pool)[segment.start : segment.start + segment.length]
        if segment != self.used_segments[ref_id]:
            with self.lock:
                if ref_id not in self.used_segments:
                    raise ValueError("Invalid reference ID.")
                self.read_locks += 1
                segment = self.used_segments[ref_id]
                view = memoryview(self.pool)[segment.start : segment.start + segment.length]
        return view

    def release(self, ref_id: int):
        self.releases += 1
        with self.lock:
            if ref_id not in self.used_segments:
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            segment = self.used_segments.pop(ref_id)
            self.free_segments.append(segment)

    def __del__(self):
        pass
        # DEBUG: log (f"Memory Pool ({self.name}) <size={self.size}, commits={self.commits}, reads={self.reads}, releases={self.releases}, L1={self.l1_compaction}, L2={self.l2_compaction}>")
