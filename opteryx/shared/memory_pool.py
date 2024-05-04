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

"""
Memory Pool is used to manage access to arbitrary blocks of memory.

This is designed to be thread-safe with non-blocking reads.

This module includes an async wrapper around the memory pool
"""

import asyncio
from multiprocessing import Lock
from typing import Dict
from typing import Union

from orso.tools import random_int


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
        if size <= 0:  # pragma: no cover
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
        """
        Merges adjacent free segments (Level 1 compaction).

        This is intended to a fast way to get larger contiguous blocks.
        """
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
        """
        Aggressively compacts by pushing all free memory to the end (Level 2 compaction).

        This is slower, but ensures we get the maximum free space.
        """
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

    def can_commit(self, data: bytes) -> bool:
        return sum(segment.length for segment in self.free_segments) > len(data)

    def available_space(self) -> int:
        return sum(segment.length for segment in self.free_segments)

    def commit(self, data: bytes) -> int:
        """
        Add an item to the pool and return its reference.

        If we can't find a free block large enough we perform compaction,
        first we combine adjacent free blocks into larger blocks. If that's
        not enough, we consolidate all of the free blocks together.
        """
        len_data = len(data)
        # special case for 0 byte segments
        if len_data == 0:
            new_segment = MemorySegment(0, 0)
            ref_id = random_int()
            self.used_segments[ref_id] = new_segment
            self.commits += 1
            return ref_id

        # always acquire a lock to write
        with self.lock:
            segment_index = self._find_free_segment(len_data)
            if segment_index == -1:
                # avoid trying to compact if it won't release enough space anyway
                total_free_space = sum(segment.length for segment in self.free_segments)
                if total_free_space < len_data:
                    self.failed_commits += 1
                    return None
                # avoid trying to compact, if we're already compacted
                if len(self.free_segments) <= 1:
                    self.failed_commits += 1
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
            self.commits += 1
            return ref_id

    def read(self, ref_id: int, zero_copy: bool = True) -> Union[bytes, memoryview]:
        """
        We're using an optimistic locking strategy where we do not acquire
        a lock, perform the read and then check that the metadata hasn't changed
        and if it's the same, we assume no writes have updated it. If it has
        changed, we acquire a lock and try again.

        We use this approach because locks are expensive and memory pools are
        likely to be read heavy.
        """
        if ref_id not in self.used_segments:  # pragma: no cover
            raise ValueError("Invalid reference ID.")
        self.reads += 1
        segment = self.used_segments[ref_id]
        view = memoryview(self.pool)[segment.start : segment.start + segment.length]
        if segment != self.used_segments[ref_id]:  # pragma: no cover
            with self.lock:
                if ref_id not in self.used_segments:
                    raise ValueError("Invalid reference ID.")
                self.read_locks += 1
                segment = self.used_segments[ref_id]
                view = memoryview(self.pool)[segment.start : segment.start + segment.length]
        if zero_copy:
            return view
        return bytes(view)

    def release(self, ref_id: int):
        """
        Remove an item from the pool
        """
        self.releases += 1
        with self.lock:
            if ref_id not in self.used_segments:  # pragma: no cover
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            segment = self.used_segments.pop(ref_id)
            self.free_segments.append(segment)

    def read_and_release(self, ref_id: int, zero_copy: bool = True) -> Union[bytes, memoryview]:
        """
        Combine two steps together, we lock everytime here
        """
        with self.lock:
            self.reads += 1
            self.releases += 1
            if ref_id not in self.used_segments:  # pragma: no cover
                raise ValueError("Invalid reference ID.")
            self.read_locks += 1
            segment = self.used_segments.pop(ref_id)
            view = memoryview(self.pool)[segment.start : segment.start + segment.length]
            self.free_segments.append(segment)
            if zero_copy:
                return view
            return bytes(view)

    @property
    def stats(self) -> dict:  # pragma: no cover
        return {
            "free_segments": len(self.free_segments),
            "used_segments": len(self.used_segments),
            "commits": self.commits,
            "failed_commits": self.failed_commits,
            "reads": self.reads,
            "read_locks": self.read_locks,
            "l1_compaction": self.l1_compaction,
            "l2_compaction": self.l2_compaction,
            "releases": self.releases,
        }

    def __del__(self):
        """
        This function exists just to wrap the debug logging
        """
        pass
        # DEBUG: log (f"Memory Pool ({self.name}) <size={self.size}, commits={self.commits} ({self.failed_commits}), reads={self.reads}, releases={self.releases}, L1={self.l1_compaction}, L2={self.l2_compaction}>")


class AsyncMemoryPool:
    def __init__(self, pool: MemoryPool):
        self.pool: MemoryPool = pool
        self.lock = asyncio.Lock()

    async def commit(self, data: bytes) -> int:
        async with self.lock:
            return self.pool.commit(data)

    async def read(self, ref_id: int) -> bytes:
        """
        In an async environment, we much more certain the bytes will be overwritten
        if we don't materialize them
        """
        async with self.lock:
            return bytes(self.pool.read(ref_id))

    async def release(self, ref_id: int):
        async with self.lock:
            self.pool.release(ref_id)
