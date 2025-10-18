# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is the async wrapper around the memory pool, it is used by the
async blob readers to move data to the synchronous code.
"""

import asyncio

from opteryx.compiled.structures.memory_pool import MemoryPool


class AsyncMemoryPool:
    def __init__(self, pool: MemoryPool):
        self.pool: MemoryPool = pool
        # MemoryPool is a blocking, thread-safe object (uses an RLock). We avoid
        # serialising all async operations on a single asyncio.Lock which causes
        # the event loop to become a bottleneck when many coroutines call
        # commit/read/release concurrently. Instead we run the blocking calls in
        # the default thread pool executor. This keeps the event loop responsive
        # while still using the compiled MemoryPool which provides C-level
        # performance for the actual memory operations.
        self.lock = None

    async def commit(self, data: bytes) -> int:
        loop = asyncio.get_running_loop()
        # Offload the blocking commit to a thread to avoid blocking the event loop
        return await loop.run_in_executor(None, self.pool.commit, data)

    async def read(self, ref_id: int, zero_copy=True, latch=True) -> bytes:
        """
        In an async environment, we much more certain the bytes will be overwritten
        if we don't materialize them so we always create a copy.
        """
        loop = asyncio.get_running_loop()
        # Offload the blocking read to a thread. The compiled MemoryPool.read
        # is fast but still uses locks and may block; moving it to a thread
        # allows multiple reads/commits to progress concurrently.
        return await loop.run_in_executor(None, self.pool.read, ref_id, zero_copy, latch)

    async def release(self, ref_id: int):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.pool.release, ref_id)

    def size(self):
        return self.pool.size
