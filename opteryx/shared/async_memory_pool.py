# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is the async wrapper around the memory pool, it is used by the
async blob readers to move data to the synchronous code.
"""

import asyncio

from opteryx.compiled.structures import MemoryPool


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
        if we don't materialize them so we always create a copy.
        """
        async with self.lock:
            return self.pool.read(ref_id, zero_copy=False)

    async def release(self, ref_id: int):
        async with self.lock:
            self.pool.release(ref_id)

    def size(self):
        return self.pool.size
