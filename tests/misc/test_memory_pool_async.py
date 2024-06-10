"""
Stress test the memory pool in an asynchronous execution environment.

We add and remove items from the pool in quick succession, random sizes
and random items removed, to see how it responds to random concurrent
access. This is targetting the async wrapper, but that is a relatively
thin layer over the memory pool.
"""

import os
import sys
import random

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

os.environ["OPTERYX_DEBUG"] = "1"

from opteryx.shared import MemoryPool, AsyncMemoryPool


import asyncio
import random


async def stress_with_random_sized_data():
    bmp = MemoryPool(size=100000)
    mp = AsyncMemoryPool(bmp)
    refs = {}

    async def add_random_data():
        for _ in range(500):
            size = random.randint(10, 100)
            data = bytes([random.randint(0, 255) for _ in range(size)])
            ref = await mp.commit(data)
            if ref is not None:
                refs[ref] = data
            else:
                # Memory pool is likely full, start removing
                await remove_random_data()

            # Simulate asynchronous behavior
            await asyncio.sleep(random.random() * 0.005)

    async def remove_random_data():
        for _ in range(random.randint(10, 25)):
            if refs:
                ref = random.choice(list(refs.keys()))
                correct_data = refs.pop(ref)
                data_removed = await mp.read(ref)
                data_removed = await mp.read(ref)
                await mp.release(ref)
                assert data_removed == correct_data, "Data integrity check failed"

    # Start tasks for adding and randomly removing data
    tasks = [add_random_data() for _ in range(50)]  # Simulating concurrent access
    await asyncio.gather(*tasks)

    # Final cleanup: remove remaining items
    for ref in list(refs):
        await mp.read(ref)
        await mp.release(ref)

    # Ensure all memory is accounted for
    bmp._level1_compaction()
    assert bmp.available_space() == bmp.size, "Memory leak or fragmentation detected."


def test_async_memorypool():
    asyncio.run(stress_with_random_sized_data())


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
