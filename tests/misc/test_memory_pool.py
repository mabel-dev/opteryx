import os
import sys
import pytest
import random
import time
import threading

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.shared import MemoryPool

# from opteryx.compiled.structures import MemoryPool
from orso.tools import random_string


def test_commit_and_read():
    mp = MemoryPool(size=100)
    ref = mp.commit(b"Hello World")
    assert mp.read(ref) == b"Hello World"


def test_commit_insufficient_space():
    mp = MemoryPool(size=10)
    ref = mp.commit(b"This is too long")
    assert ref is None


def test_commit_exact_space():
    mp = MemoryPool(size=11)
    ref = mp.commit(b"Hello World")
    assert mp.read(ref) == b"Hello World"


def test_read_invalid_ref():
    mp = MemoryPool(size=100)
    with pytest.raises(ValueError):
        mp.read(999)


def test_release():
    mp = MemoryPool(size=100)
    ref = mp.commit(b"Temporary")
    mp.release(ref)
    with pytest.raises(ValueError):
        mp.read(ref)


def test_release_invalid_ref():
    mp = MemoryPool(size=100)
    with pytest.raises(ValueError):
        mp.release(999)


def test_compaction():
    mp = MemoryPool(size=100)
    ref1 = mp.commit(b"First")
    ref2 = mp.commit(b"Second")
    mp.release(ref1)
    ref3 = mp.commit(b"Third")
    # Ensure that the third commit succeeds after compaction, despite the first segment being released
    assert mp.read(ref3) == b"Third"


def test_multiple_commits_and_reads():
    mp = MemoryPool(size=50)
    ref1 = mp.commit(b"First")
    ref2 = mp.commit(b"Second")
    assert mp.read(ref1) == b"First"
    assert mp.read(ref2) == b"Second"


def test_overlapping_writes():
    mp = MemoryPool(size=20)
    ref1 = mp.commit(b"12345")
    ref2 = mp.commit(b"abcde")
    mp.release(ref1)
    ref3 = mp.commit(b"XYZ")
    # Test if the new write overlaps correctly and does not corrupt other data
    assert mp.read(ref2) == b"abcde"
    assert mp.read(ref3) == b"XYZ"


def test_pool_exhaustion_and_compaction():
    mp = MemoryPool(size=20)
    ref1 = mp.commit(b"123456")
    ref2 = mp.commit(b"123456")
    ref3 = mp.commit(b"123456")

    ref = mp.commit(b"123")
    assert ref is None  # failed to commit
    mp.release(ref1)
    ref = mp.commit(b"123456789")
    assert ref is None  # failed to commit
    ref4 = mp.commit(b"12345678")  # This should succeed because of compaction (L2)
    assert mp.l2_compaction > 0, mp.l2_compaction


def test_pool_only_l1_compaction():
    mp = MemoryPool(size=20)
    ref1 = mp.commit(b"12345")
    ref2 = mp.commit(b"12345")
    ref3 = mp.commit(b"1234567890")
    # this should free up two adjacent 5 byte blocks
    mp.release(ref1)
    mp.release(ref2)
    # this won't fit in either free block so the adjacent
    # blocks should be consolidated (L1) but doesn't need
    # rearranging of blocks (L2)
    ref4 = mp.commit(b"123456")

    # test we've executed L1 at least once, and we haven't
    # executed L2 compaction every time we executed L1
    assert mp.l1_compaction > 0
    assert mp.l2_compaction < mp.l1_compaction


def test_repeated_commits_and_releases():
    mp = MemoryPool(size=4000)
    refs = []
    for _ in range(1000):
        ref = mp.commit(b"Data")
        assert ref is not None
        refs.append(ref)
    for ref in refs:
        mp.release(ref)
    # Optional: Check internal state to ensure all resources are available again
    mp._level1_compaction()

    assert (
        mp.free_segments[0].length == mp.size
    ), "Memory leak detected after repeated commits and releases."


def test_stress_with_random_sized_data():
    mp = MemoryPool(size=500 * 200)
    refs = []
    try:
        for _ in range(500):
            size = random.randint(20, 200)  # Random data size between 20 and 200 bytes
            data = bytes(size)
            ref = mp.commit(data)
            if ref is not None:
                refs.append((ref, size))
    finally:
        for ref, _ in refs:
            mp.release(ref)
    # Ensure that the pool is not fragmented or leaking
    assert mp.available_space() >= mp.size - sum(
        size for _, size in refs if size < mp.size
    ), "Memory fragmentation or leak detected."


def test_compaction_effectiveness():
    mp = MemoryPool(size=15)
    # Fill the pool with alternating commitments and releases to create fragmentation
    ref1 = mp.commit(b"AAA")
    ref2 = mp.commit(b"BBBBB")
    ref3 = mp.commit(b"CCCCC")
    mp.release(ref2)
    mp.release(ref1)
    # This should trigger a compaction
    ref4 = mp.commit(b"DDDDDDDD")
    assert ref4 is not None, "Compaction failed to consolidate free memory effectively."
    mp.release(ref3)
    mp.release(ref4)
    assert mp.l1_compaction > 0, "Expected L1 compaction did not occur."


def test_repeated_zero_length_commits():
    mp = MemoryPool(size=100)  # Assuming a moderate size for the memory pool.
    refs = []
    try:
        # Commit a zero-length array many times.
        for _ in range(1000):
            ref = mp.commit(b"")  # Committing an empty byte string.
            assert ref is not None, "Commit of zero-length data should not fail."
            refs.append(ref)

        # Check if each reference is unique which implies correct handling of zero-length data.
        assert len(set(refs)) == len(refs), "References should be unique even for zero-length data."

    finally:
        # Cleanup: release all references.
        for ref in refs:
            mp.release(ref)

    # Optional: Verify if the memory pool is back to its initial state.
    assert mp.available_space() == mp.size, "Memory pool did not recover fully after releases."


def test_rapid_commit_release():
    mp = MemoryPool(size=1000)
    for _ in range(1000):
        ref = mp.commit(b"Some data")
        assert ref is not None, "Failed to commit data."
        mp.release(ref)
    # Verify memory integrity and state post rapid operations
    assert mp.available_space() == mp.size, "Memory pool did not return to full availability."


def test_commit_max_capacity():
    data = b"a" * 1000  # Assuming the pool size is 1000 bytes.
    mp = MemoryPool(size=1000)
    ref = mp.commit(data)
    assert ref is not None, "Failed to commit data that exactly matches the pool capacity."
    mp.release(ref)
    assert mp.available_space() == mp.size, "Memory pool did not correctly free up space."


def test_sequential_commits_without_space():
    mp = MemoryPool(size=500)
    mp.commit(b"a" * 250)
    mp.commit(b"b" * 250)
    ref = mp.commit(b"Third commit should fail")
    assert ref is None, "Memory pool erroneously allowed overcommitment."


def test_stress_with_variable_data_sizes():
    mp = MemoryPool(size=10000)
    refs = []
    try:
        for _ in range(100):
            size = random.randint(1, 200)  # Data size between 1 and 200 bytes.
            data = bytes(random.choices(range(256), k=size))
            ref = mp.commit(data)
            if ref is not None:
                refs.append(ref)
    finally:
        for ref in refs:
            mp.release(ref)
    # Ensure all space is reclaimed
    assert mp.available_space() == mp.size, "Memory leakage detected with variable data sizes."


def test_zero_byte_commit_on_full_pool():
    mp = MemoryPool(size=500)
    # Fill up the pool
    mp.commit(b"x" * 500)
    # Try committing zero bytes
    ref = mp.commit(b"")
    assert ref is not None, "Memory pool failed to handle zero-byte commit on a full pool."


def test_random_release_order():
    mp = MemoryPool(size=1000)
    refs = [mp.commit(b"Data" + bytes([i])) for i in range(10)]
    random.shuffle(refs)  # Randomize the order of releases
    for ref in refs:
        mp.release(ref)
    assert (
        mp.available_space() == mp.size
    ), "Memory pool failed to reclaim space correctly after random releases."


def test_concurrent_access():
    pool_size = 100
    memory_pool = MemoryPool(size=pool_size)
    threads = []
    errors = []

    def thread_task():
        try:
            for _ in range(500):  # Number of operations per thread
                time.sleep(0.001)  # It's fast, add in a delay to the threads overlap
                data = random_string().encode()  # unique per write
                ref = memory_pool.commit(data)
                if ref is not None:
                    read_data = memory_pool.read(ref)
                    assert read_data == data, "Data integrity check failed"
                    memory_pool.release(ref)
                else:
                    # Handle commit failure (e.g., no space)
                    pass
        except AssertionError as e:
            errors.append(str(e))

    # Create multiple threads to simulate concurrent access
    for _ in range(32):
        thread = threading.Thread(target=thread_task)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check for errors collected during the test
    assert not errors, f"Errors encountered during concurrent access: {errors}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
