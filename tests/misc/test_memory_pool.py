import os
import random
import sys
import threading
import time

import pytest

os.environ["OPTERYX_DEBUG"] = "1"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.tools import random_string

from opteryx.shared import MemoryPool


def test_commit_and_read():
    mp = MemoryPool(size=100)
    ref = mp.commit(b"Hello World")
    assert mp.read(ref, False) == b"Hello World"


def test_commit_insufficient_space():
    mp = MemoryPool(size=10)
    ref = mp.commit(b"This is too long")
    assert ref is None


def test_commit_exact_space():
    mp = MemoryPool(size=11)
    ref = mp.commit(b"Hello World")
    assert mp.read(ref, False) == b"Hello World"


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


def test_read_and_release():
    mp = MemoryPool(size=100)
    ref = mp.commit(b"Temporary")
    mp.read_and_release(ref)
    with pytest.raises(ValueError):
        mp.read(ref)


def test_read_and_release_invalid_ref():
    mp = MemoryPool(size=100)
    with pytest.raises(ValueError):
        mp.read_and_release(999)


def test_compaction():
    mp = MemoryPool(size=100)
    ref1 = mp.commit(b"First")
    ref2 = mp.commit(b"Second")
    mp.release(ref1)
    ref3 = mp.commit(b"Third")
    # Ensure that the third commit succeeds after compaction, despite the first segment being released
    data = mp.read(ref3, False)
    assert data == b"Third"


def test_multiple_commits_and_reads():
    mp = MemoryPool(size=50)
    ref1 = mp.commit(b"First")
    ref2 = mp.commit(b"Second")
    assert mp.read(ref1, False) == b"First"
    assert mp.read(ref2, False) == b"Second"


def test_overlapping_writes():
    mp = MemoryPool(size=20)
    ref1 = mp.commit(b"12345")
    ref2 = mp.commit(b"abcde")
    mp.release(ref1)
    ref3 = mp.commit(b"XYZ")
    # Test if the new write overlaps correctly and does not corrupt other data
    assert mp.read(ref2, False) == b"abcde"
    assert mp.read(ref3, False) == b"XYZ"


def test_overlapping_writes_memcopy():
    mp = MemoryPool(size=20)
    ref1 = mp.commit(b"12345")
    ref2 = mp.commit(b"abcde")
    mp.release(ref1)
    ref3 = mp.commit(b"XYZ")
    # Test if the new write overlaps correctly and does not corrupt other data
    r2_memcopy = bytes(mp.read(ref2, True))
    r2_no_memcopy = mp.read(ref2, False)
    r3_memcopy = bytes(mp.read(ref3, True))
    r3_no_memcopy = mp.read(ref3, False)

    assert r2_memcopy == r2_no_memcopy == b"abcde", f"{r2_memcopy} / {r2_no_memcopy} / abcde"
    assert r3_memcopy == r3_no_memcopy == b"XYZ", f"{r3_memcopy} / {r3_no_memcopy} / XYZ"


def test_zero_copy_vs_copy_reads():
    mp = MemoryPool(size=30)

    # Initial commits
    ref1 = mp.commit(b"12345")
    ref2 = mp.commit(b"abcde")
    ref3 = mp.commit(b"ABCDE")

    # Release one segment to create free space
    mp.release(ref1)

    # Commit more data to fill the pool
    ref4 = mp.commit(b"XYZ")
    ref5 = mp.commit(b"7890")

    # Additional activity
    ref6 = mp.commit(b"LMNOP")
    mp.release(ref3)
    ref7 = mp.commit(b"qrst")
    mp.release(ref2)
    ref8 = mp.commit(b"uvwxyz")

    # Reading segments with and without zero-copy
    r4_memcopy = bytes(mp.read(ref4, True))
    r4_no_memcopy = mp.read(ref4, False)
    r5_memcopy = bytes(mp.read(ref5, True))
    r5_no_memcopy = mp.read(ref5, False)
    r6_memcopy = bytes(mp.read(ref6, True))
    r6_no_memcopy = mp.read(ref6, False)
    r7_memcopy = bytes(mp.read(ref7, True))
    r7_no_memcopy = mp.read(ref7, False)
    r8_memcopy = bytes(mp.read(ref8, True))
    r8_no_memcopy = mp.read(ref8, False)

    assert r4_memcopy == r4_no_memcopy == b"XYZ", f"{r4_memcopy} / {r4_no_memcopy} / XYZ"
    assert r5_memcopy == r5_no_memcopy == b"7890", f"{r5_memcopy} / {r5_no_memcopy} / 7890"
    assert r6_memcopy == r6_no_memcopy == b"LMNOP", f"{r6_memcopy} / {r6_no_memcopy} / LMNOP"
    assert r7_memcopy == r7_no_memcopy == b"qrst", f"{r7_memcopy} / {r7_no_memcopy} / qrst"
    assert r8_memcopy == r8_no_memcopy == b"uvwxyz", f"{r8_memcopy} / {r8_no_memcopy} / uvwxyz"


def test_zero_copy_vs_copy_reads_and_release():
    mp = MemoryPool(size=30)

    # Initial commits
    ref1 = mp.commit(b"12345")
    ref2 = mp.commit(b"abcde")
    ref3 = mp.commit(b"ABCDE")

    # Release one segment to create free space
    mp.release(ref1)

    # Commit more data to fill the pool
    ref4 = mp.commit(b"XYZ")
    ref5 = mp.commit(b"7890")

    # Additional activity
    ref6 = mp.commit(b"LMNOP")
    mp.release(ref3)
    ref7 = mp.commit(b"qrst")
    mp.release(ref2)
    ref8 = mp.commit(b"uvwxyz")

    # Reading segments with and without zero-copy, alternating read and read_and_release
    # read no zero copy, release zero copy
    r4_read_no_memcopy = bytes(mp.read(ref4, False))
    r4_release_memcopy = bytes(mp.read_and_release(ref4, True))

    # read zero copy, release no zero copy
    r5_read_memcopy = bytes(mp.read(ref5, True))
    r5_release_no_memcopy = bytes(mp.read_and_release(ref5, False))

    # read zero copy, release zero copy
    r6_read_memcopy = bytes(mp.read(ref6, True))
    r6_release_memcopy = bytes(mp.read_and_release(ref6, True))

    # read no zero copy, release no zero copy
    r7_read_no_memcopy = bytes(mp.read(ref7, False))
    r7_release_no_memcopy = bytes(mp.read_and_release(ref7, False))

    # read zero copy, release zero copy
    r8_read_memcopy = bytes(mp.read(ref8, True))
    r8_release_memcopy = bytes(mp.read_and_release(ref8, True))

    assert (
        r4_read_no_memcopy == r4_release_memcopy == b"XYZ"
    ), f"{r4_read_no_memcopy} / {r4_release_memcopy} / XYZ"
    assert (
        r5_read_memcopy == r5_release_no_memcopy == b"7890"
    ), f"{r5_read_memcopy} / {r5_release_no_memcopy} / 7890"
    assert (
        r6_read_memcopy == r6_release_memcopy == b"LMNOP"
    ), f"{r6_read_memcopy} / {r6_release_memcopy} / LMNOP"
    assert (
        r7_read_no_memcopy == r7_release_no_memcopy == b"qrst"
    ), f"{r7_read_no_memcopy} / {r7_release_no_memcopy} / qrst"
    assert (
        r8_read_memcopy == r8_release_memcopy == b"uvwxyz"
    ), f"{r8_read_memcopy} / {r8_release_memcopy} / uvwxyz"

    # Ensure that the segments are released and available for new commits
    ref9 = mp.commit(b"newdata")
    r9_memcopy = bytes(mp.read(ref9, True))
    r9_no_memcopy = mp.read(ref9, False)

    assert r9_memcopy == r9_no_memcopy == b"newdata", f"{r9_memcopy} / {r9_no_memcopy} / newdata"


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
        mp.free_segments[0]["length"] == mp.size
    ), f"Memory leak detected after repeated commits and releases. {mp.free_segments[0]['length']} != {mp.size}\n{mp.free_segments}"


def test_stress_with_random_sized_data():
    """
    This is designed to create situations where we have fragmentation and
    many removes and adds.

    This has been the most useful test in finding edge cases.
    """
    seed = random.randint(0, 2 ^ 32 - 1)
    random.seed(seed)
    mp = MemoryPool(size=1000 * 200)
    refs = set()
    #    saved_bytes = 0
    #    used_counter = 0

    for _ in range(10000):
        size = random.randint(20, 50)  # Random data size between 20 and 200 bytes
        data = bytes(size)
        ref = mp.commit(data)
        if ref is not None:
            refs.add(ref)
        #            saved_bytes += size
        #            used_counter += 1
        else:
            selected = random.sample(list(refs), random.randint(1, len(refs) // 10))
            for ref in selected:
                refs.discard(ref)
                data = mp.read_and_release(ref, False)
        #                saved_bytes -= len(data)
        #                used_counter -= 1

        #        assert len(mp.used_segments) == used_counter, f"\n{len(mp.used_segments)} != {used_counter}\n{saved_bytes}"
        #        assert saved_bytes + mp.available_space() == mp.size, _
        for ref in list(refs):
            data = mp.read_and_release(ref, False)
            #            saved_bytes -= len(data)
            #            used_counter -= 1
            refs.discard(ref)

    # Ensure that the pool or leaking
    mp._level1_compaction()
    assert (
        mp.available_space() == mp.size
    ), f"Memory fragmentation or leak detected.\n{mp.available_space()} != {mp.size}\n{mp.free_segments}\n{mp.used_segments}\nseed:{seed}"

    assert len(mp.free_segments) == 1
    assert len(mp.used_segments) == 0


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
                    read_data = memory_pool.read(ref, False)
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


def test_return_types():
    pool_size = 100
    memory_pool = MemoryPool(size=pool_size)
    abc = memory_pool.commit(b"abc")

    read = memory_pool.read(abc)
    assert isinstance(read, memoryview), type(read)

    read = memory_pool.read(abc, True)
    assert isinstance(read, memoryview), type(read)

    read = memory_pool.read(abc, False)
    assert isinstance(read, bytes), type(read)

    read = memory_pool.read(abc, zero_copy=True)
    assert isinstance(read, memoryview), type(read)

    read = memory_pool.read(abc, zero_copy=False)
    assert isinstance(read, bytes), type(read)

    abc = memory_pool.commit(b"abc")
    read = memory_pool.read_and_release(abc)
    assert isinstance(read, memoryview), type(read)

    abc = memory_pool.commit(b"abc")
    read = memory_pool.read_and_release(abc, True)
    assert isinstance(read, memoryview), type(read)

    abc = memory_pool.commit(b"abc")
    read = memory_pool.read_and_release(abc, False)
    assert isinstance(read, bytes), type(read)

    abc = memory_pool.commit(b"abc")
    read = memory_pool.read_and_release(abc, zero_copy=True)
    assert isinstance(read, memoryview), type(read)

    abc = memory_pool.commit(b"abc")
    read = memory_pool.read_and_release(abc, zero_copy=False)
    assert isinstance(read, bytes), type(read)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
