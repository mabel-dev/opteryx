"""
Memory Pool Tests for Opteryx

This module contains rigorous and stress-oriented unit tests for the MemoryPool
component used in Opteryx. The MemoryPool is a manually managed, fixed-size memory
buffer that supports in-place allocation, zero-copy reads, latching (locks) for
concurrency control, and multiple compaction strategies.

Tests cover:
- Basic allocation and release functionality
- Latching behavior and its protection against data movement
- Data integrity guarantees across compaction cycles
- Handling of fragmentation and overlapping memory regions
- Stress scenarios with randomized operations and repeated compaction

Because the MemoryPool is a foundational component, these tests are intentionally
aggressive and exhaustive. They are designed to surface off-by-one errors,
overlapping segment bugs, and state leakage across operations.

If any test in this module fails, memory safety and correctness guarantees in Opteryx
may be compromised.

These tests assume a single-threaded environment, but simulate complex memory
access patterns to mimic concurrent behavior where relevant.
"""

import os
import random
import sys
import threading
import time

import pytest

# os.environ["OPTERYX_DEBUG"] = "1"

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from orso.tools import random_string

from opteryx.shared import MemoryPool


def test_commit_and_read():
    mp = MemoryPool(size=100)
    ref = mp.commit(b"Hello World")
    assert mp.read(ref, False) == b"Hello World"


def test_commit_insufficient_space():
    mp = MemoryPool(size=10)
    ref = mp.commit(b"This is too long")
    assert ref == -1


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


def test_pool_exhaustion_and_compaction():
    mp = MemoryPool(size=20)
    ref1 = mp.commit(b"123456")
    ref2 = mp.commit(b"123456")
    ref3 = mp.commit(b"123456")

    ref = mp.commit(b"123")
    assert ref == -1  # failed to commit
    mp.release(ref1)
    ref = mp.commit(b"123456789")
    assert ref == -1  # failed to commit
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
        assert ref != -1
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
        if ref != -1:
            refs.add(ref)
        #            saved_bytes += size
        #            used_counter += 1
        else:
            selected = random.sample(list(refs), random.randint(1, len(refs) // 10))
            for ref in selected:
                refs.discard(ref)
                data = mp.read(ref)
                mp.release(ref)
        #                saved_bytes -= len(data)
        #                used_counter -= 1

        #        assert len(mp.used_segments) == used_counter, f"\n{len(mp.used_segments)} != {used_counter}\n{saved_bytes}"
        #        assert saved_bytes + mp.available_space() == mp.size, _
        for ref in list(refs):
            data = mp.read(ref)
            mp.release(ref)
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
    assert ref4 != -1, "Compaction failed to consolidate free memory effectively."
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
            assert ref != -1, "Commit of zero-length data should not fail."
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
        assert ref != -1, "Failed to commit data."
        mp.release(ref)
    # Verify memory integrity and state post rapid operations
    assert mp.available_space() == mp.size, "Memory pool did not return to full availability."


def test_commit_max_capacity():
    data = b"a" * 1000  # Assuming the pool size is 1000 bytes.
    mp = MemoryPool(size=1000)
    ref = mp.commit(data)
    assert ref != -1, "Failed to commit data that exactly matches the pool capacity."
    mp.release(ref)
    assert mp.available_space() == mp.size, "Memory pool did not correctly free up space."


def test_sequential_commits_without_space():
    mp = MemoryPool(size=500)
    mp.commit(b"a" * 250)
    mp.commit(b"b" * 250)
    ref = mp.commit(b"Third commit should fail")
    assert ref == -1, "Memory pool erroneously allowed overcommitment."


def test_stress_with_variable_data_sizes():
    mp = MemoryPool(size=10000)
    refs = []
    try:
        for _ in range(100):
            size = random.randint(1, 200)  # Data size between 1 and 200 bytes.
            data = bytes(random.choices(range(256), k=size))
            ref = mp.commit(data)
            if ref != -1:
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
    assert ref != -1, "Memory pool failed to handle zero-byte commit on a full pool."


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
                if ref != -1:
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
    assert isinstance(read, bytes), type(read)

    read = memory_pool.read(abc, True)
    assert isinstance(read, memoryview), type(read)

    read = memory_pool.read(abc, False)
    assert isinstance(read, bytes), type(read)

    read = memory_pool.read(abc, zero_copy=True)
    assert isinstance(read, memoryview), type(read)

    read = memory_pool.read(abc, zero_copy=False)
    assert isinstance(read, bytes), type(read)

    abc = memory_pool.commit(b"abc")
    read = memory_pool.read(abc)
    memory_pool.release(abc)  # in case read_and_release fails
    assert isinstance(read, bytes), type(read)


def test_latch_and_unlatch_behavior():
    mp = MemoryPool(size=1024)
    data = b"abc" * 10
    ref = mp.commit(data)
    
    # Read with latch
    view = mp.read(ref, zero_copy=True, latch=True)
    assert isinstance(view, memoryview)
    
    # Unlatch should succeed
    mp.unlatch(ref)
    
    # Release should now succeed
    mp.release(ref)
    
    assert mp.available_space() == mp.size


def test_latch_counting():
    mp = MemoryPool(size=1024)

    ref = mp.commit(b"abc")

    assert mp.used_segments[ref]["latches"] == 0

    mp.read(ref, latch=1)
    assert mp.used_segments[ref]["latches"] == 1

    mp.unlatch(ref)
    assert mp.used_segments[ref]["latches"] == 0

    mp.read(ref, latch=True)
    mp.read(ref, latch=True)
    mp.read(ref, latch=True)
    assert mp.used_segments[ref]["latches"] == 3

    mp.unlatch(ref)
    assert mp.used_segments[ref]["latches"] == 2

    mp.read(ref)
    assert mp.used_segments[ref]["latches"] == 2
    mp.read(ref, latch=True)
    assert mp.used_segments[ref]["latches"] == 3

    mp.unlatch(ref)
    assert mp.used_segments[ref]["latches"] == 2
    mp.unlatch(ref)
    assert mp.used_segments[ref]["latches"] == 1
    mp.unlatch(ref)
    assert mp.used_segments[ref]["latches"] == 0


def test_unlatch_without_latching_raises():
    mp = MemoryPool(size=1024)
    ref = mp.commit(b"abc")
    
    with pytest.raises(RuntimeError):
        mp.unlatch(ref)

    mp.release(ref)


def test_double_latch_and_unlatch():
    mp = MemoryPool(size=1024)
    ref = mp.commit(b"x" * 32)
    
    # Latch twice (should just set flag)
    mp.read(ref, latch=True)
    mp.read(ref, latch=True)
    
    # Unlatch once - no problem
    mp.unlatch(ref)

    # Unlatch twice - no problem
    mp.unlatch(ref)

    # Third unlatch should now fail
    with pytest.raises(RuntimeError):
        mp.unlatch(ref)
    
    mp.release(ref)

def test_latching_sets_flag():
    pool = MemoryPool(1000)
    ref = pool.commit(b"test")
    assert pool.used_segments[ref]["latches"] == 0

    pool.read(ref, latch=1)
    assert pool.used_segments[ref]["latches"] == 1

    pool.unlatch(ref)
    assert pool.used_segments[ref]["latches"] == 0


def test_unlatch_without_latch_raises():
    pool = MemoryPool(1000)
    ref = pool.commit(b"hello")

    with pytest.raises(RuntimeError):
        pool.unlatch(ref)


def test_double_unlatch_raises():
    pool = MemoryPool(1000)
    ref = pool.commit(b"data")
    pool.read(ref, latch=1)
    pool.unlatch(ref)

    with pytest.raises(RuntimeError):
        pool.unlatch(ref)


def test_release_latched_segment_then_unlatch_is_invalid():
    pool = MemoryPool(1000)
    ref = pool.commit(b"segment")
    pool.read(ref, latch=1)
    pool.release(ref)

    # Should not be able to unlatch a released segment
    with pytest.raises(ValueError):
        pool.unlatch(ref)


def test_compaction_skips_latched_segment():
    pool = MemoryPool(100)
    ref1 = pool.commit(b"A" * 10)
    ref2 = pool.commit(b"B" * 10)
    ref3 = pool.commit(b"C" * 10)

    # Latch ref2 so it can't be moved
    pool.read(ref2, latch=1)

    pool.release(ref1)
    pool.release(ref3)
    assert pool.used_segments[ref2]["latches"] == 1

    # This should leave ref2 where it is
    pool._level2_compaction()

    # Sanity: ref2 is still valid and still latched
    data = pool.read(ref2)
    assert data == b"B" * 10
    assert pool.used_segments[ref2]["latches"] == 1


def test_aggressive_compaction_respects_latches():
    pool = MemoryPool(1000)
    refs = {}
    data_map = {}
    latched_refs = set()
    released_refs = set()

    # Allocate up to ~100 segments of random sizes (5–20 bytes)
    for i in range(100):
        size = random.randint(5, 20)
        data = bytes([i % 256]) * size
        ref = pool.commit(data)
        if ref == -1:
            break  # pool is full
        refs[ref] = size
        data_map[ref] = data

    all_refs = list(refs.keys())

    # Randomly latch about 1/3 of them
    latched_refs = set(random.sample(all_refs, k=len(all_refs) // 3))
    for ref in latched_refs:
        pool.read(ref, latch=1)

    # Randomly release another 1/3 (non-latched only)
    unlatching_candidates = list(set(all_refs) - latched_refs)
    released_refs = set(random.sample(unlatching_candidates, k=len(unlatching_candidates) // 2))
    for ref in released_refs:
        pool.release(ref)

    # Record locations of latched segments
    pre_compaction_positions = {
        ref: pool.used_segments[ref]["start"] for ref in latched_refs if ref in pool.used_segments
    }

    # pre-check positions and data for latched segments
    for ref in latched_refs:
        assert ref in pool.used_segments, f"Latched ref {ref} was removed!"
        seg = pool.used_segments[ref]
        assert seg["latches"] == 1, f"Latched ref {ref} is no longer latched!"
        assert seg["start"] == pre_compaction_positions[ref], (
            f"Latched ref {ref} moved from {pre_compaction_positions[ref]} to {seg['start']}"
        )
        assert pool.read(ref) == data_map[ref], f"Data corruption on latched ref {ref}! {pool.read(ref)} != {data_map[ref]}"

    # Run compaction
    pool._level2_compaction()

    # Re-check positions and data for latched segments
    for ref in latched_refs:
        assert ref in pool.used_segments, f"Latched ref {ref} was removed!"
        seg = pool.used_segments[ref]
        assert seg["latches"] == 1, f"Latched ref {ref} is no longer latched!"
        assert seg["start"] == pre_compaction_positions[ref], (
            f"Latched ref {ref} moved from {pre_compaction_positions[ref]} to {seg['start']}"
        )
        assert pool.read(ref) == data_map[ref], f"Data corruption on latched ref {ref}! {pool.read(ref)} != {data_map[ref]}"

    # Ensure free segments do not overlap with any latched segments
    for free_seg in pool.free_segments:
        for ref in latched_refs:
            used = pool.used_segments[ref]
            f_start = free_seg["start"]
            f_end = f_start + free_seg["length"]
            u_start = used["start"]
            u_end = u_start + used["length"]

            assert f_end <= u_start or f_start >= u_end, (
                f"Free segment ({f_start}-{f_end}) overlaps latched segment {ref} "
                f"({u_start}-{u_end})"
            )


def test_latch_blocks_compaction_and_unlatch_allows_it():
    pool = MemoryPool(100)
    ref1 = pool.commit(b"A" * 10)
    ref2 = pool.commit(b"B" * 10)
    pool.read(ref1, latch=1)

    pool.release(ref2)

    old_start = pool.used_segments[ref1]["start"]
    pool._level2_compaction()

    # Should not move ref1
    assert pool.used_segments[ref1]["start"] == old_start

    # Now unlatch and compact again
    pool.unlatch(ref1)
    pool._level2_compaction()

    # Should now move ref1 to 0
    assert pool.used_segments[ref1]["start"] == 0


def test_zero_copy_latch_flag():
    pool = MemoryPool(100)
    ref = pool.commit(b"quick brown fox")
    mv = pool.read(ref, latch=1, zero_copy=1)
    assert isinstance(mv, memoryview)
    assert pool.used_segments[ref]["latches"] == 1


def test_multiple_commits_and_random_latch_release():
    pool = MemoryPool(1000)
    refs = []

    for i in range(10):
        data = bytes([i]) * 50
        ref = pool.commit(data)
        refs.append(ref)

    # Latch even refs
    for ref in refs:
        if ref % 2 == 0:
            pool.read(ref, latch=1)

    # Unlatch them again
    for ref in refs:
        if ref % 2 == 0:
            pool.unlatch(ref)

    # All segments should now be unlatched
    assert all(pool.used_segments[ref]["latches"] == 0 for ref in refs if ref in pool.used_segments)


def test_multiple_latches_block_compaction_selectively():
    pool = MemoryPool(200)

    refs = [
        pool.commit(b"A" * 10),  # ref0
        pool.commit(b"B" * 10),  # ref1
        pool.commit(b"C" * 10),  # ref2
        pool.commit(b"D" * 10),  # ref3
        pool.commit(b"E" * 10),  # ref4
    ]

    pool.read(refs[1], latch=1)  # latch ref1
    pool.read(refs[3], latch=1)  # latch ref3

    pool.release(refs[0])
    pool.release(refs[2])
    pool.release(refs[4])

    starts_before = {r: pool.used_segments[r]["start"] for r in refs if r in pool.used_segments}

    pool._level2_compaction()

    # latched refs should not move
    assert pool.used_segments[refs[1]]["start"] == starts_before[refs[1]]
    assert pool.used_segments[refs[3]]["start"] == starts_before[refs[3]]

    # everything else should be gone or moved (released)
    assert refs[0] not in pool.used_segments
    assert refs[2] not in pool.used_segments
    assert refs[4] not in pool.used_segments


def test_latch_causes_persistent_fragmentation():
    pool = MemoryPool(100)

    ref1 = pool.commit(b"A" * 30)
    ref2 = pool.commit(b"B" * 30)
    ref3 = pool.commit(b"C" * 30)

    pool.read(ref2, latch=1)
    pool.release(ref1)
    pool.release(ref3)

    # Now, available space is 60 (30 + 30 + 10), but fragmented around latched ref2
    assert pool.available_space() == 70
    pool._level2_compaction()
    # The fragmentation remains because ref2 is latched
    # So no new allocation of 60 should not be possible
    ref4 = pool.commit(b"X" * 60)
    assert ref4 == -1  # Should fail to commit due to fragmentation

    # Unlatch ref2 and try again
    pool.unlatch(ref2)
    pool._level2_compaction()
    ref4 = pool.commit(b"X" * 60)
    assert ref4 != -1, "Failed to allocate after unlatching and compaction"


def test_staggered_latch_unlatch_compaction():
    pool = MemoryPool(300)
    refs = [pool.commit(bytes([i]) * 30) for i in range(6)]  # Fill the pool with 6 segments

    # Latch alternating segments
    for i, ref in enumerate(refs):
        if i % 2 == 0:
            pool.read(ref, latch=1)

    # Release the others
    for i, ref in enumerate(refs):
        if i % 2 == 1:
            pool.release(ref)

    # Compaction shouldn't move latched segments
    starts = {ref: pool.used_segments[ref]["start"] for ref in refs if ref in pool.used_segments}
    pool._level2_compaction()

    for ref in refs:
        if ref in pool.used_segments and pool.used_segments[ref]["latches"] == 1:
            assert pool.used_segments[ref]["start"] == starts[ref]

    # Now unlatch all
    for ref in refs:
        if ref in pool.used_segments and pool.used_segments[ref]["latches"] == 1:
            pool.unlatch(ref)

    # Compaction should now move everything to front
    pool._level2_compaction()
    sorted_refs = sorted((r for r in refs if r in pool.used_segments), key=lambda r: pool.used_segments[r]["start"])
    for i, ref in enumerate(sorted_refs):
        assert pool.used_segments[ref]["start"] == i * 30


def test_repeated_latch_compact_unlatch_cycles():
    pool = MemoryPool(1000)
    ref_data = {}
    latched_refs = set()
    released_refs = set()
    ref_start_positions = {}

    def validate_integrity():
        # Check latched segment data is intact and position unchanged
        for ref in latched_refs:
            assert ref in pool.used_segments
            seg = pool.used_segments[ref]
            assert seg["latches"] == 1
            assert seg["start"] == ref_start_positions[ref]
            assert pool.read(ref) == ref_data[ref]

        # Ensure free segments don’t overlap with latched
        for fseg in pool.free_segments:
            f_start = fseg["start"]
            f_end = f_start + fseg["length"]
            for ref in latched_refs:
                if ref not in pool.used_segments:
                    continue
                useg = pool.used_segments[ref]
                u_start = useg["start"]
                u_end = u_start + useg["length"]
                assert f_end <= u_start or f_start >= u_end, (
                    f"Free {f_start}-{f_end} overlaps with latched {u_start}-{u_end}"
                )

    for phase in range(5):  # multiple compaction phases
        # Phase 1: fill up with random segments
        for _ in range(100):
            size = random.randint(5, 20)
            data = bytes([random.randint(0, 255)]) * size
            ref = pool.commit(data)
            if ref == -1:
                break
            ref_data[ref] = data

        all_refs = [r for r in ref_data if r not in released_refs]

        # Phase 2: randomly latch a third
        new_latched = set(random.sample(all_refs, k=len(all_refs) // 3))
        for ref in new_latched:
            if ref not in latched_refs:
                pool.read(ref, latch=1)
                ref_start_positions[ref] = pool.used_segments[ref]["start"]
        latched_refs.update(new_latched)

        # Phase 3: randomly release a third of non-latched
        candidates = list(set(all_refs) - latched_refs)
        to_release = set(random.sample(candidates, k=len(candidates) // 3))
        for ref in to_release:
            pool.release(ref)
            released_refs.add(ref)

        # Phase 4: compact while some are latched
        pool._level2_compaction()
        validate_integrity()

        # Phase 5: write more data to force fragmentation
        for _ in range(25):
            size = random.randint(5, 30)
            data = bytes([random.randint(0, 255)]) * size
            ref = pool.commit(data)
            if ref != -1:
                ref_data[ref] = data

        # Phase 6: unlatch a few
        if latched_refs:
            to_unlatch = set(random.sample(list(latched_refs), k=max(1, len(latched_refs) // 4)))
            for ref in to_unlatch:
                pool.unlatch(ref)
                latched_refs.remove(ref)

        # Phase 7: final compaction
        pool._level2_compaction()
        validate_integrity()

    # Sanity check: all remaining latched data is intact
    for ref in latched_refs:
        assert ref in pool.used_segments
        assert pool.used_segments[ref]["latches"] == 1
        assert pool.read(ref) == ref_data[ref]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
