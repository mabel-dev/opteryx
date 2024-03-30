import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.shared.memory_pool import MemoryPool


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
    assert ref is None # failed to commit
    mp.release(ref1)
    ref = mp.commit(b"123456789")
    assert ref is None # failed to commit
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


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
