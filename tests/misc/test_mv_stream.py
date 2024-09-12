import io
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.memory_view_stream import MemoryViewStream


def test_read_full():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)
    assert stream.read() == data


def test_read_in_chunks():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)
    assert stream.read(5) == b"Hello"
    assert stream.read(2) == b", "
    assert stream.read(6) == b"World!"


def test_seek_and_tell():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)
    stream.seek(7)
    assert stream.tell() == 7
    assert stream.read(5) == b"World"


def test_read_past_end():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)
    stream.seek(12)
    assert stream.read(1) == b"!"
    assert stream.read(1) == b""  # Reading past the end returns an empty bytes object


def test_close():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)
    assert not stream.closed
    stream.close()
    assert stream.closed


def test_context_manager():
    data = b"Hello, World!"
    mv = memoryview(data)
    with MemoryViewStream(mv) as stream:
        assert not stream.closed
    assert stream.closed


def test_seek():
    def inner(offset, whence, expected_offset):
        data = b"Hello, World!"
        mv = memoryview(data)
        stream = MemoryViewStream(mv)
        stream.seek(offset, whence)
        assert stream.tell() == expected_offset

    params = [
        (5, 0, 5),  # Absolute positioning
        (2, 1, 2),  # Relative to current position
        (-3, 2, 10),  # Relative to file's end
    ]

    for param in params:
        inner(*param)

def test_unsupported_operations():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)
    with pytest.raises(io.UnsupportedOperation):
        stream.readline()
    with pytest.raises(io.UnsupportedOperation):
        stream.readlines()
    with pytest.raises(io.UnsupportedOperation):
        stream.truncate()
    with pytest.raises(io.UnsupportedOperation):
        stream.write()
    with pytest.raises(io.UnsupportedOperation):
        stream.writelines()
    with pytest.raises(io.UnsupportedOperation):
        stream.flush()


def test_other_attributes():
    data = b"Hello, World!"
    mv = memoryview(data)
    stream = MemoryViewStream(mv)

    from typing import Iterator

    assert stream.readable()
    assert not stream.writable()
    assert stream.seekable()
    assert isinstance(iter(stream), Iterator)

    for i in stream:
        pass

    assert stream.fileno() == -1
    assert not stream.isatty()

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
