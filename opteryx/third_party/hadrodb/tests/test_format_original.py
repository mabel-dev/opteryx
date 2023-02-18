import random
import time
import typing
import uuid

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.hadrodb.record import (
    encode_kv,
    decode_kv,
    HEADER_SIZE,
    KeyEntry,
)


def get_random_header() -> typing.Tuple[int, int, int]:
    max_size: int = (2**32) - 1
    random_int: typing.Callable[[], int] = lambda: random.randint(0, max_size)
    return random_int(), random_int(), random_int()


def get_random_kv() -> typing.Tuple[int, bytes, str, int]:
    return (
        int(time.time()),
        str(uuid.uuid4()).encode(),
        str(uuid.uuid4()),
        HEADER_SIZE + (2 * len(str(uuid.uuid4()))) + 2,
    )


class Header(typing.NamedTuple):
    timestamp: int
    key_size: int
    val_size: int


class KeyValue(typing.NamedTuple):
    timestamp: int
    key: typing.Union[bytes, str]
    val: typing.Union[bytes, str]
    sz: int


def test_header_serialisation():
    tests: typing.List[Header] = [
        Header(10, 10, 10),
        Header(0, 0, 0),
        Header(10000, 10000, 10000),
    ]
    for tt in tests:
        header_test(tt)


def test_random_header():
    for _ in range(100):
        tt = Header(*get_random_header())
        header_test(tt)


def header_test(tt):
    size, encoded = encode_kv(tt.timestamp, b"", b"")
    assert tt.timestamp == decode_kv(encoded)[0]


def test_KV_serialisation():
    tests: typing.List[KeyValue] = [
        KeyValue(
            10, b"hello", "world", HEADER_SIZE + 10 + 1
        ),  # len(hello) = 5, len(world) = 5, + 1 type
        KeyValue(0, b"", "", HEADER_SIZE + 1),
    ]
    for tt in tests:
        kv_test(tt)


def test_random_kv():
    for _ in range(100):
        tt = KeyValue(*get_random_kv())
        kv_test(tt)


def kv_test(tt):
    sz, data = encode_kv(tt.timestamp, tt.key, tt.val)
    t, k, v = decode_kv(data)
    assert tt.timestamp == t
    assert tt.key == k
    assert tt.val == v
    assert tt.sz == sz, f"{tt.sz} != {sz}"


def test_init():
    ke = KeyEntry(10, 10, 10)
    assert ke.timestamp == 10
    assert ke.position == 10
    assert ke.total_size == 10


if __name__ == "__main__":  # pragma: no cover
    test_header_serialisation()
    test_init()
    test_KV_serialisation()
    test_random_header()
    test_random_kv()

    print("okay")
