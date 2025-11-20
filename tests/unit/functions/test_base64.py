import os
import sys
import base64
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.third_party.alantsd.base64 import encode, decode

# -------------------
# CORE FUNCTIONALITY
# -------------------

def test_base64_encode_simple_string():
    data = b"hello world"
    result = encode(data)
    expected = base64.b64encode(data)
    assert result == expected


def test_base64_decode_simple_string():
    encoded = base64.b64encode(b"hello world")
    result = decode(encoded)
    assert result == b"hello world"


def test_base64_encode_decode_roundtrip():
    data = b"x" * 13333336
    encoded = encode(data)
    decoded = decode(encoded)
    assert decoded == data


def test_base64_encode_empty():
    data = b""
    result = encode(data)
    assert result == b""


def test_base64_decode_empty():
    encoded = b""
    result = decode(encoded)
    assert result == b""


def test_base64_known_values():
    known_pairs = [
        (b"", b""),
        (b"f", b"Zg=="),
        (b"fo", b"Zm8="),
        (b"foo", b"Zm9v"),
        (b"foob", b"Zm9vYg=="),
        (b"fooba", b"Zm9vYmE="),
        (b"foobar", b"Zm9vYmFy"),
    ]
    for raw, expected_b64 in known_pairs:
        assert encode(raw) == expected_b64
        assert decode(expected_b64) == raw


def test_base64_decode_invalid_input_raises():
    invalid = b"not@base64###"
    try:
        r = decode(invalid)
        assert r == b""
    except Exception:
        assert False, "decode raised Exception unexpectedly!"

# -------------------
# EMPTY + SHORT INPUTS
# -------------------

def test_base64_empty_encode_decode():
    assert encode(b"") == b""
    assert decode(b"") == b""

def test_base64_single_byte():
    assert decode(encode(b"a")) == b"a"

def test_base64_two_bytes():
    assert decode(encode(b"ab")) == b"ab"

def test_base64_three_bytes():
    assert decode(encode(b"abc")) == b"abc"

# -------------------
# LENGTH ALIGNMENTS
# -------------------

def test_base64_1_byte_mod_3():
    data = b"A" * 1
    assert decode(encode(data)) == data

def test_base64_2_byte_mod_3():
    data = b"A" * 2
    assert decode(encode(data)) == data

def test_base64_3_byte_block():
    data = b"A" * 3
    assert decode(encode(data)) == data

def test_base64_non_multiple_of_three():
    for i in range(1, 100):
        data = b"x" * i
        r = decode(encode(data))
        assert r == data, f"Expected {data} but got {r}"

# -------------------
# HIGH BIT DATA
# -------------------

def test_base64_high_bytes():
    data = bytes(range(256))
    assert decode(encode(data)) == data

# -------------------
# NON-BYTES INPUT HANDLING
# -------------------

def test_base64_encode_accepts_only_bytes():
    with pytest.raises(TypeError):
        encode("not bytes")  # str instead of bytes

def test_base64_decode_accepts_only_bytes():
    with pytest.raises(TypeError):
        decode("not bytes")

# -------------------
# INVALID BASE64 DECODE
# -------------------

def test_base64_decode_invalid_characters():
    r = decode(b"!@#$%^&*")
    assert r == b"", r

def test_base64_decode_non_base64_byte():
    r = decode(b"Zm9vYmFy\xFF")  # invalid byte at end
    assert r == b"", r

# -------------------
# LARGE INPUTS
# -------------------

def test_base64_large_binary():
    data = os.urandom(10_000_001)  # Just over 10MB
    assert decode(encode(data)) == data


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    run_tests()