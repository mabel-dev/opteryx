import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures import jsonl_decoder


def test_strings_with_delimiters_inside():
    """Test strings that contain delimiter characters."""

    test_cases = [
        (b'{"text": "Hello, World"}', 'text', b'Hello, World'),
        (b'{"text": "data: {value}"}', 'text', b'data: {value}'),
        (b'{"text": "array[0]"}', 'text', b'array[0]'),
        (b'{"text": "key:value"}', 'text', b'key:value'),
        (b'{"text": "a,b,c,d"}', 'text', b'a,b,c,d'),
        (b'{"text": "end}"}', 'text', b'end}'),
        (b'{"text": "Hello,"}', 'text', b'Hello,'),
        (b'{"name": "test,", "id": 1}', 'name', b'test,'),
        (b'{"path": "/usr/bin/"}', 'path', b'/usr/bin/'),
    ]

    for jsonl_line, key, expected_value in test_cases:
        num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            jsonl_line, [key], {key: 'str'}
        )

        assert num_rows == 1
        assert result[key][0] == expected_value


def test_strings_at_end_of_line():
    """Test strings that are the last value before newline."""

    test_cases = [
        (b'{"text": "Hello,"}', 'text', b'Hello,'),
        (b'{"text": "Hello}"}', 'text', b'Hello}'),
        (b'{"text": "Hello]"}', 'text', b'Hello]'),
        (b'{"id": 1, "text": "Hello,"}', 'text', b'Hello,'),
        (b'{"id": 1, "text": "test}"}', 'text', b'test}'),
    ]

    for jsonl_line, key, expected_value in test_cases:
        num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            jsonl_line, [key], {key: 'str'}
        )

        assert num_rows == 1
        assert result[key][0] == expected_value


if __name__ == "__main__":
    from tests import run_tests

    run_tests()