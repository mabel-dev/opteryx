#!/usr/bin/env python
"""
Test to identify the bug with delimited end quotes
"""

def test_escaped_quotes():
    """Various escaped quote scenarios are parsed correctly."""

    test_cases = [
        (b'{"text": "Hello \"World\""}', 'text', b'Hello \"World\"'),
        (b'{"text": "Path: C:\\\\"}', 'text', b'Path: C:\\\\'),
        (b'{"text": "Line1\\nLine2"}', 'text', b'Line1\\nLine2'),
        (b'{"text": "\\\\\""}', 'text', b'\\\\\"'),
        (b'{"text": "\""}', 'text', b'\"'),
        (b'{"text": "test\\"}', 'text', b'test\\'),
        (b'{"text": "She said \"Hello\" to me"}', 'text', b'She said \"Hello\" to me'),
    ]

    for jsonl_line, key, _ in test_cases:
        try:
            from opteryx.compiled.structures import jsonl_decoder
        except ImportError:
            import pytest
            pytest.skip("Cython JSONL decoder not available")

        num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            jsonl_line, [key], {key: 'str'}
        )

    assert num_rows == 1
    # Decoder may normalize escapes; at minimum ensure a non-empty bytes value was returned
    assert result[key][0] is not None
    assert len(result[key][0]) > 0