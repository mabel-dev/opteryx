import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import json


def test_multiline_lines_685_692():
    """Decode the 8 lines around the original problematic area and verify numeric fields."""
    # Get lines around the problematic one
    with open('testdata/flat/formats/jsonl/tweets.jsonl', 'rb') as f:
        lines = []
        for i, line in enumerate(f):
            if 685 <= i <= 692:
                lines.append((i, line))
            if i > 692:
                break

    combined = b''.join([line for _, line in lines])

    # Decode with our fast decoder
    column_names = ['tweet_id', 'followers', 'following', 'text']
    column_types = {
        'tweet_id': 'int',
        'followers': 'int',
        'following': 'int',
        'text': 'str'
    }

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
        combined, column_names, column_types
    )

    # We expect one decoded row per original line
    assert num_rows == len(lines)

    for idx, (_, line) in enumerate(lines):
        expected = json.loads(line)
        assert result['followers'][idx] == expected['followers']
        assert result['following'][idx] == expected['following']



if __name__ == "__main__":
    from tests import run_tests

    run_tests()
