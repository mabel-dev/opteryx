#!/usr/bin/env python
"""
Test to compare JSONL decoder results with expected values
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import json


def test_jsonl_decoder_matches_jsonlib():
    with open('testdata/flat/formats/jsonl/tweets.jsonl', 'rb') as f:
        data = f.read()

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['tweet_id', 'followers', 'following']
    column_types = {k: 'int' for k in column_names}

    num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
        data, column_names, column_types
    )

    lines = [l for l in data.split(b'\n') if l.strip()]
    assert num_rows == len(lines)

    for i, line in enumerate(lines):
        expected = json.loads(line)
        assert result['followers'][i] == expected['followers']
        assert result['following'][i] == expected['following']


def test_following_less_than_followers_count():
    with open('testdata/flat/formats/jsonl/tweets.jsonl', 'rb') as f:
        data = f.read()

    lines = [l for l in data.split(b'\n') if l.strip()]
    expected_count = 0
    for line in lines:
        expected = json.loads(line)
        if expected['following'] < expected['followers']:
            expected_count += 1

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['followers', 'following']
    column_types = {k: 'int' for k in column_names}
    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(data, column_names, column_types)

    got_count = 0
    for i in range(len(lines)):
        if i < len(result['following']) and result['following'][i] is not None and result['followers'][i] is not None and result['following'][i] < result['followers'][i]:
            got_count += 1

    assert expected_count == got_count


# module provides pytest tests